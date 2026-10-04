"""Archive failures stop publication; chunks stay bounded and source scoped."""
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from nexus_spark_lib import backfill, backfill_archive as archive


def wire(tenant="tenant-a", connector="connector-a", batch="batch-a", record_id="0"):
    from nexus_core.messaging import NexusMessage
    return NexusMessage(topic="raw", tenant_id=tenant, source_system="postgresql",
        source_record_id=record_id, payload={"connector_id": connector,
            "backfill_batch_id": batch, "source_table": "public.orders",
            "source_op": "SNAPSHOT_READ", "after_payload": {"id": int(record_id)}}).to_json()


def confirmed(monkeypatch, storage):
    kafka = Mock()
    callbacks = []
    kafka.produce.side_effect = lambda *args, **kwargs: callbacks.append(kwargs["on_delivery"])
    def flush(_timeout):
        for callback in callbacks:
            callback(None, None)
        callbacks.clear()
        return 0
    kafka.flush.side_effect = flush
    monkeypatch.setattr(backfill, "Producer", lambda config: kafka)
    return backfill.ConfirmedProducer({}, archive=storage), kafka


@pytest.mark.parametrize("failure", ["put", "head", "size", "digest"])
def test_archive_failure_stops_kafka_and_flush_never_succeeds(monkeypatch, failure):
    client = Mock()
    client.stat_object.return_value = SimpleNamespace(size=0, metadata={})
    if failure == "put":
        client.put_object.side_effect = RuntimeError("opaque-source-secret")
    elif failure == "head":
        client.stat_object.side_effect = RuntimeError("opaque-source-secret")
    else:
        def put(bucket, name, data, length, **options):
            client.stat_object.return_value = SimpleNamespace(
                size=0 if failure == "size" else length,
                metadata={"x-amz-meta-sha256": "wrong" if failure == "digest" else options["metadata"]["sha256"]})
        client.put_object.side_effect = put
    producer, kafka = confirmed(monkeypatch, archive.MinioRawArchive(client, "nexus-raw"))
    producer.publish_raw("raw", key=b"tenant-a", value=wire())
    with pytest.raises(RuntimeError, match="publication stopped") as error:
        producer.flush()
    assert "opaque-source-secret" not in str(error.value)
    kafka.produce.assert_not_called()
    assert producer.archive_records == 0


def test_chunk_limits_and_tenant_connector_batch_boundaries(monkeypatch):
    monkeypatch.setattr(archive, "MAX_RECORDS", 2)
    storage = Mock()
    producer, kafka = confirmed(monkeypatch, storage)
    fixtures = [wire(record_id="0"), wire(record_id="1"), wire(tenant="tenant-b"),
                wire(tenant="tenant-b", connector="connector-b"),
                wire(tenant="tenant-b", connector="connector-b", batch="batch-b")]
    for value in fixtures:
        producer.publish_raw("raw", key=json.loads(value)["tenant_id"].encode(), value=value)
    producer.flush()
    assert [len(call.args[1]) for call in storage.persist.call_args_list] == [2, 1, 1, 1]
    assert [call.kwargs["value"] for call in kafka.produce.call_args_list] == fixtures
    assert producer.archive_chunks == 4 and producer.archive_records == 5


def test_byte_limit_splits_chunks_and_rejects_oversized_record(monkeypatch):
    first = wire()
    monkeypatch.setattr(archive, "MAX_BYTES", len(first) + 20)
    storage = Mock()
    producer, kafka = confirmed(monkeypatch, storage)
    producer.publish_raw("raw", key=b"tenant-a", value=first)
    producer.publish_raw("raw", key=b"tenant-a", value=wire(record_id="1"))
    producer.flush()
    assert [len(call.args[1]) for call in storage.persist.call_args_list] == [1, 1]
    oversized = json.loads(first)
    oversized["payload"]["after_payload"]["large"] = "x" * 1000
    with pytest.raises(ValueError, match="byte bounds"):
        producer.publish_raw("raw", key=b"tenant-a", value=json.dumps(oversized).encode())
    assert kafka.produce.call_count == 2


@pytest.mark.parametrize("mutation", ["tenant", "flat", "missing_source", "missing_raw", "operation"])
def test_incomplete_or_cross_tenant_raw_is_not_archived(monkeypatch, mutation):
    value = json.loads(wire())
    if mutation == "tenant":
        value["tenant_id"] = "tenant-b"
    elif mutation == "flat":
        value = value["payload"]
    elif mutation == "missing_source":
        value["source_record_id"] = ""
    elif mutation == "missing_raw":
        del value["payload"]["after_payload"]
    elif mutation == "operation":
        value["payload"]["source_op"] = "RECLASSIFY"
    storage = Mock()
    producer, kafka = confirmed(monkeypatch, storage)
    with pytest.raises(ValueError, match="tenant-bound"):
        producer.publish_raw("raw", key=b"tenant-a", value=json.dumps(value).encode())
    storage.persist.assert_not_called()
    kafka.produce.assert_not_called()


def test_archive_is_opt_in_and_incomplete_configuration_fails():
    assert archive.archive_from_settings(lambda name: None) is None
    with pytest.raises(RuntimeError, match="MinIO endpoint"):
        archive.archive_from_settings(lambda name: "nexus-raw" if name == "BACKFILL_RAW_ARCHIVE_BUCKET" else None)


def test_content_address_and_exact_wire_bytes_are_stable():
    client = Mock()
    stored = {}
    def put(bucket, name, data, length, **options):
        assert isinstance(data, io.BytesIO)
        stored[name] = data.read()
        client.stat_object.return_value = SimpleNamespace(size=length,
            metadata={"x-amz-meta-sha256": options["metadata"]["sha256"]})
    client.put_object.side_effect = put
    storage = archive.MinioRawArchive(client, "nexus-raw")
    value = wire(connector="../../sensitive-source")
    scope = archive.raw_scope("raw", b"tenant-a", value)
    first = storage.persist(scope, [value])
    assert storage.persist(scope, [value]) == first
    assert stored[first["object_name"]] == value + b"\n"
    assert ".." not in first["object_name"] and "sensitive-source" not in first["object_name"]


def test_existing_core_publisher_uses_archive_path(monkeypatch):
    storage = Mock()
    producer, kafka = confirmed(monkeypatch, storage)
    connector = SimpleNamespace(tenant_id="tenant-a", connector_id="connector-a")
    payload = backfill._base_payload(connector, "postgresql", "public.orders", "0",
                                    "2026-10-01T00:00:00Z", {"id": 0})
    backfill.publish_raw_record(producer, "raw", payload)
    kafka.produce.assert_not_called()
    producer.flush()
    exact = kafka.produce.call_args.kwargs["value"]
    assert storage.persist.call_args.args[1] == [exact]
    assert json.loads(exact)["payload"]["after_payload"] == {"id": 0}


def test_public_extraction_has_one_batch_and_context_is_restored(monkeypatch):
    monkeypatch.delenv("BACKFILL_BATCH_ID", raising=False)
    monkeypatch.setattr(backfill, "_validate_connector", lambda connector: None)
    connector = SimpleNamespace(tenant_id="tenant-a", connector_id="connector-a", source_type="fixture")
    batches = []
    def extract(*args):
        for record in ("0", "1"):
            batches.append(backfill._base_payload(connector, "postgresql", "public.orders", record,
                "2026-10-01T00:00:00Z", {"id": int(record)})["backfill_batch_id"])
    owner = SimpleNamespace(snapshot=extract)
    registry = backfill.ExtractRegistry()
    registry.register("fixture", owner)
    monkeypatch.setattr(backfill, "registry", registry)
    backfill.run_snapshot_extract(connector, True, "raw")
    assert batches[0] == batches[1]
    assert backfill._batch_id.get() is None
    with backfill.backfill_batch("airflow-run-fixture"):
        backfill.run_snapshot_extract(connector, True, "raw")
        assert backfill._batch_id.get() == "airflow-run-fixture"
    assert batches[2:] == ["airflow-run-fixture", "airflow-run-fixture"]
    with pytest.raises(RuntimeError):
        with backfill.backfill_batch("failed-fixture"):
            raise RuntimeError("fixture")
    assert backfill._batch_id.get() is None
