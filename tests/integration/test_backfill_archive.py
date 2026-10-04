"""Isolated real MinIO: exact archived raw bytes precede Kafka publication."""
import hashlib
import json
import os
import subprocess
import time
from types import SimpleNamespace

import pytest
import requests
from minio import Minio

from nexus_spark_lib import backfill
from nexus_spark_lib.backfill_archive import MinioRawArchive


@pytest.mark.integration
def test_real_minio_preserves_envelopes_and_separates_source_scopes(monkeypatch):
    image = os.environ.get("BACKFILL_MINIO_FIXTURE_IMAGE")
    if not image:
        raise RuntimeError("Build the pinned Core MinIO fixture and set BACKFILL_MINIO_FIXTURE_IMAGE")
    container = subprocess.check_output(["docker", "run", "--rm", "-d", "-p", "127.0.0.1::9000",
        "-e", "MINIO_ROOT_USER=archive-fixture", "-e", "MINIO_ROOT_PASSWORD=archive-fixture-password",
        image, "server", "/data"], text=True).strip()
    try:
        port = subprocess.check_output(["docker", "port", container, "9000/tcp"], text=True).strip().rsplit(":", 1)[1]
        endpoint = f"127.0.0.1:{port}"
        for attempt in range(30):
            try:
                response = requests.get(f"http://{endpoint}/minio/health/ready", timeout=1)
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 29:
                    raise
                time.sleep(1)
        client = Minio(endpoint, access_key="archive-fixture", secret_key="archive-fixture-password", secure=False)
        client.make_bucket("raw-fixture")
        storage = MinioRawArchive(client, "raw-fixture")
        callbacks, published = [], []
        class Kafka:
            def produce(self, topic, key, value, on_delivery):
                # The exact message must already be readable from real storage.
                archived = []
                for obj in client.list_objects("raw-fixture", recursive=True):
                    stream = client.get_object("raw-fixture", obj.object_name)
                    try:
                        body = stream.read()
                        assert obj.object_name.endswith(hashlib.sha256(body).hexdigest() + ".ndjson")
                        archived.extend(body.splitlines())
                    finally:
                        stream.close()
                        stream.release_conn()
                assert value in archived
                assert key == json.loads(value)["tenant_id"].encode()
                published.append(value)
                callbacks.append(on_delivery)
            def poll(self, timeout):
                return None
            def flush(self, timeout):
                for callback in callbacks:
                    callback(None, None)
                callbacks.clear()
                return 0
        monkeypatch.setattr(backfill, "Producer", lambda config: Kafka())
        producer = backfill.ConfirmedProducer({}, archive=storage)
        for tenant, connector_id in [("tenant-a", "connector-a"), ("tenant-b", "connector-a"),
                                     ("tenant-b", "connector-b")]:
            connector = SimpleNamespace(tenant_id=tenant, connector_id=connector_id)
            payload = backfill._base_payload(connector, "postgresql", "public.orders", "0",
                "2026-10-01T00:00:00Z", {"id": 0, "description": "fixture\naccent é"})
            backfill.publish_raw_record(producer, "raw-fixture-topic", payload)
        assert producer.flush() == 0
        assert len(published) == 3 and producer.archive_records == 3
        objects = list(client.list_objects("raw-fixture", recursive=True))
        assert len(objects) == 3
        assert len({obj.object_name.rsplit("/", 1)[0] for obj in objects}) == 3
        assert all(json.loads(value)["payload"]["after_payload"]["id"] == 0 for value in published)
        # An inaccessible bucket must fail without invoking any Kafka publication.
        failed = backfill.ConfirmedProducer({}, archive=MinioRawArchive(client, "missing-fixture"))
        backfill.publish_raw_record(failed, "raw-fixture-topic", payload)
        with pytest.raises(RuntimeError, match="publication stopped"):
            failed.flush()
        assert len(published) == 3
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)
