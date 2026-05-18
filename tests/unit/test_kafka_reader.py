from __future__ import annotations

from nexus_spark_lib.config.constants import Topics
from nexus_spark_lib.kafka.reader import read_raw_records_stream


class _FakeStreamReader:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.options: dict[str, object] = {}

    def format(self, name: str):
        self.format_name = name
        return self

    def option(self, key: str, value: object):
        self.options[key] = value
        return self

    def load(self):
        return self


class _FakeSparkSession:
    def __init__(self) -> None:
        self.readStream = _FakeStreamReader()


def test_read_raw_records_stream_uses_configured_consumer_group(monkeypatch):
    fake_spark = _FakeSparkSession()

    monkeypatch.setattr("nexus_spark_lib.kafka.reader._unpack_nexus_message", lambda raw: raw)
    monkeypatch.setattr("nexus_spark_lib.kafka.reader.settings.kafka_bootstrap", "kafka.example:9092")
    monkeypatch.setattr("nexus_spark_lib.kafka.reader.settings.kafka_consumer_group", "spark-live-smoke")

    result = read_raw_records_stream(fake_spark, starting_offsets="latest", max_offsets_per_trigger=123)

    assert result is fake_spark.readStream
    assert fake_spark.readStream.format_name == "kafka"
    assert fake_spark.readStream.options == {
        "kafka.bootstrap.servers": "kafka.example:9092",
        "subscribe": Topics.RAW_RECORDS,
        "startingOffsets": "latest",
        "maxOffsetsPerTrigger": 123,
        "kafka.group.id": "spark-live-smoke",
        "failOnDataLoss": "false",
    }