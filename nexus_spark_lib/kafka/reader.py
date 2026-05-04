"""Kafka reader — reads m1.int.raw_records as a Spark Structured Streaming source.

Messages on m1.int.raw_records are NexusMessage envelopes produced by nexus-m1-worker
via NexusProducer. This reader unpacks the NexusMessage outer envelope and flattens
both envelope fields (tenant_id, message_id, trace_id …) and inner payload fields
(connector_id, source_table, after_payload …) as top-level DataFrame columns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from nexus_spark_lib.config.constants import ConsumerGroups, Topics
from nexus_spark_lib.config.settings import settings

# ── NexusMessage.payload schema for m1.int.raw_records ────────────────────────
# Record-specific data carried inside the NexusMessage envelope.
RAW_RECORD_PAYLOAD_SCHEMA = StructType([
    StructField("connector_id",      StringType(),                        True),
    StructField("source_table",      StringType(),                        True),
    StructField("source_op",         StringType(),                        True),
    StructField("source_ts",         TimestampType(),                     True),
    StructField("after_payload",     MapType(StringType(), StringType()), True),
    StructField("before_payload",    MapType(StringType(), StringType()), True),
    StructField("backfill_batch_id", StringType(),                        True),
])

# ── Full NexusMessage envelope schema ─────────────────────────────────────────
# Mirrors nexus_core.messaging.NexusMessage dataclass (v2).
NEXUS_MESSAGE_SCHEMA = StructType([
    StructField("topic",            StringType(),                        True),
    StructField("tenant_id",        StringType(),                        False),
    StructField("source_system",    StringType(),                        True),
    StructField("source_record_id", StringType(),                        True),
    StructField("entity_type",      StringType(),                        True),
    StructField("permission_scope", MapType(StringType(), StringType()), True),
    StructField("payload",          RAW_RECORD_PAYLOAD_SCHEMA,           True),
    StructField("message_id",       StringType(),                        True),
    StructField("correlation_id",   StringType(),                        True),
    StructField("trace_id",         StringType(),                        True),
    StructField("schema_version",   StringType(),                        True),
    StructField("published_at",     StringType(),                        True),
    StructField("event_action",     StringType(),                        True),
])


def read_raw_records_stream(
    spark: SparkSession,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 50_000,
) -> DataFrame:
    """Return a Structured Streaming DataFrame reading from m1.int.raw_records.

    Each Kafka message is a NexusMessage envelope. The envelope is unpacked and
    both envelope fields (tenant_id, message_id, trace_id, correlation_id …) and
    payload fields (connector_id, source_table, after_payload …) are flattened as
    top-level columns. Malformed messages are routed to dead-letter by the caller.

    Args:
        spark:                   Active SparkSession.
        starting_offsets:        "latest" (streaming) or "earliest" (backfill replay).
        max_offsets_per_trigger: Kafka records per micro-batch per partition.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", Topics.RAW_RECORDS)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("kafka.group.id", ConsumerGroups.SPARK_TRANSFORMER)
        .option("kafka.enable.auto.commit", "false")
        .option("failOnDataLoss", "false")
        .load()
    )
    return _unpack_nexus_message(raw)


def read_raw_records_batch(
    spark: SparkSession,
    starting_offsets: dict | str = "earliest",
    ending_offsets: dict | str = "latest",
) -> DataFrame:
    """Return a batch DataFrame reading from m1.int.raw_records. Used by Airflow batch jobs."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", Topics.RAW_RECORDS)
        .option("startingOffsets", starting_offsets if isinstance(starting_offsets, str)
                else str(starting_offsets))
        .option("endingOffsets", ending_offsets if isinstance(ending_offsets, str)
                else str(ending_offsets))
        .load()
    )
    return _unpack_nexus_message(raw)


# ── NexusMessage unpack helper ────────────────────────────────────────────────

def _unpack_nexus_message(raw: DataFrame) -> DataFrame:
    """Parse the NexusMessage envelope and flatten all fields into top-level columns.

    Promoted from envelope : tenant_id, source_system, source_record_id,
                              message_id, trace_id, correlation_id,
                              schema_version, event_action.
    Promoted from payload  : connector_id, source_table, source_op, source_ts,
                              after_payload, before_payload, backfill_batch_id.
    Kafka metadata          : offset, partition, kafka_ts.
    """
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), NEXUS_MESSAGE_SCHEMA).alias("msg"),
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select(
            # ── NexusMessage envelope ─────────────────────────────────────────
            F.col("msg.tenant_id").alias("tenant_id"),
            F.col("msg.source_system").alias("source_system"),
            F.col("msg.source_record_id").alias("source_record_id"),
            F.col("msg.message_id").alias("message_id"),
            F.col("msg.trace_id").alias("trace_id"),
            F.col("msg.correlation_id").alias("correlation_id"),
            F.col("msg.schema_version").alias("schema_version"),
            F.col("msg.event_action").alias("event_action"),
            # ── NexusMessage.payload (raw record data) ────────────────────────
            F.col("msg.payload.connector_id").alias("connector_id"),
            F.col("msg.payload.source_table").alias("source_table"),
            F.col("msg.payload.source_op").alias("source_op"),
            F.col("msg.payload.source_ts").alias("source_ts"),
            F.col("msg.payload.after_payload").alias("after_payload"),
            F.col("msg.payload.before_payload").alias("before_payload"),
            F.col("msg.payload.backfill_batch_id").alias("backfill_batch_id"),
            # ── Kafka metadata ────────────────────────────────────────────────
            F.col("offset"),
            F.col("partition"),
            F.col("kafka_ts"),
        )
    )
