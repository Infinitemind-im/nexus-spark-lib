"""Kafka writer — publishes SparkTransformResult rows to m1.int.transformed_records.

write_transformed_records() is the final step of the pipeline. It serialises
the enriched DataFrame to JSON and writes to Kafka using Spark's built-in
Kafka connector.

Topic naming:
    Global topic: m1.int.transformed_records  (no tenant prefix)
    This matches CrossModuleTopicNamer.m1_transformed_records() in nexus_core v2.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType

from nexus_spark_lib.config.constants import Topics
from nexus_spark_lib.config.settings import settings
from nexus_spark_lib.observability.metrics import KAFKA_RECORDS_WRITTEN
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


def write_transformed_records(
    df: DataFrame,
    topic: str = Topics.TRANSFORMED_RECORDS,
    checkpoint_location: str | None = None,
    trigger_seconds: int | None = None,
) -> StreamingQuery:
    """Write the fully transformed DataFrame to m1.int.transformed_records.

    The DataFrame must contain a 'kafka_value' column (JSON string) and
    a 'kafka_key' column (cdm_entity_id string). Both are produced by
    prepare_kafka_output() in this module.

    Offset commits happen ONLY after the micro-batch is fully processed and
    published (FR-ST-M-10). Kafka auto-commit is disabled.

    Args:
        df:                 DataFrame with 'kafka_key' and 'kafka_value' columns.
        topic:              Kafka topic to write to (default: m1.int.transformed_records).
        checkpoint_location: Spark streaming checkpoint directory (S3 or local).
        trigger_seconds:    Micro-batch interval. Defaults to settings value.

    Returns:
        The StreamingQuery for monitoring and graceful shutdown.
    """
    trigger_secs = trigger_seconds or settings.spark_stream_trigger_seconds

    query = (
        df.selectExpr("kafka_key AS key", "kafka_value AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("topic", topic)
        # Disable auto-commit — offsets are committed by Spark after foreachBatch completes
        .option("kafka.enable.auto.commit", "false")
        .option("failOnDataLoss", "false")
        .trigger(processingTime=f"{trigger_secs} seconds")
    )

    if checkpoint_location:
        query = query.option("checkpointLocation", checkpoint_location)

    return query.start()


def write_batch_to_kafka(
    df: DataFrame,
    topic: str = Topics.TRANSFORMED_RECORDS,
) -> None:
    """Write a batch (non-streaming) DataFrame to Kafka. Used by Airflow batch jobs."""
    df.selectExpr("kafka_key AS key", "kafka_value AS value").write.format("kafka").option(
        "kafka.bootstrap.servers", settings.kafka_bootstrap
    ).option("topic", topic).save()


def prepare_kafka_output(df: DataFrame) -> DataFrame:
    """Wrap each enriched record in a NexusMessage envelope.

    Per nexus_core rules:
    - Every Kafka message MUST be a NexusMessage instance.
    - kafka_key = tenant_id (per-tenant partition ordering, not cdm_entity_id).
    - permission_scope = {} in Iteration 1.

    NexusMessage is constructed inside a Spark UDF so it runs on executors.
    NexusMessage itself has no network I/O — only NexusProducer does, which we
    do NOT use here (Spark's distributed Kafka connector handles transport).

    The NexusMessage.payload carries the full SparkTransformResult dict.
    """

    @F.udf(StringType())
    def _to_nexus_message(
        tenant_id: str,
        source_system: str,
        source_record_id: str,
        cdm_entity_id: str,
        cdm_entity_type: str,
        transform_result_json: str,
        trace_id: str,
        correlation_id: str,
    ) -> str | None:
        if not tenant_id or not transform_result_json:
            return None
        import json
        from nexus_core.messaging import NexusMessage
        from nexus_core.topics import CrossModuleTopicNamer
        msg = NexusMessage(
            topic=CrossModuleTopicNamer.M1Internal.TRANSFORMED_RECORDS,
            tenant_id=tenant_id,
            source_system=source_system or "",
            source_record_id=source_record_id or "",
            entity_type=cdm_entity_type or "",
            permission_scope={},
            payload=json.loads(transform_result_json),
            trace_id=trace_id or "",
            correlation_id=correlation_id or "",
            event_action="updated",
        )
        return msg.to_json().decode("utf-8")

    return df.withColumn(
        "kafka_key", F.col("tenant_id").cast("string")
    ).withColumn(
        "kafka_value",
        _to_nexus_message(
            F.col("tenant_id"),
            F.col("source_system"),
            F.col("source_record_id"),
            F.col("cdm_entity_id"),
            F.col("cdm_entity_type"),
            F.col("transform_result_json"),
            F.col("trace_id"),
            F.col("correlation_id"),
        ),
    )
