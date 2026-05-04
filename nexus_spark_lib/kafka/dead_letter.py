"""Dead-letter writer — routes unrecoverable records to m1.int.dead_letter.

FR-ST-M-09: Records that fail transformation irrecoverably must be published
to m1.int.dead_letter with the original payload and a structured error reason.
The pipeline must NEVER silently drop a record.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nexus_spark_lib.config.constants import Topics
from nexus_spark_lib.config.settings import settings
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


def write_dead_letter_batch(
    spark_or_df: DataFrame,
    *,
    topic: str = Topics.DEAD_LETTER,
) -> None:
    """Write a DataFrame of NexusMessage-wrapped dead-letter records to m1.int.dead_letter.

    The DataFrame must have columns kafka_key and kafka_value produced by
    build_dead_letter_row().
    """
    spark_or_df.select(
        F.col("kafka_key").alias("key"),
        F.col("kafka_value").alias("value"),
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", settings.kafka_bootstrap
    ).option("topic", topic).save()


def build_dead_letter_row(
    tenant_id: str,
    message_id: str,
    original_payload: dict,
    error_reason: str,
    stage: str,
    trace_id: str | None = None,
) -> dict:
    """Build a NexusMessage-wrapped dead-letter record. Payload is preserved for replay.

    Returns a dict with kafka_key and kafka_value ready for write_dead_letter_batch().
    Runs on the driver (not a Spark UDF), so NexusMessage can be imported directly.
    """
    from nexus_core.messaging import NexusMessage
    msg = NexusMessage(
        topic=Topics.DEAD_LETTER,
        tenant_id=tenant_id,
        source_record_id=message_id,
        permission_scope={},
        payload={
            "original_payload": original_payload,
            "error_reason": error_reason,
            "stage": stage,
            "failed_at": datetime.utcnow().isoformat(),
        },
        message_id=message_id,
        trace_id=trace_id or "",
    )
    return {
        "tenant_id": tenant_id,
        "kafka_key": tenant_id,
        "kafka_value": msg.to_json().decode("utf-8"),
    }
