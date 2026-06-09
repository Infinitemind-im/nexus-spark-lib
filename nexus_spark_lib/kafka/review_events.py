"""Kafka — emit `nexus.er.review_queued` (spec §6.3) after DB insert."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def publish_er_review_queued(
    *,
    tenant_id: str,
    cdm_entity_type: str,
    candidate_a_id: str,
    candidate_b_id: str,
    combined_score: float,
    signal_breakdown: dict[str, Any],
    trace_id: str = "",
) -> None:
    """Publish a NexusMessage envelope to Topics.ER_REVIEW_QUEUED.

    Requires optional dependency `confluent-kafka` (same as nexus_core.NexusProducer).
    """
    try:
        from nexus_core.messaging import NexusMessage, NexusProducer
        from nexus_spark_lib.config.constants import Topics
        from nexus_spark_lib.config.settings import settings
    except ImportError as exc:
        logger.warning("Kafka publish skipped (import): %s", exc)
        return

    try:
        producer = NexusProducer(settings.kafka_bootstrap)
        msg = NexusMessage(
            topic=Topics.ER_REVIEW_QUEUED,
            tenant_id=tenant_id,
            source_system="nexus_er",
            source_record_id=candidate_a_id,
            entity_type=cdm_entity_type,
            payload={
                "candidate_a_id": candidate_a_id,
                "candidate_b_id": candidate_b_id,
                "combined_score": combined_score,
                "signal_breakdown": signal_breakdown,
            },
            trace_id=trace_id or "",
            event_action="created",
        )
        producer.publish(msg)
        producer.flush(timeout=10.0)
    except ImportError:
        logger.warning(
            "confluent-kafka not installed; skipping ER_REVIEW_QUEUED "
            "(pip install confluent-kafka)"
        )
    except Exception as exc:
        logger.error("Failed to publish ER_REVIEW_QUEUED: %s", exc)
        raise
