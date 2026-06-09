"""Tests for persist_entity_resolution_outcome (ER index + review + Kafka hook)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from nexus_spark_lib.db.resolve_persist import persist_entity_resolution_outcome


@pytest.mark.asyncio
async def test_review_band_persists_queues_and_emits_kafka():
    conn = AsyncMock()
    with (
        patch(
            "nexus_spark_lib.db.resolve_persist.GoldenRecordStateMachine"
        ) as MockSM,
        patch(
            "nexus_spark_lib.db.resolve_persist.upsert_batch",
            new_callable=AsyncMock,
        ),
        patch(
            "nexus_spark_lib.db.resolve_persist.queue_for_review",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_queue,
        patch(
            "nexus_spark_lib.db.resolve_persist.publish_er_review_queued",
        ) as mock_kafka,
    ):
        MockSM.return_value.provisional = AsyncMock()
        await persist_entity_resolution_outcome(
            conn,
            tenant_id="tenant_a",
            connector_id="c1",
            source_table="Contact",
            source_system="salesforce",
            source_record_id="r1",
            cdm_entity_id="gr:new",
            cdm_entity_type="contact",
            er_confidence=0.88,
            er_resolution_method="review_band",
            er_is_provisional=True,
            er_review_peer_cdm_id="gr:existing",
            er_signal_breakdown_json='{"signal_b": 0.88}',
            trace_id="tr-1",
        )
        mock_queue.assert_awaited_once()
        mock_kafka.assert_called_once()
        call_kw = mock_kafka.call_args.kwargs
        assert call_kw["candidate_a_id"] == "gr:new"
        assert call_kw["candidate_b_id"] == "gr:existing"


@pytest.mark.asyncio
async def test_review_replay_skips_kafka_when_queue_dedupes():
    conn = AsyncMock()
    with (
        patch(
            "nexus_spark_lib.db.resolve_persist.GoldenRecordStateMachine"
        ) as MockSM,
        patch(
            "nexus_spark_lib.db.resolve_persist.upsert_batch",
            new_callable=AsyncMock,
        ),
        patch(
            "nexus_spark_lib.db.resolve_persist.queue_for_review",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            "nexus_spark_lib.db.resolve_persist.publish_er_review_queued",
        ) as mock_kafka,
    ):
        MockSM.return_value.provisional = AsyncMock()
        await persist_entity_resolution_outcome(
            conn,
            tenant_id="tenant_a",
            connector_id="c1",
            source_table="Contact",
            source_system="salesforce",
            source_record_id="r1",
            cdm_entity_id="gr:new",
            cdm_entity_type="contact",
            er_confidence=0.88,
            er_resolution_method="review_band",
            er_is_provisional=True,
            er_review_peer_cdm_id="gr:existing",
            er_signal_breakdown_json="{}",
            emit_review_kafka=True,
        )
        mock_kafka.assert_not_called()
