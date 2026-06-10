"""Persist Stage-2 ER outcome: GR state, entity_resolution_index, review queue, Kafka.

Intended for Spark driver / foreachBatch after `resolve()` — not inside executors.
Spec: FR-Dev 3-M-02 (review band → er_review_queue + provisional), FR-Dev 3-M-08 idempotent writes.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from nexus_spark_lib.db.er_index import upsert_batch
from nexus_spark_lib.db.redirects import queue_for_review
from nexus_spark_lib.kafka.review_events import publish_er_review_queued
from nexus_spark_lib.models.er_types import ResolutionMethod
from nexus_spark_lib.transform.stage2_resolve.state_machine import GoldenRecordStateMachine

_METHOD_DB: dict[str, str] = {
    "fast_path": ResolutionMethod.DETERMINISTIC.value,
    "spark_deterministic": ResolutionMethod.DETERMINISTIC.value,
    "spark_probabilistic": ResolutionMethod.PROBABILISTIC.value,
    "spark_graph": ResolutionMethod.GRAPH.value,
    "review_band": ResolutionMethod.PROBABILISTIC.value,
    "warm_new": "spark_auto",
    "new": "spark_auto",
    "cold_skip": "spark_auto",
}


def _resolution_method_for_db(er_resolution_method: str) -> str:
    return _METHOD_DB.get(er_resolution_method, er_resolution_method or "spark_auto")


async def persist_entity_resolution_outcome(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    connector_id: str,
    source_table: str,
    source_system: str,
    source_record_id: str,
    cdm_entity_id: str,
    cdm_entity_type: str,
    er_confidence: float,
    er_resolution_method: str,
    er_is_provisional: bool,
    er_review_peer_cdm_id: str | None,
    er_signal_breakdown_json: str | None,
    trace_id: str = "",
    emit_review_kafka: bool = True,
) -> None:
    """Upsert ER index, set GR state, queue review + Kafka when in review band."""
    breakdown_dict: dict[str, Any]
    if er_signal_breakdown_json:
        try:
            raw = json.loads(er_signal_breakdown_json)
            breakdown_dict = raw if isinstance(raw, dict) else {}
        except json.JSONDecodeError:
            breakdown_dict = {}
    else:
        breakdown_dict = {}

    method_db = _resolution_method_for_db(er_resolution_method)
    sm = GoldenRecordStateMachine(conn, tenant_id)
    if er_is_provisional:
        await sm.provisional(cdm_entity_id, cdm_entity_type)
    else:
        await sm.create_or_activate(cdm_entity_id, cdm_entity_type, reason="resolve")

    await upsert_batch(
        conn,
        [
            (
                tenant_id,
                connector_id,
                source_system or "",
                source_table,
                source_record_id,
                cdm_entity_id,
                cdm_entity_type,
                float(er_confidence),
                method_db,
                er_is_provisional,
            )
        ],
    )

    if (
        er_is_provisional
        and er_resolution_method == "review_band"
        and er_review_peer_cdm_id
    ):
        inserted = await queue_for_review(
            conn,
            tenant_id,
            cdm_entity_type,
            cdm_entity_id,
            er_review_peer_cdm_id,
            float(er_confidence),
            breakdown_dict,
        )
        if inserted and emit_review_kafka:
            publish_er_review_queued(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                candidate_a_id=cdm_entity_id,
                candidate_b_id=er_review_peer_cdm_id,
                combined_score=float(er_confidence),
                signal_breakdown=breakdown_dict,
                trace_id=trace_id,
            )
