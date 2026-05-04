"""CRUD propagation — handles ER index and Golden Record state for all SourceOp types.

This is the "hard case" for Entity Resolution, especially for DELETE operations
which require re-evaluating survivorship for all remaining sources.

Supported operations:
  INSERT / SNAPSHOT_READ / RELEVEL → upsert ER index + create/activate GR
  UPDATE → diff-based re-ER (update ER index + re-synthesise)
  DELETE → delete source from ER index + provenance; re-synthesise; TOMBSTONE if empty
"""

from __future__ import annotations

import asyncpg

from nexus_spark_lib.db.er_index import delete_by_source, upsert_batch
from nexus_spark_lib.db.golden_records import (
    delete_provenance_for_source,
    get_all_provenance,
    has_any_provenance,
)
from nexus_spark_lib.db.redirects import queue_for_review
from nexus_spark_lib.models.er_types import ErOperation, GoldenRecordState
from nexus_spark_lib.models.raw_record import SourceOp
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib.transform.stage2_resolve.state_machine import GoldenRecordStateMachine

logger = get_stage_logger(__name__)


async def propagate_insert(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_id: str,
    cdm_entity_type: str,
    source_system: str,
    source_record_id: str,
    blocking_key: str,
) -> ErOperation:
    """Handle INSERT / SNAPSHOT_READ / RELEVEL — create or update ER index entry."""
    sm = GoldenRecordStateMachine(conn, tenant_id)
    await sm.create_or_activate(cdm_entity_id, cdm_entity_type, reason="insert")

    await upsert_batch(
        conn,
        [
            {
                "tenant_id": tenant_id,
                "source_system": source_system,
                "source_record_id": source_record_id,
                "cdm_entity_id": cdm_entity_id,
                "cdm_entity_type": cdm_entity_type,
                "blocking_key": blocking_key,
            }
        ],
    )
    return ErOperation.UPSERT


async def propagate_delete(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_type: str,
    source_system: str,
    source_record_id: str,
) -> tuple[ErOperation, str | None]:
    """Handle DELETE — remove source from ER index + provenance.

    Returns (operation, cdm_entity_id). The cdm_entity_id is returned so
    Stage 3 can re-synthesise the surviving attributes.

    If the last source is removed, the GR is TOMBSTONED.
    """
    # 1. Look up the entity this source belongs to
    cdm_entity_id = await delete_by_source(
        conn, tenant_id, source_system, source_record_id
    )
    if cdm_entity_id is None:
        logger.debug("DELETE: source not indexed — %s/%s", source_system, source_record_id)
        return ErOperation.REMOVE, None

    # 2. Delete provenance rows for this source
    affected_attrs = await delete_provenance_for_source(
        conn, cdm_entity_id, tenant_id, source_system, source_record_id
    )
    logger.debug("DELETE: removed provenance for attrs %s", affected_attrs)

    # 3. Check whether any provenance remains → TOMBSTONE if empty
    sm = GoldenRecordStateMachine(conn, tenant_id)
    still_has_sources = await has_any_provenance(conn, cdm_entity_id, tenant_id)
    if not still_has_sources:
        await sm.tombstone(cdm_entity_id, cdm_entity_type)
        return ErOperation.REMOVE, cdm_entity_id

    # 4. Still has sources → re-synthesise will be triggered downstream
    return ErOperation.UPSERT, cdm_entity_id


async def propagate_merge(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_type: str,
    loser_id: str,
    survivor_id: str,
) -> ErOperation:
    """Handle MERGE — supersede loser, install redirect, repoint ER index entries."""
    from nexus_spark_lib.db.er_index import repoint_to_survivor

    sm = GoldenRecordStateMachine(conn, tenant_id)
    await sm.merge(loser_id, survivor_id, cdm_entity_type)
    await repoint_to_survivor(conn, loser_id, survivor_id)
    return ErOperation.MERGE


async def queue_review(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_type: str,
    candidate_a: str,
    candidate_b: str,
    score: float,
    signal_breakdown: dict,
) -> None:
    """Insert a review-band match into er_review_queue (confidence ∈ [0.70, 0.95))."""
    await queue_for_review(
        conn, tenant_id, cdm_entity_type, candidate_a, candidate_b,
        score, signal_breakdown,
    )
    logger.info(
        "Review queued: %s vs %s score=%.3f", candidate_a, candidate_b, score
    )
