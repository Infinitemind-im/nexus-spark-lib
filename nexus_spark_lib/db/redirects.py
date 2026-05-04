"""CRUD for golden_record_redirects and er_review_queue."""

from __future__ import annotations

import asyncpg

from nexus_spark_lib.observability.metrics import DB_WRITES
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_REDIRECTS = "nexus_system.golden_record_redirects"
_REVIEW = "nexus_system.er_review_queue"


async def insert_redirect(
    conn: asyncpg.Connection,
    loser_id: str,
    survivor_id: str,
    tenant_id: str,
) -> None:
    """Record a MERGE redirect so queries on the loser are routed to the survivor."""
    await conn.execute(
        f"""
        INSERT INTO {_REDIRECTS} (from_cdm_entity_id, to_cdm_entity_id, tenant_id, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (from_cdm_entity_id) DO UPDATE
            SET to_cdm_entity_id = EXCLUDED.to_cdm_entity_id,
                created_at = NOW()
        """,
        loser_id, survivor_id, tenant_id,
    )
    DB_WRITES.labels(table=_REDIRECTS, operation="upsert", status="ok").inc()


async def resolve_redirect(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
) -> str:
    """Follow any redirect chain. Returns the canonical (survivor) cdm_entity_id."""
    visited: set[str] = set()
    current = cdm_entity_id
    while True:
        if current in visited:
            logger.warning("Circular redirect detected starting at %s", cdm_entity_id)
            break
        visited.add(current)
        row = await conn.fetchrow(
            f"SELECT to_cdm_entity_id FROM {_REDIRECTS} WHERE from_cdm_entity_id = $1",
            current,
        )
        if row is None:
            break
        current = row["to_cdm_entity_id"]
    return current


async def queue_for_review(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_type: str,
    candidate_a_id: str,
    candidate_b_id: str,
    combined_score: float,
    signal_breakdown: dict,
) -> None:
    """Insert an ER pair into er_review_queue for human review."""
    await conn.execute(
        f"""
        INSERT INTO {_REVIEW}
            (tenant_id, cdm_entity_type, candidate_a_id, candidate_b_id,
             combined_score, signal_breakdown, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        ON CONFLICT DO NOTHING
        """,
        tenant_id, cdm_entity_type, candidate_a_id, candidate_b_id,
        combined_score, signal_breakdown,
    )
    DB_WRITES.labels(table=_REVIEW, operation="insert", status="ok").inc()
