"""CRUD for golden_records_index and golden_record_provenance.

golden_records_index: one row per Golden Record (cdm_entity_id → state + metadata).
golden_record_provenance: one row per (cdm_entity_id, attribute_name) winner.

All writes are idempotent. Survivorship is applied deterministically:
given the same contributing sources, the same provenance rows result regardless
of processing order (NFR-D3-05).
"""

from __future__ import annotations

from datetime import datetime, timezone

import asyncpg

from nexus_spark_lib.models.er_types import GoldenRecordState
from nexus_spark_lib.models.survivorship import ProvenanceRow, SynthesisResult
from nexus_spark_lib.observability.metrics import DB_WRITE_LATENCY, DB_WRITES
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_GRI = "nexus_system.golden_records_index"
_GRP = "nexus_system.golden_record_provenance"


def _coerce_observed_at(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            raise ValueError("observed_at must not be empty")
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    raise TypeError(
        f"observed_at must be a datetime or ISO-8601 string, got {type(value)!r}"
    )


async def upsert_golden_record(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
    tenant_id: str,
    cdm_entity_type: str,
    state: GoldenRecordState = GoldenRecordState.ACTIVE,
    state_change_reason: str | None = None,
    successor_id: str | None = None,
) -> None:
    """Insert or update a Golden Record header row. Idempotent."""
    await conn.execute(
        f"""
        INSERT INTO {_GRI}
            (cdm_entity_id, tenant_id, cdm_entity_type, state, successor_id,
             created_at, updated_at, state_changed_at, state_change_reason)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW(), $6)
        ON CONFLICT (cdm_entity_id)
        DO UPDATE SET
            state               = EXCLUDED.state,
            successor_id        = EXCLUDED.successor_id,
            updated_at          = NOW(),
            state_changed_at    = CASE
                WHEN {_GRI}.state != EXCLUDED.state THEN NOW()
                ELSE {_GRI}.state_changed_at
            END,
            state_change_reason = EXCLUDED.state_change_reason
        """,
        cdm_entity_id,
        tenant_id,
        cdm_entity_type,
        state.value,
        successor_id,
        state_change_reason,
    )
    DB_WRITES.labels(table=_GRI, operation="upsert", status="ok").inc()


async def get_golden_record_state(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
) -> GoldenRecordState | None:
    """Return the current state of a Golden Record, or None if not found."""
    row = await conn.fetchrow(
        f"SELECT state FROM {_GRI} WHERE cdm_entity_id = $1",
        cdm_entity_id,
    )
    if row:
        return GoldenRecordState(row["state"])
    return None


async def resolve_successor(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
) -> str:
    """Follow successor_id until the active survivor is reached."""
    visited: set[str] = set()
    current = cdm_entity_id

    while True:
        if current in visited:
            logger.warning("Circular successor_id chain detected starting at %s", cdm_entity_id)
            return current

        visited.add(current)
        row = await conn.fetchrow(
            f"SELECT successor_id FROM {_GRI} WHERE cdm_entity_id = $1",
            current,
        )
        if row is None or row["successor_id"] is None:
            return current

        current = row["successor_id"]


async def apply_synthesis_result(
    conn: asyncpg.Connection,
    result: SynthesisResult,
    tenant_id: str,
) -> None:
    """Apply a SynthesisResult to golden_record_provenance. Idempotent.

    - Upserts rows that should exist.
    - Deletes rows that should no longer exist (source was removed or replaced).
    """
    if result.rows_to_upsert:
        await conn.executemany(
            f"""
            INSERT INTO {_GRP}
                (cdm_entity_id, tenant_id, attribute_name, winning_connector_id,
                 winning_source_table, winning_record_id, observed_value_hash,
                 observed_at, rule_applied)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (cdm_entity_id, attribute_name)
            DO UPDATE SET
                winning_connector_id = EXCLUDED.winning_connector_id,
                winning_source_table = EXCLUDED.winning_source_table,
                winning_record_id    = EXCLUDED.winning_record_id,
                observed_value_hash  = EXCLUDED.observed_value_hash,
                observed_at          = EXCLUDED.observed_at,
                rule_applied         = EXCLUDED.rule_applied
            WHERE {_GRP}.observed_value_hash != EXCLUDED.observed_value_hash
               OR {_GRP}.winning_record_id != EXCLUDED.winning_record_id
            """,
            [
                (
                    r.cdm_entity_id, tenant_id, r.attribute_name,
                    r.winning_connector_id, r.winning_source_table,
                    r.winning_record_id, r.observed_value_hash,
                    _coerce_observed_at(r.observed_at), r.rule_applied,
                )
                for r in result.rows_to_upsert
            ],
        )
        DB_WRITES.labels(table=_GRP, operation="upsert", status="ok").inc(len(result.rows_to_upsert))

    if result.rows_to_delete:
        await conn.executemany(
            f"""
            DELETE FROM {_GRP}
            WHERE cdm_entity_id = $1 AND attribute_name = $2
              AND source_system = $3
            """,
            [(entity_id, attr, source) for entity_id, attr, source in result.rows_to_delete],
        )
        DB_WRITES.labels(table=_GRP, operation="delete", status="ok").inc(len(result.rows_to_delete))


async def get_all_provenance(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
    tenant_id: str,
) -> list[ProvenanceRow]:
    """Return ALL provenance rows for a Golden Record.

    Used during DELETE propagation to re-evaluate survivorship after a source is removed.
    """
    rows = await conn.fetch(
        f"""
        SELECT cdm_entity_id, attribute_name, winning_connector_id,
               winning_source_table, winning_record_id, observed_value_hash,
               observed_at, rule_applied
        FROM {_GRP}
        WHERE cdm_entity_id = $1 AND tenant_id = $2
        ORDER BY attribute_name, winning_connector_id
        """,
        cdm_entity_id, tenant_id,
    )
    return [
        ProvenanceRow(
            cdm_entity_id=r["cdm_entity_id"],
            attribute_name=r["attribute_name"],
            winning_connector_id=r["winning_connector_id"],
            winning_source_table=r["winning_source_table"],
            winning_record_id=r["winning_record_id"],
            observed_value_hash=r["observed_value_hash"],
            observed_at=str(r["observed_at"]),
            rule_applied=r["rule_applied"],
            tenant_id=tenant_id,
        )
        for r in rows
    ]


async def delete_provenance_for_source(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
    tenant_id: str,
    source_system: str,
    source_record_id: str,
) -> list[str]:
    """Delete all provenance rows for a specific source record.

    Returns list of attribute_names that were affected — used to determine
    which attributes need survivorship re-evaluation.
    """
    rows = await conn.fetch(
        f"""
        DELETE FROM {_GRP}
        WHERE cdm_entity_id = $1 AND tenant_id = $2
                    AND winning_connector_id = $3 AND winning_record_id = $4
        RETURNING attribute_name
        """,
        cdm_entity_id, tenant_id, source_system, source_record_id,
    )
    affected = [r["attribute_name"] for r in rows]
    if affected:
        DB_WRITES.labels(table=_GRP, operation="delete", status="ok").inc(len(affected))
    return affected


async def has_any_provenance(
    conn: asyncpg.Connection,
    cdm_entity_id: str,
    tenant_id: str,
) -> bool:
    """Return True if any provenance row exists for the entity.

    Used to determine whether a GR should be tombstoned after a DELETE.
    """
    row = await conn.fetchrow(
        f"SELECT 1 FROM {_GRP} WHERE cdm_entity_id = $1 AND tenant_id = $2 LIMIT 1",
        cdm_entity_id, tenant_id,
    )
    return row is not None
