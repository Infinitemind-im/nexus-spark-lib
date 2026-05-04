"""Write to materialization_decision_log and update schema_snapshots."""

from __future__ import annotations

import asyncpg

from nexus_spark_lib.models.materialization import MaterializationDecision
from nexus_spark_lib.observability.metrics import DB_WRITES
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_DECISION_LOG = "nexus_system.materialization_decision_log"
_SCHEMA_SNAPSHOTS = "nexus_system.schema_snapshots"


async def write_decision_log_batch(
    conn: asyncpg.Connection,
    entries: list[tuple[str, str, str, MaterializationDecision]],
    # (tenant_id, cdm_entity_id, cdm_entity_type, decision)
) -> None:
    """Batch-write materialization decisions. Best-effort (errors are logged, not raised)."""
    if not entries:
        return
    try:
        await conn.executemany(
            f"""
            INSERT INTO {_DECISION_LOG}
                (tenant_id, cdm_entity_id, cdm_entity_type, level,
                 applied_rule_id, evaluated_at, predicate_debug)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            [
                (
                    e[0], e[1], e[2],
                    e[3].level.value,
                    e[3].applied_rule_id,
                    e[3].evaluated_at,
                    e[3].predicate_debug,
                )
                for e in entries
            ],
        )
        DB_WRITES.labels(table=_DECISION_LOG, operation="insert", status="ok").inc(len(entries))
    except Exception as exc:
        # Best-effort — decision log failure must not abort record processing (FR-ST-M-08)
        logger.warning("Decision log write failed (non-fatal): %s", exc)
        DB_WRITES.labels(table=_DECISION_LOG, operation="insert", status="error").inc(len(entries))


async def upsert_schema_snapshot(
    conn: asyncpg.Connection,
    tenant_id: str,
    connector_id: str,
    source_table: str,
    column_stats: dict,
) -> None:
    """Update schema_snapshots with running column statistics (FR-ST-M-08).

    This is a best-effort update — failure must not abort record processing.
    """
    try:
        await conn.execute(
            f"""
            INSERT INTO {_SCHEMA_SNAPSHOTS}
                (tenant_id, connector_id, source_table, column_profiles, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, NOW())
            ON CONFLICT (tenant_id, connector_id, source_table)
            DO UPDATE SET
                column_profiles = {_SCHEMA_SNAPSHOTS}.column_profiles || EXCLUDED.column_profiles,
                updated_at = NOW()
            """,
            tenant_id, connector_id, source_table, column_stats,
        )
        DB_WRITES.labels(table=_SCHEMA_SNAPSHOTS, operation="upsert", status="ok").inc()
    except Exception as exc:
        logger.warning("Schema snapshot update failed (non-fatal): %s", exc)
        DB_WRITES.labels(table=_SCHEMA_SNAPSHOTS, operation="upsert", status="error").inc()
