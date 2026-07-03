"""CRUD operations for nexus_system.entity_resolution_index.

All writes are idempotent (ON CONFLICT DO UPDATE). Re-delivery of any event
produces no incremental side effects (NFR-D3-04).
"""

from __future__ import annotations

from datetime import datetime

import asyncpg

from nexus_spark_lib._internal.hash_utils import (
    er_deterministic_lookup_key,
    er_legacy_source_lookup_key,
    er_source_lookup_key,
)
from nexus_spark_lib.models.er_types import ErMatchResult, ResolutionMethod
from nexus_spark_lib.models.broadcasts import ErIndexSnapshot
from nexus_spark_lib.observability.metrics import DB_WRITE_LATENCY, DB_WRITES
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib.db.survivorship_rules import load_deterministic_id_columns

logger = get_stage_logger(__name__)

_TABLE = "nexus_system.entity_resolution_index"


async def load_er_index_snapshot(conn: asyncpg.Connection) -> ErIndexSnapshot:
    """Load the shared ER broadcast snapshot from PostgreSQL.

    This keeps the ER-index schema contract in nexus-spark-lib so runtime
    consumers do not embed SQL for `nexus_system` tables directly.
    """
    rows = await conn.fetch(
        """
        SELECT tenant_id,
               connector_id,
               source_system,
               source_table,
               source_record_id,
               cdm_entity_id,
               cdm_entity_type,
               confidence,
               provisional
        FROM   nexus_system.entity_resolution_index
        """
    )
    deterministic_columns = await load_deterministic_id_columns(conn)
    deterministic_rows = await conn.fetch(
        """
        SELECT grp.tenant_id,
               grp.cdm_entity_id,
               grp.attribute_name,
               grp.observed_value_hash,
               gri.cdm_entity_type
        FROM   nexus_system.golden_record_provenance grp
        JOIN   nexus_system.golden_records_index gri
          ON   gri.cdm_entity_id = grp.cdm_entity_id
        JOIN   nexus_system.deterministic_id_columns dic
          ON   dic.tenant_id = grp.tenant_id
         AND   dic.cdm_entity_type = gri.cdm_entity_type
         AND   dic.attribute_name = grp.attribute_name
        WHERE  gri.state IN ('active', 'provisional')
        """
    )
    threshold_rows = await conn.fetch(
        """
        SELECT tenant_id,
               cdm_entity_type,
               weights,
               auto_apply_threshold,
               review_lower_bound
        FROM   nexus_system.er_thresholds
        """
    )

    snapshot: dict[str, str] = {}
    for row in rows:
        tenant_id = str(row["tenant_id"])
        connector_id = row["connector_id"] or row["source_system"] or ""
        source_table = row["source_table"] or ""
        source_record_id = row["source_record_id"] or ""
        snapshot[
            er_source_lookup_key(
                tenant_id,
                connector_id,
                source_table,
                source_record_id,
            )
        ] = row["cdm_entity_id"]

        source_system = row["source_system"] or row["connector_id"] or ""
        if source_system:
            snapshot.setdefault(
                er_legacy_source_lookup_key(
                    tenant_id,
                    source_system,
                    source_record_id,
                ),
                row["cdm_entity_id"],
            )

    for row in deterministic_rows:
        snapshot.setdefault(
            er_deterministic_lookup_key(
                str(row["tenant_id"]),
                row["cdm_entity_type"],
                row["attribute_name"],
                row["observed_value_hash"],
            ),
            row["cdm_entity_id"],
        )

    thresholds = {
        (str(row["tenant_id"]), row["cdm_entity_type"]): {
            "weights": row["weights"] or {},
            "auto_apply_threshold": float(row["auto_apply_threshold"]),
            "review_lower_bound": float(row["review_lower_bound"]),
        }
        for row in threshold_rows
    }

    source_records_by_entity: dict[tuple[str, str, str, str], str] = {}
    for row in rows:
        tenant_id = str(row["tenant_id"])
        cdm_entity_type = str(row["cdm_entity_type"] or "")
        source_record_id = str(row["source_record_id"] or "")
        source_system = str(row["source_system"] or row["connector_id"] or "")
        if not cdm_entity_type or not source_record_id or not source_system:
            continue
        source_records_by_entity.setdefault(
            (tenant_id, cdm_entity_type, source_system, source_record_id),
            row["cdm_entity_id"],
        )

    return ErIndexSnapshot(
        snapshot=snapshot,
        deterministic_columns=deterministic_columns,
        thresholds=thresholds,
        snapshot_ts=datetime.utcnow().isoformat(),
        deterministic_hash_count=len(deterministic_rows),
        _source_records_by_entity=source_records_by_entity,
    )


async def lookup_batch(
    conn: asyncpg.Connection,
    keys: list[tuple[str, str, str, str]],  # (tenant_id, connector_id, source_table, source_record_id)
) -> dict[tuple[str, str, str, str], str]:
    """Batch lookup: return map of key → cdm_entity_id for all known records.

    Uses a single query with unnest() for performance (avoids N+1 queries).
    """
    if not keys:
        return {}
    tenant_ids, connector_ids, source_tables, source_record_ids = zip(*keys)
    rows = await conn.fetch(
        f"""
        SELECT tenant_id, connector_id, source_table, source_record_id, cdm_entity_id
        FROM {_TABLE}
        WHERE (tenant_id, connector_id, source_table, source_record_id) IN (
            SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
        )
        AND is_active = TRUE
        """,
        list(tenant_ids),
        list(connector_ids),
        list(source_tables),
        list(source_record_ids),
    )
    return {
        (
            str(r["tenant_id"]),
            str(r["connector_id"]),
            str(r["source_table"]),
            str(r["source_record_id"]),
        ): r["cdm_entity_id"]
        for r in rows
    }


async def upsert_batch(
    conn: asyncpg.Connection,
    entries: list[tuple[str, str, str, str, str, str, str, float, str, bool]],
    # (tenant_id, connector_id, source_system, source_table, source_record_id,
    #  cdm_entity_id, cdm_entity_type, confidence, method, provisional)
) -> None:
    """Batch upsert entity_resolution_index rows. Idempotent.

    On conflict (same PK), updates confidence, resolution_method, provisional, resolved_at.
    """
    if not entries:
        return
    start = datetime.utcnow()
    try:
        await conn.executemany(
            f"""
            INSERT INTO {_TABLE}
                (cdm_entity_id, tenant_id, connector_id, source_system,
                 entity_type, source_table, source_record_id, cdm_entity_type,
                 confidence, resolution_method, resolved_at, provisional)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)
            ON CONFLICT (tenant_id, connector_id, source_table, source_record_id)
            DO UPDATE SET
                cdm_entity_id      = EXCLUDED.cdm_entity_id,
                entity_type        = EXCLUDED.entity_type,
                cdm_entity_type    = EXCLUDED.cdm_entity_type,
                source_system      = EXCLUDED.source_system,
                confidence         = EXCLUDED.confidence,
                resolution_method  = EXCLUDED.resolution_method,
                resolved_at        = NOW(),
                provisional        = EXCLUDED.provisional
            """,
            [
                (
                    e[5],
                    e[0],
                    e[1],
                    e[2],
                    e[6],
                    e[3],
                    e[4],
                    e[6],
                    e[7],
                    e[8],
                    e[9],
                )
                for e in entries
            ],
        )
        DB_WRITES.labels(table=_TABLE, operation="upsert", status="ok").inc(len(entries))
    except Exception as exc:
        DB_WRITES.labels(table=_TABLE, operation="upsert", status="error").inc(len(entries))
        raise
    finally:
        elapsed = (datetime.utcnow() - start).total_seconds()
        DB_WRITE_LATENCY.labels(table=_TABLE).observe(elapsed)


async def delete_by_source(
    conn: asyncpg.Connection,
    tenant_id: str,
    connector_id: str,
    source_table: str,
    source_record_id: str,
) -> str | None:
    """Delete a single ER index entry. Returns the cdm_entity_id that was affected."""
    row = await conn.fetchrow(
        f"""
        DELETE FROM {_TABLE}
        WHERE tenant_id = $1 AND connector_id = $2
          AND source_table = $3 AND source_record_id = $4
        RETURNING cdm_entity_id
        """,
        tenant_id, connector_id, source_table, source_record_id,
    )
    if row:
        DB_WRITES.labels(table=_TABLE, operation="delete", status="ok").inc()
        return row["cdm_entity_id"]
    return None


async def get_sources_for_entity(
    conn: asyncpg.Connection,
    tenant_id: str,
    cdm_entity_id: str,
) -> list[dict]:
    """Return all source records currently mapped to a cdm_entity_id."""
    rows = await conn.fetch(
        f"""
        SELECT connector_id, source_system, source_table, source_record_id,
               confidence, resolution_method, provisional
        FROM {_TABLE}
        WHERE tenant_id = $1 AND cdm_entity_id = $2
        """,
        tenant_id, cdm_entity_id,
    )
    return [dict(r) for r in rows]


async def repoint_to_survivor(
    conn: asyncpg.Connection,
    tenant_id: str,
    loser_id: str,
    survivor_id: str,
) -> int:
    """After a MERGE: rewrite all ER index entries pointing at loser → survivor."""
    result = await conn.execute(
        f"""
        UPDATE {_TABLE}
        SET cdm_entity_id = $1,
            resolution_method = $2,
            resolved_at = NOW()
        WHERE tenant_id = $3 AND cdm_entity_id = $4
        """,
        survivor_id,
        ResolutionMethod.MERGE_INHERITANCE.value,
        tenant_id,
        loser_id,
    )
    count = int(result.split()[-1])
    DB_WRITES.labels(table=_TABLE, operation="upsert", status="ok").inc(count)
    return count
