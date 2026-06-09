from __future__ import annotations

"""Load ``entity_store_presence`` snapshot for Spark broadcast."""

import logging
from typing import Any

from nexus_spark_lib.models.entity_store_presence import (
    EntityStorePresence,
    EntityStoreState,
    classify_entity_store_presence,
)

logger = logging.getLogger(__name__)


def load_entity_store_presence_snapshot(
    dsn: str,
    *,
    tenant_ids: list[str] | None = None,
) -> dict[tuple[str, str], EntityStoreState]:
    """
    Load presence rows into a lookup map for executor UDFs.

    Key: (tenant_id, cdm_entity_id) → cold | warm | hot
    """
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:
        raise RuntimeError("psycopg2 required to load entity_store_presence") from exc

    sql = """
        SELECT tenant_id::text, cdm_entity_id, es_present, neo4j_present, ts_present
        FROM nexus_system.entity_store_presence
    """
    params: tuple[Any, ...] = ()
    if tenant_ids:
        sql += " WHERE tenant_id::text = ANY(%s)"
        params = (tenant_ids,)

    out: dict[tuple[str, str], EntityStoreState] = {}
    try:
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                for raw in cur.fetchall():
                    row = EntityStorePresence(
                        tenant_id=str(raw["tenant_id"]),
                        cdm_entity_id=str(raw["cdm_entity_id"]),
                        es_present=bool(raw["es_present"]),
                        neo4j_present=bool(raw["neo4j_present"]),
                        ts_present=bool(raw["ts_present"]),
                    )
                    out[(row.tenant_id, row.cdm_entity_id)] = classify_entity_store_presence(row)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("entity_store_presence snapshot load failed: %s", exc)
    return out


def lookup_entity_store_state(
    snapshot: dict[tuple[str, str], EntityStoreState],
    tenant_id: str,
    cdm_entity_id: str,
) -> EntityStoreState:
    return snapshot.get((tenant_id, cdm_entity_id), EntityStoreState.COLD)
