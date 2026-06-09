from __future__ import annotations

"""Read ``nexus_system.entity_store_presence`` from the Spark driver (sync psycopg2)."""

import logging
from typing import Any

from nexus_spark_lib.models.entity_store_presence import (
    EntityStorePresence,
    EntityStoreState,
    classify_entity_store_presence,
)

logger = logging.getLogger(__name__)

# Back-compat alias used by materialization_gate imports
classify_presence_row = classify_entity_store_presence


class EntityStorePresenceReader:
    """Sync Postgres reader for materialization gate (Op 3 / Worked Example)."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._cache: dict[tuple[str, str], EntityStorePresence | None] = {}

    def get(self, tenant_id: str, cdm_entity_id: str, *, use_cache: bool = True) -> EntityStorePresence | None:
        key = (tenant_id, cdm_entity_id)
        if use_cache and key in self._cache:
            return self._cache[key]

        row = self._fetch_one(tenant_id, cdm_entity_id)
        if use_cache:
            self._cache[key] = row
        return row

    def get_state(self, tenant_id: str, cdm_entity_id: str) -> EntityStoreState:
        return classify_entity_store_presence(self.get(tenant_id, cdm_entity_id))

    def invalidate(self, tenant_id: str, cdm_entity_id: str) -> None:
        self._cache.pop((tenant_id, cdm_entity_id), None)

    def clear_cache(self) -> None:
        self._cache.clear()

    def _fetch_one(self, tenant_id: str, cdm_entity_id: str) -> EntityStorePresence | None:
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError as exc:
            raise RuntimeError("psycopg2 required for EntityStorePresenceReader") from exc

        try:
            conn = psycopg2.connect(self._dsn)
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT tenant_id, cdm_entity_id, es_present, neo4j_present, ts_present
                        FROM nexus_system.entity_store_presence
                        WHERE tenant_id::text = %s AND cdm_entity_id = %s
                        """,
                        (tenant_id, cdm_entity_id),
                    )
                    raw = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("entity_store_presence read failed for %s/%s: %s", tenant_id, cdm_entity_id, exc)
            return None

        if not raw:
            return None
        return _row_to_presence(raw)


def _row_to_presence(raw: dict[str, Any]) -> EntityStorePresence:
    return EntityStorePresence(
        tenant_id=str(raw["tenant_id"]),
        cdm_entity_id=str(raw["cdm_entity_id"]),
        es_present=bool(raw.get("es_present")),
        neo4j_present=bool(raw.get("neo4j_present")),
        ts_present=bool(raw.get("ts_present")),
    )
