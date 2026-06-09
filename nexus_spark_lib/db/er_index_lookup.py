from __future__ import annotations

"""Fast-path + Signal A lookups on ``nexus_system.entity_resolution_index`` (Op 3)."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ErIndexLookup:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def fast_path(
        self,
        *,
        tenant_id: str,
        source_connector: str,
        source_record_id: str,
    ) -> str | None:
        rows = self._fetch(
            """
            SELECT cdm_entity_id
            FROM nexus_system.entity_resolution_index
            WHERE tenant_id::text = %s
              AND source_connector = %s
              AND source_record_id = %s
              AND COALESCE(is_active, TRUE) IS TRUE
            ORDER BY resolved_at DESC NULLS LAST
            LIMIT 1
            """,
            tenant_id,
            source_connector,
            source_record_id,
        )
        return rows[0] if rows else None

    def signal_a(
        self,
        *,
        tenant_id: str,
        cdm_entity_type: str,
        deterministic_id_column: str,
        deterministic_id_value: str,
    ) -> str | None:
        rows = self._fetch(
            """
            SELECT cdm_entity_id
            FROM nexus_system.entity_resolution_index
            WHERE tenant_id::text = %s
              AND cdm_entity_type = %s
              AND deterministic_id_column = %s
              AND deterministic_id_value = %s
              AND COALESCE(is_active, TRUE) IS TRUE
            ORDER BY confidence DESC NULLS LAST, resolved_at DESC NULLS LAST
            LIMIT 1
            """,
            tenant_id,
            cdm_entity_type,
            deterministic_id_column,
            deterministic_id_value,
        )
        return rows[0] if rows else None

    def _fetch(self, sql: str, *args: Any) -> list[str]:
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError("psycopg2 required for ErIndexLookup") from exc

        try:
            conn = psycopg2.connect(self._dsn)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, args)
                    return [str(row[0]) for row in cur.fetchall() if row and row[0]]
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("entity_resolution_index lookup failed: %s", exc)
            return []
