"""Redis C1 pre-warm from PostgreSQL's authoritative ER partition.

The Airflow worker calls this without starting a Spark session. Existing cache
entries win NX races with a concurrent writer; pre-warm never replaces them
with an older snapshot. Invalidations remain the ER writer's responsibility.
"""
from __future__ import annotations

from typing import Any


class ERLookupClient:
    def __init__(self, *, redis: Any, postgres: Any) -> None:
        self.redis = redis
        self.postgres = postgres

    def prewarm(self, tenant_id: str, connector_id: str, *, batch_size: int = 1000) -> int:
        if not tenant_id or not connector_id or batch_size < 1:
            raise ValueError('tenant, connector and a positive batch size are required')
        with self.postgres.transaction():
            self.postgres.execute("SELECT set_config('nexus.current_tenant_id', %s, true)", (tenant_id,))
            # The specified C1 key omits source_table. Reject ambiguous source
            # IDs before putting any entry in that shared connector keyspace.
            conflict = self.postgres.execute(
                "SELECT source_record_id FROM nexus_system.entity_resolution_index "
                "WHERE tenant_id = %s AND connector_id::text = %s AND COALESCE(is_active, true) "
                "GROUP BY source_record_id HAVING count(DISTINCT cdm_entity_id) > 1 LIMIT 1",
                (tenant_id, connector_id),
            ).fetchone()
            if conflict:
                raise ValueError('ER C1 key is ambiguous across source tables')
            count = 0
            with self.postgres.cursor(name='nexus_er_prewarm') as cursor:
                cursor.execute(
                    "SELECT source_record_id, cdm_entity_id FROM nexus_system.entity_resolution_index "
                    "WHERE tenant_id = %s AND connector_id::text = %s AND COALESCE(is_active, true) "
                    "AND source_record_id IS NOT NULL AND cdm_entity_id IS NOT NULL",
                    (tenant_id, connector_id),
                )
                while rows := cursor.fetchmany(batch_size):
                    with self.redis.pipeline(transaction=False) as pipeline:
                        for source_record_id, entity_id in rows:
                            pipeline.set(f'er:{tenant_id}:{connector_id}:{source_record_id}',
                                         str(entity_id), ex=3600, nx=True)
                        pipeline.execute(raise_on_error=True)
                    count += len(rows)
            return count
