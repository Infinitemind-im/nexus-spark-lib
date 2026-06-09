"""Spark ER pipeline wiring: presence broadcast → resolve → envelope → Kafka."""

from __future__ import annotations

from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame, SparkSession

from nexus_spark_lib.db.entity_store_presence_loader import load_entity_store_presence_snapshot
from nexus_spark_lib.kafka.envelope import (
    attach_transform_envelope,
    filter_er_index_only,
    filter_m3_eligible,
)
from nexus_spark_lib.kafka.entity_routed import attach_entity_routed_kafka
from nexus_spark_lib.kafka.writer import prepare_kafka_output
from nexus_spark_lib.transform.stage2_resolve import resolve


def broadcast_entity_store_presence(
    spark: SparkSession,
    system_dsn: str,
    *,
    tenant_ids: list[str] | None = None,
) -> Broadcast:
    snapshot = load_entity_store_presence_snapshot(system_dsn, tenant_ids=tenant_ids)
    return spark.sparkContext.broadcast(snapshot)


def run_entity_resolution_stage(
    df: DataFrame,
    er_index_broadcast: Broadcast,
    *,
    entity_store_presence_broadcast: Broadcast | None = None,
    neo4j_driver: Any | None = None,
    mode: str = "streaming",
) -> DataFrame:
    """Stage 2 resolve + entity_store_presence columns + ``er_publish_to_m3``."""
    return resolve(
        df,
        er_index_broadcast,
        neo4j_driver=neo4j_driver,
        mode=mode,
        entity_store_presence_broadcast=entity_store_presence_broadcast,
    )


def prepare_m3_kafka_batch(
    resolved_df: DataFrame,
    *,
    include_entity_routed: bool = False,
    tenant_id: str | None = None,
) -> DataFrame:
    """
    Envelope + optional ``entity_routed`` columns for M3-eligible rows only.

    Cold entity-store rows are excluded; register them in foreachBatch via
    ``persist_cold_er_rows``.
    """
    m3_df = filter_m3_eligible(resolved_df)
    m3_df = attach_transform_envelope(m3_df)
    m3_df = prepare_kafka_output(m3_df)
    if include_entity_routed:
        m3_df = attach_entity_routed_kafka(m3_df, tenant_id=tenant_id)
    return m3_df


def cold_er_rows(resolved_df: DataFrame) -> DataFrame:
    """Rows requiring ER index registration only (entity_store cold)."""
    return filter_er_index_only(resolved_df)


async def persist_cold_er_rows(
    conn: Any,
    rows: list[dict[str, Any]],
    *,
    connector_id: str = "spark",
    source_table: str = "raw",
) -> int:
    """
    foreachBatch helper: upsert ER index for cold entity-store rows, skip M3/Kafka.

    Returns count of rows persisted.
    """
    from nexus_spark_lib.db.resolve_persist import persist_entity_resolution_outcome

    count = 0
    for row in rows:
        cdm_entity_id = row.get("cdm_entity_id")
        if not cdm_entity_id:
            continue
        await persist_entity_resolution_outcome(
            conn,
            tenant_id=str(row["tenant_id"]),
            connector_id=connector_id,
            source_table=str(row.get("source_table") or source_table),
            source_system=str(row.get("source_system") or ""),
            source_record_id=str(row["source_record_id"]),
            cdm_entity_id=str(cdm_entity_id),
            cdm_entity_type=str(row.get("cdm_entity_type") or "party"),
            er_confidence=1.0,
            er_resolution_method=str(row.get("er_resolution_method") or "signal_a"),
            er_is_provisional=False,
            er_review_peer_cdm_id=None,
            er_signal_breakdown_json=None,
            trace_id=str(row.get("trace_id") or ""),
            emit_review_kafka=False,
        )
        count += 1
    return count
