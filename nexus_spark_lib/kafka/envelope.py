"""Build SparkTransformResult JSON and ER routing columns for Kafka."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

from nexus_spark_lib.models.entity_store_presence import EntityStoreState


def apply_er_routing(df: DataFrame) -> DataFrame:
    """Ensure ``er_publish_to_m3`` exists (call after ``resolve()`` without presence broadcast)."""
    if "er_publish_to_m3" in df.columns:
        return df
    mat = F.lower(F.coalesce(F.col("materialization_level"), F.lit("warm")))
    return df.withColumn("er_publish_to_m3", mat != F.lit(EntityStoreState.COLD.value))


def filter_m3_eligible(df: DataFrame) -> DataFrame:
    """Drop entity-store-cold rows before Stage 3 / Kafka (ER index only in foreachBatch)."""
    routed = apply_er_routing(df)
    return routed.filter(F.col("er_publish_to_m3") == F.lit(True))


def filter_er_index_only(df: DataFrame) -> DataFrame:
    """Rows that need ER index registration but must not reach M3."""
    routed = apply_er_routing(df)
    return routed.filter(F.col("er_publish_to_m3") == F.lit(False))


def attach_transform_envelope(df: DataFrame) -> DataFrame:
    """
    Add ``transform_result_json`` for ``prepare_kafka_output()``.

    ``materialization_level`` in the envelope uses ``effective_materialization_level``
    when present so CDM Mapper and M3 Writer receive entity-store-aware routing.
    """
    mat_expr = F.coalesce(
        F.col("effective_materialization_level"),
        F.col("materialization_level"),
        F.lit(EntityStoreState.WARM.value),
    )

    @F.udf(StringType())
    def _build_transform_result_json(
        tenant_id: str,
        cdm_entity_id: str,
        cdm_entity_type: str,
        source_system: str,
        source_record_id: str,
        source_table: str,
        source_op: str,
        source_ts: str,
        materialization_level: str,
        entity_store_materialization: str | None,
        effective_materialization_level: str | None,
        normalised_json: str,
        golden_fields_json: str | None,
        provenance_hash: str | None,
        er_publish_to_m3: bool | None,
    ) -> str:
        fields_payload: list[dict[str, Any]] = []
        if golden_fields_json:
            try:
                parsed = json.loads(golden_fields_json)
                if isinstance(parsed, list):
                    fields_payload = parsed
            except json.JSONDecodeError:
                pass
        elif normalised_json:
            try:
                norm = json.loads(normalised_json)
                if isinstance(norm, dict):
                    for name, raw in norm.items():
                        val = raw.get("value") if isinstance(raw, dict) else raw
                        fields_payload.append(
                            {
                                "attribute_name": name,
                                "value": val,
                                "quality": "good",
                            }
                        )
            except json.JSONDecodeError:
                pass

        effective = (effective_materialization_level or materialization_level or "warm").lower()
        envelope: dict[str, Any] = {
            "tenant_id": tenant_id,
            "cdm_entity_id": cdm_entity_id,
            "cdm_entity_type": cdm_entity_type,
            "operation": "UPSERT",
            "materialization_level": effective,
            "effective_materialization_level": effective,
            "contributing_record": {
                "source_system": source_system or "",
                "source_record_id": source_record_id or "",
                "source_table": source_table or "",
                "source_op": source_op or "upsert",
                "source_ts": source_ts or "",
            },
            "provenance_summary": {
                "provenance_hash": provenance_hash or "",
            },
            "fields": fields_payload,
            "er_publish_to_m3": bool(er_publish_to_m3) if er_publish_to_m3 is not None else effective != "cold",
        }
        if entity_store_materialization:
            envelope["entity_store_materialization"] = entity_store_materialization
        return json.dumps(envelope, default=str)

    def _col(name: str, default: Any = None):
        return F.col(name) if name in df.columns else F.lit(default)

    return df.withColumn(
        "transform_result_json",
        _build_transform_result_json(
            F.col("tenant_id"),
            F.col("cdm_entity_id"),
            F.col("cdm_entity_type"),
            F.col("source_system"),
            F.col("source_record_id"),
            _col("source_table", ""),
            _col("source_op", "upsert"),
            _col("source_ts", ""),
            mat_expr,
            _col("entity_store_materialization"),
            _col("effective_materialization_level"),
            F.col("normalised_json"),
            _col("golden_fields_json"),
            _col("provenance_hash"),
            _col("er_publish_to_m3"),
        ),
    )
