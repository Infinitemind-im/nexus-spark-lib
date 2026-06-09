"""Build ``{tenant}.m1.entity_routed`` payloads from Spark ER output (Op 8)."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _effective_level_col():
    return F.coalesce(
        F.col("effective_materialization_level"),
        F.col("materialization_level"),
        F.lit("warm"),
    )


def build_entity_routed_payload(
    *,
    tenant_id: str,
    cdm_entity_id: str,
    cdm_entity_type: str,
    materialization_level: str,
    provenance_hash: str = "",
    source_ts: str = "",
    fields: dict[str, Any] | list[Any] | None = None,
    roles: dict[str, Any] | None = None,
    golden_record_id: str | None = None,
    entity_store_materialization: str | None = None,
) -> dict[str, Any]:
    """Flat Iter2 ``entity_routed`` body (CDM Mapper compatible)."""
    level = (materialization_level or "warm").lower()
    body: dict[str, Any] = {
        "tenant_id": tenant_id,
        "cdm_entity_id": cdm_entity_id,
        "cdm_entity_type": cdm_entity_type,
        "golden_record_id": golden_record_id or cdm_entity_id,
        "materialization_level": level,
        "effective_materialization_level": level,
        "provenance_hash": provenance_hash,
        "source_ts": source_ts,
    }
    if entity_store_materialization:
        body["entity_store_materialization"] = entity_store_materialization
    if isinstance(fields, list):
        body["fields"] = fields
    elif isinstance(fields, dict):
        body["cdm_fields"] = fields
    if roles:
        body["roles"] = roles
    return body


def attach_entity_routed_kafka(
    df: DataFrame,
    *,
    tenant_id: str | None = None,
) -> DataFrame:
    """
    Add ``entity_routed_key`` / ``entity_routed_value`` for M3-bound rows.

    Expects M3-eligible rows only (``filter_m3_eligible``). Key = ``cdm_entity_id``.
    """
    topic_tenant = tenant_id or F.col("tenant_id").cast("string")

    @F.udf(StringType())
    def _entity_routed_value(
        tid: str,
        cdm_entity_id: str,
        cdm_entity_type: str,
        materialization_level: str,
        entity_store_materialization: str | None,
        provenance_hash: str | None,
        source_ts: str | None,
        transform_result_json: str | None,
        normalised_json: str | None,
    ) -> str:
        fields: dict[str, Any] | list[Any] | None = None
        if transform_result_json:
            try:
                tr = json.loads(transform_result_json)
                fields = tr.get("fields")
            except json.JSONDecodeError:
                pass
        if fields is None and normalised_json:
            try:
                norm = json.loads(normalised_json or "{}")
                if isinstance(norm, dict):
                    fields = {
                        k: (v.get("value") if isinstance(v, dict) else v)
                        for k, v in norm.items()
                    }
            except json.JSONDecodeError:
                fields = {}

        body = build_entity_routed_payload(
            tenant_id=tid,
            cdm_entity_id=cdm_entity_id,
            cdm_entity_type=cdm_entity_type,
            materialization_level=materialization_level,
            provenance_hash=provenance_hash or "",
            source_ts=source_ts or "",
            fields=fields,
            entity_store_materialization=entity_store_materialization,
        )
        return json.dumps(body, default=str)

    mat = _effective_level_col()

    def _col(name: str, default: Any = None):
        return F.col(name) if name in df.columns else F.lit(default)

    return df.withColumn("entity_routed_key", F.col("cdm_entity_id").cast("string")).withColumn(
        "entity_routed_value",
        _entity_routed_value(
            topic_tenant if tenant_id else F.col("tenant_id"),
            F.col("cdm_entity_id"),
            F.col("cdm_entity_type"),
            mat,
            _col("entity_store_materialization"),
            _col("provenance_hash"),
            _col("source_ts"),
            _col("transform_result_json"),
            F.col("normalised_json"),
        ),
    )
