"""Stage 0 — Normalisation.

First stage in the pipeline. Applies CDM field mapping to raw, already-typed
payloads and produces quality-scored normalised fields.

Pipeline order:
    reader.py → stage0_normalise → stage1_materialization → stage2_resolve → stage3_synthesise

Responsibilities (BUSINESS LOGIC ONLY):
  1. CDM field mapping via broadcast (connector_id × source_table → cdm_entity_type + field map)
  2. CRUD routing: DELETE → before_payload, INSERT/UPDATE/SNAPSHOT_READ/RELEVEL → after_payload
  3. FX currency conversion at source_ts rate (NOT the processing-time rate) — business rule
  4. DQ scoring: proportion of mapped fields / total fields in payload — business rule
  5. Fields with no CDM mapping → source_extras

NOT in scope (belongs to nexus-spark-transformer before calling this lib):
  - Type coercion (dates, booleans, decimals, strings)
  - Strip whitespace / null-like string normalisation
  - Within-batch deduplication
  - Kafka offset management
  - Spark configuration

The transformer calls this function AFTER cleaning and typing the data.
All values in after_payload / before_payload are already clean Python-native types.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.models.transformed_record import FieldQuality
from nexus_spark_lib.observability.metrics import NORMALISE_RECORDS
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def normalise(
    df: DataFrame,
    cdm_mapping_broadcast: Broadcast,
    fx_rates_broadcast: Broadcast,
) -> DataFrame:
    """Apply CDM field mapping, CRUD routing, FX conversion and DQ scoring.

    Precondition: df contains clean, already-typed values in after_payload /
    before_payload. Type coercion and null normalisation are the transformer's
    responsibility and must be done before calling this function.

    Args:
        df:                    Parsed raw-record DataFrame (from kafka/reader.py).
        cdm_mapping_broadcast: Broadcast[CdmMappingBroadcast] — field maps per connector.
        fx_rates_broadcast:    Broadcast[FxRatesBroadcast] — historical FX rates.

    Returns:
        DataFrame with added columns:
        - cdm_entity_type  (str)   — resolved from connector_id × source_table
        - normalised_json  (str)   — JSON: cdm_attr → {value, quality, source_attribute, pii_flag}
        - source_extras    (str)   — JSON: unmapped raw fields
        - dq_score         (str)   — serialised float: mapped_fields / total_fields
        - dedup_key        (str)   — natural key: tenant_id|cdm_entity_type|source_record_id
    """
    normalise_udf = F.udf(
        _normalise_row(cdm_mapping_broadcast, fx_rates_broadcast),
        _NORMALISE_OUTPUT_SCHEMA,
    )

    enriched = df.withColumn(
        "_norm",
        normalise_udf(
            F.col("tenant_id"),
            F.col("connector_id"),
            F.col("source_table"),
            F.col("source_record_id"),
            F.col("source_op"),
            F.col("source_ts"),
            F.col("after_payload"),
            F.col("before_payload"),
        ),
    ).select(
        "*",
        F.col("_norm.cdm_entity_type").alias("cdm_entity_type"),
        F.col("_norm.normalised_json").alias("normalised_json"),
        F.col("_norm.source_extras").alias("source_extras"),
        F.col("_norm.dq_score").alias("dq_score"),
        F.col("_norm.dedup_key").alias("dedup_key"),
    ).drop("_norm")

    NORMALISE_RECORDS.labels(status="ok").inc()
    return enriched


# ---------------------------------------------------------------------------
# Spark UDF — runs on executors
# ---------------------------------------------------------------------------

_NORMALISE_OUTPUT_SCHEMA = StructType([
    StructField("cdm_entity_type", StringType(), True),
    StructField("normalised_json", StringType(), True),
    StructField("source_extras", StringType(), True),
    StructField("dq_score", StringType(), True),
    StructField("dedup_key", StringType(), True),
])


def _normalise_row(cdm_mapping_bc: Broadcast, fx_rates_bc: Broadcast):
    """Return a closure for the normalise UDF. Captures broadcasts by closure."""

    def _fn(
        tenant_id: str,
        connector_id: str,
        source_table: str,
        source_record_id: str,
        source_op: str,
        source_ts,
        after_payload: dict | None,
        before_payload: dict | None,
    ) -> tuple:
        mapping = cdm_mapping_bc.value
        fx_rates = fx_rates_bc.value

        # --- Business rule 1: CDM field mapping ---
        cdm_entity_type = _resolve_cdm_type(mapping, connector_id, source_table)
        field_map = _resolve_field_map(mapping, connector_id, source_table)

        # --- Business rule 2: CRUD routing ---
        # DELETE uses before_payload (the row as it existed before deletion).
        # All other ops (INSERT, UPDATE, SNAPSHOT_READ, RELEVEL) use after_payload.
        op = source_op or "INSERT"
        payload: dict[str, Any] = {}
        if op == "DELETE":
            payload = dict(before_payload or {})
        else:
            payload = dict(after_payload or {})

        normalised_fields: dict[str, dict] = {}
        source_extras: dict[str, Any] = {}

        for raw_key, raw_value in payload.items():
            cdm_attr = field_map.get(raw_key)
            if cdm_attr is None:
                # No CDM mapping — preserve in source_extras unchanged
                source_extras[raw_key] = raw_value
                continue

            # --- Business rule 3: FX conversion at source_ts ---
            field_meta = field_map.get(f"__meta__{raw_key}", {})
            value, quality = _apply_fx_if_monetary(
                raw_value=raw_value,
                field_meta=field_meta,
                source_ts=source_ts,
                fx_rates=fx_rates,
                tenant_id=tenant_id,
            )

            normalised_fields[cdm_attr] = {
                "value": value,
                "quality": quality.value,
                "source_attribute": raw_key,
                "pii_flag": bool(field_meta.get("pii", False)),
            }

        # --- Business rule 4: DQ score ---
        # Proportion of payload fields that have a CDM mapping.
        # Fields that went to source_extras count as unmapped.
        total = len(payload)
        mapped = len(normalised_fields)
        dq_score = round(mapped / total, 4) if total > 0 else 1.0

        dedup_key = f"{tenant_id}|{cdm_entity_type}|{source_record_id}"

        return (
            cdm_entity_type,
            json.dumps(normalised_fields, default=str),
            json.dumps(source_extras, default=str),
            str(dq_score),
            dedup_key,
        )

    return _fn


# ---------------------------------------------------------------------------
# Business logic helpers
# ---------------------------------------------------------------------------

def _apply_fx_if_monetary(
    raw_value: Any,
    field_meta: dict,
    source_ts: Any,
    fx_rates: Any,
    tenant_id: str,
) -> tuple[Any, FieldQuality]:
    """Apply FX conversion if this field is monetary.

    Business rule: use the rate at source_ts, NOT the processing time.
    If the field is not monetary, or FX data is unavailable, return unchanged.
    """
    if raw_value is None:
        return (None, FieldQuality.MISSING)

    src_currency = field_meta.get("currency_field")
    if not src_currency or fx_rates is None or source_ts is None:
        return (raw_value, FieldQuality.GOOD)

    target_currency = field_meta.get("target_currency", "USD")
    try:
        from datetime import datetime
        ts = source_ts if isinstance(source_ts, datetime) else datetime.fromisoformat(str(source_ts))
        result = fx_rates.convert(raw_value, str(src_currency), target_currency, ts)
        return (result.converted_amount, FieldQuality.GOOD)
    except Exception:
        # FX lookup failed — return original value, flag as suspect
        return (raw_value, FieldQuality.SUSPECT)


# ---------------------------------------------------------------------------
# CDM mapping resolution helpers (read from broadcast — no I/O)
# ---------------------------------------------------------------------------

def _resolve_cdm_type(mapping: Any, connector_id: str, source_table: str) -> str:
    try:
        return mapping.get_cdm_entity_type(connector_id, source_table) or "unknown"
    except Exception:
        return "unknown"


def _resolve_field_map(mapping: Any, connector_id: str, source_table: str) -> dict:
    try:
        return mapping.get_field_map(connector_id, source_table) or {}
    except Exception:
        return {}
