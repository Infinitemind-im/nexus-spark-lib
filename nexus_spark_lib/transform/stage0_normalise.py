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
  6. Type coercion: coerce each CDM-mapped field to the Python-native type declared in
     field_meta (boolean, decimal, integer, string). Null-like strings ("null", "N/A", …) → None.
  7. Timestamp canonicalisation: any date/datetime/timestamp field is normalised to
     ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SS+00:00) — business rule.
  8. Within-batch deduplication: for duplicate dedup_keys in the same micro-batch,
     keep only the record with the latest source_ts — business rule.

NOT in scope (belongs to nexus-spark-transformer before calling this lib):
  - Kafka offset management
  - Spark configuration
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

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
    blocking_rules_broadcast: Broadcast | None = None,
) -> DataFrame:
    """Apply CDM field mapping, type coercion, timestamp canonicalisation, CRUD routing,
    FX conversion, DQ scoring, within-batch deduplication, and blocking key computation.

    Args:
        df:                       Parsed raw-record DataFrame (from kafka/reader.py).
        cdm_mapping_broadcast:    Broadcast[CdmMappingBroadcast] — field maps per connector.
        fx_rates_broadcast:       Broadcast[FxRatesBroadcast] — historical FX rates.
        blocking_rules_broadcast: Optional Broadcast[dict] — entity_blocking_rules per
                                  (tenant_id, cdm_entity_type) → list[str] of CDM field
                                  names used to form the LSH blocking key for Stage 2 ER.
                                  When None, blocking_key falls back to a type-level hash.

    Returns:
        DataFrame with added columns:
        - cdm_entity_type  (str)   — resolved from connector_id × source_table
        - normalised_json  (str)   — JSON: cdm_attr → {value, quality, source_attribute, pii_flag}
        - source_extras    (str)   — JSON: unmapped raw fields
        - dq_score         (str)   — serialised float: mapped_fields / total_fields
        - dedup_key        (str)   — natural key: tenant_id|cdm_entity_type|source_record_id
        - blocking_key     (str)   — LSH bucket key derived from entity_blocking_rules;
                                     consumed by Stage 2 Signal B probabilistic ER
    """
    normalise_udf = F.udf(
        _normalise_row(cdm_mapping_broadcast, fx_rates_broadcast, blocking_rules_broadcast),
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
        F.col("_norm.blocking_key").alias("blocking_key"),
    ).drop("_norm")

    # --- Business rule 8: Within-batch deduplication ---
    # For duplicate dedup_keys in the same micro-batch, keep only the latest by source_ts.
    _window = Window.partitionBy("dedup_key").orderBy(F.col("source_ts").desc())
    enriched = (
        enriched
        .withColumn("_row_num", F.row_number().over(_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

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
    StructField("blocking_key", StringType(), True),
])


def _normalise_row(
    cdm_mapping_bc: Broadcast,
    fx_rates_bc: Broadcast,
    blocking_rules_bc: Broadcast | None,
):
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
            field_meta = field_map.get(f"__meta__{raw_key}", {})

            if cdm_attr is None:
                # No CDM mapping — apply basic null normalisation then preserve in source_extras
                source_extras[raw_key] = strip_null_like(raw_value)
                continue

            # --- Business rule 6 & 7: Type coercion + timestamp canonicalisation ---
            coerced = coerce_value(raw_value, field_meta)

            # --- Business rule 3: FX conversion at source_ts ---
            value, quality = _apply_fx_if_monetary(
                raw_value=coerced,
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

        # --- Business rule: blocking key (for Stage 2 Signal B LSH) ---
        # Derived from entity_blocking_rules: concatenate configured CDM field values
        # to form the value fed into the blocking hash.
        blocking_key = _compute_blocking_key(
            tenant_id, cdm_entity_type, normalised_fields, blocking_rules_bc
        )

        return (
            cdm_entity_type,
            json.dumps(normalised_fields, default=str),
            json.dumps(source_extras, default=str),
            str(dq_score),
            dedup_key,
            blocking_key,
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
# Blocking key computation (Stage 0 → Stage 2 handoff for Signal B)
# ---------------------------------------------------------------------------

def _compute_blocking_key(
    tenant_id: str,
    cdm_entity_type: str,
    normalised_fields: dict,
    blocking_rules_bc,
) -> str:
    """Compute the LSH blocking key from entity_blocking_rules.

    The blocking key groups candidate pairs for Signal B similarity scoring.
    It is derived from the CDM field values listed in entity_blocking_rules for
    this (tenant_id, cdm_entity_type). The values are lowercased and truncated
    to 32 chars each, then joined and hashed.

    Falls back to a type-level hash when no blocking rules are configured,
    which places all records of the same entity type in the same bucket —
    correct but less efficient for large corpora.
    """
    from nexus_spark_lib._internal.hash_utils import blocking_key_hash

    blocking_columns: list[str] = []
    if blocking_rules_bc is not None:
        try:
            rules = blocking_rules_bc.value
            blocking_columns = rules.get((tenant_id, cdm_entity_type), [])
        except Exception:
            pass

    if blocking_columns:
        parts = []
        for col in blocking_columns:
            entry = normalised_fields.get(col)
            val = entry.get("value") if isinstance(entry, dict) else entry
            if val is not None:
                parts.append(str(val).lower()[:32])
        blocking_value = "|".join(parts) if parts else cdm_entity_type
    else:
        blocking_value = cdm_entity_type

    return blocking_key_hash(tenant_id, cdm_entity_type, blocking_value)


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


# ---------------------------------------------------------------------------
# Pre-CDM Spark-level preprocessing (called by the transformer before stage0)
# ---------------------------------------------------------------------------

def coerce_raw_payloads(df: DataFrame) -> DataFrame:
    """Apply null-like normalisation and whitespace stripping to after_payload / before_payload.

    Business rule: any value that is null-like ("null", "N/A", "", …) is set to None.
    Strings are whitespace-stripped.
    This is a pre-CDM step — no CDM type metadata is available yet.
    Called by the transformer BEFORE stage0 normalise().
    """
    @F.udf("map<string,string>")
    def _clean_map(payload_map):
        if payload_map is None:
            return {}
        return {k: strip_null_like(v) for k, v in payload_map.items()}

    return (
        df
        .withColumn("after_payload", _clean_map(F.col("after_payload")))
        .withColumn("before_payload", _clean_map(F.col("before_payload")))
    )


def deduplicate_raw_batch(df: DataFrame) -> DataFrame:
    """Within-batch deduplication on raw source keys, before CDM mapping.

    Business rule: keep only the record with the latest source_ts for each
    (tenant_id, connector_id, source_table, source_record_id) tuple.
    Called by the transformer BEFORE stage0 normalise().
    """
    dedup_cols = ["tenant_id", "connector_id", "source_table", "source_record_id"]
    _w = Window.partitionBy(*dedup_cols).orderBy(F.col("source_ts").desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(_w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Type coercion and null normalisation helpers (business logic — run on executors)
# ---------------------------------------------------------------------------

def strip_null_like(value: Any) -> Any:
    """Return None for null-like string values; leave non-strings unchanged."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    return None if stripped.lower() in ("", "null", "none", "n/a", "na", "nan", "-") else stripped


def canonicalise_timestamp(raw_value: Any) -> Any:
    """Normalise any timestamp-like value to ISO 8601 UTC (YYYY-MM-DDTHH:MM:SS+00:00).

    Returns the original value unchanged if it cannot be parsed, so that
    _apply_fx_if_monetary can later label the field SUSPECT.
    """
    from datetime import datetime, timezone

    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        dt = raw_value
    else:
        s = str(raw_value).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        else:
            return raw_value  # unparseable — return as-is
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def coerce_value(raw_value: Any, field_meta: dict) -> Any:
    """Coerce raw payload value to the Python-native type declared in CDM field_meta.

    Business rules:
    - Null-like strings ("null", "N/A", …) → None regardless of declared type.
    - boolean: accept "true"/"false"/"1"/"0"/"yes"/"no" case-insensitively.
    - decimal/float/number: convert via Decimal to avoid float rounding drift.
    - integer: strict int() cast; returns raw_value unchanged on failure.
    - date/datetime/timestamp: delegate to canonicalise_timestamp().
    - string (default): strip whitespace; null-like → None.
    """
    if raw_value is None:
        return None

    cdm_type = str(field_meta.get("type", "string")).lower()

    if cdm_type in ("date", "datetime", "timestamp"):
        return canonicalise_timestamp(raw_value)

    if cdm_type == "boolean":
        if isinstance(raw_value, bool):
            return raw_value
        return str(raw_value).strip().lower() in ("true", "1", "yes", "y", "t")

    if cdm_type in ("decimal", "float", "number"):
        try:
            from decimal import Decimal
            return float(Decimal(str(raw_value)))
        except Exception:
            return raw_value

    if cdm_type == "integer":
        try:
            return int(raw_value)
        except Exception:
            return raw_value

    # Default: string — strip whitespace and normalise null-like values
    return strip_null_like(raw_value)
