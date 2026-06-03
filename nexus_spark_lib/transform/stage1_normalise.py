"""Stage 1 — Normalisation.

Runs AFTER Stage 0 (materialization gate). By the time this stage is called,
the DataFrame already has cdm_entity_type and materialization_level columns
set by stage0_materialization.materialization_gate(). COLD records have been
dropped by drop_cold().

Pipeline order (per NEXUS-Iter2-REF-DataPaths §1.4–1.5):
    reader.py → stage0_materialization → stage1_normalise

Responsibilities (BUSINESS LOGIC ONLY):
    1. CDM field-level mapping via broadcast ((connector_id or source_system) ×
         source_table → field map). cdm_entity_type is already set by Stage 0 —
         NOT re-resolved here.
  2. CRUD routing: DELETE → before_payload, INSERT/UPDATE/SNAPSHOT_READ/RELEVEL → after_payload
  3. FX currency conversion at source_ts rate (NOT the processing-time rate) — business rule
  4. DQ scoring: proportion of mapped fields / total fields in payload — business rule
  5. Fields with no CDM mapping → source_extras
  6. Type coercion: coerce each CDM-mapped field to the Python-native type declared in
     field_meta (boolean, decimal, integer, string). Null-like strings ("null", "N/A", …) → None.
  7. Timestamp canonicalisation: any date/datetime/timestamp field is normalised to
     ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SS+00:00) — business rule.
  8. Blocking key computation from entity_blocking_rules.
  9. Within-batch deduplication: for duplicate dedup_keys in the same micro-batch,
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

    Must be called AFTER stage0_materialization.materialization_gate(). The DataFrame
    must already have cdm_entity_type (from Stage 0) and COLD records must have been
    removed with drop_cold().

    Args:
        df:                       DataFrame from Stage 0 (warm + hot records only).
                      Required columns: tenant_id, connector_id, source_table,
                      source_record_id, source_op, source_ts, after_payload,
                      before_payload, cdm_entity_type (from Stage 0).
                      source_system is used as a fallback lookup key when
                      present on the DataFrame.
        cdm_mapping_broadcast:    Broadcast[CdmMappingBroadcast] — field-level maps per
                      connector or source_system (source_field → cdm_attr +
                      type metadata).
        fx_rates_broadcast:       Broadcast[FxRatesBroadcast] — historical FX rates.
        blocking_rules_broadcast: Optional Broadcast[dict] — entity_blocking_rules per
                                  (tenant_id, cdm_entity_type) → list[str] of CDM field
                                  names used to form the LSH blocking key.
                                  When None, blocking_key falls back to a type-level hash.

    Returns:
        DataFrame with added columns:
        - normalised_json  (str)   — JSON: cdm_attr → {value, quality, source_attribute, pii_flag}
        - source_extras    (str)   — JSON: unmapped raw fields
        - dq_score         (str)   — serialised float: mapped_fields / total_fields
        - dedup_key        (str)   — natural key: tenant_id|cdm_entity_type|source_record_id
        - blocking_key     (str)   — LSH bucket key derived from entity_blocking_rules;
                                     LSH blocking key for downstream consumers
        - changed_canonical_attributes_json (str) — JSON array of canonical attributes
                                     whose normalised values changed on UPDATE
    """
    normalise_udf = F.udf(
        _normalise_row(cdm_mapping_broadcast, fx_rates_broadcast, blocking_rules_broadcast),
        _NORMALISE_OUTPUT_SCHEMA,
    )

    source_system_col = F.col("source_system") if "source_system" in df.columns else F.lit(None)

    enriched = df.withColumn(
        "_norm",
        normalise_udf(
            F.col("tenant_id"),
            F.col("connector_id"),
            source_system_col,
            F.col("source_table"),
            F.col("cdm_entity_type"),   # already set by Stage 0 — do not re-resolve
            F.col("source_record_id"),
            F.col("source_op"),
            F.col("source_ts"),
            F.col("after_payload"),
            F.col("before_payload"),
        ),
    ).select(
        "*",
        F.col("_norm.normalised_json").alias("normalised_json"),
        F.col("_norm.source_extras").alias("source_extras"),
        F.col("_norm.dq_score").alias("dq_score"),
        F.col("_norm.dedup_key").alias("dedup_key"),
        F.col("_norm.blocking_key").alias("blocking_key"),
        F.col("_norm.changed_canonical_attributes_json").alias("changed_canonical_attributes_json"),
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

    NORMALISE_RECORDS.labels(tenant_id="system", status="ok").inc()
    return enriched


# ---------------------------------------------------------------------------
# Spark UDF — runs on executors
# ---------------------------------------------------------------------------

_NORMALISE_OUTPUT_SCHEMA = StructType([
    StructField("normalised_json", StringType(), True),
    StructField("source_extras", StringType(), True),
    StructField("dq_score", StringType(), True),
    StructField("dedup_key", StringType(), True),
    StructField("blocking_key", StringType(), True),
    StructField("changed_canonical_attributes_json", StringType(), True),
])


def _normalise_row(
    cdm_mapping_bc: Broadcast,
    fx_rates_bc: Broadcast,
    blocking_rules_bc: Broadcast | None,
):
    """Return a closure for the normalise UDF. Captures broadcasts by closure."""
    # Extract plain Python values BEFORE defining _fn to avoid pickling Broadcast objects.
    _cdm_mapping_val = cdm_mapping_bc.value
    _fx_rates_val = fx_rates_bc.value
    _blocking_rules_val = blocking_rules_bc.value if blocking_rules_bc is not None else None

    def _fn(
        tenant_id: str,
        connector_id: str,
        source_system: str | None,
        source_table: str,
        cdm_entity_type: str,     # already resolved by Stage 0 materialization gate
        source_record_id: str,
        source_op: str,
        source_ts,
        after_payload: dict | None,
        before_payload: dict | None,
    ) -> tuple:
        mapping = _cdm_mapping_val
        fx_rates = _fx_rates_val

        # --- Business rule 1: CDM field-level mapping ---
        # cdm_entity_type is pre-resolved by Stage 0; only field map is needed here.
        field_map = _resolve_field_map(mapping, tenant_id, connector_id, source_system, source_table)

        def _normalise_payload(payload: dict[str, Any]) -> tuple[dict[str, dict], dict[str, Any]]:
            normalised_fields: dict[str, dict] = {}
            source_extras: dict[str, Any] = {}

            for raw_key, raw_value in payload.items():
                cdm_attr = field_map.get(raw_key)
                field_meta = field_map.get(f"__meta__{raw_key}", {})

                if cdm_attr is None:
                    source_extras[raw_key] = strip_null_like(raw_value)
                    continue

                coerced = coerce_value(raw_value, field_meta)
                value, quality = _apply_fx_if_monetary(
                    raw_value=coerced,
                    field_meta=field_meta,
                    source_ts=source_ts,
                    fx_rates=fx_rates,
                    tenant_id=tenant_id,
                )

                normalised_entry = {
                    "value": value,
                    "quality": quality.value,
                    "source_attribute": raw_key,
                    "pii_flag": bool(field_meta.get("pii", False)),
                }

                attribute_kind = str(field_meta.get("attribute_kind") or "").strip().lower()
                if attribute_kind:
                    normalised_entry["attribute_kind"] = attribute_kind

                fk_target_entity_type = str(field_meta.get("fk_target_entity_type") or "").strip()
                if fk_target_entity_type:
                    normalised_entry["fk_target_entity_type"] = fk_target_entity_type

                normalised_fields[cdm_attr] = normalised_entry

            return normalised_fields, source_extras

        # --- Business rule 2: CRUD routing ---
        # DELETE uses before_payload (the row as it existed before deletion).
        # All other ops (INSERT, UPDATE, SNAPSHOT_READ, RELEVEL) use after_payload.
        op = source_op or "INSERT"
        payload: dict[str, Any]
        changed_canonical_attributes: list[str] = []
        if op == "DELETE":
            payload = dict(before_payload or {})
            normalised_fields, source_extras = _normalise_payload(payload)
        elif op == "UPDATE":
            before_fields, _ = _normalise_payload(dict(before_payload or {}))
            payload = dict(after_payload or {})
            normalised_fields, source_extras = _normalise_payload(payload)
            changed_canonical_attributes = _compute_changed_canonical_attributes(before_fields, normalised_fields)
        else:
            payload = dict(after_payload or {})
            normalised_fields, source_extras = _normalise_payload(payload)

        # --- Business rule 4: DQ score ---
        # Proportion of payload fields that have a CDM mapping.
        # Fields that went to source_extras count as unmapped.
        total = len(payload)
        mapped = len(normalised_fields)
        dq_score = round(mapped / total, 4) if total > 0 else 1.0

        dedup_key = f"{tenant_id}|{cdm_entity_type}|{source_record_id}"

        # --- Business rule: blocking key (LSH) ---
        # Derived from entity_blocking_rules: concatenate configured CDM field values
        # to form the value fed into the blocking hash.
        blocking_key = _compute_blocking_key(
            tenant_id, cdm_entity_type, normalised_fields, _blocking_rules_val
        )

        return (
            json.dumps(normalised_fields, default=str),
            json.dumps(source_extras, default=str),
            str(dq_score),
            dedup_key,
            blocking_key,
            json.dumps(changed_canonical_attributes, default=str),
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
# Blocking key computation
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
            # Accept either a plain value or a Broadcast (backward compat)
            rules = blocking_rules_bc.value if hasattr(blocking_rules_bc, 'value') else blocking_rules_bc
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


def _compute_changed_canonical_attributes(
    before_fields: dict[str, Any],
    after_fields: dict[str, Any],
) -> list[str]:
    changed: list[str] = []
    for attr_name in sorted(set(before_fields) | set(after_fields)):
        before_value = _extract_normalised_value(before_fields.get(attr_name))
        after_value = _extract_normalised_value(after_fields.get(attr_name))
        if before_value != after_value:
            changed.append(attr_name)
    return changed


def _extract_normalised_value(field: Any) -> Any:
    if isinstance(field, dict):
        return field.get("value")
    return field


# ---------------------------------------------------------------------------
# CDM mapping resolution helpers (read from broadcast — no I/O)
# ---------------------------------------------------------------------------

def _resolve_cdm_type(
    mapping: Any,
    tenant_id: str,
    connector_id: str | None,
    source_system: str | None,
    source_table: str,
) -> str:
    for lookup_key in (connector_id, source_system):
        if not lookup_key:
            continue
        try:
            try:
                cdm_entity_type = mapping.get_cdm_entity_type(tenant_id, lookup_key, source_table) or "unknown"
            except TypeError:
                cdm_entity_type = mapping.get_cdm_entity_type(lookup_key, source_table) or "unknown"
        except Exception:
            continue
        if cdm_entity_type != "unknown":
            return cdm_entity_type
    return "unknown"


def _resolve_field_map(
    mapping: Any,
    tenant_id: str,
    connector_id: str | None,
    source_system: str | None,
    source_table: str,
) -> dict:
    for lookup_key in (connector_id, source_system):
        if not lookup_key:
            continue
        try:
            try:
                field_map = mapping.get_field_map(tenant_id, lookup_key, source_table) or {}
            except TypeError:
                field_map = mapping.get_field_map(lookup_key, source_table) or {}
        except Exception:
            continue
        if field_map:
            return field_map
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

    raw_value = strip_null_like(raw_value)
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
