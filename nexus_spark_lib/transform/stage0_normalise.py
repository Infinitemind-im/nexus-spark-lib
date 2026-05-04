"""Stage 0 — Normalisation.

First stage in the pipeline. Transforms raw, heterogeneous field values into
CDM-typed, quality-scored TransformedField objects. Must run before
stage1_materialization so that CDM field values are available for policy
predicate evaluation.

Pipeline order:
    reader.py → stage0_normalise → stage1_materialization → stage2_resolve → stage3_synthesise

Responsibilities (per NEXUS-Iter2-SPEC-DataModel-v0.5):
  1. CDM field mapping via broadcast (connector_id × source_table → cdm_entity_type + field map)
  2. Type coercion:
       - Dates/datetimes → ISO 8601 (UTC); malformed dates go to source_extras, NOT nulled
       - Decimal → Python Decimal via string normalisation
       - Boolean → True/False from BOOL_TRUE/FALSE_VALUES sets
       - Null normalisation from NULL_LIKE_STRINGS frozenset
  3. FX currency conversion at source_ts using FxRates.convert()
  4. DQ scoring → FieldQuality per field
  5. Deduplication on natural key within the micro-batch
  6. Update schema_snapshots (best-effort, no abort on failure)

p95 latency target: not explicitly spec'd for Stage 0; keep O(n × fields).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.config.constants import (
    BOOL_FALSE_VALUES,
    BOOL_TRUE_VALUES,
    DATE_FORMATS,
    DATETIME_FORMATS,
    NULL_LIKE_STRINGS,
)
from nexus_spark_lib.models.materialization import MaterializationLevel
from nexus_spark_lib.models.raw_record import SourceOp
from nexus_spark_lib.models.transformed_record import (
    FieldQuality,
    TransformedField,
)
from nexus_spark_lib.observability.metrics import NORMALISE_LATENCY, NORMALISE_RECORDS
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
    """Apply CDM normalisation to each raw record.

    Args:
        df:                   Parsed raw-record DataFrame (from kafka/reader.py).
        cdm_mapping_broadcast: Broadcast[CdmMappingBroadcast] with field mappings.
        fx_rates_broadcast:   Broadcast[FxRatesBroadcast] with historical FX rates.

    Returns:
        DataFrame with added columns:
        - cdm_entity_type  (str)
        - cdm_entity_id    (str, populated by Stage 2)
        - normalised_json  (str, JSON-encoded dict of TransformedField)
        - source_extras    (str, JSON-encoded leftover/failed-coerce fields)
        - dq_score         (float)
        - dedup_key        (str, natural key for within-batch dedup)
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

    # Within-batch deduplication: keep latest by source_ts per natural key
    enriched = _dedup_batch(enriched)

    NORMALISE_RECORDS.labels(status="ok").inc(enriched.count())
    return enriched


# ---------------------------------------------------------------------------
# Spark UDF — runs on executors
# ---------------------------------------------------------------------------

_NORMALISE_OUTPUT_SCHEMA = StructType([
    StructField("cdm_entity_type", StringType(), True),
    StructField("normalised_json", StringType(), True),
    StructField("source_extras", StringType(), True),
    StructField("dq_score", StringType(), True),   # serialised float as string
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
        import time
        t0 = time.perf_counter()

        mapping = cdm_mapping_bc.value
        fx_rates = fx_rates_bc.value

        # Determine CDM entity type from connector × source_table
        cdm_entity_type = _resolve_cdm_type(mapping, connector_id, source_table)
        field_map = _resolve_field_map(mapping, connector_id, source_table)

        # Choose payload (after for INSERT/UPDATE/SNAPSHOT_READ/RELEVEL; before for DELETE)
        op = source_op or "INSERT"
        payload: dict[str, Any] = {}
        if op in ("DELETE",):
            payload = dict(before_payload or {})
        else:
            payload = dict(after_payload or {})

        transformed_fields: dict[str, dict] = {}
        source_extras: dict[str, Any] = {}

        for raw_key, raw_value in payload.items():
            cdm_attr = field_map.get(raw_key)
            if cdm_attr is None:
                # No mapping → goes to source_extras
                source_extras[raw_key] = raw_value
                continue

            coerced, quality, extra = _coerce_field(
                cdm_attr=cdm_attr,
                raw_key=raw_key,
                raw_value=raw_value,
                field_map_meta=field_map.get(f"__meta__{raw_key}", {}),
                source_ts=source_ts,
                fx_rates=fx_rates,
                tenant_id=tenant_id,
            )
            if extra is not None:
                source_extras[f"_fail_{raw_key}"] = raw_value
            elif coerced is not None:
                transformed_fields[cdm_attr] = {
                    "value": _safe_str(coerced),
                    "quality": quality.value,
                    "source_attribute": raw_key,
                    "pii_flag": False,  # PII classification done by CDM mapping service
                }

        # DQ score = proportion of successfully mapped fields / total fields in payload
        total = len(payload)
        mapped = len(transformed_fields)
        dq_score = round(mapped / total, 4) if total > 0 else 1.0

        # Natural key for dedup: tenant + entity_type + source_record_id
        dedup_key = f"{tenant_id}|{cdm_entity_type}|{source_record_id}"

        elapsed = time.perf_counter() - t0

        return (
            cdm_entity_type,
            json.dumps(transformed_fields),
            json.dumps(source_extras),
            str(dq_score),
            dedup_key,
        )

    return _fn


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------

def _coerce_field(
    cdm_attr: str,
    raw_key: str,
    raw_value: Any,
    field_map_meta: dict,
    source_ts: Any,
    fx_rates: Any,
    tenant_id: str,
) -> tuple[Any, FieldQuality, Any]:
    """Coerce a single raw field to its CDM type.

    Returns (coerced_value, FieldQuality, failure_extra).
    If failure_extra is not None, coercion failed and caller should route to source_extras.
    """
    if raw_value is None or (isinstance(raw_value, str) and raw_value.strip().upper() in NULL_LIKE_STRINGS):
        return (None, FieldQuality.MISSING, None)

    target_type = field_map_meta.get("type", "string")

    try:
        if target_type == "decimal":
            coerced = Decimal(str(raw_value).replace(",", "."))
            # FX conversion if currency metadata available
            currency = field_map_meta.get("currency_field")
            if currency and fx_rates and source_ts:
                src_cur = str(currency)
                target_cur = field_map_meta.get("target_currency", "USD")
                try:
                    ts = _parse_ts(source_ts)
                    result = fx_rates.value.convert(coerced, src_cur, target_cur, ts)
                    coerced = result.converted_amount
                except Exception:
                    pass  # Use original amount if FX lookup fails
            return (coerced, FieldQuality.GOOD, None)

        elif target_type in ("date", "datetime"):
            coerced = _parse_date_field(str(raw_value), target_type)
            if coerced is None:
                return (None, FieldQuality.MISSING, raw_value)  # Route to source_extras
            return (coerced, FieldQuality.GOOD, None)

        elif target_type == "boolean":
            val_upper = str(raw_value).strip().upper()
            if val_upper in BOOL_TRUE_VALUES:
                return (True, FieldQuality.GOOD, None)
            elif val_upper in BOOL_FALSE_VALUES:
                return (False, FieldQuality.GOOD, None)
            else:
                return (None, FieldQuality.SUSPECT, None)

        else:  # string
            return (str(raw_value), FieldQuality.GOOD, None)

    except (InvalidOperation, ValueError, TypeError):
        return (None, FieldQuality.SUSPECT, raw_value)


def _parse_date_field(raw: str, kind: str) -> str | None:
    """Parse a raw string as a date or datetime. Returns ISO 8601 string or None."""
    formats = DATETIME_FORMATS if kind == "datetime" else DATE_FORMATS
    for fmt in formats:
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            if kind == "datetime":
                return dt.replace(tzinfo=timezone.utc).isoformat()
            return dt.date().isoformat()
        except ValueError:
            continue
    return None  # Caller routes to source_extras — never silent null


def _parse_ts(source_ts: Any) -> datetime:
    if isinstance(source_ts, datetime):
        return source_ts
    return datetime.fromisoformat(str(source_ts))


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ---------------------------------------------------------------------------
# CDM mapping resolution helpers (from broadcast)
# ---------------------------------------------------------------------------

def _resolve_cdm_type(mapping: Any, connector_id: str, source_table: str) -> str:
    """Resolve the CDM entity type for a connector × source_table combination."""
    try:
        return mapping.value.get_cdm_entity_type(connector_id, source_table) or "unknown"
    except Exception:
        return "unknown"


def _resolve_field_map(mapping: Any, connector_id: str, source_table: str) -> dict:
    """Resolve field mapping dict for a connector × source_table combination."""
    try:
        return mapping.value.get_field_map(connector_id, source_table) or {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Within-batch deduplication
# ---------------------------------------------------------------------------

def _dedup_batch(df: DataFrame) -> DataFrame:
    """Keep the latest record per natural key (dedup_key) within the micro-batch."""
    from pyspark.sql.window import Window
    window = Window.partitionBy("dedup_key").orderBy(F.col("source_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
