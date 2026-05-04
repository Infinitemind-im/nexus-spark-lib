"""Stage 3 — Golden Record Synthesis.

Applies survivorship rules to produce a single canonical Golden Record
from all contributing sources.

Survivorship rules (per NEXUS-Iter2-SVC-nexus-cdm-mapper-v0.3):
  MOST_RECENT          — field from source with latest source_ts
  HIGHEST_CONFIDENCE   — field with highest DQ score
  SOURCE_PRIORITY      — source_system in priority_sources list wins
  MOST_COMPLETE        — source with fewest null fields wins
  LONGEST_VALUE        — longest non-null string value wins
  EXACT_MATCH          — only include if all sources agree exactly

NFR-D3-05: Synthesis is deterministic — same contributing sources, same
output, regardless of processing order.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet, SurvivorshipRuleType
from nexus_spark_lib.observability.metrics import SYNTHESIS_LATENCY, SYNTHESIS_RECORDS
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib._internal.hash_utils import provenance_hash

logger = get_stage_logger(__name__)


def synthesise(
    df: DataFrame,
    survivorship_broadcast: Broadcast,
) -> DataFrame:
    """Apply survivorship rules and produce the Golden Record canonical values.

    Args:
        df:                   Enriched DataFrame with cdm_entity_id from Stage 2.
        survivorship_broadcast: Broadcast[SurvivorshipBroadcast].

    Returns:
        DataFrame with added columns:
        - golden_fields_json    (str, JSON: attribute_name → canonical value)
        - provenance_hash       (str, SHA-256/128 hex)
    """
    synthesise_udf = F.udf(
        _synthesise_row(survivorship_broadcast),
        StringType(),
    )
    hash_udf = F.udf(
        _compute_provenance_hash(survivorship_broadcast),
        StringType(),
    )

    result = df.withColumn(
        "golden_fields_json",
        synthesise_udf(
            F.col("tenant_id"),
            F.col("cdm_entity_type"),
            F.col("cdm_entity_id"),
            F.col("normalised_json"),
            F.col("source_system"),
            F.col("source_record_id"),
            F.col("source_ts"),
            F.col("dq_score"),
        ),
    ).withColumn(
        "provenance_hash",
        hash_udf(F.col("cdm_entity_id"), F.col("golden_fields_json")),
    )

    SYNTHESIS_RECORDS.labels(status="ok").inc()
    return result


def _synthesise_row(survivorship_bc: Broadcast):
    """UDF closure: apply survivorship rules to a single record."""

    def _fn(
        tenant_id: str,
        cdm_entity_type: str,
        cdm_entity_id: str,
        normalised_json: str,
        source_system: str,
        source_record_id: str,
        source_ts: Any,
        dq_score: str,
    ) -> str:
        import time
        t0 = time.perf_counter()

        ruleset: SurvivorshipRuleSet = survivorship_bc.value
        fields: dict[str, Any] = json.loads(normalised_json or "{}")

        golden: dict[str, Any] = {}
        for attr, field_val in fields.items():
            rule = ruleset.get_rule(tenant_id, cdm_entity_type, attr)
            value = field_val.get("value") if isinstance(field_val, dict) else field_val
            raw_dq = float(dq_score or "1.0")

            canonical = _apply_rule(
                rule_type=rule.rule_type if rule else SurvivorshipRuleType.MOST_RECENT,
                candidate_value=value,
                candidate_ts=str(source_ts or ""),
                candidate_system=source_system,
                candidate_dq=raw_dq,
                priority_sources=rule.priority_sources if rule else [],
                existing=golden.get(attr),
            )
            if canonical is not None:
                golden[attr] = canonical

        return json.dumps(golden, default=str)

    return _fn


def _compute_provenance_hash(survivorship_bc: Broadcast):
    """UDF: compute deterministic provenance hash from canonical fields."""

    def _fn(cdm_entity_id: str, golden_fields_json: str) -> str:
        summary = f"{cdm_entity_id}:{golden_fields_json}"
        return provenance_hash(summary)

    return _fn


# ---------------------------------------------------------------------------
# Survivorship rule application
# ---------------------------------------------------------------------------

def _apply_rule(
    rule_type: SurvivorshipRuleType,
    candidate_value: Any,
    candidate_ts: str,
    candidate_system: str,
    candidate_dq: float,
    priority_sources: list[str],
    existing: Any,
) -> Any:
    """Apply one survivorship rule to decide between existing and candidate values.

    In streaming mode, each record is processed independently. For multi-source
    survivorship (e.g. MOST_COMPLETE), full re-evaluation happens in foreachBatch
    via Stage 3 re-synthesis using get_all_provenance().
    """
    if candidate_value is None:
        return existing

    if existing is None:
        return candidate_value

    if rule_type == SurvivorshipRuleType.MOST_RECENT:
        # Caller must pass source_ts; choose whichever is more recent
        # In UDF we can only compare within one record at a time; return candidate
        # (full multi-source comparison happens in foreachBatch re-synthesis)
        return candidate_value

    elif rule_type == SurvivorshipRuleType.SOURCE_PRIORITY:
        # If candidate comes from a priority source, it wins
        if candidate_system in priority_sources:
            return candidate_value
        return existing

    elif rule_type == SurvivorshipRuleType.HIGHEST_CONFIDENCE:
        return candidate_value  # DQ scoring re-evaluated in foreachBatch

    elif rule_type == SurvivorshipRuleType.LONGEST_VALUE:
        return candidate_value if len(str(candidate_value)) >= len(str(existing)) else existing

    elif rule_type == SurvivorshipRuleType.EXACT_MATCH:
        return candidate_value if candidate_value == existing else None

    # Default: MOST_RECENT
    return candidate_value
