"""Stage 1 — Materialization Tier Evaluation.

Runs AFTER Stage 0 (normalisation). CDM field values (normalised_json) must
be available before this stage so that policy predicates can match against
typed values — e.g. predicate="AGE(created_at) <= '90 days'" requires
created_at to have been coerced to ISO 8601 by Stage 0 first.

Pipeline order (per NEXUS-Iter2-REF-DataPaths §1.4–1.5):
    reader.py → stage0_normalise → stage1_materialization → stage2_resolve → stage3_synthesise

Assigns one of: HOT / WARM / COLD.

NFR-D4-01: p95 latency ≤ 1ms per record (evaluated in Spark executor memory;
no network I/O in the critical path — policy is a broadcast variable).

Post-decision routing (caller's responsibility):
  - Call drop_cold(df) immediately after materialization_decide() to remove
    COLD records from the pipeline. COLD records must not reach Stage 2 or Stage 3.
  - WARM records proceed to Stage 2 Signal A only (no Signal B/C, no synthesis,
    no M3 writes).
  - HOT records run the full pipeline.
"""

from __future__ import annotations

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.models.materialization import MaterializationLevel, MaterializationPolicy
from nexus_spark_lib.observability.metrics import MATERIALIZATION_DECISIONS
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


_DECIDE_SCHEMA = StructType([
    StructField("level", StringType(), False),
    StructField("rule_id", StringType(), True),
])


def materialization_decide(
    df: DataFrame,
    policy_broadcast: Broadcast,
) -> DataFrame:
    """Add 'materialization_level' and 'materialization_rule_id' columns to df.

    Must be called AFTER stage0_normalise. The normalised_json column produced
    by Stage 1 carries typed CDM field values (e.g. priority_level="2") that
    policy predicates are evaluated against. Without these values, predicates
    like "priority_level=2" can never match and every record falls back to WARM.

    Uses a single struct-returning UDF so policy.evaluate() is called once per
    record (not twice as separate level + rule_id UDFs would require).

    Args:
        df:               Input DataFrame. Must have: tenant_id, cdm_entity_type,
                          normalised_json (produced by stage0_normalise).
        policy_broadcast: Spark Broadcast[MaterializationPolicy].

    Returns:
        Same DataFrame with two new columns:
        - materialization_level   (str: "hot" | "warm" | "cold")
        - materialization_rule_id (str | null: rule_id that matched, None for default fallback)
    """

    @F.udf(_DECIDE_SCHEMA)
    def _decide(tenant_id: str, cdm_entity_type: str, normalised_json: str):
        import json
        policy: MaterializationPolicy = policy_broadcast.value

        # Parse normalised field values so predicates can match (e.g. priority_level=2).
        # normalised_json maps cdm_field → {"value": ..., "quality": ...}
        field_values: dict = {}
        if normalised_json:
            try:
                raw = json.loads(normalised_json)
                field_values = {
                    k: v.get("value")
                    for k, v in raw.items()
                    if isinstance(v, dict) and "value" in v
                }
            except (ValueError, AttributeError):
                pass  # malformed JSON → empty field_values → WARM fallback

        decision = policy.evaluate(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            field_values=field_values,
        )
        MATERIALIZATION_DECISIONS.labels(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            level=decision.level.value,
        ).inc()
        return (decision.level.value, decision.applied_rule_id)

    return (
        df.withColumn(
            "_mat",
            _decide(F.col("tenant_id"), F.col("cdm_entity_type"), F.col("normalised_json")),
        )
        .withColumn("materialization_level", F.col("_mat.level"))
        .withColumn("materialization_rule_id", F.col("_mat.rule_id"))
        .drop("_mat")
    )


def drop_cold(df: DataFrame) -> DataFrame:
    """Remove COLD records from the pipeline after materialization_decide().

    Per NEXUS-Iter2-REF-DataPaths §1.4:
      cold  → dropped immediately; no ER, no synthesis, no M3 writes.
      warm  → Stage 0 normalise + Signal A ER only; no synthesis; no M3 writes.
      hot   → full pipeline (Stages 1–3 + M3 projection).

    Must be called immediately after materialization_decide() and before
    passing the DataFrame to stage2_resolve.resolve().
    """
    return df.filter(F.col("materialization_level") != MaterializationLevel.COLD.value)
