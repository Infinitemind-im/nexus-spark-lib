"""Stage 1 — Materialization Tier Evaluation.

Runs AFTER Stage 0 (normalisation). CDM field values (normalised_json) must
be available before this stage so that policy predicates can match against
typed values — e.g. predicate="priority_level=2" requires priority_level to
have been coerced from the raw source string already.

Pipeline order:
    reader.py → stage0_normalise → stage1_materialization → stage2_resolve → stage3_synthesise

Assigns one of: HOT / WARM / COLD.

NFR-D4-01: p95 latency ≤ 1ms per record (evaluated in Spark executor memory;
no network I/O in the critical path — policy is a broadcast variable).

The tier decision is carried in the pipeline payload for downstream routing.
COLD records are still fully transformed and published to Kafka; Stage 1 does
NOT filter them out.
"""

from __future__ import annotations

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.models.materialization import MaterializationPolicy
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
