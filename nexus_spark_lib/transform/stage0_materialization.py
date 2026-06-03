"""Stage 0 — Materialization Gate.

FIRST stage in the pipeline (per NEXUS-Iter2-REF-DataPaths §1.4, SystemOrch §3).

Runs BEFORE Stage 1 (normalise). Two responsibilities:
    1. Resolve cdm_entity_type from connector_id × source_table using the CDM
         mappings broadcast. If the live connector_id differs from the canonical
         source_system stored in cdm_mappings, source_system is used as a fallback
         lookup key. This is a lightweight lookup — no type coercion.
  2. Evaluate the materialization policy against raw payload field values to
     assign hot / warm / cold. Predicates are evaluated against the raw
     after_payload (DELETE → before_payload) values. String coercion is
     sufficient for the predicate types used at this stage (equality, range
     comparisons, AGE checks on date strings).

Pipeline order (per NEXUS-Iter2-REF-DataPaths §1.4–1.5):
    reader.py → stage0_materialization → stage1_normalise

Tier semantics (DataPaths §1.4):
    cold  → record dropped (call drop_cold() after this stage).
    warm  → Stage 1 normalise + Signal A ER only; no synthesis; no M3 writes.
    hot   → full pipeline (Stages 1–3 + M3 projection).

NFR-D4-01: p95 latency ≤ 1ms per record — policy is a Spark broadcast; no I/O.
"""

from __future__ import annotations

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.models.materialization import MaterializationLevel, MaterializationPolicy, Stage0Output
from nexus_spark_lib.observability.metrics import MATERIALIZATION_DECISIONS
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


_GATE_SCHEMA = StructType([
    StructField("cdm_entity_type", StringType(), True),
    StructField("level", StringType(), False),
    StructField("rule_id", StringType(), True),
])


def materialization_gate(
    df: DataFrame,
    cdm_mapping_broadcast: Broadcast,
    policy_broadcast: Broadcast,
) -> DataFrame:
    """Resolve cdm_entity_type and assign materialization level before normalisation.

    This is Stage 0 — it runs on RAW records (no normalised_json available yet).
    Predicates in the policy are evaluated against raw payload field values.
    Basic null-stripping is applied so predicates like 'industry = Healthcare'
    work on uncoerced strings. AGE() predicates use the raw source_ts value.

    Args:
        df:                   Raw-record DataFrame from kafka/reader.py.
                      Required columns: tenant_id, connector_id, source_table,
                      source_op, source_ts, after_payload, before_payload.
                      source_system is used as a fallback lookup key when
                      present on the DataFrame.
        cdm_mapping_broadcast: Broadcast[CdmMappingBroadcast] — resolves
                       (connector_id or source_system, source_table)
                       → cdm_entity_type.
        policy_broadcast:     Broadcast[MaterializationPolicy].

    Returns:
        DataFrame with three new columns added (per Stage0Output contract):
        - cdm_entity_type       (str)        — canonical entity type from CDM mappings
        - materialization_level (str)        — "hot" | "warm" | "cold" (from policy)
        - materialization_rule_id (str|null) — rule_id that fired, None for WARM default

        See: nexus_spark_lib.models.Stage0Output (public contract).
    """

    # Extract plain Python values from Broadcast wrappers BEFORE defining the UDF.
    # pyspark.Broadcast objects (and Prometheus counters) contain _thread.lock
    # and are not picklable — cloudpickle serialises the entire UDF closure.
    _cdm_mapping_val = cdm_mapping_broadcast.value
    _policy_val = policy_broadcast.value

    @F.udf(_GATE_SCHEMA)
    def _gate(
        tenant_id: str,
        connector_id: str,
        source_system: str | None,
        source_table: str,
        source_op: str,
        source_ts,
        after_payload,
        before_payload,
    ):
        # ── 1. Resolve cdm_entity_type from CDM mappings broadcast ──────────
        cdm_entity_type = "unknown"
        for lookup_key in (connector_id, source_system):
            if not lookup_key:
                continue
            try:
                try:
                    cdm_entity_type = _cdm_mapping_val.get_cdm_entity_type(tenant_id, lookup_key, source_table) or "unknown"
                except TypeError:
                    cdm_entity_type = _cdm_mapping_val.get_cdm_entity_type(lookup_key, source_table) or "unknown"
            except Exception:
                continue
            if cdm_entity_type != "unknown":
                break

        # ── 2. Extract raw field values for predicate evaluation ─────────────
        # DELETE uses before_payload; all other ops use after_payload.
        op = (source_op or "INSERT").upper()
        raw_payload: dict = {}
        if op == "DELETE":
            raw_payload = dict(before_payload or {})
        else:
            raw_payload = dict(after_payload or {})

        # Strip obvious null-likes so predicates like 'industry = Healthcare' work
        field_values: dict = {
            k: v
            for k, v in raw_payload.items()
            if v is not None and str(v).strip().lower() not in ("", "null", "none", "n/a", "na")
        }

        # Inject source_ts as a virtual field so AGE(source_ts) predicates work
        if source_ts is not None:
            field_values.setdefault("source_ts", str(source_ts))

        # ── 3. Evaluate materialization policy ───────────────────────────────
        decision = _policy_val.evaluate(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            field_values=field_values,
        )

        # Note: MATERIALIZATION_DECISIONS Prometheus counter is not tracked here
        # (Prometheus counters are not picklable). Metrics are tracked post-UDF.

        return (cdm_entity_type, decision.level.value, decision.applied_rule_id)

    source_system_col = F.col("source_system") if "source_system" in df.columns else F.lit(None)

    return (
        df.withColumn(
            "_gate",
            _gate(
                F.col("tenant_id"),
                F.col("connector_id"),
                source_system_col,
                F.col("source_table"),
                F.col("source_op"),
                F.col("source_ts"),
                F.col("after_payload"),
                F.col("before_payload"),
            ),
        )
        .withColumn("cdm_entity_type", F.col("_gate.cdm_entity_type"))
        .withColumn("materialization_level", F.col("_gate.level"))
        .withColumn("materialization_rule_id", F.col("_gate.rule_id"))
        .drop("_gate")
    )


# Backward-compatible alias
materialization_decide = materialization_gate


def drop_cold(df: DataFrame) -> DataFrame:
    """Remove COLD records from the pipeline immediately after materialization_gate().

    Per NEXUS-Iter2-REF-DataPaths §1.4:
      cold  → dropped; no Stage 1 normalise, no ER, no synthesis, no M3 writes.
      warm  → Stage 1 normalise + Signal A ER only; no synthesis; no M3 writes.
      hot   → full pipeline (Stages 1–3 + M3 projection).

    Must be called before passing the DataFrame to stage1_normalise.normalise().
    """
    return df.filter(F.col("materialization_level") != MaterializationLevel.COLD.value)


def materialization_decide(
    df: DataFrame,
    policy_broadcast: Broadcast,
) -> DataFrame:
    """Backward-compatible alias for materialization_gate (two-argument form).

    DEPRECATED: prefer materialization_gate(df, cdm_mapping_broadcast, policy_broadcast).
    This alias is retained for callers that haven't yet been updated; it passes an empty
    dict as the cdm_mapping_broadcast value so cdm_entity_type must already be on the DataFrame.

    Args:
        df:               Input DataFrame. Must have: tenant_id, cdm_entity_type.
        policy_broadcast: Spark Broadcast[MaterializationPolicy].

    Returns:
        Same DataFrame with two new columns:
        - materialization_level   (str: "hot" | "warm" | "cold")
        - materialization_rule_id (str | null: rule_id that matched, None for default fallback)
    """

    _policy_val = policy_broadcast.value

    @F.udf(_DECIDE_SCHEMA)
    def _decide(tenant_id: str, cdm_entity_type: str, normalised_json: str):
        import json

        # Parse normalised field values so predicates can match (e.g. priority_level=2).
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

        decision = _policy_val.evaluate(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            field_values=field_values,
        )
        # Note: MATERIALIZATION_DECISIONS Prometheus counter not tracked here (not picklable).
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
      cold  → dropped immediately.
      warm  → Stage 1 normalise only; no M3 writes.
      hot   → full pipeline (Stage 1 normalise + M3 projection).

    Must be called immediately after materialization_decide() and before
    passing the DataFrame to stage1_normalise.normalise().
    """
    return df.filter(F.col("materialization_level") != MaterializationLevel.COLD.value)
