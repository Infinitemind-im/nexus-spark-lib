"""Stage 2 — Entity Resolution (ER).

The resolve() function is the most complex stage. It assigns a stable
cdm_entity_id to every incoming record using a 3-signal algorithm:

  Signal A — Deterministic exact match on deterministic_id_columns
  Signal B — Probabilistic (LSH + Jaro-Winkler / Levenshtein / Soundex / Metaphone)
  Signal C — Neo4j 2-hop graph lift (+0.05 depth1, +0.02 depth2, capped +0.10)

Fast path: if the source record is already indexed, skip the 3-signal pipeline
and return the known cdm_entity_id directly (ER index lookup, p95 ≤ 10ms).

NFR-D3-01: p95 ≤ 5s per record including all signals.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from nexus_spark_lib._internal.hash_utils import (
    er_legacy_source_lookup_key,
    er_source_lookup_key,
)
from nexus_spark_lib.models.er_types import ResolutionMethod
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib.transform.stage2_resolve.id_generator import generate_cdm_entity_id
from nexus_spark_lib.transform.stage2_resolve.signals.signal_a_deterministic import run_signal_a
from nexus_spark_lib.transform.stage2_resolve.signals.signal_b_probabilistic import _DEFAULT_WEIGHTS, run_signal_b
from nexus_spark_lib.transform.stage2_resolve.signals.signal_c_graph import run_signal_c
logger = get_stage_logger(__name__)

_NEO4J_EXECUTOR_DRIVER: Any | None = None
_NEO4J_EXECUTOR_DRIVER_KEY: tuple[str, str, str, int, float] | None = None
_DEFAULT_AUTO_APPLY_THRESHOLD = 0.92
_DEFAULT_REVIEW_LOWER_BOUND = 0.75


def resolve(
    df: DataFrame,
    er_index_broadcast: Broadcast,
    neo4j_driver: Any | None = None,
    mode: str = "streaming",
) -> DataFrame:
    """Assign or resolve cdm_entity_id for each record.

    Args:
        df:                 Normalised DataFrame (output of stage1).
        er_index_broadcast: Broadcast[ErIndexBroadcast] — full snapshot of ER index.
        neo4j_driver:       Neo4j driver instance (None disables Signal C).
        mode:               "streaming" | "backfill"

    Returns:
        DataFrame with Stage 2 result columns added:
        - cdm_entity_id
        - er_resolution_method
        - er_confidence
        - is_provisional
        - er_review_candidate_id
        - er_signal_b_score
        - er_signal_c_lift
    """
    if "changed_canonical_attributes_json" not in df.columns:
        df = df.withColumn("changed_canonical_attributes_json", F.lit("[]"))
    if "blocking_key" not in df.columns:
        df = df.withColumn(
            "blocking_key",
            F.concat_ws("|", F.col("cdm_entity_type"), F.col("source_record_id")),
        )

    result_schema = StructType([
        StructField("cdm_entity_id", StringType(), False),
        StructField("er_resolution_method", StringType(), False),
        StructField("er_confidence", DoubleType(), False),
        StructField("is_provisional", BooleanType(), False),
        StructField("er_review_candidate_id", StringType(), True),
        StructField("er_signal_b_score", DoubleType(), False),
        StructField("er_signal_c_lift", DoubleType(), False),
    ])

    def _resolve_udf(
        tenant_id: str,
        cdm_entity_type: str,
        connector_id: str,
        source_system: str,
        source_table: str,
        source_record_id: str,
        source_op: str,
        normalised_json: str,
        changed_canonical_attributes_json: str,
        materialization_level: str,
        blocking_key: str,
    ) -> str:
        import json
        from nexus_spark_lib.observability.metrics import (
            ER_FAST_PATH_HITS,
            ER_RECORDS,
            ER_SIGNAL_SCORES,
        )

        def _result(
            cdm_entity_id: str,
            resolution_method: str,
            confidence: float,
            *,
            is_provisional: bool = False,
            review_candidate_id: str | None = None,
            signal_b_score: float = 0.0,
            signal_c_lift: float = 0.0,
        ) -> dict[str, object]:
            return {
                "cdm_entity_id": cdm_entity_id,
                "er_resolution_method": resolution_method,
                "er_confidence": float(confidence),
                "is_provisional": bool(is_provisional),
                "er_review_candidate_id": review_candidate_id,
                "er_signal_b_score": float(signal_b_score),
                "er_signal_c_lift": float(signal_c_lift),
            }

        mat_level = (materialization_level or "warm").lower()
        resolved_blocking_key = str(blocking_key or "").strip() or f"{cdm_entity_type}|{source_record_id}"
        if mat_level == "cold":
            generated_id = generate_cdm_entity_id(
                tenant_id, cdm_entity_type, resolved_blocking_key
            )
            ER_RECORDS.labels(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                resolution_method="cold_skip",
                status="cold_skip",
            ).inc()
            return _result(
                generated_id,
                "cold_skip",
                1.0,
            )

        er_index = er_index_broadcast.value
        should_rerun_update = _update_requires_reresolution(
            er_index=er_index,
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            source_op=source_op,
            changed_canonical_attributes_json=changed_canonical_attributes_json,
        )

        lookup_keys = [
            er_source_lookup_key(
                tenant_id,
                connector_id or source_system or "",
                source_table or "",
                source_record_id,
            ),
        ]
        if source_system:
            lookup_keys.append(
                er_legacy_source_lookup_key(
                    tenant_id,
                    source_system,
                    source_record_id,
                )
            )

        if not should_rerun_update:
            for lookup_key in lookup_keys:
                known = er_index.snapshot.get(lookup_key)
                if known:
                    ER_FAST_PATH_HITS.labels(tenant_id=tenant_id).inc()
                    ER_RECORDS.labels(
                        tenant_id=tenant_id,
                        cdm_entity_type=cdm_entity_type,
                        resolution_method="fast_path",
                        status="fast_path",
                    ).inc()
                    return _result(known, "fast_path", 1.0)

        fields = json.loads(normalised_json or "{}")

        result_a = run_signal_a(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            fields=fields,
            er_index=er_index,
        )
        if result_a:
            ER_SIGNAL_SCORES.labels(tenant_id=tenant_id, cdm_entity_type=cdm_entity_type).observe(1.0)
            ER_RECORDS.labels(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                resolution_method="deterministic",
                status="signal_a",
            ).inc()
            return _result(result_a, ResolutionMethod.DETERMINISTIC.value, 1.0)

        if mat_level == "warm":
            generated_id = generate_cdm_entity_id(tenant_id, cdm_entity_type, blocking_key)
            ER_RECORDS.labels(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                resolution_method="warm_new",
                status="warm_new",
            ).inc()
            return _result(generated_id, "warm_new", 1.0)

        score_b, candidate_b = run_signal_b(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            fields=fields,
            er_index=er_index,
        )
        ER_SIGNAL_SCORES.labels(tenant_id=tenant_id, cdm_entity_type=cdm_entity_type).observe(score_b or 0.0)

        score_c_lift = 0.0
        signal_c_driver = _get_signal_c_driver(neo4j_driver)
        if signal_c_driver and candidate_b:
            score_c_lift = run_signal_c(
                driver=signal_c_driver,
                cdm_entity_id=candidate_b,
                tenant_id=tenant_id,
            )
            ER_SIGNAL_SCORES.labels(tenant_id=tenant_id, cdm_entity_type=cdm_entity_type).observe(score_c_lift)

        final_score = (score_b or 0.0) + score_c_lift

        threshold_config = _get_threshold_config(er_index, tenant_id, cdm_entity_type)
        auto_apply = threshold_config["auto_apply_threshold"]
        review_lower = threshold_config["review_lower_bound"]

        if final_score >= auto_apply and candidate_b:
            resolved_method = (
                ResolutionMethod.GRAPH.value
                if score_c_lift > 0.0
                else ResolutionMethod.PROBABILISTIC.value
            )
            ER_RECORDS.labels(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                resolution_method="graph" if score_c_lift > 0.0 else "probabilistic",
                status="signal_b_merge",
            ).inc()
            return _result(
                candidate_b,
                resolved_method,
                final_score,
                signal_b_score=score_b or 0.0,
                signal_c_lift=score_c_lift,
            )

        if review_lower <= final_score < auto_apply and candidate_b:
            generated_id = generate_cdm_entity_id(tenant_id, cdm_entity_type, resolved_blocking_key)
            ER_RECORDS.labels(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                resolution_method="review_band",
                status="review_queued",
            ).inc()
            return _result(
                generated_id,
                "review_band",
                final_score,
                is_provisional=True,
                review_candidate_id=candidate_b,
                signal_b_score=score_b or 0.0,
                signal_c_lift=score_c_lift,
            )

        generated_id = generate_cdm_entity_id(tenant_id, cdm_entity_type, resolved_blocking_key)
        ER_RECORDS.labels(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            resolution_method="new_entity",
            status="new",
        ).inc()
        return _result(
            generated_id,
            "new_entity",
            1.0,
            signal_b_score=score_b or 0.0,
            signal_c_lift=score_c_lift,
        )

    resolve_udf = F.udf(_resolve_udf, result_schema)

    result = df.withColumn(
        "_er_result",
        resolve_udf(
            F.col("tenant_id"),
            F.col("cdm_entity_type"),
            F.col("connector_id"),
            F.col("source_system"),
            F.col("source_table"),
            F.col("source_record_id"),
            F.col("source_op"),
            F.col("normalised_json"),
            F.col("changed_canonical_attributes_json"),
            F.col("materialization_level"),
            F.col("blocking_key"),
        ),
    )

    return (
        result
        .withColumn("cdm_entity_id", F.col("_er_result.cdm_entity_id"))
        .withColumn("er_resolution_method", F.col("_er_result.er_resolution_method"))
        .withColumn("er_confidence", F.col("_er_result.er_confidence"))
        .withColumn("is_provisional", F.col("_er_result.is_provisional"))
        .withColumn("er_review_candidate_id", F.col("_er_result.er_review_candidate_id"))
        .withColumn("er_signal_b_score", F.col("_er_result.er_signal_b_score"))
        .withColumn("er_signal_c_lift", F.col("_er_result.er_signal_c_lift"))
        .drop("_er_result")
    )


def _update_requires_reresolution(
    *,
    er_index: Any,
    tenant_id: str,
    cdm_entity_type: str,
    source_op: str,
    changed_canonical_attributes_json: str,
) -> bool:
    if str(source_op or "").upper() != "UPDATE":
        return False

    try:
        changed_attributes = json.loads(changed_canonical_attributes_json or "[]")
    except Exception:
        return False

    if not isinstance(changed_attributes, list) or not changed_attributes:
        return False

    deterministic_columns = _get_deterministic_columns(er_index, tenant_id, cdm_entity_type)
    similarity_weights = _get_similarity_weights(er_index, tenant_id, cdm_entity_type)
    relevant_weighted_attributes = [
        attr_name
        for attr_name, weight in similarity_weights.items()
        if _safe_float(weight) > 0.0
    ]

    for attr_name in changed_attributes:
        attr = str(attr_name or "")
        if _matches_any_attribute(attr, deterministic_columns):
            return True
        if _matches_any_attribute(attr, relevant_weighted_attributes):
            return True

    return False


def _get_deterministic_columns(er_index: Any, tenant_id: str, cdm_entity_type: str) -> list[str]:
    try:
        return list(er_index.deterministic_columns.get((tenant_id, cdm_entity_type), []))
    except Exception:
        return []


def _get_similarity_weights(er_index: Any, tenant_id: str, cdm_entity_type: str) -> dict[str, float]:
    try:
        config = er_index.thresholds.get((tenant_id, cdm_entity_type), {})
    except Exception:
        config = {}
    weights = config.get("weights") if isinstance(config, dict) else None
    if isinstance(weights, dict) and weights:
        return weights
    return dict(_DEFAULT_WEIGHTS)


def _get_threshold_config(er_index: Any, tenant_id: str, cdm_entity_type: str) -> dict[str, float]:
    try:
        config = er_index.thresholds.get((tenant_id, cdm_entity_type), {})
    except Exception:
        config = {}

    auto_apply = _safe_float(config.get("auto_apply_threshold"))
    review_lower = _safe_float(config.get("review_lower_bound"))

    if auto_apply <= 0.0:
        auto_apply = _DEFAULT_AUTO_APPLY_THRESHOLD
    if review_lower <= 0.0:
        review_lower = _DEFAULT_REVIEW_LOWER_BOUND

    return {
        "auto_apply_threshold": auto_apply,
        "review_lower_bound": review_lower,
    }


def _matches_any_attribute(attribute_name: str, configured_attributes: list[str]) -> bool:
    normalised = attribute_name.lower()
    for configured in configured_attributes:
        configured_name = str(configured or "").lower()
        if not configured_name:
            continue
        if normalised == configured_name:
            return True
        if normalised.endswith(f".{configured_name}"):
            return True
        if configured_name.endswith(f".{normalised}"):
            return True
    return False


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _get_signal_c_driver(neo4j_driver: Any | None) -> Any | None:
    global _NEO4J_EXECUTOR_DRIVER, _NEO4J_EXECUTOR_DRIVER_KEY

    if neo4j_driver is None:
        return None

    if hasattr(neo4j_driver, "session"):
        return neo4j_driver

    if not isinstance(neo4j_driver, dict):
        return None

    uri = str(neo4j_driver.get("uri") or "").strip()
    user = str(neo4j_driver.get("user") or "neo4j")
    password = str(neo4j_driver.get("password") or "")
    if not uri or not password:
        return None

    max_pool_size = int(neo4j_driver.get("max_connection_pool_size") or 5)
    timeout = float(neo4j_driver.get("connection_timeout_seconds") or 10.0)
    driver_key = (uri, user, password, max_pool_size, timeout)

    if _NEO4J_EXECUTOR_DRIVER is not None and _NEO4J_EXECUTOR_DRIVER_KEY == driver_key:
        return _NEO4J_EXECUTOR_DRIVER

    try:
        from neo4j import GraphDatabase
    except ImportError:
        logger.warning("Signal C requested but neo4j dependency is unavailable")
        return None

    if _NEO4J_EXECUTOR_DRIVER is not None:
        try:
            _NEO4J_EXECUTOR_DRIVER.close()
        except Exception:
            pass

    _NEO4J_EXECUTOR_DRIVER = GraphDatabase.driver(
        uri,
        auth=(user, password),
        max_connection_pool_size=max_pool_size,
        connection_timeout=timeout,
    )
    _NEO4J_EXECUTOR_DRIVER_KEY = driver_key
    return _NEO4J_EXECUTOR_DRIVER
