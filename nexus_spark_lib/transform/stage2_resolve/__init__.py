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

from typing import Any

from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from nexus_spark_lib.observability.metrics import (
    ER_FAST_PATH_HITS,
    ER_LATENCY,
    ER_RECORDS,
    ER_SIGNAL_SCORES,
)
from nexus_spark_lib.observability.structured_log import get_stage_logger
from nexus_spark_lib.transform.stage2_resolve.id_generator import generate_cdm_entity_id
from nexus_spark_lib.transform.stage2_resolve.signals.signal_a_deterministic import run_signal_a
from nexus_spark_lib.transform.stage2_resolve.signals.signal_b_probabilistic import run_signal_b
from nexus_spark_lib.transform.stage2_resolve.signals.signal_c_graph import run_signal_c
from nexus_spark_lib.transform.stage2_resolve.state_machine import GoldenRecordStateMachine

logger = get_stage_logger(__name__)


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
        DataFrame with 'cdm_entity_id' and 'er_resolution_method' columns added.
    """
    # Capture broadcasts for UDF closures
    def _resolve_udf(
        tenant_id: str,
        cdm_entity_type: str,
        source_system: str,
        source_record_id: str,
        normalised_json: str,
        materialization_level: str,
    ) -> str:
        import json
        import time

        t0 = time.perf_counter()

        # --- FR-Dev3-M-03: materialization_level short-circuit ---
        # COLD records skip ER entirely — no I/O, just generate a fresh ID.
        mat_level = (materialization_level or "warm").lower()
        if mat_level == "cold":
            ER_RECORDS.labels(tenant_id=tenant_id, status="cold_skip").inc()
            return generate_cdm_entity_id(
                tenant_id, cdm_entity_type, f"{cdm_entity_type}|{source_record_id}"
            )

        er_index = er_index_broadcast.value
        blocking_key = f"{cdm_entity_type}|{source_record_id}"

        # --- Fast path: already indexed ---
        lookup_key = f"{tenant_id}|{source_system}|{source_record_id}"
        known = er_index.snapshot.get(lookup_key)
        if known:
            ER_FAST_PATH_HITS.labels(tenant_id=tenant_id).inc()
            ER_RECORDS.labels(tenant_id=tenant_id, status="fast_path").inc()
            return known

        # --- 3-signal resolution ---
        fields = json.loads(normalised_json or "{}")

        # Signal A — deterministic (WARM and HOT both run Signal A)
        result_a = run_signal_a(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            fields=fields,
            er_index=er_index,
        )
        if result_a:
            ER_SIGNAL_SCORES.labels(signal="A", tenant_id=tenant_id).observe(1.0)
            ER_RECORDS.labels(tenant_id=tenant_id, status="signal_a").inc()
            return result_a

        # --- FR-Dev3-M-03: WARM runs Signal A only ---
        # No Signal A match — WARM skips probabilistic signals, generates new ID.
        if mat_level == "warm":
            ER_RECORDS.labels(tenant_id=tenant_id, status="warm_new").inc()
            return generate_cdm_entity_id(tenant_id, cdm_entity_type, blocking_key)

        # HOT — Signal B and C
        score_b, candidate_b = run_signal_b(
            tenant_id=tenant_id,
            cdm_entity_type=cdm_entity_type,
            fields=fields,
            er_index=er_index,
        )
        ER_SIGNAL_SCORES.labels(signal="B", tenant_id=tenant_id).observe(score_b or 0.0)

        # Signal C — graph lift (optional)
        score_c_lift = 0.0
        if neo4j_driver and candidate_b:
            score_c_lift = run_signal_c(
                driver=neo4j_driver,
                cdm_entity_id=candidate_b,
                tenant_id=tenant_id,
            )
            ER_SIGNAL_SCORES.labels(signal="C", tenant_id=tenant_id).observe(score_c_lift)

        final_score = (score_b or 0.0) + score_c_lift

        # Threshold logic (using defaults if not in broadcast)
        auto_apply = 0.95
        review_lower = 0.70

        if final_score >= auto_apply and candidate_b:
            ER_RECORDS.labels(tenant_id=tenant_id, status="signal_b_merge").inc()
            return candidate_b

        if review_lower <= final_score < auto_apply and candidate_b:
            # Needs human review — assign new ID for now, queue review in foreachBatch
            ER_RECORDS.labels(tenant_id=tenant_id, status="review_queued").inc()
            return generate_cdm_entity_id(tenant_id, cdm_entity_type, blocking_key)

        # No match — generate a new Golden Record ID
        ER_RECORDS.labels(tenant_id=tenant_id, status="new").inc()
        return generate_cdm_entity_id(tenant_id, cdm_entity_type, blocking_key)

    resolve_udf = F.udf(_resolve_udf, StringType())

    return df.withColumn(
        "cdm_entity_id",
        resolve_udf(
            F.col("tenant_id"),
            F.col("cdm_entity_type"),
            F.col("source_system"),
            F.col("source_record_id"),
            F.col("normalised_json"),
            F.col("materialization_level"),
        ),
    )
