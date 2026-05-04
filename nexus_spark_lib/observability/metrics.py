"""Prometheus metrics for all pipeline stages.

One metrics registry per Spark executor process. Labels follow the pattern:
    stage, tenant_id, operation_status (ok|error|dead_letter)

NFR-ST-02: /metrics endpoint is served by the Spark PrometheusServlet.
These counters and histograms are pushed to the Spark metrics system via the
JMX sink and scraped by the PrometheusServlet configured in spark_config.py.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ── Stage 0 — Materialization gate ───────────────────────────────────────────

MATERIALIZATION_DECISIONS = Counter(
    "nexus_spark_materialization_decisions_total",
    "Records evaluated by Stage 0 materialization policy",
    ["tenant_id", "cdm_entity_type", "level", "rule_type"],
)

MATERIALIZATION_POLICY_CACHE_AGE = Gauge(
    "nexus_spark_materialization_policy_cache_age_seconds",
    "Seconds since the materialization policy broadcast was last refreshed",
)

# ── Stage 1 — Normalisation ───────────────────────────────────────────────────

NORMALISE_RECORDS = Counter(
    "nexus_spark_normalise_records_total",
    "Records processed by Stage 1 normalisation",
    ["tenant_id", "status"],  # status: ok | coercion_error | dead_letter
)

FX_CONVERSIONS = Counter(
    "nexus_spark_fx_conversions_total",
    "FX conversions performed during normalisation",
    ["from_currency", "to_currency", "approximate"],
)

NORMALISE_LATENCY = Histogram(
    "nexus_spark_normalise_latency_seconds",
    "Stage 1 normalisation latency per record",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# ── Stage 2 — Entity Resolution ───────────────────────────────────────────────

ER_RECORDS = Counter(
    "nexus_spark_er_records_total",
    "Records processed by Stage 2 entity resolution",
    ["tenant_id", "cdm_entity_type", "resolution_method", "status"],
)

ER_FAST_PATH_HITS = Counter(
    "nexus_spark_er_fast_path_hits_total",
    "Stage 2 records resolved via entity_resolution_index fast path (cache hit)",
    ["tenant_id"],
)

ER_SIGNAL_SCORES = Histogram(
    "nexus_spark_er_signal_scores",
    "Combined entity resolution signal scores",
    ["tenant_id", "cdm_entity_type"],
    buckets=[0.0, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
)

ER_STATE_TRANSITIONS = Counter(
    "nexus_spark_er_state_transitions_total",
    "Golden Record state machine transitions",
    ["from_state", "to_state", "tenant_id"],
)

ER_LATENCY = Histogram(
    "nexus_spark_er_latency_seconds",
    "Stage 2 entity resolution latency per record (p95 target: ≤5s — NFR-D3-01)",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# ── Stage 3 — Synthesis ───────────────────────────────────────────────────────

SYNTHESIS_RECORDS = Counter(
    "nexus_spark_synthesis_records_total",
    "Records processed by Stage 3 Golden Record synthesis",
    ["tenant_id", "cdm_entity_type", "status"],
)

SYNTHESIS_LATENCY = Histogram(
    "nexus_spark_synthesis_latency_seconds",
    "Stage 3 synthesis latency per record",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# ── Kafka I/O ─────────────────────────────────────────────────────────────────

KAFKA_RECORDS_WRITTEN = Counter(
    "nexus_spark_kafka_records_written_total",
    "Records written to Kafka output topics",
    ["topic", "tenant_id", "status"],
)

DEAD_LETTER_RECORDS = Counter(
    "nexus_spark_dead_letter_records_total",
    "Records written to m1.int.dead_letter",
    ["tenant_id", "stage", "reason"],
)

# ── DB I/O ────────────────────────────────────────────────────────────────────

DB_WRITES = Counter(
    "nexus_spark_db_writes_total",
    "Database writes from Spark executors",
    ["table", "operation", "status"],  # operation: upsert | delete | insert
)

DB_WRITE_LATENCY = Histogram(
    "nexus_spark_db_write_latency_seconds",
    "Database write latency from Spark executors",
    ["table"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
