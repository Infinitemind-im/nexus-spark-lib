from nexus_spark_lib.observability.er_trace import ErTraceEntry, ErTraceWriter
from nexus_spark_lib.observability.metrics import (
    DEAD_LETTER_RECORDS,
    DB_WRITE_LATENCY,
    DB_WRITES,
    ER_FAST_PATH_HITS,
    ER_LATENCY,
    ER_RECORDS,
    ER_SIGNAL_SCORES,
    ER_STATE_TRANSITIONS,
    FX_CONVERSIONS,
    KAFKA_RECORDS_WRITTEN,
    MATERIALIZATION_DECISIONS,
    MATERIALIZATION_POLICY_CACHE_AGE,
    NORMALISE_LATENCY,
    NORMALISE_RECORDS,
    SYNTHESIS_LATENCY,
    SYNTHESIS_RECORDS,
)
from nexus_spark_lib.observability.structured_log import get_stage_logger, log_pii_safe
from nexus_spark_lib.observability.tracing import get_tracer, init_tracer, stage_span

__all__ = [
    "get_stage_logger",
    "log_pii_safe",
    "get_tracer",
    "init_tracer",
    "stage_span",
    "ErTraceEntry",
    "ErTraceWriter",
    # Metrics
    "MATERIALIZATION_DECISIONS",
    "MATERIALIZATION_POLICY_CACHE_AGE",
    "NORMALISE_RECORDS",
    "NORMALISE_LATENCY",
    "FX_CONVERSIONS",
    "ER_RECORDS",
    "ER_FAST_PATH_HITS",
    "ER_SIGNAL_SCORES",
    "ER_STATE_TRANSITIONS",
    "ER_LATENCY",
    "SYNTHESIS_RECORDS",
    "SYNTHESIS_LATENCY",
    "KAFKA_RECORDS_WRITTEN",
    "DEAD_LETTER_RECORDS",
    "DB_WRITES",
    "DB_WRITE_LATENCY",
]
