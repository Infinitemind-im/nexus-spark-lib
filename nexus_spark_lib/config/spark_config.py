"""SparkSession builder with production-grade tuning.

Each Spark application (streaming or batch) calls build_spark_session()
at startup. Configuration is layered: defaults → environment overrides → caller overrides.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str,
    extra_config: dict[str, str] | None = None,
    *,
    streaming: bool = True,
) -> SparkSession:
    """Build and return a production-tuned SparkSession.

    Args:
        app_name:     Spark UI application name.
        extra_config: Additional Spark config key/value pairs (caller-level overrides).
        streaming:    If True, applies streaming-specific tuning.

    Returns:
        Configured SparkSession (or existing session if already active).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # ── Serialisation ────────────────────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        # ── Memory ────────────────────────────────────────────────────────────
        .config("spark.executor.memory", "4g")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.driver.memory", "2g")
        # ── Shuffle ───────────────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ── Broadcast join threshold ──────────────────────────────────────────
        .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))  # 100 MB
        # ── Kafka integration ─────────────────────────────────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        # ── Delta Lake ────────────────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Observability ─────────────────────────────────────────────────────
        .config("spark.metrics.conf.*.sink.prometheusServlet.class",
                "org.apache.spark.metrics.sink.PrometheusServlet")
        .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus")
        .config("spark.ui.prometheus.enabled", "true")
    )

    if streaming:
        builder = (
            builder
            # ── Structured Streaming ──────────────────────────────────────────
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.metricsEnabled", "true")
            # Checkpoint location must be set by the calling application
            # (different per job — not set here)
        )

    if extra_config:
        for key, value in extra_config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
