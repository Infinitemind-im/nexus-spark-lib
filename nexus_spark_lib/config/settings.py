"""Settings — loaded from environment variables via pydantic-settings.

All configuration for nexus_spark_lib comes from the environment or Kubernetes
secrets. No hardcoded values except safe defaults.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus_spark_lib.config.constants import ConsumerGroups


class NexusSparkLibSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEXUS_SPARK_",
        case_sensitive=False,
    )

    # ── Database ──────────────────────────────────────────────────────────────
    db_dsn: str = Field(
        ...,
        alias="NEXUS_DB_DSN",
        description="PostgreSQL DSN e.g. postgresql://user:pass@host/nexus",
    )
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10         # Per executor — keep low, many executors run in parallel
    db_pool_max_inactive_connection_lifetime: float = 300.0

    # ── Neo4j (Signal C entity resolution) ───────────────────────────────────
    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j bolt URI e.g. bolt://neo4j:7687",
    )
    neo4j_user: str = "neo4j"
    neo4j_password: str = Field(
        default="",
        description="Neo4j password — set from Vault in production",
    )
    neo4j_max_connection_pool_size: int = 5  # Per executor
    neo4j_connection_timeout_seconds: float = 10.0

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP",
    )
    kafka_consumer_group: str = Field(
        default=ConsumerGroups.SPARK_TRANSFORMER,
        description="Kafka consumer group ID for the Spark transformer runtime.",
    )

    # ── Entity Resolution ─────────────────────────────────────────────────────
    er_auto_apply_threshold_default: float = 0.92
    er_review_lower_bound_default: float = 0.75
    er_lsh_num_perm: int = 128          # MinHash permutations for LSH
    er_lsh_threshold: float = 0.5       # Jaccard threshold for LSH candidate generation
    er_signal_c_max_hops: int = 2       # Neo4j traversal depth cap

    # ── FX ────────────────────────────────────────────────────────────────────
    fx_rates_cache_ttl_seconds: int = 3600  # Reload FX rates every hour

    # ── Materialization ───────────────────────────────────────────────────────
    materialization_policy_cache_ttl_seconds: int = 300  # 5-minute refresh (NFR-D4-03)
    materialization_default_level: str = "warm"

    # ── Broadcast ─────────────────────────────────────────────────────────────
    cdm_mapping_cache_ttl_seconds: int = 300   # Same as CDMRegistryService default
    survivorship_cache_ttl_seconds: int = 300

    # ── Observability ─────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    enable_er_trace: bool = False       # FR-Dev3-S-01: diagnostic trace mode

    # ── Spark streaming ───────────────────────────────────────────────────────
    # NFR-D3-01 targets p95 <= 5s/record; 1s micro-batch keeps idle wait low.
    # Iteration 2 worked-example doc mentions 30s dedup windows — override via env for backfill.
    spark_stream_trigger_seconds: int = 1
    er_use_map_in_pandas: bool = True
    er_driver_singleton_resolve: bool = True
    dead_letter_max_retries: int = 3

    @field_validator("materialization_default_level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        if v not in ("hot", "warm", "cold"):
            raise ValueError(f"Invalid materialization level: {v}")
        return v


# Module-level singleton — loaded once per process
settings = NexusSparkLibSettings()
