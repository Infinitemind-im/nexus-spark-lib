"""Platform-wide constants for nexus_spark_lib.

All cross-module Kafka topic names are delegated to CrossModuleTopicNamer from
nexus_core v2.  Topics that exist only in nexus_spark_lib (ER/GR events not yet
promoted to nexus_core) are kept as local string constants.
"""

from __future__ import annotations

from nexus_core.topics import CrossModuleTopicNamer


# ── Kafka topic names ─────────────────────────────────────────────────────────

class Topics:
    """Kafka topic name constants — delegates to CrossModuleTopicNamer (nexus_core v2).

    Static attributes are resolved once at import time.  Topics that are not yet
    in nexus_core are kept as local literals (marked with # local).
    """

    # ── M1 internal topics (nexus_core v2) ───────────────────────────────────
    RAW_RECORDS         = CrossModuleTopicNamer.M1Internal.RAW_RECORDS
    TRANSFORMED_RECORDS = CrossModuleTopicNamer.M1Internal.TRANSFORMED_RECORDS
    DEAD_LETTER         = CrossModuleTopicNamer.M1Internal.DEAD_LETTER

    # ── Global observability topics (nexus_core v2) ───────────────────────────
    M3_WRITE_COMPLETED      = CrossModuleTopicNamer.m3_write_completed()
    MATERIALIZATION_CHANGED = CrossModuleTopicNamer.materialization_changed()

    # ── ER / GR events — local only (not yet in nexus_core) ──────────────────
    ER_REVIEW_QUEUED = "nexus.er.review_queued"  # local
    GR_STATE_CHANGED = "nexus.gr.state_changed"  # local
    GR_MERGED        = "nexus.gr.merged"          # local
    GR_SPLIT         = "nexus.gr.split"           # local

    # ── Tenant-scoped topics (nexus_core v2) ─────────────────────────────────

    @staticmethod
    def entity_routed(tenant_id: str) -> str:
        return CrossModuleTopicNamer.m1_entity_routed(tenant_id)

    @staticmethod
    def entity_removed(tenant_id: str) -> str:
        return CrossModuleTopicNamer.m1_entity_removed(tenant_id)

    @staticmethod
    def classification_produced(tenant_id: str) -> str:
        return CrossModuleTopicNamer.m1_classification_produced(tenant_id)

    @staticmethod
    def validation_decision(tenant_id: str) -> str:
        return CrossModuleTopicNamer.m4(tenant_id, "validation_decision")


# ── Consumer group names ───────────────────────────────────────────────────────

class ConsumerGroups:
    SPARK_TRANSFORMER = "m1-spark-transformer"
    CDM_MAPPER = "m1-cdm-mapper"
    M3_WRITER_ENTITIES = "m3-writer-entities"


# ── Schema versions ────────────────────────────────────────────────────────────

NEXUS_MESSAGE_SCHEMA_VERSION = "2.0"
SPARK_TRANSFORM_RESULT_SCHEMA_VERSION = "1.0"

# ── Entity Resolution defaults ─────────────────────────────────────────────────

ER_CDM_ENTITY_ID_PREFIX = "gr:"
ER_AUTO_APPLY_THRESHOLD_DEFAULT = 0.95
ER_REVIEW_LOWER_BOUND_DEFAULT = 0.75

# Signal C (Neo4j) lift values
ER_SIGNAL_C_DEPTH1_LIFT = 0.05
ER_SIGNAL_C_DEPTH2_LIFT = 0.02
ER_SIGNAL_C_MAX_LIFT = 0.10

# ── Batch checkpointing ────────────────────────────────────────────────────────

BATCH_CHECKPOINT_INTERVAL = 10_000    # Write checkpoint every N records
DELTA_CHECKPOINT_THRESHOLD_DEFAULT = 500_000  # FR-ST-C-01 default

# ── Normalisation parsing helpers (Stage 0) ──────────────────────────────────
#
# These are used by stage0_normalise and exposed via nexus_spark_lib.config.
# They are kept here (rather than settings) because they are pure constants.

NULL_LIKE_STRINGS = {
    "",
    "null",
    "none",
    "n/a",
    "na",
    "nil",
    "undefined",
    "nan",
}

BOOL_TRUE_VALUES = {"true", "t", "yes", "y", "1", "on"}
BOOL_FALSE_VALUES = {"false", "f", "no", "n", "0", "off"}

# Common date/time formats seen in source systems (best-effort parsing).
DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
)

DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
)
