from nexus_core.db import SYSTEM_TENANT, get_tenant_scoped_connection  # noqa: F401 — platform RLS helper
from nexus_spark_lib.db.connection import close_pool, get_pool
from nexus_spark_lib.db.decision_log import upsert_schema_snapshot, write_decision_log_batch
from nexus_spark_lib.db.er_index import (
    delete_by_source,
    get_sources_for_entity,
    load_er_index_snapshot,
    lookup_batch,
    repoint_to_survivor,
    upsert_batch,
)
from nexus_spark_lib.db.golden_records import (
    apply_synthesis_result,
    delete_provenance_for_source,
    get_all_provenance,
    get_golden_record_state,
    has_any_provenance,
    upsert_golden_record,
)
from nexus_spark_lib.db.redirects import insert_redirect, queue_for_review, resolve_redirect
from nexus_spark_lib.db.survivorship_rules import (
    load_deterministic_id_columns,
    load_er_thresholds,
    load_materialization_policy,
    load_materialization_runtime_config,
    load_survivorship_rules,
)

__all__ = [
    # connection
    "get_pool",
    "close_pool",
    "get_tenant_scoped_connection",
    "SYSTEM_TENANT",
    # er_index
    "lookup_batch",
    "load_er_index_snapshot",
    "upsert_batch",
    "delete_by_source",
    "get_sources_for_entity",
    "repoint_to_survivor",
    # golden_records
    "upsert_golden_record",
    "get_golden_record_state",
    "apply_synthesis_result",
    "get_all_provenance",
    "delete_provenance_for_source",
    "has_any_provenance",
    # redirects + review queue
    "insert_redirect",
    "resolve_redirect",
    "queue_for_review",
    # policy loaders
    "load_survivorship_rules",
    "load_materialization_policy",
    "load_materialization_runtime_config",
    "load_er_thresholds",
    "load_deterministic_id_columns",
    # decision log + schema snapshots
    "write_decision_log_batch",
    "upsert_schema_snapshot",
]
