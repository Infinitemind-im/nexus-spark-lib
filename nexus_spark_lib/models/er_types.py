"""Entity Resolution domain types.

These types are used internally by Stage 2 (resolve) and referenced by
the DB layer and Golden Record state machine.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ResolutionMethod(str, Enum):
    """How the cdm_entity_id was determined for a record."""

    DETERMINISTIC = "spark_deterministic"    # Signal A — exact match on deterministic IDs
    PROBABILISTIC = "spark_probabilistic"    # Signal B — LSH + weighted similarity
    GRAPH = "spark_graph"                    # Signal C — Neo4j graph lift pushed over threshold
    HUMAN = "human"                          # Steward override via M4 review UI
    MERGE_INHERITANCE = "merge_inheritance"  # Assigned during a MERGE operation


class GoldenRecordState(str, Enum):
    """Lifecycle state of a Golden Record in golden_records_index."""

    ACTIVE = "active"               # Normal operating state
    PROVISIONAL = "provisional"     # In review band — resolution confidence < auto-apply threshold
    SUPERSEDED = "superseded"       # Lost a MERGE — redirects in golden_record_redirects
    TOMBSTONED = "tombstoned"       # All contributing sources deleted


class ErOperation(str, Enum):
    """Operation type emitted on m1.int.transformed_records."""

    UPSERT = "UPSERT"         # Standard create or update
    MERGE = "MERGE"           # This GR absorbs another — is the survivor
    SUPERSEDE = "SUPERSEDE"   # This GR was merged into another — it is the loser
    REMOVE = "REMOVE"         # All sources deleted — GR transitions to tombstoned
    RELEVEL = "RELEVEL"       # Materialization tier changed — re-synthesise without re-resolving


@dataclass
class ErMatchResult:
    """Result of a single entity resolution attempt."""

    cdm_entity_id: str
    confidence: float                  # [0.0, 1.0]
    resolution_method: ResolutionMethod
    is_provisional: bool = False       # True when confidence is in [review_lower_bound, auto_apply_threshold)
    signal_breakdown: dict[str, float] | None = None  # {"signal_a": 1.0, "signal_b": 0.82, "signal_c": 0.05}


@dataclass
class ErThresholds:
    """Per-tenant per-entity-type resolution thresholds from nexus_system.er_thresholds."""

    tenant_id: str
    cdm_entity_type: str
    weights: dict[str, float]          # Attribute weights for Signal B scoring
    auto_apply_threshold: float        # >= this → deterministic assignment
    review_lower_bound: float          # >= this and < auto_apply → review queue


@dataclass
class BlockingRule:
    """Blocking formula for an entity type from nexus_system.entity_blocking_rules."""

    tenant_id: str
    cdm_entity_type: str
    blocking_formula: str              # e.g. "lower(left(legal_name,4)) || ':' || left(domain,6)"


@dataclass
class DeterministicIdColumn:
    """Deterministic identifier column from nexus_system.deterministic_id_columns."""

    tenant_id: str
    cdm_entity_type: str
    attribute_name: str                # e.g. "tax_id", "domain", "duns_number"
