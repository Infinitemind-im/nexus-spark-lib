"""Transformed record models — the output shape of the full NEXUS pipeline.

SparkTransformResult is the ER-enriched envelope published to m1.int.transformed_records.
TransformedField and FieldStats (quality statistics) are imported from nexus_core.models
(v2) — single source of truth for field-level canonical types.

Published by: nexus-spark-transformer (via nexus_spark_lib.kafka.writer).
Consumed by:  nexus-cdm-mapper.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# ── Canonical field-level types from nexus_core v2 ───────────────────────────
from nexus_core.models import FieldQuality as FieldStats   # aggregate stats dataclass
from nexus_core.models import TransformedField             # noqa: F401 — re-exported


# ── Stage 1 per-field quality label ──────────────────────────────────────────

class FieldQuality(str, Enum):
    """Per-field data-quality label assigned during Stage 1 normalisation.

    This is distinct from nexus_core.models.FieldQuality (imported above as FieldStats),
    which is a dataclass holding aggregate metrics (null_rate, format_valid, cardinality).
    This local enum is used exclusively by the Stage 1 UDF to label each field.
    """

    GOOD    = "good"
    MISSING = "missing"
    SUSPECT = "suspect"


# ── Pipeline-internal ER-enrichment models (not in nexus_core) ───────────────

@dataclass
class ContributingRecord:
    """Reference back to the source record that produced this transformation."""

    source_system: str
    source_record_id: str
    source_table: str
    source_op: str        # SourceOp value
    source_ts: datetime


@dataclass
class AttributeProvenance:
    """Tracks which source record currently supplies each canonical attribute."""

    # Map of canonical_attribute → "source_system:source_record_id"
    attribute_provenance: dict[str, str] = field(default_factory=dict)
    contributing_sources: list[str] = field(default_factory=list)
    provenance_hash: str = ""   # SHA-256 over canonicalised provenance summary


@dataclass
class OperationMetadata:
    """Extra context for MERGE, SUPERSEDE, REMOVE, and SPLIT operations."""

    merge_target: str | None = None            # cdm_entity_id of the loser (MERGE)
    supersession_target: str | None = None     # cdm_entity_id of the survivor (SUPERSEDE)
    deletion_reason: str | None = None         # "all_sources_removed" (REMOVE)
    split_partition_id: str | None = None      # UUID for split-history record (SPLIT)


@dataclass
class TransformHeaders:
    """Message-level metadata propagated through the pipeline."""

    schema_version: str = "2.0"
    backfill_batch_id: str | None = None
    trace_id: str | None = None


@dataclass
class SparkTransformResult:
    """ER-enriched pipeline envelope published to m1.int.transformed_records.

    Extends the nexus_core v2 SparkTransformResult (raw Spark output) with
    entity-resolution context: operation semantics, provenance, and materialization level.
    The fields list uses TransformedField from nexus_core.models v2.

    This is the contract between nexus-spark-transformer and nexus-cdm-mapper.
    Serialisation to JSON is handled by prepare_kafka_output() in kafka.writer.
    """

    tenant_id: str
    cdm_entity_id: str             # "gr:" + sha256 truncated 128-bit
    cdm_entity_type: str           # "Party", "Transaction", etc.
    operation: str                 # UPSERT | MERGE | SUPERSEDE | REMOVE | RELEVEL
    contributing_record: ContributingRecord
    provenance_summary: AttributeProvenance
    materialization_level: str     # hot | warm | cold
    fields: list[TransformedField] = field(default_factory=list)
    operation_metadata: OperationMetadata = field(default_factory=OperationMetadata)
    headers: TransformHeaders = field(default_factory=TransformHeaders)
    transformation_ms: float = 0.0
    is_provisional: bool = False   # True when ER confidence is in the review band
