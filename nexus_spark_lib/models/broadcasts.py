"""Typed broadcast variable wrappers.

Spark Broadcast objects are untyped at runtime. These wrappers carry the type
information and allow the library to validate that callers pass the correct
broadcast type to each stage function.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark import Broadcast

    from nexus_spark_lib.models.fx import FxRates
    from nexus_spark_lib.models.materialization import MaterializationPolicy
    from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet

# Type alias for CDM mapping broadcast payload
# Key: (tenant_id, source_system, source_table, source_field) → CDM mapping info
CdmMappingEntry = dict  # {"cdm_field": str, "cdm_entity": str, "tier": int, "canonical_type": str}
CdmMappingIndex = dict[tuple[str, str, str, str], CdmMappingEntry]


@dataclass
class CdmMappingBroadcast:
    """Typed wrapper for the CDM mapping broadcast (Stage 1).

    Contains the CDM field mappings for all active tenants, loaded from
    nexus_system.cdm_mappings via CDMRegistryService.
    """

    broadcast: "Broadcast[CdmMappingIndex]"
    snapshot_ts: str

    def value(self) -> CdmMappingIndex:
        return self.broadcast.value


@dataclass
class FxRatesBroadcast:
    """Typed wrapper for the FX rates broadcast (Stage 1)."""

    broadcast: "Broadcast[FxRates]"
    snapshot_ts: str

    def value(self) -> "FxRates":
        return self.broadcast.value


@dataclass
class ErIndexSnapshot:
    """Snapshot of entity_resolution_index for batch broadcast (Batch Backfill mode).

    In streaming mode (CDC), entity lookups hit PostgreSQL directly for low latency.
    In batch mode (Backfill), the full index is loaded into a broadcast for throughput.

    Key: (tenant_id, connector_id, source_table, source_record_id) → cdm_entity_id
    """

    index: dict[tuple[str, str, str, str], str]  # → cdm_entity_id
    snapshot_ts: str


@dataclass
class ErIndexBroadcast:
    """Typed wrapper for the ER index broadcast (Stage 2, batch mode)."""

    broadcast: "Broadcast[ErIndexSnapshot]"
    snapshot_ts: str

    def value(self) -> ErIndexSnapshot:
        return self.broadcast.value


@dataclass
class SurvivorshipBroadcast:
    """Typed wrapper for the survivorship rules broadcast (Stage 3)."""

    broadcast: "Broadcast[SurvivorshipRuleSet]"
    snapshot_ts: str

    def value(self) -> "SurvivorshipRuleSet":
        return self.broadcast.value


@dataclass
class MaterializationPolicyBroadcast:
    """Typed wrapper for the materialization policy broadcast (Stage 0)."""

    broadcast: "Broadcast[MaterializationPolicy]"
    snapshot_ts: str

    def value(self) -> "MaterializationPolicy":
        return self.broadcast.value
