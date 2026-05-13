"""Typed broadcast variable wrappers.

Spark Broadcast objects are untyped at runtime. These wrappers carry the type
information and allow the library to validate that callers pass the correct
broadcast type to each stage function.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark import Broadcast

    from nexus_spark_lib.models.fx import FxRates
    from nexus_spark_lib.models.materialization import MaterializationPolicy, MaterializationRuntimeConfig
    from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet

# Type alias for CDM mapping broadcast payload
# Key: (tenant_id, source_system, source_table, source_field) → CDM mapping info
CdmMappingEntry = dict  # {"cdm_field": str, "cdm_entity": str, "tier": int, "canonical_type": str}
CdmMappingIndex = dict  # Type alias: Dict[tuple[str, str, str, str], CdmMappingEntry]


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
    """Serializable ER snapshot shared by streaming and batch resolution paths.

    `snapshot` contains fast-path source lookups plus deterministic Signal A hashes.
    `deterministic_columns` and `thresholds` carry the additional metadata needed
    by the current shared-library Stage 2 implementation.
    """

    snapshot: dict[str, str] = field(default_factory=dict)
    deterministic_columns: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    thresholds: dict[tuple[str, str], dict] = field(default_factory=dict)
    snapshot_ts: str = ""
    deterministic_hash_count: int = 0
    lsh_index: object | None = None
    _fields_by_entity: dict[str, dict] = field(default_factory=dict, repr=False)

    @property
    def index(self) -> dict[str, str]:
        """Backward-compatible alias for older batch-oriented callers."""
        return self.snapshot

    def get_fields(self, cdm_entity_id: str) -> dict:
        return self._fields_by_entity.get(cdm_entity_id, {})


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
    """Typed wrapper for the Stage 0 materialization broadcast.

    The payload may be the legacy MaterializationPolicy or the MD-aligned
    MaterializationRuntimeConfig that prefers cdm_entity_materialization and
    falls back to policy evaluation.
    """

    broadcast: "Broadcast[MaterializationPolicy | MaterializationRuntimeConfig]"
    snapshot_ts: str

    def value(self) -> "MaterializationPolicy | MaterializationRuntimeConfig":
        return self.broadcast.value
