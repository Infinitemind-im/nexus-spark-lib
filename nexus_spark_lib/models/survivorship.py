"""Survivorship rule models for Stage 3 Golden Record synthesis.

Survivorship rules define which source record's value wins when multiple sources
contribute the same canonical attribute. Rules are evaluated deterministically:
given the same set of contributing source records, the same output is produced
regardless of the order events were processed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SurvivorshipRuleType(str, Enum):
    """Strategy used to select the winning value for an attribute."""

    MOST_RECENT = "most_recent"                  # Source with the latest source_ts wins
    MOST_CONFIDENT = "most_confident"            # Spec name: source with the highest ER confidence wins
    HIGHEST_CONFIDENCE = "most_confident"        # Backward-compatible alias for older code/tests
    SOURCE_PRIORITY = "source_priority"          # Explicit list of preferred sources
    MOST_COMPLETE = "most_complete"              # Source with the fewest null attributes wins
    FIRST_OBSERVED = "first_observed"            # Once chosen, keep the original source
    MANUAL = "manual"                            # Steward-managed override; pipeline must not replace it

    # Legacy extensions retained for compatibility with the current simplified
    # row-level Stage 3 implementation and its tests.
    LONGEST_VALUE = "longest_value"
    EXACT_MATCH = "exact_match"


@dataclass
class SurvivorshipRule:
    """One rule from nexus_system.survivorship_rules.

    Each rule applies to a specific (tenant_id, cdm_entity_type, attribute_name) triple.
    """

    tenant_id: str
    cdm_entity_type: str
    attribute_name: str
    rule_type: SurvivorshipRuleType
    # Ordered list of preferred source systems (used by SOURCE_PRIORITY)
    priority_sources: list[str] = field(default_factory=list)
    # Fallback rule type if the primary rule cannot resolve (e.g. tie-break)
    fallback_rule_type: SurvivorshipRuleType | None = None


@dataclass
class SurvivorshipRuleSet:
    """Full survivorship configuration for one or more tenants.

    Used as a Spark broadcast variable.
    Key: (tenant_id, cdm_entity_type, attribute_name) → SurvivorshipRule
    Default rule (MOST_RECENT) applies when no explicit rule exists.
    """

    rules: dict[tuple[str, str, str], SurvivorshipRule] = field(default_factory=dict)

    def get_rule(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        attribute_name: str,
    ) -> SurvivorshipRule:
        """Return the survivorship rule for an attribute, defaulting to MOST_RECENT."""
        key = (tenant_id, cdm_entity_type, attribute_name)
        return self.rules.get(
            key,
            SurvivorshipRule(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                attribute_name=attribute_name,
                rule_type=SurvivorshipRuleType.MOST_RECENT,
            ),
        )


@dataclass
class ProvenanceRow:
    """One row of golden_record_provenance — one winning source per attribute.

    The Iteration 2 contract stores only source pointers plus a hash of the
    winning value. Raw business values do not belong in provenance.
    """

    cdm_entity_id: str
    attribute_name: str
    winning_connector_id: str
    winning_source_table: str
    winning_record_id: str
    observed_value_hash: str
    observed_at: str
    rule_applied: str
    tenant_id: str | None = None

    @property
    def source_system(self) -> str:
        """Backward-compatible alias for older call sites."""
        return self.winning_connector_id

    @property
    def source_record_id(self) -> str:
        """Backward-compatible alias for older call sites."""
        return self.winning_record_id

    @property
    def source_ts(self) -> str:
        """Backward-compatible alias for older call sites."""
        return self.observed_at

    @property
    def survivorship_rule(self) -> str:
        """Backward-compatible alias for older call sites."""
        return self.rule_applied


@dataclass
class SynthesisResult:
    """Output of Stage 3 for one record: new provenance state + provenance hash."""

    cdm_entity_id: str
    rows_to_upsert: list[ProvenanceRow] = field(default_factory=list)
    rows_to_delete: list[tuple[str, str, str]] = field(default_factory=list)  # (entity_id, attr, source)
    provenance_hash: str = ""
    hash_changed: bool = False
    contributing_sources: list[str] = field(default_factory=list)
    attribute_provenance: dict[str, str] = field(default_factory=dict)
