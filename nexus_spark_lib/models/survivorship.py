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

    MOST_RECENT = "most_recent"              # Source with the latest source_ts wins
    HIGHEST_CONFIDENCE = "highest_confidence"  # Source with the highest mapping confidence
    SOURCE_PRIORITY = "source_priority"      # Explicit list of preferred sources
    MOST_COMPLETE = "most_complete"          # Source with the fewest null attributes wins
    LONGEST_VALUE = "longest_value"          # Longest non-null string value wins
    EXACT_MATCH = "exact_match"              # All sources must agree; else flag for review


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
    """One row of golden_record_provenance — one attribute from one source."""

    cdm_entity_id: str
    tenant_id: str
    attribute_name: str
    source_system: str
    source_record_id: str
    source_value: str | None           # Serialised canonical value (None = source contributes null)
    source_ts: str                     # ISO 8601 timestamp from the contributing record
    survivorship_rule: str             # Rule type that selected this source
    observed_at: str                   # When this provenance row was last written


@dataclass
class SynthesisResult:
    """Output of Stage 3 for one record: new provenance state + provenance hash."""

    cdm_entity_id: str
    rows_to_upsert: list[ProvenanceRow] = field(default_factory=list)
    rows_to_delete: list[tuple[str, str, str]] = field(default_factory=list)  # (entity_id, attr, source)
    provenance_hash: str = ""
    contributing_sources: list[str] = field(default_factory=list)
    attribute_provenance: dict[str, str] = field(default_factory=dict)
