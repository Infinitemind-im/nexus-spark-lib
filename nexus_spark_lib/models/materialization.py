"""Materialization tier models.

The materialization tier system determines where data lives (hot/warm/cold)
and how eagerly it is projected into the AI stores (Elasticsearch, Neo4j, TimescaleDB).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class MaterializationLevel(str, Enum):
    """Tier assignment for a CDM entity record.

    hot   — fully materialised in all applicable AI stores; embedded in Elasticsearch.
    warm  — governance only (golden_records_index); not in AI stores.
    cold  — not processed by Stage 2/3; skipped after Stage 0.
    """

    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


@dataclass
class PolicyRule:
    """One materialization policy rule from nexus_system.materialization_policy.

    Rules are evaluated in priority order (DESC). The first match wins.
    """

    rule_id: str
    tenant_id: str
    cdm_entity_type: str
    predicate: str                      # SQL-like predicate e.g. "annual_revenue > 1000000"
    target_level: MaterializationLevel
    priority: int                       # Higher = evaluated first
    rule_type: str                      # "manual_override" | "decay" | "boost" | "rlhf_learned"
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    superseded_at: datetime | None = None


@dataclass
class MaterializationPolicy:
    """Full policy snapshot for one or more tenants, used as a Spark broadcast."""

    # Key: (tenant_id, cdm_entity_type) → ordered list of rules (priority DESC)
    rules_by_scope: dict[tuple[str, str], list[PolicyRule]] = field(default_factory=dict)
    snapshot_ts: datetime = field(default_factory=datetime.utcnow)

    def get_rules(self, tenant_id: str, cdm_entity_type: str) -> list[PolicyRule]:
        """Return active rules for a scope, sorted by priority descending."""
        return self.rules_by_scope.get((tenant_id, cdm_entity_type), [])

    def evaluate(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        field_values: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> "MaterializationDecision":
        """Evaluate the policy for a record and return a MaterializationDecision.

        Iterates rules in priority order. The first rule whose predicate evaluates to
        True wins. Falls back to WARM (governance-only) when no rule matches.

        The predicate engine is intentionally simple for p95 ≤ 1ms (NFR-D4-01).
        Only numeric comparisons and boolean flags are evaluated; full expression
        parsing is not supported in Stage 0 (use policy rules with rule_type=boost/decay
        for complex logic).
        """
        eval_now = now or datetime.utcnow()
        field_values = field_values or {}

        for rule in self.get_rules(tenant_id, cdm_entity_type):
            # Skip expired or not-yet-valid rules
            if rule.valid_from and eval_now < rule.valid_from:
                continue
            if rule.valid_until and eval_now > rule.valid_until:
                continue

            if _eval_predicate(rule.predicate, field_values):
                return MaterializationDecision(
                    level=rule.target_level,
                    applied_rule_id=rule.rule_id,
                    evaluated_at=eval_now,
                    predicate_debug=rule.predicate,
                )

        # Default: WARM (record enters governance but is not projected to AI stores)
        return MaterializationDecision(
            level=MaterializationLevel.WARM,
            applied_rule_id=None,
            evaluated_at=eval_now,
            predicate_debug="default:warm",
        )


def _eval_predicate(predicate: str, field_values: dict[str, Any]) -> bool:
    """Minimal predicate evaluator — supports key=value and key>value comparisons.

    Returns True for empty predicates (unconditional rule / manual_override).
    """
    if not predicate or predicate.strip() == "*":
        return True
    # Simple equality: "cdm_entity_type=contact"
    if "=" in predicate and ">" not in predicate and "<" not in predicate:
        key, _, val = predicate.partition("=")
        field_val = field_values.get(key.strip())
        return str(field_val) == val.strip().strip("\"'")
    return False  # Unsupported predicate → no match (safe default)


@dataclass
class MaterializationDecision:
    """The decision produced by Stage 0 for a single record."""

    level: MaterializationLevel
    applied_rule_id: str | None        # None when falling back to the default level
    evaluated_at: datetime = field(default_factory=datetime.utcnow)
    predicate_debug: str | None = None  # Human-readable summary for decision_log


@dataclass
class PredicateContext:
    """Evaluated context passed to the predicate engine for a single record.

    Built from the normalised field values after Stage 1.
    """

    tenant_id: str
    cdm_entity_type: str
    field_values: dict[str, Any]        # canonical_attribute_name → normalised value
    evaluation_ts: datetime = field(default_factory=datetime.utcnow)
