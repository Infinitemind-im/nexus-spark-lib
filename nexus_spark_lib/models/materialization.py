"""Materialization tier models.

The materialization tier system determines where data lives (hot/warm/cold)
and how eagerly it is projected into the AI stores (Elasticsearch, Neo4j, TimescaleDB).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class MaterializationLevel(str, Enum):
    """Tier assignment for a CDM entity record.

    hot   — fully materialised in all applicable AI stores; embedded in Elasticsearch.
    warm  — Signal A ER only; governance (golden_records_index); not in AI stores.
    cold  — dropped before Stage 1 normalise; no ER, no synthesis, no M3 writes.
    """

    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


@dataclass
class Stage0Output:
    """Columns added to DataFrame by Stage 0 materialization_gate().

    Per NEXUS-Iter2-REF-DataPaths §1.4–1.5, Stage 0 resolves the CDM entity type
    from the connector_id × source_table broadcast and assigns a materialization tier
    by evaluating the policy against raw payload field values.

    These three columns are appended to the RawRecord DataFrame and passed to Stage 1.
    """

    cdm_entity_type: str
    """Canonical CDM entity type (e.g. 'contact', 'account', 'incident').
    Resolved from CDM mappings broadcast; used by all downstream stages for type-specific logic.
    """

    materialization_level: MaterializationLevel
    """Tier assignment: HOT (fully stored) | WARM (ER + governance only) | COLD (dropped).
    Assigned by policy evaluation; used by drop_cold() to prune stage."""

    materialization_rule_id: str | None = None
    """rule_id from nexus_system.materialization_policy that matched and fired.
    None when falling back to WARM default (no explicit rule matched).
    Used for audit + decision logging."""


# Rule-type priority for tie-breaking (higher = wins): manual > boost > learned > decay > base
_RULE_TYPE_RANK: dict[str, int] = {
    "manual": 5,
    "boost": 4,
    "learned": 3,
    "decay": 2,
    "base": 1,
}


@dataclass
class PolicyRule:
    """One materialization policy rule from nexus_system.materialization_policy.

    Rules are evaluated in (priority DESC, rule_type rank DESC, valid_until ASC) order.
    The first matching rule wins. Rule types:
      base    — fallback level per entity type; one required per (tenant, scope).
      decay   — ages records down over time (AGE predicates).
      boost   — time-bounded elevation for a cohort (valid_from/valid_until).
      learned — proposed by the RLHF loop from observed query behaviour.
      manual  — tenant admin pin; highest effective rank; cannot be overridden.
    """

    rule_id: str
    tenant_id: str
    scope: str                          # CDM entity type, or "*" to match all types
    predicate: str                      # Boolean expression; "TRUE" for unconditional rules
    target_level: MaterializationLevel
    priority: int                       # Higher number = evaluated first (on equal rank)
    rule_type: str                      # "base" | "decay" | "boost" | "learned" | "manual"
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    source: str = "system"              # "system" | "admin" | "rlhf" | "fiscal_calendar"
    learned_metadata: dict | None = None  # Evidence payload; only set for rule_type="learned"


@dataclass
class MaterializationPolicy:
    """Full policy snapshot for one or more tenants, used as a Spark broadcast.

    Refreshed every 5 minutes or on nexus.materialization_policy.changed Kafka event.
    """

    # Key: (tenant_id, scope) — scope may be "*" for tenant-wide wildcard rules
    rules_by_scope: dict[tuple[str, str], list[PolicyRule]] = field(default_factory=dict)
    snapshot_ts: datetime = field(default_factory=datetime.utcnow)

    def get_rules(self, tenant_id: str, cdm_entity_type: str) -> list[PolicyRule]:
        """Return all applicable rules for a record scope.

        Merges exact-scope rules with wildcard ("*") rules so that tenant-wide
        defaults (e.g. a decay rule for all entity types) are always considered.
        Global rules stored under tenant_id="*" are also inherited by real tenants.
        """
        exact = self.rules_by_scope.get((tenant_id, cdm_entity_type), [])
        wildcard = self.rules_by_scope.get((tenant_id, "*"), [])
        global_exact = self.rules_by_scope.get(("*", cdm_entity_type), [])
        global_wildcard = self.rules_by_scope.get(("*", "*"), [])
        return exact + wildcard + global_exact + global_wildcard

    def evaluate(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        field_values: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> "MaterializationDecision":
        """Evaluate the policy for a record and return a MaterializationDecision.

        Resolution algorithm (per NEXUS-Iter2-SPEC-MaterializationPolicy §2.4):
          1. Filter rules: scope matches AND time window is active.
          2. Evaluate predicates against post-normalisation field values.
          3. Sort matching rules by (priority DESC, rule_type rank DESC, valid_until ASC).
          4. Return the target_level of the top rule.
          5. Falls back to WARM when no rule matches (governance-only default).

        NFR-D4-01: p95 ≤ 1ms per record (policy is a Spark broadcast — no I/O).
        """
        eval_now = now or datetime.utcnow()
        field_values = field_values or {}

        # Step 1 — filter by time window
        active: list[PolicyRule] = []
        for rule in self.get_rules(tenant_id, cdm_entity_type):
            if rule.valid_from and eval_now < rule.valid_from:
                continue
            if rule.valid_until and eval_now >= rule.valid_until:
                continue
            active.append(rule)

        # Step 2 — evaluate predicates; keep matching rules
        matching = [
            r for r in active
            if _eval_predicate(r.predicate, field_values, eval_now) is True
        ]

        # Step 3 — sort: priority DESC, rule_type rank DESC, valid_until ASC (soonest expiry first)
        _max_dt = datetime(9999, 12, 31)
        matching.sort(
            key=lambda r: (
                -r.priority,
                -_RULE_TYPE_RANK.get(r.rule_type, 0),
                r.valid_until or _max_dt,
            )
        )

        if matching:
            winner = matching[0]
            return MaterializationDecision(
                level=winner.target_level,
                applied_rule_id=winner.rule_id,
                evaluated_at=eval_now,
                predicate_debug=winner.predicate,
            )

        # Default: WARM (record enters governance but is not projected to AI stores)
        return MaterializationDecision(
            level=MaterializationLevel.WARM,
            applied_rule_id=None,
            evaluated_at=eval_now,
            predicate_debug="default:warm",
        )


@dataclass
class MaterializationAssignment:
    """Direct per-entity materialization assignment from cdm_entity_materialization.

    This is the MD-authoritative Stage 0 source when present. It bypasses
    predicate evaluation entirely and assigns the configured level for the
    (tenant_id, cdm_entity_type) pair.
    """

    tenant_id: str
    cdm_entity_type: str
    level: MaterializationLevel
    assigned_by: str = "default"
    updated_at: datetime | None = None


@dataclass
class MaterializationRuntimeConfig:
    """Stage 0 runtime view that prefers MD assignments and falls back to policy.

    `cdm_entity_materialization` is authoritative for Iteration 2 MD alignment.
    `materialization_policy` remains as a backward-compatible fallback so the
    existing Spark tests and local environments continue to work during the
    migration period.
    """

    assignments: dict[tuple[str, str], MaterializationAssignment] = field(default_factory=dict)
    policy: MaterializationPolicy | None = None
    snapshot_ts: datetime = field(default_factory=datetime.utcnow)

    def _get_assignment(self, tenant_id: str, cdm_entity_type: str) -> MaterializationAssignment | None:
        return self.assignments.get((tenant_id, cdm_entity_type)) or self.assignments.get((tenant_id, "*"))

    def evaluate(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        field_values: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> "MaterializationDecision":
        eval_now = now or datetime.utcnow()
        assignment = self._get_assignment(tenant_id, cdm_entity_type)
        if assignment is not None:
            return MaterializationDecision(
                level=assignment.level,
                applied_rule_id=f"cdm_entity_materialization:{tenant_id}:{assignment.cdm_entity_type}",
                evaluated_at=eval_now,
                predicate_debug=f"assignment:{assignment.assigned_by}",
            )

        if self.policy is not None:
            return self.policy.evaluate(
                tenant_id=tenant_id,
                cdm_entity_type=cdm_entity_type,
                field_values=field_values,
                now=eval_now,
            )

        return MaterializationDecision(
            level=MaterializationLevel.WARM,
            applied_rule_id=None,
            evaluated_at=eval_now,
            predicate_debug="default:warm",
        )


# ---------------------------------------------------------------------------
# Predicate evaluator
# ---------------------------------------------------------------------------

def _eval_predicate(
    predicate: str,
    field_values: dict[str, Any],
    now: datetime | None = None,
) -> bool | None:
    """Evaluate a materialization policy predicate against a record's field values.

    Grammar (NEXUS-Iter2-SPEC-MaterializationPolicy §2.2):
      expr         := or_expr
      or_expr      := and_expr ('OR' and_expr)*
      and_expr     := not_expr ('AND' not_expr)*
      not_expr     := 'NOT' not_expr | primary
      primary      := '(' expr ')'
                    | 'TRUE' | 'FALSE'
                    | attribute op literal
                    | attribute 'IN' '(' literal_list ')'
                    | attribute 'BETWEEN' literal 'AND' literal
                    | attribute 'MATCHES' regex_literal
                    | 'AGE' '(' attribute ')' op interval_literal
                    | 'COHORT' '(' cohort_id ')'

    Three-valued logic (SQL-style): None is the null/unknown result.
    Returns True/False/None. The caller must treat None as non-matching.
    """
    if now is None:
        now = datetime.utcnow()

    predicate = predicate.strip()
    if not predicate or predicate.strip() == "*":
        return True

    upper = predicate.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False

    # OR  (lowest precedence — split outermost first)
    or_parts = _split_top_level(predicate, "OR")
    if len(or_parts) > 1:
        results = [_eval_predicate(p, field_values, now) for p in or_parts]
        if any(r is True for r in results):
            return True
        if all(r is False for r in results):
            return False
        return None

    # AND
    and_parts = _split_top_level(predicate, "AND")
    if len(and_parts) > 1:
        results = [_eval_predicate(p, field_values, now) for p in and_parts]
        if any(r is False for r in results):
            return False
        if all(r is True for r in results):
            return True
        return None

    # NOT
    if upper.startswith("NOT "):
        inner_result = _eval_predicate(predicate[4:].strip(), field_values, now)
        return None if inner_result is None else not inner_result

    # Parentheses — check that the parens wrap the whole expression
    if predicate.startswith("("):
        close = _matching_close_paren(predicate, 0)
        if close == len(predicate) - 1:
            return _eval_predicate(predicate[1:-1].strip(), field_values, now)

    # AGE(attribute) op interval_literal
    m = re.match(
        r"^AGE\s*\(\s*(\w+)\s*\)\s*(<=?|>=?|!=|=)\s*'([^']+)'$",
        predicate, re.IGNORECASE,
    )
    if m:
        attr, op, interval_str = m.group(1), m.group(2), m.group(3)
        attr_val = field_values.get(attr)
        if attr_val is None:
            return None
        try:
            if isinstance(attr_val, str):
                attr_dt = datetime.fromisoformat(attr_val.replace("Z", "+00:00"))
            else:
                attr_dt = attr_val
            # Strip timezone for arithmetic with utcnow()
            attr_dt_naive = attr_dt.replace(tzinfo=None) if attr_dt.tzinfo else attr_dt
            age_seconds = (now - attr_dt_naive).total_seconds()
            interval_seconds = _parse_interval_seconds(interval_str)
            return _numeric_compare(age_seconds, op, interval_seconds)
        except Exception:
            return None

    # COHORT(cohort_id) — cohort membership injected as virtual field __cohort_<id>__
    m = re.match(r"^COHORT\s*\(\s*['\"]?(\w+)['\"]?\s*\)$", predicate, re.IGNORECASE)
    if m:
        cohort_id = m.group(1)
        return bool(field_values.get(f"__cohort_{cohort_id}__", False))

    # attribute IN (v1, v2, ...)
    m = re.match(r"^(\w+)\s+IN\s*\((.+)\)$", predicate, re.IGNORECASE)
    if m:
        attr, list_str = m.group(1), m.group(2)
        attr_val = field_values.get(attr)
        if attr_val is None:
            return None
        literals = [v.strip().strip("'\"") for v in list_str.split(",")]
        return str(attr_val) in literals

    # attribute BETWEEN lo AND hi
    m = re.match(
        r"^(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$",
        predicate, re.IGNORECASE,
    )
    if m:
        attr, lo_str, hi_str = m.group(1), m.group(2).strip(), m.group(3).strip()
        attr_val = field_values.get(attr)
        if attr_val is None:
            return None
        try:
            lo = _coerce_to_number(lo_str)
            hi = _coerce_to_number(hi_str)
            val = _coerce_to_number(str(attr_val))
            return lo <= val <= hi
        except Exception:
            # String BETWEEN: lexicographic
            try:
                lo_s = lo_str.strip("'\"")
                hi_s = hi_str.strip("'\"")
                return lo_s <= str(attr_val) <= hi_s
            except Exception:
                return None

    # attribute MATCHES regex
    m = re.match(r"^(\w+)\s+MATCHES\s+'([^']+)'$", predicate, re.IGNORECASE)
    if m:
        attr, pattern = m.group(1), m.group(2)
        attr_val = field_values.get(attr)
        if attr_val is None:
            return None
        try:
            return bool(re.search(pattern, str(attr_val)))
        except re.error:
            return None

    # Simple comparison: attribute op literal
    m = re.match(r"^(\w+)\s*(<=?|>=?|!=|=)\s*(.+)$", predicate)
    if m:
        attr, op, literal = m.group(1), m.group(2), m.group(3).strip().strip("'\"")
        attr_val = field_values.get(attr)
        if attr_val is None:
            return None
        # Try numeric comparison first, fall back to string
        try:
            return _numeric_compare(float(str(attr_val)), op, float(literal))
        except (ValueError, TypeError):
            return _string_compare(str(attr_val), op, literal)

    # Unknown predicate — safe default: no match (None propagates to non-firing)
    return None


# ---------------------------------------------------------------------------
# Predicate helpers
# ---------------------------------------------------------------------------

def _split_top_level(expr: str, keyword: str) -> list[str]:
    """Split expr on keyword that is not inside parentheses or single-quoted strings."""
    depth = 0
    in_quote = False
    between_pending = False
    parts: list[str] = []
    start = 0
    kw = f" {keyword.upper()} "
    kw_len = len(kw)
    i = 0
    while i < len(expr):
        c = expr[i]
        if c == "'" and not in_quote:
            in_quote = True
        elif c == "'" and in_quote:
            in_quote = False
        elif not in_quote:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and expr[i: i + len(" BETWEEN ")].upper() == " BETWEEN ":
                between_pending = True
            elif depth == 0 and expr[i: i + kw_len].upper() == kw:
                if keyword.upper() == "AND" and between_pending:
                    between_pending = False
                    i += kw_len - 1
                    continue
                parts.append(expr[start:i].strip())
                start = i + kw_len
                i += kw_len - 1
        i += 1
    parts.append(expr[start:].strip())
    return parts if len(parts) > 1 else [expr]


def _matching_close_paren(s: str, open_idx: int) -> int:
    """Return the index of the closing paren that matches s[open_idx]."""
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _parse_interval_seconds(interval_str: str) -> float:
    """Parse an interval like '30 days' or '2 years' into seconds."""
    m = re.match(
        r"(\d+(?:\.\d+)?)\s+"
        r"(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)",
        interval_str.strip(), re.IGNORECASE,
    )
    if not m:
        raise ValueError(f"Unparseable interval: {interval_str!r}")
    amount = float(m.group(1))
    unit = m.group(2).lower()
    table = {
        "second": 1, "seconds": 1,
        "minute": 60, "minutes": 60,
        "hour": 3600, "hours": 3600,
        "day": 86400, "days": 86400,
        "week": 604800, "weeks": 604800,
        "month": 2592000, "months": 2592000,   # 30 days
        "year": 31536000, "years": 31536000,    # 365 days
    }
    return amount * table[unit]


def _coerce_to_number(s: str) -> float:
    return float(s.strip().strip("'\""))


def _numeric_compare(a: float, op: str, b: float) -> bool:
    if op == "=":
        return a == b
    if op == "!=":
        return a != b
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == ">=":
        return a >= b
    return False


def _string_compare(a: str, op: str, b: str) -> bool:
    if op == "=":
        return a == b
    if op == "!=":
        return a != b
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == ">=":
        return a >= b
    return False


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

    Built from the normalised field values after Stage 0 normalise.
    """

    tenant_id: str
    cdm_entity_type: str
    field_values: dict[str, Any]        # canonical_attribute_name → normalised value
    evaluation_ts: datetime = field(default_factory=datetime.utcnow)
