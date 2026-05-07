"""Load survivorship rules and materialization policy from PostgreSQL."""

from __future__ import annotations

import asyncpg

from nexus_spark_lib.models.materialization import MaterializationLevel, MaterializationPolicy, PolicyRule
from nexus_spark_lib.models.survivorship import SurvivorshipRule, SurvivorshipRuleSet, SurvivorshipRuleType
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


async def load_survivorship_rules(conn: asyncpg.Connection) -> SurvivorshipRuleSet:
    """Load all survivorship rules from nexus_system.survivorship_rules."""
    rows = await conn.fetch(
        """
        SELECT tenant_id, cdm_entity_type, attribute_name, rule_type,
               priority_sources, fallback_rule_type
        FROM nexus_system.survivorship_rules
        WHERE superseded_at IS NULL
        """
    )
    ruleset = SurvivorshipRuleSet()
    for r in rows:
        rule = SurvivorshipRule(
            tenant_id=r["tenant_id"],
            cdm_entity_type=r["cdm_entity_type"],
            attribute_name=r["attribute_name"],
            rule_type=SurvivorshipRuleType(r["rule_type"]),
            priority_sources=r["priority_sources"] or [],
            fallback_rule_type=(
                SurvivorshipRuleType(r["fallback_rule_type"])
                if r["fallback_rule_type"]
                else None
            ),
        )
        ruleset.rules[(r["tenant_id"], r["cdm_entity_type"], r["attribute_name"])] = rule
    logger.info("Loaded %d survivorship rules", len(ruleset.rules))
    return ruleset


async def load_materialization_policy(conn: asyncpg.Connection) -> MaterializationPolicy:
    """Load all active materialization policy rules from nexus_system.materialization_policy.

    Active = valid_until IS NULL (always-on) OR valid_until > NOW() (not yet expired).
    Expired rules are excluded so the broadcast stays small; the daily
    materialization-recommend job purges them from the table.
    """
    rows = await conn.fetch(
        """
        SELECT rule_id, tenant_id, scope, predicate, target_level,
               priority, rule_type, valid_from, valid_until,
               source, learned_metadata
        FROM nexus_system.materialization_policy
        WHERE (valid_until IS NULL OR valid_until > NOW())
        ORDER BY tenant_id, scope, priority DESC
        """
    )
    policy = MaterializationPolicy()
    for r in rows:
        rule = PolicyRule(
            rule_id=str(r["rule_id"]),
            tenant_id=str(r["tenant_id"]),
            scope=r["scope"],
            predicate=r["predicate"] or "TRUE",
            target_level=MaterializationLevel(r["target_level"]),
            priority=int(r["priority"]),
            rule_type=r["rule_type"],
            valid_from=r["valid_from"],
            valid_until=r["valid_until"],
            source=r["source"] or "system",
            learned_metadata=dict(r["learned_metadata"]) if r["learned_metadata"] else None,
        )
        key = (str(r["tenant_id"]), r["scope"])
        policy.rules_by_scope.setdefault(key, []).append(rule)
    logger.info("Loaded materialization policy: %d scopes", len(policy.rules_by_scope))
    return policy


async def load_er_thresholds(conn: asyncpg.Connection, tenant_id: str) -> dict:
    """Load ER thresholds for a tenant from nexus_system.er_thresholds."""
    rows = await conn.fetch(
        """
        SELECT cdm_entity_type, weights, auto_apply_threshold, review_lower_bound
        FROM nexus_system.er_thresholds
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return {
        r["cdm_entity_type"]: {
            "weights": r["weights"],
            "auto_apply_threshold": float(r["auto_apply_threshold"]),
            "review_lower_bound": float(r["review_lower_bound"]),
        }
        for r in rows
    }


async def load_deterministic_id_columns(conn: asyncpg.Connection) -> dict[tuple[str, str], list[str]]:
    """Load deterministic ID columns for all tenants/entity types."""
    rows = await conn.fetch(
        "SELECT tenant_id, cdm_entity_type, attribute_name FROM nexus_system.deterministic_id_columns"
    )
    result: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        key = (r["tenant_id"], r["cdm_entity_type"])
        result.setdefault(key, []).append(r["attribute_name"])
    return result
