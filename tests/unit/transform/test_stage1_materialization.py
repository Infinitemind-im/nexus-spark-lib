"""Unit tests for Stage 0 — Materialization tier evaluation."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from nexus_spark_lib.models.materialization import (
    MaterializationDecision,
    MaterializationLevel,
    MaterializationPolicy,
    PolicyRule,
)


class TestMaterializationPolicy:
    def _make_policy(self, level: MaterializationLevel = MaterializationLevel.HOT) -> MaterializationPolicy:
        policy = MaterializationPolicy()
        rule = PolicyRule(
            rule_id="r1",
            tenant_id="t1",
            scope="contact",
            predicate="TRUE",
            target_level=level,
            priority=100,
            rule_type="manual",
        )
        policy.rules_by_scope[("t1", "contact")] = [rule]
        return policy

    def test_hot_rule_matches(self):
        policy = self._make_policy(MaterializationLevel.HOT)
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.HOT
        assert decision.applied_rule_id == "r1"

    def test_no_matching_rule_defaults_to_warm(self):
        policy = MaterializationPolicy()
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.WARM
        assert decision.applied_rule_id is None

    def test_expired_rule_is_skipped(self):
        policy = MaterializationPolicy()
        rule = PolicyRule(
            rule_id="r2",
            tenant_id="t1",
            scope="contact",
            predicate="TRUE",
            target_level=MaterializationLevel.HOT,
            priority=100,
            rule_type="decay",
            valid_until=datetime(2000, 1, 1),  # expired
        )
        policy.rules_by_scope[("t1", "contact")] = [rule]
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.WARM

    def test_not_yet_valid_rule_is_skipped(self):
        policy = MaterializationPolicy()
        rule = PolicyRule(
            rule_id="r3",
            tenant_id="t1",
            scope="contact",
            predicate="TRUE",
            target_level=MaterializationLevel.HOT,
            priority=100,
            rule_type="boost",
            valid_from=datetime(2099, 1, 1),  # future
        )
        policy.rules_by_scope[("t1", "contact")] = [rule]
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.WARM

    def test_priority_ordering(self):
        """Higher priority rule wins."""
        policy = MaterializationPolicy()
        low = PolicyRule(
            rule_id="low", tenant_id="t1", scope="contact",
            predicate="TRUE", target_level=MaterializationLevel.COLD, priority=10,
            rule_type="decay",
        )
        high = PolicyRule(
            rule_id="high", tenant_id="t1", scope="contact",
            predicate="TRUE", target_level=MaterializationLevel.HOT, priority=100,
            rule_type="manual",
        )
        # Note: rules must be pre-sorted by priority DESC (done by survivorship_rules loader)
        policy.rules_by_scope[("t1", "contact")] = [high, low]
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.HOT
        assert decision.applied_rule_id == "high"


class TestStage0Integration:
    def test_materialisation_decide_adds_columns(self, spark, mock_policy_broadcast):
        from pyspark.sql import Row

        from nexus_spark_lib.transform.stage1_materialization import materialization_decide

        df = spark.createDataFrame([
            Row(tenant_id="tenant_acme", cdm_entity_type="contact", source_record_id="r1")
        ])
        result = materialization_decide(df, mock_policy_broadcast)
        cols = result.columns
        assert "materialization_level" in cols
        assert "materialization_rule_id" in cols
        row = result.collect()[0]
        assert row["materialization_level"] == "hot"


class TestPredicateEvaluator:
    """Unit tests for the expanded _eval_predicate grammar."""

    def _make_rule(self, predicate: str, level=MaterializationLevel.HOT) -> PolicyRule:
        return PolicyRule(
            rule_id="rx", tenant_id="t1", scope="contact",
            predicate=predicate, target_level=level,
            priority=100, rule_type="base",
        )

    def _evaluate(self, predicate: str, fields: dict) -> MaterializationLevel:
        policy = MaterializationPolicy()
        policy.rules_by_scope[("t1", "contact")] = [self._make_rule(predicate)]
        return policy.evaluate("t1", "contact", field_values=fields).level

    def test_true_predicate_always_fires(self):
        assert self._evaluate("TRUE", {}) == MaterializationLevel.HOT

    def test_and_both_true(self):
        assert self._evaluate("industry = 'Healthcare' AND status = 'active'",
                              {"industry": "Healthcare", "status": "active"}) == MaterializationLevel.HOT

    def test_and_one_false(self):
        assert self._evaluate("industry = 'Healthcare' AND status = 'active'",
                              {"industry": "Healthcare", "status": "closed"}) == MaterializationLevel.WARM

    def test_or_one_true(self):
        assert self._evaluate("industry = 'Healthcare' OR industry = 'Finance'",
                              {"industry": "Finance"}) == MaterializationLevel.HOT

    def test_not_negates(self):
        assert self._evaluate("NOT status = 'archived'",
                              {"status": "active"}) == MaterializationLevel.HOT
        assert self._evaluate("NOT status = 'archived'",
                              {"status": "archived"}) == MaterializationLevel.WARM

    def test_in_list(self):
        assert self._evaluate("industry IN ('Healthcare', 'Finance', 'Tech')",
                              {"industry": "Finance"}) == MaterializationLevel.HOT
        assert self._evaluate("industry IN ('Healthcare', 'Finance', 'Tech')",
                              {"industry": "Retail"}) == MaterializationLevel.WARM

    def test_between_numeric(self):
        assert self._evaluate("annual_revenue BETWEEN 1000000 AND 5000000",
                              {"annual_revenue": "2500000"}) == MaterializationLevel.HOT
        assert self._evaluate("annual_revenue BETWEEN 1000000 AND 5000000",
                              {"annual_revenue": "500000"}) == MaterializationLevel.WARM

    def test_numeric_comparison(self):
        assert self._evaluate("score >= 0.9", {"score": "0.95"}) == MaterializationLevel.HOT
        assert self._evaluate("score >= 0.9", {"score": "0.5"}) == MaterializationLevel.WARM

    def test_null_field_returns_no_match(self):
        assert self._evaluate("annual_revenue > 1000000", {}) == MaterializationLevel.WARM

    def test_wildcard_scope_applies_to_all_types(self):
        """Rules with scope='*' apply to any cdm_entity_type."""
        policy = MaterializationPolicy()
        wildcard_rule = PolicyRule(
            rule_id="wc", tenant_id="t1", scope="*",
            predicate="TRUE", target_level=MaterializationLevel.HOT,
            priority=50, rule_type="base",
        )
        policy.rules_by_scope[("t1", "*")] = [wildcard_rule]
        # Should match any entity type
        assert policy.evaluate("t1", "Party").level == MaterializationLevel.HOT
        assert policy.evaluate("t1", "Transaction.Invoice").level == MaterializationLevel.HOT

    def test_rule_type_tiebreak_manual_beats_decay(self):
        """Same priority: manual beats decay."""
        policy = MaterializationPolicy()
        decay = PolicyRule(
            rule_id="dec", tenant_id="t1", scope="contact",
            predicate="TRUE", target_level=MaterializationLevel.COLD,
            priority=100, rule_type="decay",
        )
        manual = PolicyRule(
            rule_id="man", tenant_id="t1", scope="contact",
            predicate="TRUE", target_level=MaterializationLevel.HOT,
            priority=100, rule_type="manual",
        )
        policy.rules_by_scope[("t1", "contact")] = [decay, manual]
        decision = policy.evaluate("t1", "contact")
        assert decision.level == MaterializationLevel.HOT
        assert decision.applied_rule_id == "man"


class TestDropCold:
    def test_drop_cold_removes_cold_records(self, spark):
        from pyspark.sql import Row

        from nexus_spark_lib.transform.stage1_materialization import drop_cold

        df = spark.createDataFrame([
            Row(tenant_id="t1", materialization_level="hot"),
            Row(tenant_id="t1", materialization_level="warm"),
            Row(tenant_id="t1", materialization_level="cold"),
        ])
        result = drop_cold(df)
        levels = {r["materialization_level"] for r in result.collect()}
        assert "cold" not in levels
        assert "hot" in levels
        assert "warm" in levels
