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
            cdm_entity_type="contact",
            predicate="*",
            target_level=level,
            priority=100,
            rule_type="manual_override",
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
            cdm_entity_type="contact",
            predicate="*",
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
            cdm_entity_type="contact",
            predicate="*",
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
            rule_id="low", tenant_id="t1", cdm_entity_type="contact",
            predicate="*", target_level=MaterializationLevel.COLD, priority=10,
            rule_type="decay",
        )
        high = PolicyRule(
            rule_id="high", tenant_id="t1", cdm_entity_type="contact",
            predicate="*", target_level=MaterializationLevel.HOT, priority=100,
            rule_type="manual_override",
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
