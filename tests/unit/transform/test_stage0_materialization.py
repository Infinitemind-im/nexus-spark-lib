"""Unit tests for Stage 0 — Materialization tier evaluation."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import MapType, StringType, StructField, StructType, TimestampType

from nexus_spark_lib.models.materialization import (
    MaterializationAssignment,
    MaterializationDecision,
    MaterializationLevel,
    MaterializationPolicy,
    MaterializationRuntimeConfig,
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


class TestMaterializationRuntimeConfig:
    def test_assignment_takes_precedence_over_policy(self):
        policy = MaterializationPolicy()
        policy.rules_by_scope[("t1", "contact")] = [
            PolicyRule(
                rule_id="hot-policy",
                tenant_id="t1",
                scope="contact",
                predicate="TRUE",
                target_level=MaterializationLevel.HOT,
                priority=100,
                rule_type="manual",
            )
        ]
        config = MaterializationRuntimeConfig(
            assignments={
                ("t1", "contact"): MaterializationAssignment(
                    tenant_id="t1",
                    cdm_entity_type="contact",
                    level=MaterializationLevel.COLD,
                    assigned_by="md",
                )
            },
            policy=policy,
        )

        decision = config.evaluate("t1", "contact")

        assert decision.level == MaterializationLevel.COLD
        assert decision.applied_rule_id == "cdm_entity_materialization:t1:contact"

    def test_assignment_runtime_falls_back_to_policy_when_unassigned(self):
        policy = MaterializationPolicy()
        policy.rules_by_scope[("t1", "contact")] = [
            PolicyRule(
                rule_id="warm-policy",
                tenant_id="t1",
                scope="contact",
                predicate="TRUE",
                target_level=MaterializationLevel.WARM,
                priority=100,
                rule_type="base",
            )
        ]
        config = MaterializationRuntimeConfig(policy=policy)

        decision = config.evaluate("t1", "contact")

        assert decision.level == MaterializationLevel.WARM
        assert decision.applied_rule_id == "warm-policy"


class TestStage0Integration:
    def test_materialisation_gate_adds_columns(self, spark, mock_cdm_mapping_broadcast, mock_policy_broadcast):
        from nexus_spark_lib.transform.stage0_materialization import materialization_gate

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("source_record_id", StringType(), False),
            StructField("source_op", StringType(), False),
            StructField("source_ts", TimestampType(), True),
            StructField("after_payload", MapType(StringType(), StringType()), True),
            StructField("before_payload", MapType(StringType(), StringType()), True),
        ])

        df = spark.createDataFrame(
            [(
                "tenant_acme",
                "conn_salesforce",
                "Contact",
                "r1",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice"},
                None,
            )],
            schema=schema,
        )
        result = materialization_gate(df, mock_cdm_mapping_broadcast, mock_policy_broadcast)
        cols = result.columns
        assert "cdm_entity_type" in cols
        assert "materialization_level" in cols
        assert "materialization_rule_id" in cols
        row = result.collect()[0]
        assert row["cdm_entity_type"] == "contact"
        assert row["materialization_level"] == "hot"

    def test_materialisation_gate_accepts_md_assignment_runtime_config(self, spark, mock_cdm_mapping_broadcast):
        from nexus_spark_lib.transform.stage0_materialization import materialization_gate

        runtime_config = MaterializationRuntimeConfig(
            assignments={
                ("tenant_acme", "contact"): MaterializationAssignment(
                    tenant_id="tenant_acme",
                    cdm_entity_type="contact",
                    level=MaterializationLevel.HOT,
                    assigned_by="md",
                )
            }
        )
        broadcast = MagicMock()
        broadcast.value = runtime_config

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("source_record_id", StringType(), False),
            StructField("source_op", StringType(), False),
            StructField("source_ts", TimestampType(), True),
            StructField("after_payload", MapType(StringType(), StringType()), True),
            StructField("before_payload", MapType(StringType(), StringType()), True),
        ])

        df = spark.createDataFrame(
            [(
                "tenant_acme",
                "conn_salesforce",
                "Contact",
                "r1",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice"},
                None,
            )],
            schema=schema,
        )

        result = materialization_gate(df, mock_cdm_mapping_broadcast, broadcast)
        row = result.collect()[0]

        assert row["materialization_level"] == "hot"
        assert row["materialization_rule_id"] == "cdm_entity_materialization:tenant_acme:contact"

    def test_materialisation_gate_falls_back_to_source_system(self, spark, mock_policy_broadcast):
        from nexus_spark_lib.transform.stage0_materialization import materialization_gate

        mapping = MagicMock()

        def _get_cdm_entity_type(lookup_key, source_table):
            if lookup_key == "salesforce" and source_table == "Contact":
                return "contact"
            return "unknown"

        mapping.get_cdm_entity_type.side_effect = _get_cdm_entity_type
        broadcast = MagicMock()
        broadcast.value = mapping

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("source_table", StringType(), False),
            StructField("source_record_id", StringType(), False),
            StructField("source_op", StringType(), False),
            StructField("source_ts", TimestampType(), True),
            StructField("after_payload", MapType(StringType(), StringType()), True),
            StructField("before_payload", MapType(StringType(), StringType()), True),
        ])

        df = spark.createDataFrame(
            [(
                "tenant_acme",
                "salesforce-prod",
                "salesforce",
                "Contact",
                "r1",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice"},
                None,
            )],
            schema=schema,
        )

        result = materialization_gate(df, broadcast, mock_policy_broadcast)
        row = result.collect()[0]

        assert row["cdm_entity_type"] == "contact"
        assert row["materialization_level"] == "hot"

    def test_materialisation_gate_passes_tenant_to_mapping_lookup(self, spark, mock_policy_broadcast):
        from nexus_spark_lib.transform.stage0_materialization import materialization_gate

        mapping = MagicMock()

        def _get_cdm_entity_type(tenant_id, lookup_key, source_table):
            if tenant_id == "tenant_acme" and lookup_key == "salesforce" and source_table == "Contact":
                return "contact"
            return "unknown"

        mapping.get_cdm_entity_type.side_effect = _get_cdm_entity_type
        broadcast = MagicMock()
        broadcast.value = mapping

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("source_table", StringType(), False),
            StructField("source_record_id", StringType(), False),
            StructField("source_op", StringType(), False),
            StructField("source_ts", TimestampType(), True),
            StructField("after_payload", MapType(StringType(), StringType()), True),
            StructField("before_payload", MapType(StringType(), StringType()), True),
        ])

        df = spark.createDataFrame(
            [(
                "tenant_acme",
                "salesforce-prod",
                "salesforce",
                "Contact",
                "r1",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice"},
                None,
            )],
            schema=schema,
        )

        result = materialization_gate(df, broadcast, mock_policy_broadcast)
        row = result.collect()[0]

        assert row["cdm_entity_type"] == "contact"


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

    def test_global_tenant_rules_apply_to_real_tenants(self):
        """Rules stored under tenant_id='*' are inherited by tenant-specific evaluations."""
        policy = MaterializationPolicy()
        global_rule = PolicyRule(
            rule_id="global-hot", tenant_id="*", scope="transaction",
            predicate="TRUE", target_level=MaterializationLevel.HOT,
            priority=10, rule_type="base",
        )
        policy.rules_by_scope[("*", "transaction")] = [global_rule]

        decision = policy.evaluate("asensia189", "transaction")

        assert decision.level == MaterializationLevel.HOT
        assert decision.applied_rule_id == "global-hot"

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

        from nexus_spark_lib.transform.stage0_materialization import drop_cold

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
