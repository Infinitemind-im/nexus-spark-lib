"""Unit tests for Stage 3 — Synthesis."""

import json

import pytest
from unittest.mock import MagicMock

from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet, SurvivorshipRuleType, SurvivorshipRule
from nexus_spark_lib.transform.stage3_synthesise import _apply_rule


class TestApplyRule:
    def test_most_recent_returns_candidate(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, "Alice New", "2024-03-15", "sfdc", 0.9, [], "Alice Old"
        )
        assert result == "Alice New"

    def test_source_priority_wins(self):
        result = _apply_rule(
            SurvivorshipRuleType.SOURCE_PRIORITY, "Alice", "2024-01-01", "crm", 0.8,
            ["crm"], "Bob"
        )
        assert result == "Alice"

    def test_source_priority_non_priority_keeps_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.SOURCE_PRIORITY, "Alice", "2024-01-01", "unknown_src", 0.8,
            ["crm"], "Bob"
        )
        assert result == "Bob"

    def test_longest_value_wins(self):
        result = _apply_rule(
            SurvivorshipRuleType.LONGEST_VALUE, "AliceLongName", "2024-01-01", "sfdc", 0.9,
            [], "Bob"
        )
        assert result == "AliceLongName"

    def test_exact_match_agrees(self):
        result = _apply_rule(
            SurvivorshipRuleType.EXACT_MATCH, "Alice", "2024-01-01", "sfdc", 0.9,
            [], "Alice"
        )
        assert result == "Alice"

    def test_exact_match_disagrees_returns_none(self):
        result = _apply_rule(
            SurvivorshipRuleType.EXACT_MATCH, "Alice", "2024-01-01", "sfdc", 0.9,
            [], "Bob"
        )
        assert result is None

    def test_none_candidate_returns_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, None, "2024-01-01", "sfdc", 0.9, [], "Bob"
        )
        assert result == "Bob"

    def test_none_existing_returns_candidate(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, "Alice", "2024-01-01", "sfdc", 0.9, [], None
        )
        assert result == "Alice"


class TestStage3Integration:
    def test_synthesise_adds_columns(self, spark, mock_survivorship_broadcast):
        from pyspark.sql import Row
        from nexus_spark_lib.transform.stage3_synthesise import synthesise

        df = spark.createDataFrame([
            Row(
                tenant_id="tenant_acme",
                cdm_entity_type="contact",
                cdm_entity_id="gr:001",
                normalised_json=json.dumps({
                    "full_name": {"value": "Alice Smith", "quality": "good", "source_attribute": "full_name", "pii_flag": False}
                }),
                source_system="salesforce",
                source_record_id="003abc",
                source_ts="2024-03-01T12:00:00",
                dq_score="0.95",
            )
        ])
        result = synthesise(df, mock_survivorship_broadcast)
        cols = result.columns
        assert "golden_fields_json" in cols
        assert "provenance_hash" in cols
        row = result.collect()[0]
        assert row["provenance_hash"] is not None
