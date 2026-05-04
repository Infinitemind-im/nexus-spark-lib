"""Unit tests for Signal A — Deterministic entity resolution."""

from nexus_spark_lib.transform.stage2_resolve.signals.signal_a_deterministic import run_signal_a
from unittest.mock import MagicMock


def _make_er_index(snapshot: dict, det_columns: dict) -> MagicMock:
    er_index = MagicMock()
    er_index.snapshot = snapshot
    er_index.deterministic_columns = det_columns
    return er_index


class TestSignalA:
    def test_exact_match_returns_entity_id(self):
        er_index = _make_er_index(
            snapshot={"tenant1|contact|tax_id:12345": "gr:abc123"},
            det_columns={("tenant1", "contact"): ["tax_id"]},
        )
        fields = {"tax_id": {"value": "12345"}}
        result = run_signal_a("tenant1", "contact", fields, er_index)
        assert result == "gr:abc123"

    def test_no_match_returns_none(self):
        er_index = _make_er_index(
            snapshot={},
            det_columns={("tenant1", "contact"): ["tax_id"]},
        )
        fields = {"tax_id": {"value": "99999"}}
        result = run_signal_a("tenant1", "contact", fields, er_index)
        assert result is None

    def test_no_deterministic_columns_returns_none(self):
        er_index = _make_er_index(snapshot={}, det_columns={})
        fields = {"full_name": {"value": "Alice"}}
        result = run_signal_a("tenant1", "contact", fields, er_index)
        assert result is None

    def test_empty_field_value_skipped(self):
        er_index = _make_er_index(
            snapshot={"tenant1|contact|tax_id:": "gr:xyz"},
            det_columns={("tenant1", "contact"): ["tax_id"]},
        )
        # Empty value should NOT match (key with empty string is not a valid lookup)
        fields = {"tax_id": {"value": ""}}
        result = run_signal_a("tenant1", "contact", fields, er_index)
        assert result is None

    def test_multiple_columns_first_match_wins(self):
        er_index = _make_er_index(
            snapshot={
                "t1|contact|passport:P123": "gr:passport_match",
                "t1|contact|tax_id:T456": "gr:tax_match",
            },
            det_columns={("t1", "contact"): ["tax_id", "passport"]},
        )
        fields = {
            "tax_id": {"value": "T456"},
            "passport": {"value": "P123"},
        }
        # tax_id is first in list → should match tax_match
        result = run_signal_a("t1", "contact", fields, er_index)
        assert result == "gr:tax_match"
