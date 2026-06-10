"""Unit tests for Stage 2 outcome helpers."""

from __future__ import annotations

import unittest

from nexus_spark_lib.transform.stage2_resolve.er_outcomes import (
    infer_gr_operation,
    pick_signal_a_match,
    update_requires_reresolution,
)


class _FakeErIndex:
    def __init__(self) -> None:
        self.snapshot = {"t1|sf|Contact|001": "gr:old"}
        self.deterministic_columns = {("t1", "party"): ["tax_id"]}
        self.thresholds = {
            ("t1", "party"): {
                "weights": {"legal_name": 0.55, "industry": 0.10},
            }
        }


class TestErOutcomes(unittest.TestCase):
    def test_update_requires_weight_at_least_020(self) -> None:
        idx = _FakeErIndex()
        self.assertTrue(
            update_requires_reresolution(
                er_index=idx,
                tenant_id="t1",
                cdm_entity_type="party",
                source_op="UPDATE",
                changed_canonical_attributes_json='["legal_name"]',
            )
        )
        self.assertFalse(
            update_requires_reresolution(
                er_index=idx,
                tenant_id="t1",
                cdm_entity_type="party",
                source_op="UPDATE",
                changed_canonical_attributes_json='["industry"]',
            )
        )

    def test_infer_split_on_gr_change(self) -> None:
        self.assertEqual(
            infer_gr_operation(
                prior_cdm_entity_id="gr:old",
                new_cdm_entity_id="gr:new",
                source_op="UPDATE",
            ),
            "SPLIT",
        )

    def test_pick_signal_a_lexicographic(self) -> None:
        self.assertEqual(
            pick_signal_a_match(["gr:z", "gr:a", "gr:m"]),
            "gr:a",
        )


if __name__ == "__main__":
    unittest.main()
