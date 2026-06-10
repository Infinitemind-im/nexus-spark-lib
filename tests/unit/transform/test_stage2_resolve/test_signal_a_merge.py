"""Signal A deterministic multi-match tests."""

from __future__ import annotations

import unittest

from nexus_spark_lib.db.er_broadcast_loader import register_entity_fields
from nexus_spark_lib.models.er_resolve_index import ErResolveIndex
from nexus_spark_lib.transform.stage2_resolve.signals.signal_a_deterministic import (
    run_signal_a,
    run_signal_a_collect_matches,
)


class TestSignalAMerge(unittest.TestCase):
    def test_picks_lexicographic_when_multiple_index_keys(self) -> None:
        idx = ErResolveIndex()
        idx.snapshot["t1|party|domain:globex.com"] = "gr:beta"
        idx.snapshot["t1|party|tax_id:BE1"] = "gr:alpha"
        register_entity_fields(
            idx,
            tenant_id="t1",
            cdm_entity_type="party",
            cdm_entity_id="gr:alpha",
            fields={"tax_id": {"value": "BE1"}, "domain": {"value": "globex.com"}},
            deterministic_columns=["tax_id", "domain"],
        )
        matches = run_signal_a_collect_matches(
            "t1",
            "party",
            {"tax_id": {"value": "BE1"}, "domain": {"value": "globex.com"}},
            idx,
        )
        self.assertEqual(matches, ["gr:alpha"])
        self.assertEqual(
            run_signal_a(
                "t1",
                "party",
                {"tax_id": {"value": "BE1"}, "domain": {"value": "globex.com"}},
                idx,
            ),
            "gr:alpha",
        )


if __name__ == "__main__":
    unittest.main()
