"""resolve_record() driver helper tests."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from nexus_spark_lib.db.er_broadcast_loader import register_entity_fields
from nexus_spark_lib.models.er_resolve_index import ErResolveIndex
from nexus_spark_lib.transform.stage2_resolve.resolve_record import ResolutionAction, resolve_record


def _field(value: str) -> dict:
    return {"value": value}


class TestResolveRecord(unittest.TestCase):
    def test_signal_a_path(self) -> None:
        idx = ErResolveIndex()
        register_entity_fields(
            idx,
            tenant_id="t1",
            cdm_entity_type="contact",
            cdm_entity_id="gr:1",
            fields={"tax_id": _field("BE1")},
            deterministic_columns=["tax_id"],
        )
        out = resolve_record(
            tenant_id="t1",
            cdm_entity_type="contact",
            source_connector="sf",
            source_record_id="r1",
            fields={"tax_id": _field("BE1")},
            er_index=idx,
            apply_materialization_gate=False,
        )
        self.assertEqual(out.cdm_entity_id, "gr:1")
        self.assertEqual(out.resolution_method, "signal_a")
        self.assertEqual(out.action, ResolutionAction.AUTO_APPLY)

    def test_materialization_gate_wired(self) -> None:
        idx = ErResolveIndex()
        register_entity_fields(
            idx,
            tenant_id="t1",
            cdm_entity_type="contact",
            cdm_entity_id="gr:1",
            fields={"tax_id": _field("BE1")},
            deterministic_columns=["tax_id"],
        )
        hot = MagicMock()
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.resolve_record.run_materialization_gate",
            return_value=hot,
        ) as gate:
            out = resolve_record(
                tenant_id="t1",
                cdm_entity_type="contact",
                source_connector="sf",
                source_record_id="r1",
                fields={"tax_id": _field("BE1")},
                er_index=idx,
                system_dsn="postgresql://x",
                apply_materialization_gate=True,
            )
        gate.assert_called_once()
        self.assertIs(out.materialization, hot)


if __name__ == "__main__":
    unittest.main()
