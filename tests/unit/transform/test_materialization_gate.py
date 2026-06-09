"""Unit tests for materialization gate + entity_store_presence (Spark ER Op 3)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from nexus_spark_lib.models.entity_store_presence import EntityStorePresence, EntityStoreState
from nexus_spark_lib.transform.stage2_resolve.materialization_gate import run_materialization_gate


class TestMaterializationGate(unittest.TestCase):
    def _run(
        self,
        *,
        presence: EntityStorePresence | None,
        fast_path: str | None = None,
        signal_a: str | None = None,
    ):
        er = MagicMock()
        er.fast_path.return_value = fast_path
        er.signal_a.return_value = signal_a

        reader = MagicMock()
        reader.get.return_value = presence

        return run_materialization_gate(
            tenant_id="tenant_abc",
            cdm_entity_type="Party.Organisation",
            source_connector="salesforce-tenant-abc",
            source_record_id="0010Y00000XxXxXXAA",
            fields={"tax_id": {"value": "BE0123456789"}},
            system_dsn="postgresql://unused",
            er_index=er,
            presence_reader=reader,
        )

    def test_hot_proceeds_full_pipeline(self) -> None:
        hot = EntityStorePresence("tenant_abc", "gr:acme", True, True, True)
        out = self._run(presence=hot, signal_a="gr:acme")
        self.assertEqual(out.materialization, EntityStoreState.HOT)
        self.assertTrue(out.proceed_pipeline)
        self.assertTrue(out.write_ai_stores)
        self.assertEqual(out.resolution_method, "signal_a")

    def test_warm_delta_only(self) -> None:
        warm = EntityStorePresence("tenant_abc", "gr:acme", False, False, False)
        out = self._run(presence=warm, signal_a="gr:acme")
        self.assertEqual(out.materialization, EntityStoreState.WARM)
        self.assertTrue(out.proceed_pipeline)
        self.assertFalse(out.write_ai_stores)

    def test_cold_register_only(self) -> None:
        out = self._run(presence=None, signal_a="gr:acme")
        self.assertEqual(out.materialization, EntityStoreState.COLD)
        self.assertFalse(out.proceed_pipeline)
        self.assertTrue(out.register_er_index_only)
        self.assertEqual(out.skip_reason, "entity_store_cold")

    def test_unresolved(self) -> None:
        er = MagicMock()
        er.fast_path.return_value = None
        er.signal_a.return_value = None
        reader = MagicMock()
        out = run_materialization_gate(
            tenant_id="t",
            cdm_entity_type="Party.Organisation",
            source_connector="sf",
            source_record_id="x",
            fields={},
            system_dsn="postgresql://unused",
            er_index=er,
            presence_reader=reader,
        )
        self.assertIsNone(out.cdm_entity_id)
        self.assertEqual(out.skip_reason, "entity_unresolved")


if __name__ == "__main__":
    unittest.main()
