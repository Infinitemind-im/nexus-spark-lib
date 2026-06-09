"""Unit tests for Kafka envelope + entity_routed builders (no Spark session)."""

from __future__ import annotations

import json
import unittest

from nexus_spark_lib.kafka.entity_routed import build_entity_routed_payload


class TestEntityRoutedPayload(unittest.TestCase):
    def test_build_entity_routed_includes_effective_level(self) -> None:
        body = build_entity_routed_payload(
            tenant_id="t1",
            cdm_entity_id="gr:acme",
            cdm_entity_type="Party.Organisation",
            materialization_level="warm",
            entity_store_materialization="warm",
            fields={"name": "Acme"},
        )
        self.assertEqual(body["materialization_level"], "warm")
        self.assertEqual(body["effective_materialization_level"], "warm")
        self.assertEqual(body["entity_store_materialization"], "warm")
        self.assertEqual(body["cdm_fields"]["name"], "Acme")

    def test_build_entity_routed_fields_array(self) -> None:
        fields = [{"attribute_name": "name", "value": "Acme"}]
        body = build_entity_routed_payload(
            tenant_id="t1",
            cdm_entity_id="gr:1",
            cdm_entity_type="party",
            materialization_level="hot",
            fields=fields,
        )
        self.assertEqual(body["fields"], fields)


if __name__ == "__main__":
    unittest.main()
