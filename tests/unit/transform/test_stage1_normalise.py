"""Unit tests for Stage 1 — Normalisation (business logic only).

Stage 1 runs AFTER stage0_materialization. cdm_entity_type is already set
on the DataFrame by Stage 0 and is passed into the normalise UDF as an
explicit column — not re-resolved from CDM mappings.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import MapType, StringType, StructField, StructType, TimestampType


class TestStage1NormaliseIntegration:
    def test_normalise_adds_columns(self, spark, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast):
        from datetime import datetime

        from nexus_spark_lib.transform.stage1_normalise import normalise

        # cdm_entity_type is pre-set by Stage 0 (materialization gate)
        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "contact",
                "hot",
                "003abc",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice Smith", "email": "alice@acme.com"},
                None,
            )],
            schema=schema,
        )
        result = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
        cols = result.columns
        # cdm_entity_type already in df (from Stage 0) — Stage 1 does NOT add it
        assert "cdm_entity_type" in cols
        assert "normalised_json" in cols
        assert "dq_score" in cols
        assert "blocking_key" in cols
        assert "changed_canonical_attributes_json" in cols
        row = result.collect()[0]
        assert row["cdm_entity_type"] == "contact"
        # blocking_key should be a non-empty hash string
        assert row["blocking_key"] is not None
        assert row["blocking_key"].startswith("gr:")
        assert json.loads(row["changed_canonical_attributes_json"]) == []

    def test_normalise_emits_changed_canonical_attributes_for_update(self, spark, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast):
        from datetime import datetime

        from nexus_spark_lib.transform.stage1_normalise import normalise

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "contact",
                "hot",
                "003abc",
                "UPDATE",
                datetime(2024, 3, 1),
                {"full_name": "Alice Smith", "email": "alice.new@acme.com"},
                {"full_name": "Alice Smith", "email": "alice@acme.com"},
            )],
            schema=schema,
        )

        result = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
        row = result.collect()[0]

        assert json.loads(row["changed_canonical_attributes_json"]) == ["email"]

    def test_normalise_falls_back_to_source_system_mapping(self, spark, mock_fx_rates_broadcast):
        from datetime import datetime

        from nexus_spark_lib.transform.stage1_normalise import normalise

        mapping = MagicMock()

        def _get_field_map(lookup_key, source_table):
            if lookup_key == "salesforce" and source_table == "Contact":
                return {
                    "full_name": "full_name",
                    "email": "email",
                    "__meta__full_name": {"type": "string"},
                    "__meta__email": {"type": "string"},
                }
            return {}

        mapping.get_field_map.side_effect = _get_field_map
        broadcast = MagicMock()
        broadcast.value = mapping

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "contact",
                "hot",
                "003abc",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice Smith", "email": "alice@acme.com"},
                None,
            )],
            schema=schema,
        )

        result = normalise(df, broadcast, mock_fx_rates_broadcast)
        row = result.collect()[0]
        normalised = json.loads(row["normalised_json"])

        assert normalised["full_name"]["value"] == "Alice Smith"
        assert normalised["email"]["value"] == "alice@acme.com"

    def test_normalise_passes_tenant_to_field_map_lookup(self, spark, mock_fx_rates_broadcast):
        from datetime import datetime

        from nexus_spark_lib.transform.stage1_normalise import normalise

        mapping = MagicMock()

        def _get_field_map(tenant_id, lookup_key, source_table):
            if tenant_id == "tenant_acme" and lookup_key == "salesforce" and source_table == "Contact":
                return {
                    "full_name": "full_name",
                    "email": "email",
                    "__meta__full_name": {"type": "string"},
                    "__meta__email": {"type": "string"},
                }
            return {}

        mapping.get_field_map.side_effect = _get_field_map
        broadcast = MagicMock()
        broadcast.value = mapping

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "contact",
                "hot",
                "003abc",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice Smith", "email": "alice@acme.com"},
                None,
            )],
            schema=schema,
        )

        result = normalise(df, broadcast, mock_fx_rates_broadcast)
        row = result.collect()[0]
        normalised = json.loads(row["normalised_json"])

        assert normalised["full_name"]["value"] == "Alice Smith"
        assert normalised["email"]["value"] == "alice@acme.com"

    def test_normalise_falls_back_to_source_record_id_when_blocking_rules_missing(
        self,
        spark,
        mock_cdm_mapping_broadcast,
        mock_fx_rates_broadcast,
    ):
        from datetime import datetime

        from nexus_spark_lib._internal.hash_utils import blocking_key_hash
        from nexus_spark_lib.transform.stage1_normalise import normalise

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "contact",
                "hot",
                "003abc",
                "INSERT",
                datetime(2024, 3, 1),
                {"full_name": "Alice Smith", "email": "alice@acme.com"},
                None,
            )],
            schema=schema,
        )

        row = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast).collect()[0]
        assert row["blocking_key"] == blocking_key_hash("tenant_acme", "contact", "003abc")

    def test_normalise_uses_identifier_like_field_before_source_record_id_when_rules_missing(
        self,
        spark,
        mock_fx_rates_broadcast,
    ):
        from datetime import datetime
        from unittest.mock import MagicMock

        from nexus_spark_lib._internal.hash_utils import blocking_key_hash
        from nexus_spark_lib.transform.stage1_normalise import normalise

        mapping = MagicMock()
        mapping.get_field_map.return_value = {
            "Id": "reference.id",
            "Name": "name",
            "__meta__Id": {"type": "string"},
            "__meta__Name": {"type": "string"},
        }
        broadcast = MagicMock()
        broadcast.value = mapping

        schema = StructType([
            StructField("tenant_id", StringType(), False),
            StructField("connector_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("cdm_entity_type", StringType(), False),
            StructField("materialization_level", StringType(), False),
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
                "Opportunity",
                "transaction",
                "hot",
                "src-001",
                "INSERT",
                datetime(2024, 3, 1),
                {"Id": "OPP-001", "Name": "Big Deal"},
                None,
            )],
            schema=schema,
        )

        row = normalise(df, broadcast, mock_fx_rates_broadcast).collect()[0]
        assert row["blocking_key"] == blocking_key_hash("tenant_acme", "transaction", "opp-001")
