"""Unit tests for Stage 0 — Normalisation (business logic only).

The lib receives already-typed, already-cleaned data from the transformer.
These tests verify CDM mapping, CRUD routing, DQ scoring and source_extras
routing. Type coercion tests belong in nexus-spark-transformer, not here.
"""

from __future__ import annotations

import pytest


class TestStage0Integration:
    def test_normalise_adds_columns(self, spark, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast):
        from pyspark.sql import Row
        from datetime import datetime

        from nexus_spark_lib.transform.stage0_normalise import normalise

        df = spark.createDataFrame([
            Row(
                tenant_id="tenant_acme",
                connector_id="conn_salesforce",
                source_table="Contact",
                source_record_id="003abc",
                source_op="INSERT",
                source_ts=datetime(2024, 3, 1),
                after_payload={"full_name": "Alice Smith", "email": "alice@acme.com"},
                before_payload=None,
            )
        ])
        result = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
        cols = result.columns
        assert "cdm_entity_type" in cols
        assert "normalised_json" in cols
        assert "dq_score" in cols
        assert "blocking_key" in cols
        row = result.collect()[0]
        assert row["cdm_entity_type"] == "contact"
        # blocking_key should be a non-empty hash string
        assert row["blocking_key"] is not None
        assert row["blocking_key"].startswith("gr:")
