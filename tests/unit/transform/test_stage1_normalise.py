"""Unit tests for Stage 1 — Normalisation (business logic only).

Stage 1 runs AFTER stage0_materialization. cdm_entity_type is already set
on the DataFrame by Stage 0 and is passed into the normalise UDF as an
explicit column — not re-resolved from CDM mappings.
"""

from __future__ import annotations

import pytest


class TestStage1NormaliseIntegration:
    def test_normalise_adds_columns(self, spark, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast):
        from pyspark.sql import Row
        from datetime import datetime

        from nexus_spark_lib.transform.stage1_normalise import normalise

        # cdm_entity_type is pre-set by Stage 0 (materialization gate)
        df = spark.createDataFrame([
            Row(
                tenant_id="tenant_acme",
                connector_id="conn_salesforce",
                source_table="Contact",
                cdm_entity_type="contact",          # pre-set by Stage 0
                materialization_level="hot",         # pre-set by Stage 0
                source_record_id="003abc",
                source_op="INSERT",
                source_ts=datetime(2024, 3, 1),
                after_payload={"full_name": "Alice Smith", "email": "alice@acme.com"},
                before_payload=None,
            )
        ])
        result = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
        cols = result.columns
        # cdm_entity_type already in df (from Stage 0) — Stage 1 does NOT add it
        assert "cdm_entity_type" in cols
        assert "normalised_json" in cols
        assert "dq_score" in cols
        assert "blocking_key" in cols
        row = result.collect()[0]
        assert row["cdm_entity_type"] == "contact"
        # blocking_key should be a non-empty hash string
        assert row["blocking_key"] is not None
        assert row["blocking_key"].startswith("gr:")
