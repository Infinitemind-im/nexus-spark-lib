"""Integration test — happy path through stage 0 and stage 1."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row


@pytest.mark.integration
def test_full_pipeline_happy_path(
    spark,
    mock_cdm_mapping_broadcast,
    mock_fx_rates_broadcast,
    mock_policy_broadcast,
):
    """Smoke test: a single INSERT record flows through stage 0 and stage 1 without error."""
    from nexus_spark_lib.transform.stage0_materialization import materialization_gate, drop_cold
    from nexus_spark_lib.transform.stage1_normalise import normalise

    df = spark.createDataFrame([
        Row(
            tenant_id="tenant_acme",
            connector_id="conn_salesforce",
            source_table="Contact",
            source_record_id="003abc",
            source_op="INSERT",
            source_ts=datetime(2024, 3, 1, 12, 0, 0),
            after_payload={"full_name": "Alice Smith", "email": "alice@acme.com"},
            before_payload=None,
            message_id="msg-001",
            backfill_batch_id=None,
            trace_id="trace-001",
        )
    ])

    # Stage 0 — materialization gate (runs FIRST; resolves cdm_entity_type + level)
    df = materialization_gate(df, mock_cdm_mapping_broadcast, mock_policy_broadcast)
    df = drop_cold(df)
    assert "cdm_entity_type" in df.columns
    assert "materialization_level" in df.columns

    # Stage 1 — normalise (cdm_entity_type already set by Stage 0)
    df = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
    assert "normalised_json" in df.columns

    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]

    # materialization level must be one of the valid values
    assert row["materialization_level"] in ("hot", "warm", "cold")
