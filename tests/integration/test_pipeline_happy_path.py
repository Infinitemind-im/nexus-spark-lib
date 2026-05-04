"""Integration test — happy path through all 4 stages."""

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
    mock_survivorship_broadcast,
    mock_er_index_broadcast,
):
    """Smoke test: a single INSERT record flows through all 4 stages without error."""
    from nexus_spark_lib.transform.stage0_normalise import normalise
    from nexus_spark_lib.transform.stage1_materialization import materialization_decide
    from nexus_spark_lib.transform.stage2_resolve import resolve
    from nexus_spark_lib.transform.stage3_synthesise import synthesise

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

    # Stage 0 — materialization decision
    df = materialization_decide(df, mock_policy_broadcast)
    assert "materialization_level" in df.columns

    # Stage 1 — normalise
    df = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
    assert "normalised_json" in df.columns

    # Stage 2 — resolve
    df = resolve(df, mock_er_index_broadcast)
    assert "cdm_entity_id" in df.columns

    # Stage 3 — synthesise
    df = synthesise(df, mock_survivorship_broadcast)
    assert "golden_fields_json" in df.columns
    assert "provenance_hash" in df.columns

    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]

    # cdm_entity_id must be a stable "gr:" prefixed hash
    assert row["cdm_entity_id"].startswith("gr:")

    # provenance_hash must not be None
    assert row["provenance_hash"] is not None

    # materialization level must be one of the valid values
    assert row["materialization_level"] in ("hot", "warm", "cold")
