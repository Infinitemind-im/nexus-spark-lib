"""Integration test — happy path through stage 0 and stage 1."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row
from pyspark.sql.types import MapType, StringType, StructField, StructType, TimestampType


@pytest.mark.integration
def test_full_pipeline_happy_path(
    spark,
    mock_cdm_mapping_broadcast,
    mock_fx_rates_broadcast,
    mock_policy_broadcast,
    mock_er_index_broadcast,
    mock_survivorship_broadcast,
):
    """Smoke test: a single INSERT record flows through stage 0 and stage 1 without error."""
    from nexus_spark_lib.transform.stage0_materialization import materialization_gate, drop_cold
    from nexus_spark_lib.transform.stage1_normalise import normalise
    from nexus_spark_lib.transform.stage2_resolve import resolve
    from nexus_spark_lib.transform.stage3_synthesise import synthesise

    schema = StructType([
        StructField("tenant_id", StringType(), False),
        StructField("connector_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("source_op", StringType(), False),
        StructField("source_ts", TimestampType(), False),
        StructField("after_payload", MapType(StringType(), StringType()), False),
        StructField("before_payload", MapType(StringType(), StringType()), True),
        StructField("message_id", StringType(), False),
        StructField("backfill_batch_id", StringType(), True),
        StructField("trace_id", StringType(), True),
    ])

    df = spark.createDataFrame([
        (
            "tenant_acme",
            "conn_salesforce",
            "salesforce",
            "Contact",
            "003abc",
            "INSERT",
            datetime(2024, 3, 1, 12, 0, 0),
            {"full_name": "Alice Smith", "email": "alice@acme.com"},
            None,
            "msg-001",
            "batch-001",
            "trace-001",
        )
    ], schema=schema)

    # Stage 0 — materialization gate (runs FIRST; resolves cdm_entity_type + level)
    df = materialization_gate(df, mock_cdm_mapping_broadcast, mock_policy_broadcast)
    df = drop_cold(df)
    assert "cdm_entity_type" in df.columns
    assert "materialization_level" in df.columns

    # Stage 1 — normalise (cdm_entity_type already set by Stage 0)
    df = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
    assert "normalised_json" in df.columns

    # Stage 2 — resolve
    df = resolve(df, mock_er_index_broadcast)
    assert "cdm_entity_id" in df.columns

    # Stage 3 — synthesise
    df = synthesise(df, mock_survivorship_broadcast)
    assert "golden_fields_json" in df.columns
    assert "attribute_provenance_json" in df.columns
    assert "provenance_hash" in df.columns

    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]

    # materialization level must be one of the valid values
    assert row["materialization_level"] in ("hot", "warm", "cold")
