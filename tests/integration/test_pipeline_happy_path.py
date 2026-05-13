"""Integration test — happy path through stage 0 and stage 1."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row
from pyspark.sql.types import MapType, StringType, StructField, StructType, TimestampType

from nexus_spark_lib._internal.hash_utils import er_source_lookup_key
from nexus_spark_lib.transform.stage2_resolve.id_generator import generate_cdm_entity_id


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
    assert "er_resolution_method" in df.columns
    assert "er_confidence" in df.columns
    assert "is_provisional" in df.columns

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
    assert row["er_resolution_method"] == "new_entity"
    assert row["er_confidence"] == pytest.approx(1.0)
    assert row["is_provisional"] is False


def test_update_relevant_diff_bypasses_fast_path(
    spark,
    mock_cdm_mapping_broadcast,
    mock_fx_rates_broadcast,
    mock_er_index_broadcast,
):
    from nexus_spark_lib.transform.stage1_normalise import normalise
    from nexus_spark_lib.transform.stage2_resolve import resolve

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
        StructField("cdm_entity_type", StringType(), False),
        StructField("materialization_level", StringType(), False),
    ])

    df = spark.createDataFrame([
        (
            "tenant_acme",
            "conn_salesforce",
            "salesforce",
            "Contact",
            "003abc",
            "UPDATE",
            datetime(2024, 3, 1, 12, 0, 0),
            {"full_name": "Alice Smith", "email": "alice.new@acme.com"},
            {"full_name": "Alice Smith", "email": "alice@acme.com"},
            "contact",
            "hot",
        )
    ], schema=schema)

    mock_er_index_broadcast.value.snapshot = {
        er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "003abc"): "gr:existing-001",
    }
    mock_er_index_broadcast.value.thresholds = {
        ("tenant_acme", "contact"): {"weights": {"email": 0.30}},
    }

    df = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
    row = df.collect()[0]
    assert json.loads(row["changed_canonical_attributes_json"]) == ["email"]

    df = resolve(df, mock_er_index_broadcast)
    resolved = df.collect()[0]

    assert resolved["er_resolution_method"] == "new_entity"
    assert resolved["cdm_entity_id"] != "gr:existing-001"


def test_update_non_er_diff_keeps_fast_path(
    spark,
    mock_cdm_mapping_broadcast,
    mock_fx_rates_broadcast,
    mock_er_index_broadcast,
):
    from nexus_spark_lib.transform.stage1_normalise import normalise
    from nexus_spark_lib.transform.stage2_resolve import resolve

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
        StructField("cdm_entity_type", StringType(), False),
        StructField("materialization_level", StringType(), False),
    ])

    df = spark.createDataFrame([
        (
            "tenant_acme",
            "conn_salesforce",
            "salesforce",
            "Contact",
            "003abc",
            "UPDATE",
            datetime(2024, 3, 1, 12, 0, 0),
            {"full_name": "Alice Newname", "email": "alice@acme.com"},
            {"full_name": "Alice Smith", "email": "alice@acme.com"},
            "contact",
            "hot",
        )
    ], schema=schema)

    mock_er_index_broadcast.value.snapshot = {
        er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "003abc"): "gr:existing-001",
    }
    mock_er_index_broadcast.value.thresholds = {
        ("tenant_acme", "contact"): {"weights": {"email": 0.30}},
    }

    df = normalise(df, mock_cdm_mapping_broadcast, mock_fx_rates_broadcast)
    row = df.collect()[0]
    assert json.loads(row["changed_canonical_attributes_json"]) == ["full_name"]

    df = resolve(df, mock_er_index_broadcast)
    resolved = df.collect()[0]

    assert resolved["er_resolution_method"] == "fast_path"
    assert resolved["cdm_entity_id"] == "gr:existing-001"


def test_new_entity_uses_stage1_blocking_key(
    spark,
    mock_er_index_broadcast,
):
    from nexus_spark_lib.transform.stage2_resolve import resolve

    schema = StructType([
        StructField("tenant_id", StringType(), False),
        StructField("connector_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("source_op", StringType(), False),
        StructField("normalised_json", StringType(), False),
        StructField("changed_canonical_attributes_json", StringType(), False),
        StructField("materialization_level", StringType(), False),
        StructField("cdm_entity_type", StringType(), False),
        StructField("blocking_key", StringType(), False),
    ])

    blocking_key = "gr:blocking-key-001"
    df = spark.createDataFrame([
        (
            "tenant_acme",
            "conn_salesforce",
            "salesforce",
            "Contact",
            "003abc",
            "INSERT",
            json.dumps({"email": {"value": "alice@acme.com"}}),
            "[]",
            "hot",
            "contact",
            blocking_key,
        )
    ], schema=schema)

    result = resolve(df, mock_er_index_broadcast)
    row = result.collect()[0]

    assert row["er_resolution_method"] == "new_entity"
    assert row["cdm_entity_id"] == generate_cdm_entity_id("tenant_acme", "contact", blocking_key)


def test_resolve_uses_configured_thresholds_for_auto_apply(
    spark,
    mock_er_index_broadcast,
    monkeypatch,
):
    import nexus_spark_lib.transform.stage2_resolve as stage2_resolve

    schema = StructType([
        StructField("tenant_id", StringType(), False),
        StructField("connector_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("source_op", StringType(), False),
        StructField("normalised_json", StringType(), False),
        StructField("changed_canonical_attributes_json", StringType(), False),
        StructField("materialization_level", StringType(), False),
        StructField("cdm_entity_type", StringType(), False),
        StructField("blocking_key", StringType(), False),
    ])

    df = spark.createDataFrame([
        (
            "tenant_acme",
            "conn_salesforce",
            "salesforce",
            "Contact",
            "003abc",
            "INSERT",
            json.dumps({"email": {"value": "alice@acme.com"}}),
            "[]",
            "hot",
            "contact",
            "bk-001",
        )
    ], schema=schema)

    mock_er_index_broadcast.value.thresholds = {
        ("tenant_acme", "contact"): {
            "weights": {"email": 1.0},
            "auto_apply_threshold": 0.92,
            "review_lower_bound": 0.75,
        },
    }

    monkeypatch.setattr(stage2_resolve, "run_signal_a", lambda **_kwargs: None)
    monkeypatch.setattr(stage2_resolve, "run_signal_b", lambda **_kwargs: (0.93, "gr:candidate-001"))

    result = stage2_resolve.resolve(df, mock_er_index_broadcast)
    row = result.collect()[0]

    assert row["er_resolution_method"] == "spark_probabilistic"
    assert row["cdm_entity_id"] == "gr:candidate-001"
    assert row["is_provisional"] is False
