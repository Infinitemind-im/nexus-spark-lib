"""Unit tests for Stage 1 — Normalisation helpers."""

from __future__ import annotations

import pytest

from nexus_spark_lib.transform.stage0_normalise import (
    _coerce_field,
    _parse_date_field,
)


class TestDateParsing:
    def test_iso_date(self):
        result = _parse_date_field("2024-03-15", "date")
        assert result == "2024-03-15"

    def test_us_date_format(self):
        result = _parse_date_field("03/15/2024", "date")
        assert result is not None

    def test_malformed_date_returns_none(self):
        """Malformed date must return None — caller routes to source_extras."""
        result = _parse_date_field("not-a-date", "date")
        assert result is None

    def test_datetime_iso(self):
        result = _parse_date_field("2024-03-15T10:30:00", "datetime")
        assert result is not None
        assert "2024-03-15" in result


class TestCoercion:
    def test_null_like_string_returns_missing(self):
        from nexus_spark_lib.models.transformed_record import FieldQuality

        _, quality, _ = _coerce_field(
            "full_name", "full_name", "NULL",
            {}, None, None, "t1"
        )
        assert quality == FieldQuality.MISSING

    def test_none_value_returns_missing(self):
        from nexus_spark_lib.models.transformed_record import FieldQuality

        _, quality, _ = _coerce_field(
            "full_name", "full_name", None,
            {}, None, None, "t1"
        )
        assert quality == FieldQuality.MISSING

    def test_string_coercion(self):
        from nexus_spark_lib.models.transformed_record import FieldQuality

        value, quality, extra = _coerce_field(
            "full_name", "full_name", "Alice Smith",
            {"type": "string"}, None, None, "t1"
        )
        assert value == "Alice Smith"
        assert quality == FieldQuality.GOOD
        assert extra is None

    def test_decimal_coercion(self):
        from decimal import Decimal
        from nexus_spark_lib.models.transformed_record import FieldQuality

        value, quality, extra = _coerce_field(
            "annual_revenue", "annual_revenue", "500000.00",
            {"type": "decimal"}, None, None, "t1"
        )
        assert value == Decimal("500000.00")
        assert quality == FieldQuality.GOOD

    def test_boolean_true(self):
        from nexus_spark_lib.models.transformed_record import FieldQuality

        value, quality, _ = _coerce_field(
            "is_active", "is_active", "yes",
            {"type": "boolean"}, None, None, "t1"
        )
        assert value is True
        assert quality == FieldQuality.GOOD

    def test_boolean_false(self):
        value, _, _ = _coerce_field(
            "is_active", "is_active", "false",
            {"type": "boolean"}, None, None, "t1"
        )
        assert value is False

    def test_bad_date_routes_to_extras(self):
        value, quality, extra = _coerce_field(
            "birth_date", "birth_date", "not-a-date",
            {"type": "date"}, None, None, "t1"
        )
        assert value is None
        assert extra == "not-a-date"  # route to source_extras, not silently nulled


class TestStage1Integration:
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
        row = result.collect()[0]
        assert row["cdm_entity_type"] == "contact"
