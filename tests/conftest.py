"""Shared pytest fixtures for nexus_spark_lib test suite."""

from __future__ import annotations

import json
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

# Java 17 + Hadoop compatibility: must be set before PySpark launches its JVM subprocess.
_JAVA17_OPENS = "--add-opens java.base/javax.security.auth=ALL-UNNAMED"
os.environ.setdefault("JAVA_TOOL_OPTIONS", _JAVA17_OPENS)
os.environ.setdefault("JDK_JAVA_OPTIONS", _JAVA17_OPENS)

# ---------------------------------------------------------------------------
# SparkSession — reused across the entire test session for speed
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("nexus-spark-lib-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Sample records
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_record_dict():
    return {
        "tenant_id": "tenant_acme",
        "connector_id": "conn_salesforce",
        "source_system": "salesforce",
        "source_table": "Contact",
        "source_record_id": "003abc123",
        "source_op": "INSERT",
        "source_ts": datetime(2024, 3, 1, 12, 0, 0),
        "after_payload": {
            "full_name": "Alice Smith",
            "email": "alice@acme.com",
            "phone": "+1 800 555 0100",
            "annual_revenue": "500000.00",
        },
        "before_payload": None,
        "message_id": "msg-001",
        "backfill_batch_id": None,
        "trace_id": "trace-001",
    }


@pytest.fixture
def sample_normalised_fields():
    return {
        "full_name": {"value": "Alice Smith", "quality": "good", "source_attribute": "full_name", "pii_flag": False},
        "email": {"value": "alice@acme.com", "quality": "good", "source_attribute": "email", "pii_flag": False},
        "phone": {"value": "+18005550100", "quality": "good", "source_attribute": "phone", "pii_flag": False},
    }


# ---------------------------------------------------------------------------
# Mock DB connection
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.executemany = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    return conn


# ---------------------------------------------------------------------------
# Broadcast mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cdm_mapping_broadcast():
    mapping = MagicMock()
    mapping.get_cdm_entity_type.return_value = "contact"
    mapping.get_field_map.return_value = {
        "full_name": "full_name",
        "email": "email",
        "phone": "phone",
        "__meta__full_name": {"type": "string"},
        "__meta__email": {"type": "string"},
        "__meta__phone": {"type": "string"},
    }
    bc = MagicMock()
    bc.value = mapping
    return bc


@pytest.fixture
def mock_fx_rates_broadcast():
    from nexus_spark_lib.models.fx import FxRates

    bc = MagicMock()
    bc.value = FxRates(rates=[])
    return bc


@pytest.fixture
def mock_policy_broadcast():
    from nexus_spark_lib.models.materialization import MaterializationLevel, MaterializationPolicy, PolicyRule

    policy = MaterializationPolicy()
    rule = PolicyRule(
        rule_id="rule-001",
        tenant_id="tenant_acme",
        scope="contact",
        predicate="TRUE",
        target_level=MaterializationLevel.HOT,
        priority=100,
        rule_type="manual",
    )
    policy.rules_by_scope[("tenant_acme", "contact")] = [rule]

    bc = MagicMock()
    bc.value = policy
    return bc


@pytest.fixture
def mock_survivorship_broadcast():
    from nexus_spark_lib.models.survivorship import SurvivorshipRuleSet

    bc = MagicMock()
    bc.value = SurvivorshipRuleSet()
    return bc


@pytest.fixture
def mock_er_index_broadcast():
    er_index = MagicMock()
    er_index.snapshot = {}
    er_index.deterministic_columns = {}
    er_index.lsh_index = None
    er_index.thresholds = {}

    bc = MagicMock()
    bc.value = er_index
    return bc
