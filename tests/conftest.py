"""Shared pytest fixtures for nexus_spark_lib test suite."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Java 17 + Hadoop compatibility: only inject --add-opens when the active
# java launcher supports it. Some local test environments still resolve to an
# older JVM, and forcing these flags prevents Spark from starting at all.
_JAVA17_OPENS = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"


def _find_java_executable() -> str | None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if candidate.exists():
            return str(candidate)
    return shutil.which("java")


def _java_supports_add_opens() -> bool:
    java_executable = _find_java_executable()
    if not java_executable:
        return False

    try:
        completed = subprocess.run(
            [
                java_executable,
                _JAVA17_OPENS,
                "-version",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False

    output = f"{completed.stdout}\n{completed.stderr}"
    return completed.returncode == 0 and "Unrecognized option: --add-opens" not in output


def _configure_java_test_options() -> None:
    supports_add_opens = _java_supports_add_opens()
    for key in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"):
        current = os.environ.get(key)
        if supports_add_opens:
            if not current:
                os.environ[key] = _JAVA17_OPENS
        elif current and "--add-opens" in current:
            os.environ.pop(key, None)


_configure_java_test_options()
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def pytest_configure(config: pytest.Config) -> None:
    if os.name == "nt" and sys.version_info >= (3, 13):
        raise pytest.UsageError(
            "nexus-spark-lib tests must run with Python 3.11 on this Windows workspace. "
            "Python 3.13 crashes local PySpark workers. Use "
            "C:\\Program Files\\Python311\\python.exe."
        )

# Pydantic settings load at import time; tests do not require a live DB for most cases.
os.environ.setdefault(
    "NEXUS_DB_DSN",
    "postgresql://nexus_app:nexusapp@127.0.0.1:5444/nexus_db?sslmode=disable",
)

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
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
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
    @dataclass
    class _FakeErIndex:
        snapshot: dict = field(default_factory=dict)
        deterministic_columns: dict = field(default_factory=dict)
        lsh_index: object | None = None
        thresholds: dict = field(default_factory=dict)
        source_records_by_entity: dict = field(default_factory=dict)

        def get_fields(self, cdm_entity_id):
            return {}

        def find_entity_by_source_record(self, tenant_id, cdm_entity_type, source_system, source_record_id):
            return self.source_records_by_entity.get(
                (tenant_id, cdm_entity_type, source_system, source_record_id)
            )

    @dataclass
    class _FakeBroadcast:
        value: object

    return _FakeBroadcast(value=_FakeErIndex())
