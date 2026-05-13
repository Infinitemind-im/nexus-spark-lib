from __future__ import annotations

import pytest

from nexus_spark_lib._internal.hash_utils import (
    deterministic_value_hash,
    er_deterministic_lookup_key,
    er_legacy_source_lookup_key,
    er_source_lookup_key,
)
from nexus_spark_lib.db.er_index import load_er_index_snapshot


class _FakeConnection:
    def __init__(self, *, rows, deterministic_rows, threshold_rows) -> None:
        self._rows = rows
        self._deterministic_rows = deterministic_rows
        self._threshold_rows = threshold_rows

    async def fetch(self, query: str, *args):
        _ = args
        if "FROM   nexus_system.entity_resolution_index" in query:
            return self._rows
        if "FROM   nexus_system.golden_record_provenance" in query:
            return self._deterministic_rows
        if "FROM   nexus_system.er_thresholds" in query:
            return self._threshold_rows
        raise AssertionError(f"Unexpected query: {query}")


@pytest.mark.asyncio
async def test_load_er_index_snapshot_builds_canonical_legacy_and_threshold_keys(monkeypatch):
    conn = _FakeConnection(
        rows=[
            {
                "tenant_id": "acme",
                "connector_id": "salesforce-prod",
                "source_system": "salesforce",
                "source_table": "contact",
                "source_record_id": "003-001",
                "cdm_entity_id": "gr:existing-001",
                "cdm_entity_type": "contact",
                "confidence": 1.0,
                "provisional": False,
            }
        ],
        deterministic_rows=[],
        threshold_rows=[
            {
                "tenant_id": "acme",
                "cdm_entity_type": "contact",
                "weights": {"name": 0.7},
                "auto_apply_threshold": 0.95,
                "review_lower_bound": 0.7,
            }
        ],
    )

    async def _fake_load_deterministic_columns(_conn):
        assert _conn is conn
        return {("acme", "contact"): ["cdm.contact.email"]}

    monkeypatch.setattr(
        "nexus_spark_lib.db.er_index.load_deterministic_id_columns",
        _fake_load_deterministic_columns,
    )

    snapshot = await load_er_index_snapshot(conn)

    assert snapshot.snapshot[
        er_source_lookup_key("acme", "salesforce-prod", "contact", "003-001")
    ] == "gr:existing-001"
    assert snapshot.snapshot[
        er_legacy_source_lookup_key("acme", "salesforce", "003-001")
    ] == "gr:existing-001"
    assert snapshot.deterministic_columns == {("acme", "contact"): ["cdm.contact.email"]}
    assert snapshot.thresholds[("acme", "contact")]["auto_apply_threshold"] == 0.95
    assert snapshot.index is snapshot.snapshot


@pytest.mark.asyncio
async def test_load_er_index_snapshot_adds_deterministic_hash_keys(monkeypatch):
    email_hash = deterministic_value_hash("alice@example.com")
    conn = _FakeConnection(
        rows=[],
        deterministic_rows=[
            {
                "tenant_id": "acme",
                "cdm_entity_id": "gr:deterministic-001",
                "attribute_name": "cdm.contact.email",
                "observed_value_hash": email_hash,
                "cdm_entity_type": "contact",
            }
        ],
        threshold_rows=[],
    )

    async def _fake_load_deterministic_columns(_conn):
        assert _conn is conn
        return {("acme", "contact"): ["cdm.contact.email"]}

    monkeypatch.setattr(
        "nexus_spark_lib.db.er_index.load_deterministic_id_columns",
        _fake_load_deterministic_columns,
    )

    snapshot = await load_er_index_snapshot(conn)

    assert snapshot.snapshot[
        er_deterministic_lookup_key(
            "acme",
            "contact",
            "cdm.contact.email",
            email_hash,
        )
    ] == "gr:deterministic-001"
    assert snapshot.deterministic_hash_count == 1