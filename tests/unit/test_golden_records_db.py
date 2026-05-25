from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nexus_spark_lib.db.golden_records import (
    apply_synthesis_result,
    resolve_successor,
    upsert_golden_record,
)
from nexus_spark_lib.models.er_types import GoldenRecordState
from nexus_spark_lib.models.survivorship import ProvenanceRow, SynthesisResult


class _FakeMetricHandle:
    def inc(self, *_args, **_kwargs):
        return None


class _FakeMetric:
    def labels(self, **_kwargs):
        return _FakeMetricHandle()


class _FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: dict[str, dict[str, object] | None] = {}

    async def execute(self, query: str, *args):
        self.execute_calls.append((query, args))

    async def fetchrow(self, query: str, *args):
        key = str(args[0]) if args else ""
        return self.fetch_rows.get(key)

    async def executemany(self, query: str, args):
        self.calls.append((query, list(args)))


@pytest.mark.asyncio
async def test_apply_synthesis_result_coerces_string_observed_at_to_datetime(monkeypatch):
    conn = _FakeConnection()
    monkeypatch.setattr("nexus_spark_lib.db.golden_records.DB_WRITES", _FakeMetric())

    result = SynthesisResult(
        cdm_entity_id="gr:test-001",
        rows_to_upsert=[
            ProvenanceRow(
                cdm_entity_id="gr:test-001",
                attribute_name="cdm.transaction.amount",
                winning_connector_id="AW Sales",
                winning_source_table="salesorderdetail",
                winning_record_id="aw-fresh-001",
                observed_value_hash="hash-001",
                observed_at="2026-05-12 23:01:37",
                rule_applied="most_recent",
            )
        ],
    )

    await apply_synthesis_result(conn, result, tenant_id="tarek789")

    assert len(conn.calls) == 1
    _, params = conn.calls[0]
    assert params[0][7] == datetime(2026, 5, 12, 23, 1, 37, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_upsert_golden_record_persists_successor_id(monkeypatch):
    conn = _FakeConnection()
    monkeypatch.setattr("nexus_spark_lib.db.golden_records.DB_WRITES", _FakeMetric())

    await upsert_golden_record(
        conn,
        "gr:loser",
        "tenant_acme",
        "party",
        state=GoldenRecordState.SUPERSEDED,
        state_change_reason="merged_into:gr:survivor",
        successor_id="gr:survivor",
    )

    assert len(conn.execute_calls) == 1
    query, params = conn.execute_calls[0]
    assert "successor_id" in query
    assert params[4] == "gr:survivor"
    assert params[5] == "merged_into:gr:survivor"


@pytest.mark.asyncio
async def test_resolve_successor_follows_chain():
    conn = _FakeConnection()
    conn.fetch_rows = {
        "gr:a": {"successor_id": "gr:b"},
        "gr:b": {"successor_id": "gr:c"},
        "gr:c": {"successor_id": None},
    }

    assert await resolve_successor(conn, "gr:a") == "gr:c"