from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nexus_spark_lib.db.golden_records import apply_synthesis_result
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