from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from nexus_spark_lib.models.er_types import GoldenRecordState
from nexus_spark_lib.transform.stage2_resolve.state_machine import GoldenRecordStateMachine


class _FakeMetricHandle:
    def inc(self, *_args, **_kwargs):
        return None


class _FakeMetric:
    def labels(self, **_kwargs):
        return _FakeMetricHandle()


@pytest.mark.asyncio
async def test_create_or_activate_reactivates_superseded(monkeypatch):
    current_state = AsyncMock(return_value=GoldenRecordState.SUPERSEDED)
    upsert = AsyncMock()

    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.get_golden_record_state",
        current_state,
    )
    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
        upsert,
    )
    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.ER_STATE_TRANSITIONS",
        _FakeMetric(),
    )

    conn = AsyncMock()
    sm = GoldenRecordStateMachine(conn, "tenant_acme")

    state = await sm.create_or_activate("gr:001", "party", reason="split_reactivate")

    assert state == GoldenRecordState.ACTIVE
    upsert.assert_awaited_once_with(
        conn,
        "gr:001",
        "tenant_acme",
        "party",
        state=GoldenRecordState.ACTIVE,
        state_change_reason="split_reactivate",
        successor_id=None,
    )


@pytest.mark.asyncio
async def test_merge_sets_successor_id_on_superseded_record(monkeypatch):
    resolve = AsyncMock(return_value="gr:final")
    upsert = AsyncMock()
    insert_redirect = AsyncMock()

    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.resolve_successor",
        resolve,
    )
    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
        upsert,
    )
    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.insert_redirect",
        insert_redirect,
    )
    monkeypatch.setattr(
        "nexus_spark_lib.transform.stage2_resolve.state_machine.ER_STATE_TRANSITIONS",
        _FakeMetric(),
    )

    conn = AsyncMock()
    sm = GoldenRecordStateMachine(conn, "tenant_acme")

    await sm.merge("gr:loser", "gr:intermediate", "party")

    resolve.assert_awaited_once_with(conn, "gr:intermediate")
    upsert.assert_awaited_once_with(
        conn,
        "gr:loser",
        "tenant_acme",
        "party",
        state=GoldenRecordState.SUPERSEDED,
        state_change_reason="merged_into:gr:final",
        successor_id="gr:final",
    )
    insert_redirect.assert_awaited_once_with(
        conn,
        "gr:loser",
        "gr:final",
        "tenant_acme",
    )