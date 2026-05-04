"""Unit tests for GoldenRecordStateMachine."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from nexus_spark_lib.models.er_types import GoldenRecordState
from nexus_spark_lib.transform.stage2_resolve.state_machine import GoldenRecordStateMachine


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchrow = AsyncMock(return_value=None)
    return conn


class TestStateMachine:
    @pytest.mark.asyncio
    async def test_create_new_sets_active(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.get_golden_record_state",
            return_value=None,
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
            new_callable=AsyncMock,
        ) as mock_upsert:
            sm = GoldenRecordStateMachine(mock_conn, "t1")
            state = await sm.create_or_activate("gr:001", "contact", reason="insert")
            assert state == GoldenRecordState.ACTIVE
            mock_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_tombstoned_reactivated(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.get_golden_record_state",
            return_value=GoldenRecordState.TOMBSTONED,
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
            new_callable=AsyncMock,
        ):
            sm = GoldenRecordStateMachine(mock_conn, "t1")
            state = await sm.create_or_activate("gr:001", "contact")
            assert state == GoldenRecordState.ACTIVE

    @pytest.mark.asyncio
    async def test_merge_creates_redirect(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
            new_callable=AsyncMock,
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.insert_redirect",
            new_callable=AsyncMock,
        ) as mock_redirect:
            sm = GoldenRecordStateMachine(mock_conn, "t1")
            await sm.merge("gr:loser", "gr:survivor", "contact")
            mock_redirect.assert_called_once_with(mock_conn, "gr:loser", "gr:survivor", "t1")

    @pytest.mark.asyncio
    async def test_tombstone_transition(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.state_machine.upsert_golden_record",
            new_callable=AsyncMock,
        ) as mock_upsert:
            sm = GoldenRecordStateMachine(mock_conn, "t1")
            await sm.tombstone("gr:001", "contact")
            call_kwargs = mock_upsert.call_args
            assert call_kwargs.kwargs["state"] == GoldenRecordState.TOMBSTONED
