"""Unit tests for CRUD propagation — especially DELETE cascade."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch

from nexus_spark_lib.models.er_types import ErOperation
from nexus_spark_lib.transform.stage2_resolve.crud_propagation import (
    propagate_delete,
    propagate_insert,
)


@pytest.fixture
def mock_conn():
    return AsyncMock()


class TestPropagateInsert:
    @pytest.mark.asyncio
    async def test_insert_calls_upsert(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.GoldenRecordStateMachine"
        ) as MockSM, patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.upsert_batch",
            new_callable=AsyncMock,
        ) as mock_upsert:
            MockSM.return_value.create_or_activate = AsyncMock(return_value=None)
            op = await propagate_insert(
                mock_conn,
                "t1",
                "gr:001",
                "contact",
                "salesforce",
                "salesforce",
                "Contact",
                "003abc",
                "contact|003abc",
            )
            assert op == ErOperation.UPSERT
            mock_upsert.assert_called_once_with(
                mock_conn,
                [
                    (
                        "t1",
                        "salesforce",
                        "salesforce",
                        "Contact",
                        "003abc",
                        "gr:001",
                        "contact",
                        1.0,
                        "spark_deterministic",
                        False,
                    )
                ],
            )


class TestPropagateDelete:
    @pytest.mark.asyncio
    async def test_delete_unknown_source_returns_none(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_by_source",
            new_callable=AsyncMock, return_value=None,
        ) as mock_delete:
            op, entity_id = await propagate_delete(
                mock_conn, "t1", "contact", "salesforce", "Contact", "003xyz"
            )
            assert op == ErOperation.REMOVE
            assert entity_id is None
            mock_delete.assert_called_once_with(
                mock_conn, "t1", "salesforce", "Contact", "003xyz"
            )

    @pytest.mark.asyncio
    async def test_delete_last_source_tombstones(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_by_source",
            new_callable=AsyncMock, return_value="gr:001",
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_provenance_for_source",
            new_callable=AsyncMock, return_value=["full_name", "email"],
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.has_any_provenance",
            new_callable=AsyncMock, return_value=False,
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.GoldenRecordStateMachine"
        ) as MockSM:
            MockSM.return_value.tombstone = AsyncMock()
            op, entity_id = await propagate_delete(
                mock_conn, "t1", "contact", "salesforce", "Contact", "003abc"
            )
            assert op == ErOperation.REMOVE
            assert entity_id == "gr:001"
            MockSM.return_value.tombstone.assert_called_once_with("gr:001", "contact")

    @pytest.mark.asyncio
    async def test_delete_with_remaining_sources(self, mock_conn):
        with patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_by_source",
            new_callable=AsyncMock, return_value="gr:001",
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_provenance_for_source",
            new_callable=AsyncMock, return_value=["email"],
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.has_any_provenance",
            new_callable=AsyncMock, return_value=True,  # still has sources
        ), patch(
            "nexus_spark_lib.transform.stage2_resolve.crud_propagation.GoldenRecordStateMachine"
        ) as MockSM:
            MockSM.return_value.tombstone = AsyncMock()
            op, entity_id = await propagate_delete(
                mock_conn, "t1", "contact", "salesforce", "Contact", "003abc"
            )
            # With remaining sources, NOT tombstoned
            assert op == ErOperation.UPSERT
            assert entity_id == "gr:001"
            MockSM.return_value.tombstone.assert_not_called()
