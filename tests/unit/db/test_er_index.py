from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from nexus_spark_lib.db.er_index import upsert_batch


@pytest.mark.asyncio
async def test_upsert_batch_populates_both_entity_type_columns():
    conn = AsyncMock()

    await upsert_batch(
        conn,
        [
            (
                "tenant_a",
                "conn_1",
                "ServiceNow ITSM",
                "sys_choice",
                "record_1",
                "gr:entity-1",
                "reference",
                1.0,
                "new_entity",
                False,
            )
        ],
    )

    conn.executemany.assert_awaited_once()
    query, payload = conn.executemany.await_args.args

    assert "entity_type" in query
    assert "cdm_entity_type" in query
    assert "entity_type        = EXCLUDED.entity_type" in query
    assert payload == [
        (
            "gr:entity-1",
            "tenant_a",
            "conn_1",
            "ServiceNow ITSM",
            "reference",
            "sys_choice",
            "record_1",
            "reference",
            1.0,
            "new_entity",
            False,
        )
    ]
