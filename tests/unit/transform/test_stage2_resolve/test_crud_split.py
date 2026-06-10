"""CRUD propagation split tests."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from nexus_spark_lib.transform.stage2_resolve.crud_propagation import propagate_resolution_split


class TestPropagateResolutionSplit(unittest.IsolatedAsyncioTestCase):
    @patch("nexus_spark_lib.transform.stage2_resolve.crud_propagation.propagate_insert", new_callable=AsyncMock)
    @patch("nexus_spark_lib.transform.stage2_resolve.crud_propagation.has_any_provenance", new_callable=AsyncMock, return_value=True)
    @patch("nexus_spark_lib.transform.stage2_resolve.crud_propagation.delete_provenance_for_source", new_callable=AsyncMock)
    async def test_migrates_source_to_new_gr(self, mock_delete_prov, _mock_has, mock_insert) -> None:
        conn = AsyncMock()
        await propagate_resolution_split(
            conn,
            "t1",
            "party",
            "sf",
            "salesforce",
            "Contact",
            "001",
            "party|001",
            prior_cdm_entity_id="gr:old",
            new_cdm_entity_id="gr:new",
        )
        mock_delete_prov.assert_awaited_once()
        mock_insert.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
