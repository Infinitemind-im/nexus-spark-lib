from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

from nexus_spark_lib.transform.stage1_normalise import _normalise_row


def test_normalise_propagates_signal_c_metadata_from_field_map() -> None:
    mapping = MagicMock()
    mapping.get_field_map.return_value = {
        "AccountId": "buyer_party_id",
        "__meta__AccountId": {
            "type": "string",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "Party",
        },
    }
    fx_broadcast = MagicMock()
    fx_broadcast.value = None
    mapping_broadcast = MagicMock()
    mapping_broadcast.value = mapping

    fn = _normalise_row(mapping_broadcast, fx_broadcast, None)
    normalised_json, *_rest = fn(
        tenant_id="tenant_acme",
        connector_id="salesforce",
        source_system="salesforce",
        source_table="Order",
        cdm_entity_type="Transaction.SalesOrder",
        source_record_id="SO-001",
        source_op="INSERT",
        source_ts=datetime(2026, 5, 13, 12, 0, 0),
        after_payload={"AccountId": "001-A"},
        before_payload=None,
    )

    fields = json.loads(normalised_json)

    assert fields["buyer_party_id"]["attribute_kind"] == "foreign_key"
    assert fields["buyer_party_id"]["fk_target_entity_type"] == "Party"