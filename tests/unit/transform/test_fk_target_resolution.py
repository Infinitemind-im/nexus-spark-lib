"""Unit tests for Stage 2 FK target resolution (spec alignment)."""

from __future__ import annotations

from types import SimpleNamespace

from nexus_spark_lib.transform.fk_target_resolution import (
    enrich_normalised_json_fk_targets,
    infer_fk_target_entity_type,
    resolve_fk_targets_in_fields,
)


class _FakeErIndex:
    def __init__(self, mapping: dict[tuple[str, str, str, str], str]) -> None:
        self._source_records_by_entity = mapping

    def find_entity_by_source_record(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        source_system: str,
        source_record_id: str,
    ) -> str | None:
        return self._source_records_by_entity.get(
            (tenant_id, cdm_entity_type, source_system, source_record_id)
        )


def test_infer_fk_target_entity_type():
    assert infer_fk_target_entity_type("product_id") == "product"
    assert infer_fk_target_entity_type("product_id_ref") == "product"
    assert infer_fk_target_entity_type("buyer_party_id") == "party"
    assert infer_fk_target_entity_type("x", explicit="document") == "document"


def test_resolve_fk_targets_sets_resolved_cdm_entity_id():
    er = _FakeErIndex(
        {
            ("t1", "product", "ServiceNow", "SN-42"): "gr:product-42",
        }
    )
    fields = {
        "product_id": {
            "value": "SN-42",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "product",
        },
        "list_price": {"value": 99.0, "attribute_kind": "monetary"},
    }
    out = resolve_fk_targets_in_fields(
        fields,
        tenant_id="t1",
        source_system="ServiceNow",
        er_index=er,
    )
    assert out["product_id"]["resolved_cdm_entity_id"] == "gr:product-42"
    assert "resolved_cdm_entity_id" not in out["list_price"]


def test_resolve_fk_targets_null_when_missing_from_index():
    er = _FakeErIndex({})
    fields = {
        "product_id": {
            "value": "MISSING",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "product",
        },
    }
    out = resolve_fk_targets_in_fields(
        fields,
        tenant_id="t1",
        source_system="ServiceNow",
        er_index=er,
    )
    assert out["product_id"]["resolved_cdm_entity_id"] is None


def test_enrich_normalised_json_roundtrip():
    er = _FakeErIndex(
        {("t1", "party", "crm", "C1"): "gr:party-1"},
    )
    raw = '{"party_ref":{"value":"C1","attribute_kind":"foreign_key","fk_target_entity_type":"party"}}'
    enriched = enrich_normalised_json_fk_targets(
        raw,
        tenant_id="t1",
        source_system="crm",
        er_index=er,
    )
    import json

    parsed = json.loads(enriched)
    assert parsed["party_ref"]["resolved_cdm_entity_id"] == "gr:party-1"


def test_fallback_scan_without_matching_source_system():
    er = SimpleNamespace(
        _source_records_by_entity={
            ("t1", "product", "OtherSystem", "P9"): "gr:p9",
        },
        find_entity_by_source_record=lambda **_kwargs: None,
    )
    # Override to always miss on exact source_system, then scan fallback
    def _miss(**kwargs):
        return None

    er.find_entity_by_source_record = _miss
    fields = {
        "product_id": {
            "value": "P9",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "product",
        }
    }
    out = resolve_fk_targets_in_fields(
        fields,
        tenant_id="t1",
        source_system="ServiceNow",
        er_index=er,
    )
    assert out["product_id"]["resolved_cdm_entity_id"] == "gr:p9"
