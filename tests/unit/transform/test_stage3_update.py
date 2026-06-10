"""Unit tests for Stage 3 UPDATE hard-case synthesis."""

from __future__ import annotations

import json
import unittest
from dataclasses import dataclass, field

from nexus_spark_lib.models.survivorship import ProvenanceRow, SurvivorshipRule, SurvivorshipRuleType
from nexus_spark_lib.transform.stage3_synthesise import (
    ERIndexEntry,
    NormalisedRecord,
    ResolutionResult,
    SynthesisContext,
    parse_changed_canonical_attributes,
    synthesise,
)


@dataclass
class _Store:
    rows: dict[str, ProvenanceRow] = field(default_factory=dict)

    def get_all(self, cdm_entity_id: str) -> dict[str, ProvenanceRow]:
        _ = cdm_entity_id
        return dict(self.rows)

    def upsert_returning_changed(self, row: ProvenanceRow) -> bool:
        previous = self.rows.get(row.attribute_name)
        changed = previous != row
        self.rows[row.attribute_name] = row
        return changed

    def get_attributes_won_by(self, cdm_entity_id: str, connector_id: str, source_record_id: str) -> list[str]:
        _ = cdm_entity_id, connector_id, source_record_id
        return []

    def delete_attribute(
        self,
        cdm_entity_id: str,
        attribute_name: str,
        connector_id: str,
        source_record_id: str,
    ) -> None:
        _ = cdm_entity_id, connector_id, source_record_id
        self.rows.pop(attribute_name, None)

    def delete_all(self, cdm_entity_id: str) -> None:
        _ = cdm_entity_id
        self.rows.clear()


@dataclass
class _Rules:
    def get_all(self, tenant_id: str, cdm_entity_type: str) -> dict[str, SurvivorshipRule]:
        _ = tenant_id, cdm_entity_type
        return {}


@dataclass
class _GrIndex:
    current_hash: str = "sha256:old"

    def get_hash(self, cdm_entity_id: str) -> str:
        _ = cdm_entity_id
        return self.current_hash

    def update_hash(self, cdm_entity_id: str, value: str) -> None:
        _ = cdm_entity_id
        self.current_hash = value


@dataclass
class _ErIndex:
    cdm_entity_id: str
    entries: list[dict[str, object]] = field(default_factory=list)

    def get_all_for_entity(self, cdm_entity_id: str) -> list[dict[str, object]]:
        if cdm_entity_id != self.cdm_entity_id:
            return []
        return list(self.entries)

    def lookup(self, tenant_id: str, connector_id: str, source_table: str, source_record_id: str):
        _ = tenant_id, connector_id, source_table, source_record_id
        return None

    def delete(self, connector_id: str, source_table: str, source_record_id: str) -> None:
        _ = connector_id, source_table, source_record_id

    def get_confidence(self, cdm_entity_id: str, connector_id: str) -> float:
        _ = cdm_entity_id, connector_id
        return 0.0


class TestStage3Update(unittest.TestCase):
    def test_parse_changed_attributes_only_for_update(self) -> None:
        self.assertIsNone(parse_changed_canonical_attributes("INSERT", '["name"]'))
        self.assertEqual(parse_changed_canonical_attributes("UPDATE", '["name"]'), ["name"])

    def test_update_empty_diff_is_noop(self) -> None:
        provenance = _Store(
            rows={
                "legal_name": ProvenanceRow(
                    cdm_entity_id="gr:1",
                    attribute_name="legal_name",
                    winning_connector_id="sf",
                    winning_source_table="contact",
                    winning_record_id="001",
                    observed_value_hash="h1",
                    observed_at="2026-01-01",
                    rule_applied=SurvivorshipRuleType.MOST_RECENT.value,
                ),
            }
        )
        ctx = SynthesisContext(
            provenance=provenance,
            survivorship_rules=_Rules(),
            gr_index=_GrIndex(),
            er_index=_ErIndex(cdm_entity_id="gr:1", entries=[]),
        )
        record = NormalisedRecord(
            tenant_id="t1",
            cdm_entity_type="party",
            connector_id="sf",
            source_table="contact",
            source_record_id="001",
            source_ts="2026-02-01",
            canonical_fields={"legal_name": {"value": "Acme"}},
            source_op="UPDATE",
            changed_canonical_attributes_json="[]",
        )
        result = synthesise(
            record,
            ResolutionResult(cdm_entity_id="gr:1"),
            ctx,
        )
        self.assertFalse(result.hash_changed)

    def test_update_only_touches_listed_attributes(self) -> None:
        provenance = _Store(
            rows={
                "legal_name": ProvenanceRow(
                    cdm_entity_id="gr:1",
                    attribute_name="legal_name",
                    winning_connector_id="sf",
                    winning_source_table="contact",
                    winning_record_id="001",
                    observed_value_hash="h1",
                    observed_at="2026-01-01",
                    rule_applied=SurvivorshipRuleType.MOST_RECENT.value,
                ),
                "industry": ProvenanceRow(
                    cdm_entity_id="gr:1",
                    attribute_name="industry",
                    winning_connector_id="sf",
                    winning_source_table="contact",
                    winning_record_id="001",
                    observed_value_hash="h2",
                    observed_at="2026-01-01",
                    rule_applied=SurvivorshipRuleType.MOST_RECENT.value,
                ),
            }
        )
        ctx = SynthesisContext(
            provenance=provenance,
            survivorship_rules=_Rules(),
            gr_index=_GrIndex(),
            er_index=_ErIndex(cdm_entity_id="gr:1", entries=[]),
        )
        record = NormalisedRecord(
            tenant_id="t1",
            cdm_entity_type="party",
            connector_id="sf",
            source_table="contact",
            source_record_id="001",
            source_ts="2026-02-01",
            canonical_fields={"legal_name": {"value": "Acme"}},
            source_op="UPDATE",
            changed_canonical_attributes_json=json.dumps(["legal_name"]),
        )
        result = synthesise(
            record,
            ResolutionResult(cdm_entity_id="gr:1"),
            ctx,
        )
        self.assertIn("industry", result.attribute_provenance)
        self.assertIn("legal_name", result.attribute_provenance)

    def test_update_clears_winner_and_unwinds(self) -> None:
        provenance = _Store(
            rows={
                "phone": ProvenanceRow(
                    cdm_entity_id="gr:1",
                    attribute_name="phone",
                    winning_connector_id="sf",
                    winning_source_table="contact",
                    winning_record_id="001",
                    observed_value_hash="h-phone",
                    observed_at="2026-01-01",
                    rule_applied=SurvivorshipRuleType.MOST_RECENT.value,
                ),
            }
        )
        ctx = SynthesisContext(
            provenance=provenance,
            survivorship_rules=_Rules(),
            gr_index=_GrIndex(),
            er_index=_ErIndex(
                cdm_entity_id="gr:1",
                entries=[
                    {
                        "connector_id": "sf",
                        "source_table": "contact",
                        "source_record_id": "001",
                        "source_ts": "2026-01-01",
                    }
                ],
            ),
        )
        record = NormalisedRecord(
            tenant_id="t1",
            cdm_entity_type="party",
            connector_id="sf",
            source_table="contact",
            source_record_id="001",
            source_ts="2026-02-01",
            canonical_fields={},
            source_op="UPDATE",
            changed_canonical_attributes_json=json.dumps(["phone"]),
        )
        result = synthesise(
            record,
            ResolutionResult(cdm_entity_id="gr:1"),
            ctx,
        )
        self.assertTrue(result.hash_changed)
        self.assertNotIn("phone", result.attribute_provenance)
        self.assertEqual(result.rows_to_delete, [("gr:1", "phone", "sf")])


if __name__ == "__main__":
    unittest.main()
