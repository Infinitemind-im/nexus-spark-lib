"""Unit tests for Stage 3 — Synthesis."""

import json
from dataclasses import dataclass, field

import pytest
from unittest.mock import MagicMock

from nexus_spark_lib._internal.hash_utils import provenance_hash_from_winning_records, sha256_hex
from nexus_spark_lib.models.survivorship import ProvenanceRow, SurvivorshipRuleSet, SurvivorshipRuleType, SurvivorshipRule
from nexus_spark_lib.transform.stage3_synthesise import (
    _apply_rule,
    NormalisedRecord,
    ResolutionResult,
    SynthesisContext,
    handle_source_delete,
    synthesise,
)


@dataclass
class _FakeProvenanceStore:
    rows: dict[str, ProvenanceRow] = field(default_factory=dict)

    def get_all(self, cdm_entity_id):
        return dict(self.rows)

    def upsert_returning_changed(self, row):
        previous = self.rows.get(row.attribute_name)
        changed = previous != row
        self.rows[row.attribute_name] = row
        return changed

    def get_attributes_won_by(self, cdm_entity_id, connector_id, source_record_id):
        return [
            attr_name
            for attr_name, row in self.rows.items()
            if row.winning_connector_id == connector_id and row.winning_record_id == source_record_id
        ]

    def delete_attribute(self, cdm_entity_id, attribute_name, connector_id, source_record_id):
        self.rows.pop(attribute_name, None)

    def delete_all(self, cdm_entity_id):
        self.rows.clear()


@dataclass
class _FakeRulesAccessor:
    rules: dict[str, SurvivorshipRule]

    def get_all(self, tenant_id, cdm_entity_type):
        return dict(self.rules)


@dataclass
class _FakeGRIndex:
    hashes: dict[str, str] = field(default_factory=dict)
    updates: list[tuple[str, str, str]] = field(default_factory=list)

    def get_hash(self, cdm_entity_id):
        return self.hashes.get(cdm_entity_id, "")

    def update_hash(self, cdm_entity_id, value):
        self.hashes[cdm_entity_id] = value

    def update(self, cdm_entity_id, state, state_change_reason):
        self.updates.append((cdm_entity_id, state, state_change_reason))


@dataclass
class _FakeERIndexEntry:
    cdm_entity_id: str
    connector_id: str
    source_table: str
    source_record_id: str
    source_ts: str
    confidence: float = 0.0
    completeness_score: float = 0.0


@dataclass
class _FakeERIndex:
    lookup_entry: _FakeERIndexEntry | None = None
    confidence_by_connector: dict[str, float] = field(default_factory=dict)
    remaining_sources: list[_FakeERIndexEntry] = field(default_factory=list)
    deleted: list[tuple[str, str, str]] = field(default_factory=list)

    def lookup(self, tenant_id, connector_id, source_table, source_record_id):
        return self.lookup_entry

    def delete(self, connector_id, source_table, source_record_id):
        self.deleted.append((connector_id, source_table, source_record_id))

    def get_all_for_entity(self, cdm_entity_id):
        return list(self.remaining_sources)

    def get_confidence(self, cdm_entity_id, connector_id):
        return self.confidence_by_connector.get(connector_id, 0.0)


@dataclass
class _FakeCompletenessCache:
    values: dict[tuple[str, str], int] = field(default_factory=dict)

    def get(self, cdm_entity_id, connector_id):
        return self.values.get((cdm_entity_id, connector_id), 0)


@dataclass
class _FakeDeltaLake:
    values: dict[tuple[str, str, str, str], object] = field(default_factory=dict)

    def read_attribute(self, connector_id, source_table, source_record_id, attribute_name):
        return self.values.get((connector_id, source_table, source_record_id, attribute_name))


@dataclass
class _FakeEvents:
    removed: list[tuple[str, str]] = field(default_factory=list)
    upserts: list[tuple[str, str]] = field(default_factory=list)

    def emit_remove(self, cdm_entity_id, tenant_id):
        self.removed.append((cdm_entity_id, tenant_id))

    def emit_upsert(self, cdm_entity_id, tenant_id):
        self.upserts.append((cdm_entity_id, tenant_id))


class TestApplyRule:
    def test_enum_parses_spec_most_confident_name(self):
        assert SurvivorshipRuleType("most_confident") == SurvivorshipRuleType.MOST_CONFIDENT

    def test_most_recent_returns_candidate(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, "Alice New", "2024-03-15", "sfdc", 0.9, [], "Alice Old"
        )
        assert result == "Alice New"

    def test_most_confident_returns_candidate(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_CONFIDENT, "Alice New", "2024-03-15", "sfdc", 0.95, [], "Alice Old"
        )
        assert result == "Alice New"

    def test_source_priority_wins(self):
        result = _apply_rule(
            SurvivorshipRuleType.SOURCE_PRIORITY, "Alice", "2024-01-01", "crm", 0.8,
            ["crm"], "Bob"
        )
        assert result == "Alice"

    def test_source_priority_non_priority_keeps_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.SOURCE_PRIORITY, "Alice", "2024-01-01", "unknown_src", 0.8,
            ["crm"], "Bob"
        )
        assert result == "Bob"

    def test_longest_value_wins(self):
        result = _apply_rule(
            SurvivorshipRuleType.LONGEST_VALUE, "AliceLongName", "2024-01-01", "sfdc", 0.9,
            [], "Bob"
        )
        assert result == "AliceLongName"

    def test_exact_match_agrees(self):
        result = _apply_rule(
            SurvivorshipRuleType.EXACT_MATCH, "Alice", "2024-01-01", "sfdc", 0.9,
            [], "Alice"
        )
        assert result == "Alice"

    def test_exact_match_disagrees_returns_none(self):
        result = _apply_rule(
            SurvivorshipRuleType.EXACT_MATCH, "Alice", "2024-01-01", "sfdc", 0.9,
            [], "Bob"
        )
        assert result is None

    def test_none_candidate_returns_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, None, "2024-01-01", "sfdc", 0.9, [], "Bob"
        )
        assert result == "Bob"

    def test_none_existing_returns_candidate(self):
        result = _apply_rule(
            SurvivorshipRuleType.MOST_RECENT, "Alice", "2024-01-01", "sfdc", 0.9, [], None
        )
        assert result == "Alice"

    def test_first_observed_keeps_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.FIRST_OBSERVED, "Alice New", "2024-03-15", "sfdc", 0.9, [], "Alice Old"
        )
        assert result == "Alice Old"

    def test_manual_keeps_existing(self):
        result = _apply_rule(
            SurvivorshipRuleType.MANUAL, "Alice New", "2024-03-15", "sfdc", 0.9, [], "Alice Old"
        )
        assert result == "Alice Old"


class TestStage3Integration:
    def test_provenance_hash_uses_sorted_attribute_record_pairs(self):
        left = provenance_hash_from_winning_records({
            "industry": "001-b",
            "legal_name": "001-a",
        }, {
            "industry": "hash-b",
            "legal_name": "hash-a",
        })
        right = provenance_hash_from_winning_records({
            "legal_name": "001-a",
            "industry": "001-b",
        }, {
            "legal_name": "hash-a",
            "industry": "hash-b",
        })
        assert left == right

    def test_provenance_hash_changes_when_value_hash_changes(self):
        first = provenance_hash_from_winning_records({
            "legal_name": "001-a",
        }, {
            "legal_name": "hash-old",
        })
        second = provenance_hash_from_winning_records({
            "legal_name": "001-a",
        }, {
            "legal_name": "hash-new",
        })
        assert first != second

    def test_synthesise_adds_columns(self, spark, mock_survivorship_broadcast):
        from pyspark.sql import Row
        from nexus_spark_lib.transform.stage3_synthesise import synthesise

        df = spark.createDataFrame([
            Row(
                tenant_id="tenant_acme",
                cdm_entity_type="contact",
                cdm_entity_id="gr:001",
                normalised_json=json.dumps({
                    "full_name": {"value": "Alice Smith", "quality": "good", "source_attribute": "full_name", "pii_flag": False}
                }),
                source_system="salesforce",
                source_record_id="003abc",
                source_ts="2024-03-01T12:00:00",
                dq_score="0.95",
            )
        ])
        result = synthesise(df, mock_survivorship_broadcast)
        cols = result.columns
        assert "golden_fields_json" in cols
        assert "attribute_provenance_json" in cols
        assert "provenance_hash" in cols
        row = result.collect()[0]
        golden_fields = json.loads(row["golden_fields_json"])
        attribute_provenance = json.loads(row["attribute_provenance_json"])
        expected_value_hash = sha256_hex("Alice Smith")
        expected_provenance_hash = provenance_hash_from_winning_records(
            {"full_name": "003abc"},
            {"full_name": expected_value_hash},
        )

        assert golden_fields == {
            "full_name": "Alice Smith"
        }
        assert attribute_provenance == {
            "full_name": "salesforce:003abc"
        }
        assert row["provenance_hash"] == expected_provenance_hash


class TestStage3Section5PurePath:
    def test_synthesise_first_contributor_writes_provenance_and_hash(self):
        ctx = SynthesisContext(
            provenance=_FakeProvenanceStore(),
            survivorship_rules=_FakeRulesAccessor({}),
            gr_index=_FakeGRIndex(),
            er_index=_FakeERIndex(),
        )
        record = NormalisedRecord(
            tenant_id="tenant_acme",
            cdm_entity_type="contact",
            connector_id="salesforce",
            source_table="Contact",
            source_record_id="003abc",
            source_ts="2024-03-01T12:00:00Z",
            canonical_fields={"full_name": "Alice Smith", "email": None},
        )
        resolution = ResolutionResult(cdm_entity_id="gr:001", confidence=0.97, is_new_entity=True)

        result = synthesise(record, resolution, ctx)
        expected_value_hash = sha256_hex("Alice Smith")
        expected_provenance_hash = provenance_hash_from_winning_records(
            {"full_name": "003abc"},
            {"full_name": expected_value_hash},
        )
        full_name_row = ctx.provenance.rows["full_name"]

        assert result.cdm_entity_id == "gr:001"
        assert result.hash_changed is True
        assert result.attribute_provenance == {"full_name": "salesforce:003abc"}
        assert result.contributing_sources == ["salesforce"]
        assert full_name_row.cdm_entity_id == "gr:001"
        assert full_name_row.attribute_name == "full_name"
        assert full_name_row.winning_connector_id == "salesforce"
        assert full_name_row.winning_source_table == "Contact"
        assert full_name_row.winning_record_id == "003abc"
        assert full_name_row.observed_value_hash == expected_value_hash
        assert full_name_row.observed_at == "2024-03-01T12:00:00Z"
        assert full_name_row.rule_applied == "most_recent"
        assert result.provenance_hash == expected_provenance_hash
        assert ctx.gr_index.hashes["gr:001"] == expected_provenance_hash

    def test_synthesise_same_winner_and_value_is_idempotent(self):
        existing = ProvenanceRow(
            cdm_entity_id="gr:001",
            attribute_name="full_name",
            winning_connector_id="salesforce",
            winning_source_table="Contact",
            winning_record_id="003abc",
            observed_value_hash=sha256_hex("Alice Smith"),
            observed_at="2024-03-01T12:00:00Z",
            rule_applied="most_recent",
        )
        expected_hash = provenance_hash_from_winning_records(
            {"full_name": "003abc"},
            {"full_name": existing.observed_value_hash},
        )
        ctx = SynthesisContext(
            provenance=_FakeProvenanceStore(rows={"full_name": existing}),
            survivorship_rules=_FakeRulesAccessor({}),
            gr_index=_FakeGRIndex(hashes={"gr:001": expected_hash}),
            er_index=_FakeERIndex(),
        )
        record = NormalisedRecord(
            tenant_id="tenant_acme",
            cdm_entity_type="contact",
            connector_id="salesforce",
            source_table="Contact",
            source_record_id="003abc",
            source_ts="2024-03-01T12:00:00Z",
            canonical_fields={"full_name": "Alice Smith"},
        )
        resolution = ResolutionResult(cdm_entity_id="gr:001", confidence=0.97, is_new_entity=False)

        result = synthesise(record, resolution, ctx)

        assert result.rows_to_upsert == []
        assert result.hash_changed is False
        assert result.provenance_hash == expected_hash
        assert ctx.provenance.rows["full_name"] == existing
        assert ctx.gr_index.hashes["gr:001"] == expected_hash

    def test_synthesise_manual_rule_does_not_replace_existing_winner(self):
        existing = ProvenanceRow(
            cdm_entity_id="gr:001",
            attribute_name="legal_name",
            winning_connector_id="sap_erp",
            winning_source_table="Customer",
            winning_record_id="1001",
            observed_value_hash="old-hash",
            observed_at="2024-01-01T00:00:00Z",
            rule_applied="manual",
        )
        ctx = SynthesisContext(
            provenance=_FakeProvenanceStore(rows={"legal_name": existing}),
            survivorship_rules=_FakeRulesAccessor({
                "legal_name": SurvivorshipRule(
                    tenant_id="tenant_acme",
                    cdm_entity_type="party",
                    attribute_name="legal_name",
                    rule_type=SurvivorshipRuleType.MANUAL,
                )
            }),
            gr_index=_FakeGRIndex(hashes={"gr:001": "sha256:old"}),
            er_index=_FakeERIndex(),
        )
        record = NormalisedRecord(
            tenant_id="tenant_acme",
            cdm_entity_type="party",
            connector_id="salesforce",
            source_table="Account",
            source_record_id="001xyz",
            source_ts="2024-03-01T12:00:00Z",
            canonical_fields={"legal_name": "Globex"},
        )
        resolution = ResolutionResult(cdm_entity_id="gr:001", confidence=0.99)

        result = synthesise(record, resolution, ctx)

        assert result.rows_to_upsert == []
        assert ctx.provenance.rows["legal_name"].winning_connector_id == "sap_erp"

    def test_handle_source_delete_re_elects_surviving_source(self):
        ctx = SynthesisContext(
            provenance=_FakeProvenanceStore(rows={
                "industry": ProvenanceRow(
                    cdm_entity_id="gr:001",
                    attribute_name="industry",
                    winning_connector_id="salesforce",
                    winning_source_table="Account",
                    winning_record_id="001-delete",
                    observed_value_hash="hash-old",
                    observed_at="2024-03-01T12:00:00Z",
                    rule_applied="most_recent",
                )
            }),
            survivorship_rules=_FakeRulesAccessor({}),
            gr_index=_FakeGRIndex(),
            er_index=_FakeERIndex(
                lookup_entry=_FakeERIndexEntry(
                    cdm_entity_id="gr:001",
                    connector_id="salesforce",
                    source_table="Account",
                    source_record_id="001-delete",
                    source_ts="2024-03-01T12:00:00Z",
                ),
                remaining_sources=[
                    _FakeERIndexEntry(
                        cdm_entity_id="gr:001",
                        connector_id="sap_erp",
                        source_table="Customer",
                        source_record_id="1001",
                        source_ts="2024-03-02T10:00:00Z",
                        confidence=0.98,
                    )
                ],
            ),
            delta_lake=_FakeDeltaLake(values={
                ("sap_erp", "Customer", "1001", "industry"): "Manufacturing"
            }),
            events=_FakeEvents(),
        )

        result = handle_source_delete(
            source_record_id="001-delete",
            connector_id="salesforce",
            source_table="Account",
            tenant_id="tenant_acme",
            cdm_entity_type="party",
            ctx=ctx,
        )

        assert result.action == "re_elected"
        assert result.re_elected_attributes == ["industry"]
        assert ctx.provenance.rows["industry"].winning_connector_id == "sap_erp"
        assert ctx.gr_index.hashes["gr:001"] == provenance_hash_from_winning_records(
            {"industry": "1001"},
            {"industry": sha256_hex("Manufacturing")},
        )
        assert ctx.events.upserts == [("gr:001", "tenant_acme")]

    def test_handle_source_delete_tombstones_when_no_sources_remain(self):
        ctx = SynthesisContext(
            provenance=_FakeProvenanceStore(rows={
                "industry": ProvenanceRow(
                    cdm_entity_id="gr:001",
                    attribute_name="industry",
                    winning_connector_id="salesforce",
                    winning_source_table="Account",
                    winning_record_id="001-delete",
                    observed_value_hash="hash-old",
                    observed_at="2024-03-01T12:00:00Z",
                    rule_applied="most_recent",
                )
            }),
            survivorship_rules=_FakeRulesAccessor({}),
            gr_index=_FakeGRIndex(),
            er_index=_FakeERIndex(
                lookup_entry=_FakeERIndexEntry(
                    cdm_entity_id="gr:001",
                    connector_id="salesforce",
                    source_table="Account",
                    source_record_id="001-delete",
                    source_ts="2024-03-01T12:00:00Z",
                ),
                remaining_sources=[],
            ),
            delta_lake=_FakeDeltaLake(),
            events=_FakeEvents(),
        )

        result = handle_source_delete(
            source_record_id="001-delete",
            connector_id="salesforce",
            source_table="Account",
            tenant_id="tenant_acme",
            cdm_entity_type="party",
            ctx=ctx,
        )

        assert result.action == "tombstoned"
        assert ctx.gr_index.updates == [("gr:001", "tombstoned", "all_sources_deleted")]
        assert ctx.events.removed == [("gr:001", "tenant_acme")]
