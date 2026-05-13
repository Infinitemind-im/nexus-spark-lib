from __future__ import annotations

from dataclasses import dataclass

from nexus_spark_lib.transform.stage2_resolve import _should_run_signal_c
from nexus_spark_lib.transform.stage2_resolve.signals.signal_c_graph import run_signal_c


@dataclass
class _FakeErIndex:
    resolved: dict[tuple[str, str, str, str], str]

    def find_entity_by_source_record(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        source_system: str,
        source_record_id: str,
    ) -> str | None:
        return self.resolved.get((tenant_id, cdm_entity_type, source_system, source_record_id))


class _FakeResult:
    def __init__(self, record):
        self._record = record

    def single(self):
        return self._record


class _FakeSession:
    def __init__(self, records):
        self._records = list(records)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def run(self, query, **params):
        _ = query, params
        return _FakeResult(self._records.pop(0))


class _FakeDriver:
    def __init__(self, records):
        self._session = _FakeSession(records)

    def session(self):
        return self._session


def test_signal_c_uses_shared_neighbours_and_confidence_threshold() -> None:
    driver = _FakeDriver([
        {
            "depth1_hits": [
                {"id": "gr-party-001", "confidence": 0.92},
                {"id": "gr-party-low", "confidence": 0.80},
            ]
        },
        {
            "depth2_hits": [
                {"id": "gr-party-002", "confidence": 1.0},
                {"id": "gr-party-001", "confidence": 0.99},
            ]
        },
    ])
    er_index = _FakeErIndex(
        resolved={
            ("tenant_acme", "Party", "salesforce", "001-A"): "gr-party-001",
            ("tenant_acme", "Party", "salesforce", "001-B"): "gr-party-002",
            ("tenant_acme", "Party", "salesforce", "001-C"): "gr-party-low",
        }
    )
    fields = {
        "buyer_party_id": {
            "value": "001-A",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "Party",
        },
        "seller_party_id": {
            "value": "001-B",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "Party",
        },
        "shadow_party_id": {
            "value": "001-C",
            "attribute_kind": "foreign_key",
            "fk_target_entity_type": "Party",
        },
        "description": {"value": "plain text"},
    }

    lift = run_signal_c(
        driver=driver,
        cdm_entity_id="gr-candidate-001",
        tenant_id="tenant_acme",
        cdm_entity_type="Transaction.SalesOrder",
        source_system="salesforce",
        fields=fields,
        er_index=er_index,
    )

    assert lift == 0.066


def test_should_run_signal_c_only_for_review_band_candidates() -> None:
    assert _should_run_signal_c(0.75, "gr-001", 0.75, 0.92) is True
    assert _should_run_signal_c(0.91, "gr-001", 0.75, 0.92) is True
    assert _should_run_signal_c(0.92, "gr-001", 0.75, 0.92) is False
    assert _should_run_signal_c(0.70, "gr-001", 0.75, 0.92) is False
    assert _should_run_signal_c(0.80, None, 0.75, 0.92) is False