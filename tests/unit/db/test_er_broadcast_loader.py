"""Phone blocking index for Signal B."""

from __future__ import annotations

import pytest

from nexus_spark_lib.db.er_broadcast_loader import register_entity_fields
from nexus_spark_lib.models.er_resolve_index import ErResolveIndex
from nexus_spark_lib.transform.stage2_resolve.lsh.blocking import get_candidate_ids
from nexus_spark_lib.transform.stage2_resolve.signals.signal_b_probabilistic import run_signal_b


def _fields(name: str, email: str, phone: str) -> dict:
    return {
        "full_name": {"value": name},
        "email": {"value": email},
        "phone": {"value": phone},
    }


class TestPhoneBlocking:
    def test_same_e164_finds_candidate(self) -> None:
        idx = ErResolveIndex()
        register_entity_fields(
            idx,
            tenant_id="t1",
            cdm_entity_type="party",
            cdm_entity_id="gr:b",
            fields=_fields("Bob", "bob@x.com", "+33 612345678"),
        )
        incoming = _fields("Alice", "alice@x.com", "+33612345678")
        cands = get_candidate_ids("t1", "party", incoming, idx)
        assert "gr:b" in cands
        score, best = run_signal_b("t1", "party", incoming, idx)
        assert best == "gr:b"
        assert score >= 0.15  # phone component contributes
