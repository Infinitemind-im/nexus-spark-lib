from __future__ import annotations

from nexus_spark_lib.db.entity_store_presence_loader import lookup_entity_store_state
from nexus_spark_lib.models.entity_store_presence import EntityStoreState


def test_lookup_entity_store_state_returns_none_for_missing_entity() -> None:
    snapshot: dict[tuple[str, str], EntityStoreState] = {}

    assert lookup_entity_store_state(snapshot, "tenant_a", "gr:new") is None


def test_lookup_entity_store_state_returns_existing_state() -> None:
    snapshot = {
        ("tenant_a", "gr:existing"): EntityStoreState.HOT,
    }

    assert lookup_entity_store_state(snapshot, "tenant_a", "gr:existing") == EntityStoreState.HOT
