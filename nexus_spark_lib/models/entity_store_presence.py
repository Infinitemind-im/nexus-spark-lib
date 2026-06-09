from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class EntityStoreState(str, Enum):
    """Per-entity materialization from ``entity_store_presence`` (Worked Example Op 3)."""

    COLD = "cold"
    WARM = "warm"
    HOT = "hot"


@dataclass(frozen=True)
class EntityStorePresence:
    tenant_id: str
    cdm_entity_id: str
    es_present: bool
    neo4j_present: bool
    ts_present: bool

    @property
    def state(self) -> EntityStoreState:
        if self.es_present or self.neo4j_present or self.ts_present:
            return EntityStoreState.HOT
        return EntityStoreState.WARM


def classify_entity_store_presence(row: EntityStorePresence | None) -> EntityStoreState:
    if row is None:
        return EntityStoreState.COLD
    return row.state
