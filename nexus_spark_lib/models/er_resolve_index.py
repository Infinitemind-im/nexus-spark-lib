from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErResolveIndex:
    """
    In-memory ER broadcast index for Spark executors.

    - ``snapshot`` — Signal A deterministic keys → cdm_entity_id
    - ``entities`` — Signal B candidate field payloads per golden record
    - ``thresholds`` — per (tenant_id, cdm_entity_type) scoring config
    """

    thresholds: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    snapshot: dict[str, str] = field(default_factory=dict)
    deterministic_columns: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    entities: dict[tuple[str, str], dict[str, dict[str, Any]]] = field(default_factory=dict)
    phone_index: dict[str, list[str]] = field(default_factory=dict)

    def register_deterministic_columns(
        self,
        tenant_id: str,
        cdm_entity_type: str,
        columns: list[str],
    ) -> None:
        self.deterministic_columns[(tenant_id, cdm_entity_type)] = list(columns)

    def put_entity(
        self,
        *,
        tenant_id: str,
        cdm_entity_type: str,
        cdm_entity_id: str,
        fields: dict[str, Any],
    ) -> None:
        key = (tenant_id, cdm_entity_type)
        bucket = self.entities.setdefault(key, {})
        bucket[cdm_entity_id] = fields
        for col in self.deterministic_columns.get(key, []):
            raw = fields.get(col)
            value = raw.get("value") if isinstance(raw, dict) else raw
            if value:
                snap_key = f"{tenant_id}|{cdm_entity_type}|{col}:{value}"
                self.snapshot[snap_key] = cdm_entity_id

    def get_fields(self, cdm_entity_id: str) -> dict[str, Any] | None:
        for bucket in self.entities.values():
            if cdm_entity_id in bucket:
                return bucket[cdm_entity_id]
        return None
