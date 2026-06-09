from __future__ import annotations

"""
Materialization Gate — Iter2 Worked Example Operation 3.

After entity identity is known (fast-path or Signal A), read ``entity_store_presence``
and decide whether the record proceeds cold / warm / hot through the pipeline.
"""

from dataclasses import dataclass
from typing import Any, Callable

from nexus_spark_lib.db.entity_store_presence import EntityStorePresenceReader
from nexus_spark_lib.db.er_index_lookup import ErIndexLookup
from nexus_spark_lib.models.entity_store_presence import (
    EntityStorePresence,
    EntityStoreState,
    classify_entity_store_presence,
)


@dataclass(frozen=True)
class MaterializationGateOutcome:
    tenant_id: str
    cdm_entity_id: str | None
    cdm_entity_type: str
    source_connector: str
    source_record_id: str
    materialization: EntityStoreState
    resolution_method: str | None
    proceed_pipeline: bool
    write_ai_stores: bool
    register_er_index_only: bool
    skip_reason: str | None = None
    presence: EntityStorePresence | None = None

    @property
    def materialization_level(self) -> str:
        return self.materialization.value


def _extract_field(fields: dict[str, Any], name: str) -> str | None:
    raw = fields.get(name)
    if raw is None:
        return None
    if isinstance(raw, dict):
        val = raw.get("value")
        return str(val).strip() if val is not None and str(val).strip() else None
    text = str(raw).strip()
    return text or None


def run_materialization_gate(
    *,
    tenant_id: str,
    cdm_entity_type: str,
    source_connector: str,
    source_record_id: str,
    fields: dict[str, Any],
    system_dsn: str,
    deterministic_columns: list[str] | None = None,
    cdm_entity_id: str | None = None,
    signal_a_fn: Callable[..., str | None] | None = None,
    er_index: ErIndexLookup | None = None,
    presence_reader: EntityStorePresenceReader | None = None,
) -> MaterializationGateOutcome:
    """
    Op 3 — resolve identity then check ``entity_store_presence``.

    Returns routing decision for Spark Stage 1+ and Kafka ``materialization_level``.
    """
    er = er_index or ErIndexLookup(system_dsn)
    presence = presence_reader or EntityStorePresenceReader(system_dsn)
    resolution_method: str | None = None
    resolved_id = cdm_entity_id

    if not resolved_id:
        resolved_id = er.fast_path(
            tenant_id=tenant_id,
            source_connector=source_connector,
            source_record_id=source_record_id,
        )
        if resolved_id:
            resolution_method = "fast_path"

    if not resolved_id:
        cols = deterministic_columns or ["tax_id", "domain", "duns_number"]
        for col in cols:
            value = _extract_field(fields, col)
            if not value:
                continue
            if signal_a_fn is not None:
                resolved_id = signal_a_fn(tenant_id, cdm_entity_type, fields, col, value)
            else:
                resolved_id = er.signal_a(
                    tenant_id=tenant_id,
                    cdm_entity_type=cdm_entity_type,
                    deterministic_id_column=col,
                    deterministic_id_value=value,
                )
            if resolved_id:
                resolution_method = "signal_a"
                break

    if not resolved_id:
        return MaterializationGateOutcome(
            tenant_id=tenant_id,
            cdm_entity_id=None,
            cdm_entity_type=cdm_entity_type,
            source_connector=source_connector,
            source_record_id=source_record_id,
            materialization=EntityStoreState.COLD,
            resolution_method=None,
            proceed_pipeline=False,
            write_ai_stores=False,
            register_er_index_only=False,
            skip_reason="entity_unresolved",
        )

    row = presence.get(tenant_id, resolved_id)
    mat = classify_entity_store_presence(row)

    if mat == EntityStoreState.COLD:
        return MaterializationGateOutcome(
            tenant_id=tenant_id,
            cdm_entity_id=resolved_id,
            cdm_entity_type=cdm_entity_type,
            source_connector=source_connector,
            source_record_id=source_record_id,
            materialization=EntityStoreState.COLD,
            resolution_method=resolution_method,
            proceed_pipeline=False,
            write_ai_stores=False,
            register_er_index_only=True,
            skip_reason="entity_store_cold",
            presence=row,
        )

    if mat == EntityStoreState.WARM:
        return MaterializationGateOutcome(
            tenant_id=tenant_id,
            cdm_entity_id=resolved_id,
            cdm_entity_type=cdm_entity_type,
            source_connector=source_connector,
            source_record_id=source_record_id,
            materialization=EntityStoreState.WARM,
            resolution_method=resolution_method,
            proceed_pipeline=True,
            write_ai_stores=False,
            register_er_index_only=False,
            presence=row,
        )

    return MaterializationGateOutcome(
        tenant_id=tenant_id,
        cdm_entity_id=resolved_id,
        cdm_entity_type=cdm_entity_type,
        source_connector=source_connector,
        source_record_id=source_record_id,
        materialization=EntityStoreState.HOT,
        resolution_method=resolution_method,
        proceed_pipeline=True,
        write_ai_stores=True,
        register_er_index_only=False,
        presence=row,
    )
