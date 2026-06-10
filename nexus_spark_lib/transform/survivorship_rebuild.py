"""Survivorship re-evaluation entry point for D2 ``survivorship-rebuild`` DAG (FR-Dev 3-M-10)."""

from __future__ import annotations

from typing import Any

import asyncpg

from nexus_spark_lib.db.golden_records import apply_synthesis_result, get_all_provenance, get_sources_for_entity
from nexus_spark_lib.models.survivorship import ProvenanceRow, SynthesisResult
from nexus_spark_lib.transform.stage3_synthesise import (
    ERIndexEntry,
    SynthesisContext,
    _compute_provenance_hash,
    _re_elect_winner,
)


async def rebuild_entity_survivorship(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    cdm_entity_id: str,
    cdm_entity_type: str,
    survivorship_rules: Any,
    attribute_reader: Any,
) -> SynthesisResult:
    """
    Re-evaluate all provenance rows for one GR from the full surviving source set.

    ``attribute_reader`` must expose ``value_for(connector_id, source_table, source_record_id, attribute_name)``.
    """
    existing_rows = await get_all_provenance(conn, cdm_entity_id, tenant_id)
    source_entries = await get_sources_for_entity(conn, tenant_id, cdm_entity_id)

    remaining_sources = [
        ERIndexEntry(
            connector_id=str(entry.get("connector_id") or ""),
            source_table=str(entry.get("source_table") or ""),
            source_record_id=str(entry.get("source_record_id") or ""),
            source_ts=str(entry.get("source_ts") or ""),
            confidence=float(entry.get("confidence", 0.0) or 0.0),
            completeness_score=float(entry.get("completeness_score", 0.0) or 0.0),
            cdm_entity_id=cdm_entity_id,
        )
        for entry in source_entries
    ]

    ctx = SynthesisContext(
        provenance=_InMemoryProvenance(existing_rows),
        survivorship_rules=survivorship_rules,
        gr_index=_NoopGrIndex(),
        er_index=_StaticErIndex(cdm_entity_id, source_entries),
        delta_lake=attribute_reader,
    )

    attribute_names = sorted({row.attribute_name for row in existing_rows})
    final_rows: list[ProvenanceRow] = []
    for attribute_name in attribute_names:
        rule = survivorship_rules.get_rule(tenant_id, cdm_entity_type, attribute_name)
        winner = _re_elect_winner(
            cdm_entity_id=cdm_entity_id,
            attr_name=attribute_name,
            remaining_sources=remaining_sources,
            rule=rule,
            ctx=ctx,
        )
        if winner is not None:
            final_rows.append(winner)

    provenance_hash = _compute_provenance_hash({row.attribute_name: row for row in final_rows})
    result = SynthesisResult(
        cdm_entity_id=cdm_entity_id,
        rows_to_upsert=final_rows,
        rows_to_delete=[],
        provenance_hash=provenance_hash,
        hash_changed=True,
        contributing_sources=sorted({row.winning_connector_id for row in final_rows}),
        attribute_provenance={
            row.attribute_name: f"{row.winning_connector_id}:{row.winning_record_id}"
            for row in final_rows
        },
    )
    await apply_synthesis_result(conn, result, tenant_id)
    return result


class _InMemoryProvenance:
    def __init__(self, rows: list[ProvenanceRow]) -> None:
        self._rows = {row.attribute_name: row for row in rows}

    def get_all(self, cdm_entity_id: str) -> dict[str, ProvenanceRow]:
        _ = cdm_entity_id
        return dict(self._rows)


class _NoopGrIndex:
    def get_hash(self, cdm_entity_id: str) -> str:
        _ = cdm_entity_id
        return ""

    def update_hash(self, cdm_entity_id: str, value: str) -> None:
        _ = cdm_entity_id, value


class _StaticErIndex:
    def __init__(self, cdm_entity_id: str, entries: list[dict[str, Any]]) -> None:
        self._cdm_entity_id = cdm_entity_id
        self._entries = entries

    def get_all_for_entity(self, cdm_entity_id: str) -> list[dict[str, Any]]:
        if cdm_entity_id != self._cdm_entity_id:
            return []
        return list(self._entries)

    def get_confidence(self, cdm_entity_id: str, connector_id: str) -> float:
        if cdm_entity_id != self._cdm_entity_id:
            return 0.0
        for entry in self._entries:
            if entry.get("connector_id") == connector_id:
                return float(entry.get("confidence", 0.0) or 0.0)
        return 0.0
