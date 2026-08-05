"""Stage 2 FK edge-target resolution (spec alignment).

Per NEXUS-Iter2-SPEC-LIB-M3WriterRouting-v0.1 and DataPaths §1.10:
FK fields must carry a pre-resolved ``resolved_cdm_entity_id`` looked up
against ``entity_resolution_index`` in Stage 2 ER — not deferred to M3.

This module materialises the same ER lookup already used internally by
Signal C (``_resolve_fk_neighbours``) onto each foreign_key field so the
value travels through ``transformed_records`` → CDM Mapper → M3 Writer.
"""

from __future__ import annotations

import json
import re
from typing import Any

_TARGET_HINTS = {
    "product": "product",
    "party": "party",
    "buyer": "party",
    "customer": "party",
    "vendor": "party",
    "supplier": "party",
    "employee": "employee",
    "transaction": "transaction",
    "order": "transaction",
    "document": "document",
    "interaction": "interaction",
}


def infer_fk_target_entity_type(attribute_name: str, explicit: str | None = None) -> str | None:
    """Infer CDM entity type for an FK attribute name."""
    if explicit:
        return str(explicit).strip() or None
    base = (attribute_name or "").lower().strip()
    for suffix in ("_id_ref", "_ref", "_id"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    parts = [p for p in re.split(r"[_\s]+", base) if p]
    for part in parts:
        hinted = _TARGET_HINTS.get(part)
        if hinted:
            return hinted
    return parts[-1] if parts else None


def _lookup_target(
    er_index: Any,
    *,
    tenant_id: str,
    target_entity_type: str,
    source_system: str,
    source_record_id: str,
) -> str | None:
    """Resolve FK source id → cdm_entity_id via ER index snapshot."""
    if er_index is None or not source_record_id:
        return None

    finder = getattr(er_index, "find_entity_by_source_record", None)
    if callable(finder):
        # Prefer same source_system (typical same-connector FK).
        if source_system:
            hit = finder(
                tenant_id=tenant_id,
                cdm_entity_type=target_entity_type,
                source_system=source_system,
                source_record_id=source_record_id,
            )
            if hit:
                return str(hit)

        # Fallback: scan index map for any source_system with this record id.
        by_entity = getattr(er_index, "_source_records_by_entity", None) or {}
        for (tid, etype, _sys, rid), cdm_id in by_entity.items():
            if (
                str(tid) == str(tenant_id)
                and str(etype).lower() == str(target_entity_type).lower()
                and str(rid) == str(source_record_id)
            ):
                return str(cdm_id)
        return None

    # Dict-like / test doubles
    if isinstance(er_index, dict):
        key = (tenant_id, target_entity_type, source_system, source_record_id)
        hit = er_index.get(key)
        return str(hit) if hit else None
    return None


def resolve_fk_targets_in_fields(
    fields: dict[str, Any],
    *,
    tenant_id: str,
    source_system: str,
    er_index: Any,
) -> dict[str, Any]:
    """Return a copy of *fields* with ``resolved_cdm_entity_id`` set on FK attrs.

    A field is treated as FK when:
    - ``attribute_kind == "foreign_key"``, or
    - ``fk_target_entity_type`` is present, or
    - the attribute name ends with ``_id`` / ``_id_ref`` / ``_ref`` and has a value
      (conservative name heuristic for seed mappings that omit attribute_kind).
    """
    if not isinstance(fields, dict) or not fields:
        return fields if isinstance(fields, dict) else {}

    out: dict[str, Any] = {}
    for attr_name, raw in fields.items():
        if not isinstance(raw, dict):
            out[attr_name] = raw
            continue

        field = dict(raw)
        kind = str(field.get("attribute_kind") or "").strip().lower()
        explicit_target = field.get("fk_target_entity_type")
        value = field.get("value")
        looks_like_fk = (
            kind == "foreign_key"
            or bool(explicit_target)
            or (
                value is not None
                and value != ""
                and (
                    attr_name.lower().endswith("_id_ref")
                    or attr_name.lower().endswith("_ref")
                    or (attr_name.lower().endswith("_id") and attr_name.lower() not in {"cdm_entity_id"})
                )
            )
        )

        if looks_like_fk and value is not None and value != "":
            target_type = infer_fk_target_entity_type(attr_name, explicit_target)
            if target_type:
                resolved = _lookup_target(
                    er_index,
                    tenant_id=tenant_id,
                    target_entity_type=target_type,
                    source_system=source_system or "",
                    source_record_id=str(value),
                )
                if resolved:
                    field["resolved_cdm_entity_id"] = resolved
                elif "resolved_cdm_entity_id" not in field:
                    # Explicit null keeps contract visible downstream
                    field["resolved_cdm_entity_id"] = None

        out[attr_name] = field
    return out


def enrich_normalised_json_fk_targets(
    normalised_json: str | None,
    *,
    tenant_id: str,
    source_system: str,
    er_index: Any,
) -> str:
    """Parse normalised_json, resolve FK targets, return updated JSON string."""
    try:
        fields = json.loads(normalised_json or "{}")
    except json.JSONDecodeError:
        return normalised_json or "{}"
    if not isinstance(fields, dict):
        return normalised_json or "{}"
    enriched = resolve_fk_targets_in_fields(
        fields,
        tenant_id=tenant_id or "",
        source_system=source_system or "",
        er_index=er_index,
    )
    return json.dumps(enriched, default=str)
