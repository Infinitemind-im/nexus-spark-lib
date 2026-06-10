"""Pure helpers for Stage 2 outcome classification (unit-testable without Spark)."""

from __future__ import annotations

import json
from typing import Any

_ER_RELEVANT_WEIGHT = 0.20


def lookup_prior_cdm_entity_id(er_index: Any, lookup_keys: list[str]) -> str | None:
    snapshot = getattr(er_index, "snapshot", None) or {}
    for key in lookup_keys:
        known = snapshot.get(key)
        if known:
            return str(known)
    return None


def update_requires_reresolution(
    *,
    er_index: Any,
    tenant_id: str,
    cdm_entity_type: str,
    source_op: str,
    changed_canonical_attributes_json: str,
) -> bool:
    """True when UPDATE touches deterministic IDs or Signal B attrs weighted >= 0.20."""
    if str(source_op or "").upper() != "UPDATE":
        return False

    try:
        changed_attributes = json.loads(changed_canonical_attributes_json or "[]")
    except json.JSONDecodeError:
        return False

    if not isinstance(changed_attributes, list) or not changed_attributes:
        return False

    deterministic_columns = _get_deterministic_columns(er_index, tenant_id, cdm_entity_type)
    similarity_weights = _get_similarity_weights(er_index, tenant_id, cdm_entity_type)
    relevant_weighted_attributes = [
        attr_name
        for attr_name, weight in similarity_weights.items()
        if _safe_float(weight) >= _ER_RELEVANT_WEIGHT
    ]

    for attr_name in changed_attributes:
        attr = str(attr_name or "")
        if _matches_any_attribute(attr, deterministic_columns):
            return True
        if _matches_any_attribute(attr, relevant_weighted_attributes):
            return True

    return False


def infer_gr_operation(
    *,
    prior_cdm_entity_id: str | None,
    new_cdm_entity_id: str,
    source_op: str,
) -> str:
    """Map resolution outcome to transformed_records operation (spec §6.2)."""
    op = str(source_op or "").upper()
    if op == "RELEVEL":
        return "RELEVEL"
    if prior_cdm_entity_id and prior_cdm_entity_id != new_cdm_entity_id:
        return "SPLIT"
    return "UPSERT"


def pick_signal_a_match(matches: list[str]) -> str | None:
    """When multiple GRs share a deterministic key, pick lexicographic survivor."""
    cleaned = sorted({str(m) for m in matches if m})
    if not cleaned:
        return None
    return cleaned[0]


def _get_deterministic_columns(er_index: Any, tenant_id: str, cdm_entity_type: str) -> list[str]:
    try:
        return list(er_index.deterministic_columns.get((tenant_id, cdm_entity_type), []))
    except AttributeError:
        return []


def _get_similarity_weights(er_index: Any, tenant_id: str, cdm_entity_type: str) -> dict[str, float]:
    try:
        config = er_index.thresholds.get((tenant_id, cdm_entity_type), {})
    except AttributeError:
        config = {}
    weights = config.get("weights") if isinstance(config, dict) else None
    if isinstance(weights, dict) and weights:
        return {str(k): _safe_float(v) for k, v in weights.items()}
    return {}


def _matches_any_attribute(attribute_name: str, configured_attributes: list[str]) -> bool:
    normalised = attribute_name.lower()
    for configured in configured_attributes:
        configured_name = str(configured or "").lower()
        if not configured_name:
            continue
        if normalised == configured_name:
            return True
        if normalised.endswith(f".{configured_name}"):
            return True
        if configured_name.endswith(f".{normalised}"):
            return True
    return False


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
