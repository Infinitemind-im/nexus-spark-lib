"""Signal B — Probabilistic entity resolution using LSH + string similarity."""

from __future__ import annotations

from typing import Any

from nexus_spark_lib.transform.stage2_resolve.lsh.blocking import get_candidate_ids
from nexus_spark_lib.transform.stage2_resolve.lsh.similarity import (
    email_similarity,
    jaro_winkler_similarity,
    levenshtein_similarity,
    phonetic_match,
)
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

# Default weights if not overridden by er_thresholds broadcast
_DEFAULT_WEIGHTS = {
    "full_name": 0.35,
    "email": 0.30,
    "phone": 0.15,
    "address": 0.10,
    "date_of_birth": 0.10,
}

_ATTRIBUTE_KIND_HINTS = {
    "full_name": "name",
    "email": "email",
    "phone": "phone",
    "address": "free_text",
    "date_of_birth": "exact",
}


def run_signal_b(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> tuple[float, str | None]:
    """Return (combined_score, best_candidate_id) using LSH + similarity.

    Returns (0.0, None) when no candidates found in blocking.
    """
    candidates = get_candidate_ids(
        tenant_id=tenant_id,
        cdm_entity_type=cdm_entity_type,
        fields=fields,
        er_index=er_index,
    )

    if not candidates:
        return 0.0, None

    weights = _get_weights(er_index, tenant_id, cdm_entity_type)

    best_score = 0.0
    best_candidate = None

    for candidate_id in candidates:
        candidate_fields = er_index.get_fields(candidate_id) or {}
        score = _score_pair(fields, candidate_fields, weights)
        if score > best_score:
            best_score = score
            best_candidate = candidate_id

    return best_score, best_candidate


def _score_pair(
    record_a: dict[str, Any],
    record_b: dict[str, Any],
    weights: dict[str, float],
) -> float:
    """Compute weighted similarity between two normalised field dicts."""
    total_weight = 0.0
    weighted_score = 0.0

    for attr_name, weight in weights.items():
        w = float(weight or 0.0)
        if w <= 0.0:
            continue

        field_a = record_a.get(attr_name)
        field_b = record_b.get(attr_name)
        value_a = _field_value(field_a)
        value_b = _field_value(field_b)
        if value_a is None or value_b is None:
            continue

        sim = _attribute_similarity(attr_name, field_a, field_b, value_a, value_b)
        weighted_score += sim * w
        total_weight += w

    if total_weight == 0.0:
        return 0.0
    return weighted_score / total_weight


def _field_value(field: Any) -> Any:
    if isinstance(field, dict):
        return field.get("value")
    return field


def _attribute_similarity(
    attr_name: str,
    field_a: Any,
    field_b: Any,
    value_a: Any,
    value_b: Any,
) -> float:
    kind = _resolve_similarity_kind(attr_name, field_a, field_b)

    if kind == "name":
        a_name = str(value_a or "")
        b_name = str(value_b or "")
        if not a_name or not b_name:
            return 0.0
        sim = jaro_winkler_similarity(a_name, b_name)
        if phonetic_match(a_name, b_name):
            sim = min(sim + 0.05, 1.0)
        return sim

    if kind == "email":
        return email_similarity(str(value_a or ""), str(value_b or ""))

    if kind == "phone":
        return levenshtein_similarity(
            _normalise_phone(str(value_a or "")),
            _normalise_phone(str(value_b or "")),
        )

    if kind == "free_text":
        return levenshtein_similarity(
            _normalise_text(str(value_a or "")),
            _normalise_text(str(value_b or "")),
        )

    return 1.0 if str(value_a).strip().lower() == str(value_b).strip().lower() else 0.0


def _resolve_similarity_kind(attr_name: str, field_a: Any, field_b: Any) -> str:
    for field in (field_a, field_b):
        if not isinstance(field, dict):
            continue
        kind = str(field.get("attribute_kind") or "").strip().lower()
        if kind and kind != "foreign_key":
            return "free_text" if kind == "address" else kind

    lowered = str(attr_name or "").strip().lower()
    for hint, kind in _ATTRIBUTE_KIND_HINTS.items():
        if lowered == hint or lowered.endswith(f".{hint}") or hint in lowered:
            return kind

    return "exact"


def _normalise_phone(phone: str) -> str:
    """Strip all non-digit characters for comparison."""
    return "".join(c for c in phone if c.isdigit())


def _normalise_text(value: str) -> str:
    return " ".join(value.lower().split())


def _get_weights(er_index: Any, tenant_id: str, cdm_entity_type: str) -> dict:
    try:
        return er_index.thresholds.get((tenant_id, cdm_entity_type), {}).get("weights", _DEFAULT_WEIGHTS)
    except AttributeError:
        return _DEFAULT_WEIGHTS
