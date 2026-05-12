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

    def _val(fields: dict, key: str) -> str:
        v = fields.get(key, {})
        return (v.get("value") or "") if isinstance(v, dict) else str(v or "")

    # Full name — Jaro-Winkler
    if "full_name" in weights:
        a_name = _val(record_a, "full_name")
        b_name = _val(record_b, "full_name")
        if a_name and b_name:
            sim = jaro_winkler_similarity(a_name, b_name)
            # Phonetic bonus: Soundex/Metaphone same → +0.05
            if phonetic_match(a_name, b_name):
                sim = min(sim + 0.05, 1.0)
            w = weights["full_name"]
            weighted_score += sim * w
            total_weight += w

    # Email — local-part Levenshtein × domain exact match
    if "email" in weights:
        a_email = _val(record_a, "email")
        b_email = _val(record_b, "email")
        if a_email and b_email:
            sim = email_similarity(a_email, b_email)
            w = weights["email"]
            weighted_score += sim * w
            total_weight += w

    # Phone — Levenshtein (normalised)
    if "phone" in weights:
        a_phone = _normalise_phone(_val(record_a, "phone"))
        b_phone = _normalise_phone(_val(record_b, "phone"))
        if a_phone and b_phone:
            sim = levenshtein_similarity(a_phone, b_phone)
            w = weights["phone"]
            weighted_score += sim * w
            total_weight += w

    # Date of birth — exact match → 1.0, else 0.0
    if "date_of_birth" in weights:
        a_dob = _val(record_a, "date_of_birth")
        b_dob = _val(record_b, "date_of_birth")
        if a_dob and b_dob:
            sim = 1.0 if a_dob == b_dob else 0.0
            w = weights["date_of_birth"]
            weighted_score += sim * w
            total_weight += w

    if total_weight == 0.0:
        return 0.0
    return weighted_score / total_weight


def _normalise_phone(phone: str) -> str:
    """Strip all non-digit characters for comparison."""
    return "".join(c for c in phone if c.isdigit())


def _get_weights(er_index: Any, tenant_id: str, cdm_entity_type: str) -> dict:
    try:
        return er_index.thresholds.get((tenant_id, cdm_entity_type), {}).get("weights", _DEFAULT_WEIGHTS)
    except AttributeError:
        return _DEFAULT_WEIGHTS
