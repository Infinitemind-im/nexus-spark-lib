"""Signal A — Deterministic entity resolution.

Performs exact-match on deterministic_id_columns (e.g. tax_id, passport_number).
When a match is found, confidence = 1.000; no probabilistic comparison needed.

Returns: cdm_entity_id string if matched, else None.
"""

from __future__ import annotations

from typing import Any

from nexus_spark_lib._internal.hash_utils import (
    deterministic_value_hash,
    er_deterministic_lookup_key,
)
from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)


def run_signal_a_collect_matches(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> list[str]:
    """Return all GR ids matched by deterministic columns (may be >1 on index conflict)."""
    from nexus_spark_lib.transform.stage2_resolve.er_outcomes import pick_signal_a_match

    det_columns: list[str] = _get_deterministic_columns(er_index, tenant_id, cdm_entity_type)
    matches: list[str] = []

    for col in det_columns:
        raw_value = fields.get(col, {})
        value = raw_value.get("value") if isinstance(raw_value, dict) else raw_value
        if not value:
            continue

        hashed_lookup_key = er_deterministic_lookup_key(
            tenant_id,
            cdm_entity_type,
            col,
            deterministic_value_hash(value),
        )
        legacy_lookup_key = f"{tenant_id}|{cdm_entity_type}|{col}:{value}"

        for lookup_key in (hashed_lookup_key, legacy_lookup_key):
            cdm_entity_id = er_index.snapshot.get(lookup_key)
            if cdm_entity_id and str(cdm_entity_id) not in matches:
                matches.append(str(cdm_entity_id))
                logger.debug("Signal A match: %s=%s → %s", col, value, cdm_entity_id)

    if len(matches) > 1:
        logger.warning(
            "Signal A multiple GRs for deterministic match tenant=%s type=%s: %s",
            tenant_id,
            cdm_entity_type,
            matches,
        )
    winner = pick_signal_a_match(matches)
    return [winner] if winner else []


def run_signal_a(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> str | None:
    """Exact match against the ER index snapshot using deterministic columns."""
    matches = run_signal_a_collect_matches(tenant_id, cdm_entity_type, fields, er_index)
    return matches[0] if matches else None


def _get_deterministic_columns(er_index: Any, tenant_id: str, cdm_entity_type: str) -> list[str]:
    """Return the list of deterministic columns for this tenant/entity type."""
    try:
        return er_index.deterministic_columns.get((tenant_id, cdm_entity_type), [])
    except AttributeError:
        return []
