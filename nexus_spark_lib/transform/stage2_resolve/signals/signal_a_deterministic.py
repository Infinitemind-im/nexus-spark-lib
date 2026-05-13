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


def run_signal_a(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> str | None:
    """Exact match against the ER index snapshot using deterministic columns.

    Args:
        tenant_id:       Tenant scope.
        cdm_entity_type: e.g. "contact", "account".
        fields:          Normalised field dict from Stage 1.
        er_index:        ErIndexBroadcast (broadcast value, not the wrapper).

    Returns:
        cdm_entity_id if deterministic match found, else None.
    """
    det_columns: list[str] = _get_deterministic_columns(er_index, tenant_id, cdm_entity_type)

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
            if cdm_entity_id:
                logger.debug("Signal A match: %s=%s → %s", col, value, cdm_entity_id)
                return cdm_entity_id

    return None


def _get_deterministic_columns(er_index: Any, tenant_id: str, cdm_entity_type: str) -> list[str]:
    """Return the list of deterministic columns for this tenant/entity type."""
    try:
        return er_index.deterministic_columns.get((tenant_id, cdm_entity_type), [])
    except AttributeError:
        return []
