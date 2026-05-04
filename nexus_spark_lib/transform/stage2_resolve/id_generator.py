"""Generate stable, deterministic cdm_entity_id values."""

from __future__ import annotations

from nexus_spark_lib._internal.hash_utils import blocking_key_hash


def generate_cdm_entity_id(
    tenant_id: str,
    cdm_entity_type: str,
    blocking_key: str,
) -> str:
    """Return a stable cdm_entity_id for the given (tenant, type, key) triple.

    Format: "gr:" + SHA-256/128 truncated hex of concatenated inputs.
    Same inputs always produce the same ID — fully deterministic (NFR-D3-05).
    """
    return blocking_key_hash(tenant_id, cdm_entity_type, blocking_key)
