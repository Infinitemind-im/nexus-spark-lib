from __future__ import annotations

from typing import Any

from nexus_spark_lib.models.er_resolve_index import ErResolveIndex
from nexus_spark_lib.util.text_similarity import normalize_phone

_DEFAULT_DETERMINISTIC = ["tax_id", "domain", "duns_number"]


def _phone_blocking_key(tenant_id: str, cdm_entity_type: str, phone: str) -> str:
    digits = normalize_phone(phone)
    if not digits:
        return ""
    return f"{tenant_id}|{cdm_entity_type}|phone:{digits}"


def register_entity_fields(
    index: ErResolveIndex,
    *,
    tenant_id: str,
    cdm_entity_type: str,
    cdm_entity_id: str,
    fields: dict[str, Any],
    deterministic_columns: list[str] | None = None,
) -> None:
    """Register an entity in the broadcast index for Signal A + Signal B."""
    cols = deterministic_columns or _DEFAULT_DETERMINISTIC
    if (tenant_id, cdm_entity_type) not in index.deterministic_columns:
        index.register_deterministic_columns(tenant_id, cdm_entity_type, cols)
    index.put_entity(
        tenant_id=tenant_id,
        cdm_entity_type=cdm_entity_type,
        cdm_entity_id=cdm_entity_id,
        fields=fields,
    )
    for phone_attr in ("phone", "phone_number"):
        raw = fields.get(phone_attr)
        phone_val = raw.get("value") if isinstance(raw, dict) else raw
        if not phone_val:
            continue
        key = _phone_blocking_key(tenant_id, cdm_entity_type, str(phone_val))
        if not key:
            continue
        bucket = index.phone_index.setdefault(key, [])
        if cdm_entity_id not in bucket:
            bucket.append(cdm_entity_id)
