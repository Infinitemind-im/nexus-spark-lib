"""LSH blocking using MinHash (datasketch). Generates candidate buckets."""

from __future__ import annotations

from typing import Any

from nexus_spark_lib.observability.structured_log import get_stage_logger

logger = get_stage_logger(__name__)

_NUM_PERM = 128
_THRESHOLD = 0.5


def get_candidate_ids(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> list[str]:
    """Return candidate cdm_entity_ids using MinHash LSH blocking.

    Builds a MinHash from text representation of key fields, then queries
    the LSH index stored in the er_index broadcast snapshot.

    Falls back to empty list (no candidates) if datasketch is unavailable
    or the index doesn't contain an LSH structure.
    """
    try:
        from datasketch import MinHash

        lsh_index = getattr(er_index, "lsh_index", None)
        phone_index = getattr(er_index, "phone_index", None)
        if lsh_index is None and not phone_index:
            return _phone_blocking_candidates(tenant_id, cdm_entity_type, fields, er_index)

        if lsh_index is None:
            return _phone_blocking_candidates(tenant_id, cdm_entity_type, fields, er_index)

        m = MinHash(num_perm=_NUM_PERM)
        for attr, field_val in fields.items():
            value = field_val.get("value") if isinstance(field_val, dict) else str(field_val or "")
            if value:
                for token in _tokenise(value):
                    m.update(token.encode("utf-8"))

        # Query the broadcast LSH structure
        candidates = lsh_index.query(m)
        prefix = f"{tenant_id}|{cdm_entity_type}|"
        # Internal LSH keys may be prefixed; downstream expects bare cdm_entity_id
        scoped: list[str] = []
        for c in candidates:
            if not isinstance(c, str):
                continue
            if c.startswith(prefix):
                scoped.append(c[len(prefix) :])
            elif c.startswith("gr:"):
                scoped.append(c)
        if not scoped:
            scoped = _phone_blocking_candidates(tenant_id, cdm_entity_type, fields, er_index)
        return scoped

    except ImportError:
        logger.debug("datasketch not available — LSH blocking skipped")
        return _phone_blocking_candidates(tenant_id, cdm_entity_type, fields, er_index)
    except Exception as exc:
        logger.warning("LSH blocking error (non-fatal): %s", exc)
        return _phone_blocking_candidates(tenant_id, cdm_entity_type, fields, er_index)


def _phone_blocking_candidates(
    tenant_id: str,
    cdm_entity_type: str,
    fields: dict[str, Any],
    er_index: Any,
) -> list[str]:
    """Exact E.164 phone blocking when LSH index is not populated."""
    phone_index = getattr(er_index, "phone_index", None)
    if not phone_index:
        return []
    raw = fields.get("phone", fields.get("phone_number", {}))
    phone_val = raw.get("value") if isinstance(raw, dict) else raw
    if not phone_val:
        return []
    try:
        from nexus_spark_lib.db.er_broadcast_loader import _phone_blocking_key

        key = _phone_blocking_key(tenant_id, cdm_entity_type, str(phone_val))
        return list(phone_index.get(key, []))
    except Exception:
        return []


def _tokenise(value: str) -> list[str]:
    """Produce 2-gram + word tokens for MinHash."""
    tokens = value.lower().split()
    bigrams = [value[i:i+2] for i in range(len(value) - 1)]
    return tokens + bigrams
