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
        if lsh_index is None:
            return []

        m = MinHash(num_perm=_NUM_PERM)
        for attr, field_val in fields.items():
            value = field_val.get("value") if isinstance(field_val, dict) else str(field_val or "")
            if value:
                for token in _tokenise(value):
                    m.update(token.encode("utf-8"))

        # Query the broadcast LSH structure
        candidates = lsh_index.query(m)
        # Filter to same tenant + entity type
        scoped = [
            c for c in candidates
            if c.startswith(f"{tenant_id}|{cdm_entity_type}|")
        ]
        return scoped

    except ImportError:
        logger.debug("datasketch not available — LSH blocking skipped")
        return []
    except Exception as exc:
        logger.warning("LSH blocking error (non-fatal): %s", exc)
        return []


def _tokenise(value: str) -> list[str]:
    """Produce 2-gram + word tokens for MinHash."""
    tokens = value.lower().split()
    bigrams = [value[i:i+2] for i in range(len(value) - 1)]
    return tokens + bigrams
