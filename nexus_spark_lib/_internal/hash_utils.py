"""SHA-256 and truncated hash utilities used across the library.

All hashes are deterministic — same inputs always produce the same output.
"""

from __future__ import annotations

import hashlib


def sha256_hex(value: str) -> str:
    """Return the full SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_truncated(value: str, bits: int = 128) -> str:
    """Return a truncated SHA-256 hex digest.

    Used for cdm_entity_id generation:
        cdm_entity_id = "gr:" + sha256_truncated(tenant||type||blocking_key, bits=128)

    128 bits = 32 hex characters. Collision probability at 10^9 entities: ~10^-19.
    """
    if bits % 4 != 0:
        raise ValueError(f"bits must be a multiple of 4, got {bits}")
    hex_len = bits // 4
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:hex_len]


def provenance_hash(canonical_summary: str) -> str:
    """Compute the provenance hash for a Golden Record.

    The canonical_summary is the deterministic string representation of the
    golden_record_provenance rows (sorted by attribute_name, then source_system).
    """
    return "sha256:" + sha256_hex(canonical_summary)


def blocking_key_hash(tenant_id: str, cdm_entity_type: str, blocking_value: str) -> str:
    """Generate a stable cdm_entity_id from the blocking key components.

    Format: "gr:" + sha256_truncated(tenant_id || cdm_entity_type || blocking_value, 128)
    """
    raw = f"{tenant_id}\x00{cdm_entity_type}\x00{blocking_value}"
    return "gr:" + sha256_truncated(raw, bits=128)
