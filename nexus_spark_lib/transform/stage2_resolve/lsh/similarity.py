"""String similarity functions using jellyfish + metaphone."""

from __future__ import annotations

import jellyfish
import metaphone


def jaro_winkler_similarity(a: str, b: str) -> float:
    """Return Jaro-Winkler similarity [0.0, 1.0]."""
    if not a or not b:
        return 0.0
    return jellyfish.jaro_winkler_similarity(a.lower(), b.lower())


def levenshtein_similarity(a: str, b: str) -> float:
    """Return normalised Levenshtein similarity [0.0, 1.0]."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    dist = jellyfish.levenshtein_distance(a.lower(), b.lower())
    max_len = max(len(a), len(b))
    return round(1.0 - dist / max_len, 4)


def soundex_match(a: str, b: str) -> bool:
    """Return True if Soundex codes match."""
    if not a or not b:
        return False
    return jellyfish.soundex(a) == jellyfish.soundex(b)


def metaphone_match(a: str, b: str) -> bool:
    """Return True if Double Metaphone primary codes match."""
    if not a or not b:
        return False
    a_primary, _ = metaphone.doublemetaphone(a)
    b_primary, _ = metaphone.doublemetaphone(b)
    return bool(a_primary and a_primary == b_primary)


def phonetic_match(a: str, b: str) -> bool:
    """Return True if either Soundex or Metaphone indicates a match."""
    return soundex_match(a, b) or metaphone_match(a, b)


def phone_e164_match_score(
    a: str,
    b: str,
    default_region: str | None = None,
) -> float:
    """Exact match after E.164 normalisation (spec: exact post-E.164).

    Returns 1.0 if both numbers parse to the same E.164 string, else 0.0.
    If parsing fails, falls back to digit-only string equality.
    """
    if not a or not b:
        return 0.0
    try:
        import phonenumbers
        from phonenumbers import NumberParseException

        region = default_region or None
        try:
            pa = phonenumbers.parse(a.strip(), region)
            pb = phonenumbers.parse(b.strip(), region)
        except NumberParseException:
            raise ValueError("parse")
        if not phonenumbers.is_valid_number(pa) or not phonenumbers.is_valid_number(pb):
            raise ValueError("invalid")
        na = phonenumbers.format_number(pa, phonenumbers.PhoneNumberFormat.E164)
        nb = phonenumbers.format_number(pb, phonenumbers.PhoneNumberFormat.E164)
        return 1.0 if na == nb else 0.0
    except Exception:
        da = "".join(c for c in a if c.isdigit())
        db = "".join(c for c in b if c.isdigit())
        return 1.0 if da and da == db else 0.0


def email_similarity(a: str, b: str) -> float:
    """Compute email similarity: local-part Levenshtein × domain exact match.

    Score 0.0–1.0:
    - Domain exact match contributes 0.5
    - Local-part similarity contributes up to 0.5
    """
    if not a or not b:
        return 0.0

    a_local, _, a_domain = a.partition("@")
    b_local, _, b_domain = b.partition("@")

    domain_score = 0.5 if a_domain.lower() == b_domain.lower() else 0.0
    local_score = 0.5 * levenshtein_similarity(a_local, b_local)

    return round(domain_score + local_score, 4)
