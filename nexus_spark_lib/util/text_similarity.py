"""String similarity helpers for Signal B."""

from __future__ import annotations


def jaro_winkler(s1: str, s2: str, *, prefix_scale: float = 0.1) -> float:
    """Jaro-Winkler similarity in [0, 1]. Pure-Python (no extra deps)."""
    a = (s1 or "").strip().lower()
    b = (s2 or "").strip().lower()
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    len1, len2 = len(a), len(b)
    match_distance = max(len1, len2) // 2 - 1
    match_distance = max(0, match_distance)

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or a[i] != b[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if a[i] != b[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len1
        + matches / len2
        + (matches - transpositions / 2) / matches
    ) / 3.0

    prefix = 0
    for c1, c2 in zip(a, b, strict=False):
        if c1 != c2:
            break
        prefix += 1
        if prefix == 4:
            break

    return min(1.0, jaro + prefix * prefix_scale * (1.0 - jaro))


def normalize_phone(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    return digits


def field_similarity(name: str, left: str, right: str) -> float:
    if name in {"email", "tax_id", "domain", "duns_number"}:
        return 1.0 if left.strip().lower() == right.strip().lower() else 0.0
    if name == "phone":
        lp, rp = normalize_phone(left), normalize_phone(right)
        if not lp or not rp:
            return 0.0
        if lp == rp:
            return 1.0
        return jaro_winkler(lp, rp)
    return jaro_winkler(left, right)
