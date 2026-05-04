"""Unit tests for Signal B — Probabilistic entity resolution (similarity functions)."""

import pytest

from nexus_spark_lib.transform.stage2_resolve.lsh.similarity import (
    email_similarity,
    jaro_winkler_similarity,
    levenshtein_similarity,
    metaphone_match,
    phonetic_match,
    soundex_match,
)


class TestJaroWinkler:
    def test_identical(self):
        assert jaro_winkler_similarity("Alice Smith", "Alice Smith") == pytest.approx(1.0)

    def test_similar(self):
        score = jaro_winkler_similarity("Alice Smith", "Alyce Smith")
        assert score > 0.90

    def test_very_different(self):
        score = jaro_winkler_similarity("Alice Smith", "Bob Jones")
        assert score < 0.70

    def test_empty_returns_zero(self):
        assert jaro_winkler_similarity("", "Alice") == 0.0


class TestLevenshtein:
    def test_identical(self):
        assert levenshtein_similarity("alice@acme.com", "alice@acme.com") == pytest.approx(1.0)

    def test_one_char_diff(self):
        score = levenshtein_similarity("alice", "alicf")
        assert score >= 0.8

    def test_empty_vs_non_empty(self):
        assert levenshtein_similarity("", "alice") == 0.0

    def test_both_empty(self):
        assert levenshtein_similarity("", "") == pytest.approx(1.0)


class TestPhonetic:
    def test_soundex_match(self):
        # "Smith" and "Smyth" share Soundex code
        assert soundex_match("Smith", "Smyth") is True

    def test_soundex_no_match(self):
        assert soundex_match("Smith", "Jones") is False

    def test_metaphone_match(self):
        assert metaphone_match("Catherine", "Kathryn") is True

    def test_phonetic_match_either(self):
        assert phonetic_match("Smith", "Smyth") is True


class TestEmailSimilarity:
    def test_identical(self):
        assert email_similarity("alice@acme.com", "alice@acme.com") == pytest.approx(1.0)

    def test_same_domain_different_local(self):
        score = email_similarity("alice@acme.com", "alicia@acme.com")
        assert score > 0.6  # domain match (0.5) + partial local match

    def test_different_domain(self):
        score = email_similarity("alice@acme.com", "alice@other.com")
        assert score < 0.7  # no domain match

    def test_empty(self):
        assert email_similarity("", "alice@acme.com") == 0.0
