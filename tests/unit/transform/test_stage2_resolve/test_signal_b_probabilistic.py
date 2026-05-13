from __future__ import annotations

from nexus_spark_lib.transform.stage2_resolve.signals.signal_b_probabilistic import _score_pair


def test_score_pair_uses_address_weight_for_free_text_similarity() -> None:
    score = _score_pair(
        record_a={
            "address": {
                "value": "221B Baker Street, London",
                "attribute_kind": "free_text",
            }
        },
        record_b={
            "address": {
                "value": "221 Baker Street London",
                "attribute_kind": "free_text",
            }
        },
        weights={"address": 1.0},
    )

    assert score > 0.8


def test_score_pair_uses_metadata_attribute_kind_for_non_default_attribute_name() -> None:
    score = _score_pair(
        record_a={
            "description": {
                "value": "Global manufacturing group with strong EU footprint",
                "attribute_kind": "free_text",
            }
        },
        record_b={
            "description": {
                "value": "Global manufacturing grp with strong EU footprint",
                "attribute_kind": "free_text",
            }
        },
        weights={"description": 1.0},
    )

    assert score > 0.85