from nexus_spark_lib.transform.stage1_normalise import (
    _mapping_selection_priority,
    normalise_mapped_source_value,
    strip_null_like,
)


def test_empty_odoo_relation_is_null_not_string():
    assert strip_null_like("[]") is None
    assert normalise_mapped_source_value(
        "[]",
        cdm_attribute="product_template_id",
        field_meta={"attribute_kind": "foreign_key"},
    ) is None


def test_odoo_many2one_extracts_real_foreign_key():
    assert normalise_mapped_source_value(
        '[42, "Template A"]',
        cdm_attribute="product_template_id",
        field_meta={"attribute_kind": "foreign_key"},
    ) == 42
    assert normalise_mapped_source_value(
        "[43, 'Template B']",
        cdm_attribute="product_template_id",
        field_meta={"attribute_kind": "foreign_key"},
    ) == 43


def test_description_numeric_placeholder_is_null():
    assert normalise_mapped_source_value(
        "0.0",
        cdm_attribute="product_description",
        field_meta={"type": "string"},
    ) is None
    assert normalise_mapped_source_value(
        "Version 0.0",
        cdm_attribute="product_description",
        field_meta={"type": "string"},
    ) == "Version 0.0"


def test_explicit_mapping_priority_wins_before_confidence():
    authoritative = _mapping_selection_priority(
        source_attribute="name",
        cdm_attribute="product_name",
        field_meta={"mapping_priority": 100, "mapping_confidence": 0.80},
    )
    fallback = _mapping_selection_priority(
        source_attribute="display_name",
        cdm_attribute="product_name",
        field_meta={"mapping_priority": 10, "mapping_confidence": 0.99},
    )

    assert authoritative > fallback
