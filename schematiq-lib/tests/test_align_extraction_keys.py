"""Tests for controlled-generation column key alignment."""

from schematiq.value_extraction.utils.schema_builder import (
    align_extraction_keys_to_schema,
    sanitize_column_name,
)


def test_sanitize_column_name_spaces_and_hyphens():
    assert sanitize_column_name("judge full name") == "judge_full_name"
    assert sanitize_column_name("IssueCourt-1") == "IssueCourt_1"
    assert sanitize_column_name("plain_snake") == "plain_snake"


def test_align_extraction_keys_to_schema_remaps_sanitized_keys():
    row = {
        "_row_name": "Row A",
        "judge_full_name": {"answer": "Jane"},
        "plain_snake": {"answer": "ok"},
    }
    aligned = align_extraction_keys_to_schema(
        row, ["judge full name", "plain_snake"]
    )
    assert aligned["judge full name"]["answer"] == "Jane"
    assert "judge_full_name" not in aligned
    assert aligned["plain_snake"]["answer"] == "ok"
    assert aligned["_row_name"] == "Row A"
