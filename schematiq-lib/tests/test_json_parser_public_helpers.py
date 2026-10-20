"""Tests for JSONResponseParser's public helper methods used by
paper_processor.py's corroboration-gating (is_type_constraint,
matches_allowed_value). Both are public (no leading underscore), unlike
_normalize_to_allowed_values which they wrap -- so, unlike that private
method, they must not crash on the kind of malformed/unexpected input a
future caller (not just the one call site that exists today) might pass.
"""

import pytest

from schematiq.value_extraction.core.json_parser import JSONResponseParser


@pytest.fixture
def parser() -> JSONResponseParser:
    return JSONResponseParser()


class TestIsTypeConstraint:
    def test_recognizes_date_number_and_range_constraints(self, parser: JSONResponseParser) -> None:
        for allowed in (["date"], ["date:us"], ["number"], ["0-100"], ["0.0-1.0"]):
            assert parser.is_type_constraint(allowed), allowed

    def test_categorical_list_is_not_a_type_constraint(self, parser: JSONResponseParser) -> None:
        assert not parser.is_type_constraint(["bar chart", "line plot"])

    def test_single_categorical_value_is_not_a_type_constraint(self, parser: JSONResponseParser) -> None:
        assert not parser.is_type_constraint(["yes"])

    def test_none_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.is_type_constraint(None) is False

    def test_empty_list_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.is_type_constraint([]) is False

    def test_non_string_entry_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.is_type_constraint([42]) is False


class TestMatchesAllowedValue:
    def test_returns_canonical_value_on_match(self, parser: JSONResponseParser) -> None:
        assert parser.matches_allowed_value("Bar Chart", ["bar chart"]) == "bar chart"

    def test_returns_none_on_no_match(self, parser: JSONResponseParser) -> None:
        assert parser.matches_allowed_value("something else", ["bar chart"]) is None

    def test_none_answer_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.matches_allowed_value(None, ["bar chart"]) is None

    def test_non_string_answer_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.matches_allowed_value(42, ["bar chart"]) is None

    def test_empty_allowed_values_does_not_raise(self, parser: JSONResponseParser) -> None:
        assert parser.matches_allowed_value("bar chart", []) is None
