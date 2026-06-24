"""Tests for date normalization in JSONResponseParser."""

import pytest

from schematiq.value_extraction.core.json_parser import JSONResponseParser


@pytest.fixture
def parser() -> JSONResponseParser:
    return JSONResponseParser()


def test_date_iso_normalizes_various_inputs(parser: JSONResponseParser) -> None:
    allowed = ["date"]
    for raw in ("May 6, 2025", "7/21/2025", "2025-05-06", "May 06, 2025"):
        out, matched, _ = parser._normalize_to_allowed_values(raw, allowed)
        assert matched
        assert out == "2025-05-06"


def test_date_us_output_format(parser: JSONResponseParser) -> None:
    allowed = ["date:us"]
    out, matched, _ = parser._normalize_to_allowed_values("May 6, 2025", allowed)
    assert matched
    assert out == "05/06/2025"


def test_date_long_output_format(parser: JSONResponseParser) -> None:
    allowed = ["date:long"]
    out, matched, _ = parser._normalize_to_allowed_values("7/21/2025", allowed)
    assert matched
    assert out == "July 21, 2025"


def test_unparseable_date_keeps_and_flags(parser: JSONResponseParser) -> None:
    allowed = ["date"]
    out, matched, unmatched = parser._normalize_to_allowed_values("not a date", allowed)
    assert not matched
    assert out == "not a date"
    assert unmatched == "not a date"
