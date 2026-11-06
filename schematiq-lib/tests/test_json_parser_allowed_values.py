"""Normalization to allowed_values must not change a value's numbers (#557).

Categorical matching is exact up to case, whitespace, and punctuation at the
ends (#548); anything else is kept as written."""

from schematiq.value_extraction.core.json_parser import JSONResponseParser

CITATIONS = [
    "2025 WL 1825431",
    "404 F. Supp. 3d 109",
    "766 F.Supp.3d 266",
    "2025 WL 1233674",
    "2025 WL 1145250",
]


def _norm(answer, allowed):
    return JSONResponseParser()._normalize_to_allowed_values(answer, allowed)


def test_citation_is_not_mapped_to_a_different_citation():
    out, matched, unmatched = _norm("2025 WL 1325305", CITATIONS)
    assert out == "2025 WL 1325305"
    assert matched is False
    assert unmatched is None


def test_close_citation_number_is_not_mapped():
    out, matched, _ = _norm("2025 WL 1234567", CITATIONS)
    assert out == "2025 WL 1234567"
    assert matched is False


def test_same_citation_with_different_spacing_still_maps():
    out, matched, _ = _norm("766 F. Supp. 3d 266", CITATIONS)
    assert out == "766 F.Supp.3d 266"
    assert matched is True


def test_categorical_number_mismatch_is_not_mapped():
    out, matched, _ = _norm("phase 2", ["phase 3", "phase 4"])
    assert out == "phase 2"
    assert matched is False


def test_near_miss_is_kept_as_written():
    # No fuzzy matching: a plural or synonym is the model's job (#548).
    out, matched, _ = _norm("transformers", ["transformer", "cnn", "rnn"])
    assert out == "transformers"
    assert matched is False


def test_exact_case_insensitive_match_unchanged():
    out, matched, _ = _norm("2025 wl 1825431", CITATIONS)
    assert out == "2025 WL 1825431"
    assert matched is True
