"""Tests for PaperProcessor._deduplicate_units — numbered-figure regression.

Regression coverage for a real bug: unit identification correctly found 7
distinct units ("Fig. 1" through "Fig. 7"), but semantic-similarity-based
deduplication collapsed all 7 into 1, because short numbered names like these
embed too close together for any similarity threshold to separate "different
figure number" from "same figure, different naming convention" — empirically,
'Fig. 1' vs 'Fig. 7' scores about as similar as a genuine duplicate pair like
'Figure 1' vs 'Figure 1a'.

Driven directly against a minimal self stand-in, following this repo's
pattern of testing PaperProcessor methods unbound (see
test_ground_and_enforce.py) — real embedding model, no mocking, no LLM calls.
"""

from types import SimpleNamespace

from schematiq.value_extraction.core.paper_processor import PaperProcessor


def _units(*names):
    return [{"unit_name": n, "confidence": "high"} for n in names]


def test_distinct_figure_numbers_are_not_merged():
    units = _units("Fig. 1", "Fig. 2", "Fig. 3", "Fig. 4", "Fig. 5", "Fig. 6", "Fig. 7")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert len(result) == 7
    assert {u["unit_name"] for u in result} == {
        "Fig. 1", "Fig. 2", "Fig. 3", "Fig. 4", "Fig. 5", "Fig. 6", "Fig. 7",
    }


def test_genuine_naming_variant_duplicates_are_still_merged():
    """The fix must not break the dedup step's actual purpose: two names
    referring to the same real entity, neither matching the numbered-figure
    pattern, should still merge via the existing similarity/substring logic."""
    units = _units("D1 domain", "The D1 domain")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert len(result) == 1


def test_single_unit_is_returned_unchanged():
    units = _units("Fig. 1")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert result == units


def test_non_numbered_names_still_use_similarity_and_substring_checks():
    """A name that doesn't match the numbered-figure pattern at all must
    fall through to the existing similarity/substring logic unaffected."""
    units = _units("CD45", "CD45 protein")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert len(result) == 1  # substring match merges them, as before
