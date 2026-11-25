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


def test_numbered_units_with_different_numbers_are_not_merged():
    """Any numbered names, not only figures. 'Patient 1' is a substring of
    'Patient 12', and 'Experiment 1' / 'Experiment 10' embed above the
    threshold, so before the generic number check these rows were lost."""
    names = (
        "Patient 1", "Patient 2", "Patient 12",
        "Experiment 1", "Experiment 10",
        "Table 1", "Table 2",
    )
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), _units(*names))
    assert [u["unit_name"] for u in result] == list(names)


def test_same_number_variants_are_still_merged():
    units = _units("Patient 1", "Patient 1 (control group)")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert len(result) == 1


def test_different_hebrew_names_are_not_merged():
    """Regression: two different justices scored 0.864 with the default
    English-only model (above 0.85) and one opinion was lost. Non-Latin names
    are not compared by embedding unless a model is configured."""
    names = (
        "השופט א' רובינשטיין", "השופט א' גרוניס", "השופט ע' פוגלמן",
        "השופט ע' גרוסקופף", "השופט י' עמית", "השופט י' אלרון",
        "חלבון p53", "חלבון BRCA1",
    )
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), _units(*names))
    assert [u["unit_name"] for u in result] == list(names)


def test_hebrew_name_contained_in_another_is_merged():
    units = _units("השופט א' רובינשטיין", "השופט רובינשטיין", "חלבון p53", "p53")
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), units)
    assert [u["unit_name"] for u in result] == ["השופט א' רובינשטיין", "חלבון p53"]


def test_non_latin_names_do_not_load_the_default_model(monkeypatch):
    from schematiq.value_extraction.core import paper_processor as pp_mod

    def fail(*args, **kwargs):
        raise AssertionError("embedding computed for non-Latin names")

    monkeypatch.setattr(pp_mod, "_embed", fail)
    units = _units("השופט נ' סולברג", "השופט נ' הנדל", "קבוצת ביקורת", "קבוצת טיפול")
    assert len(PaperProcessor._deduplicate_units(SimpleNamespace(), units)) == 4


def test_latin_names_still_use_embeddings():
    units = _units("colour", "color")
    assert len(PaperProcessor._deduplicate_units(SimpleNamespace(), units)) == 1
