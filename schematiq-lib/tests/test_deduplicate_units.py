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

import pytest

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


@pytest.mark.parametrize("names", [
    # Real unit lists that dedup used to collapse into one row.
    ("BMAL1 NES1", "BMAL1 NES2", "BMAL1 NES3"),
    ("FANCA NES1 (residues 54-80)", "FANCA NES2 (residues 263-284)", "FANCA NES3 (residues 518-534)"),
    ("SBP2 NES 1", "SBP2 NES 5"),
    ("IkBa N-terminal NES (N-NES)", "IkBa C-terminal NES (C-NES)"),
    ("Survivin central NES (amino acids 84-109)", "Survivin C-terminal NES (amino acids 119-142)"),
    ("B56α NES", "B56β NES", "B56ε NES"),
    ("Topo IIα NES", "Topo IIβ NES"),
    ("Influenza A virus NEP NES1", "Influenza B virus NEP NES1", "Influenza C virus NEP NES1"),
    ("BRO-A NES", "BRO-C NES", "BRO-D NES"),
    ("POST NES-a", "POST NES-b", "POST NES-c"),
    ("PGC-1α NES 1", "PGC-1α NES 2"),
    ("ZO-2 NES-0", "ZO-2 NES-1", "ZO-2 NES-2"),
    ("p53 amino-terminal NES (p53nNES)", "p53 carboxyl-terminal NES (p53cNES)"),
    ("Eriocheir sinensis SOX14A NES", "Eriocheir sinensis SOX14B NES"),
])
def test_units_with_conflicting_markers_are_not_merged(names):
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), _units(*names))
    assert [u["unit_name"] for u in result] == list(names)


@pytest.mark.parametrize("names", [
    ("CPEB1 NES 95-104", "CPEB1 NES1 (95-104)"),  # same numbers, one more
    ("p53 NES", "p53 C-terminal NES"),  # one name only adds a marker
    ("Figure 1", "Figure 1a"),
    ("N-WASP NES", "N-WASP NES (residues 10-20)"),
])
def test_names_that_only_add_markers_still_merge(names):
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), _units(*names))
    assert len(result) == 1


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
