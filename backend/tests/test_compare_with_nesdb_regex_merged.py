"""Tests for matching rows to NESdb entries by name in backend/scripts/compare_with_nesdb_regex_merged.py."""

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("requests")

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "compare_with_nesdb_regex_merged.py"
_spec = importlib.util.spec_from_file_location("compare_with_nesdb_regex_merged", _SCRIPT)
rx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rx)


@pytest.mark.parametrize("ours, theirs", [
    ("Akt3", "Akt1"),
    ("Transcription factor SOX-8", "Transcription factor SOX-9"),
    ("cyclin B2", "Cyclin B1"),
    ("myocardin-related transcription factor B", "Myocardin-related transcription factor A"),
    ("Baculoviral IAP repeat-containing protein 3", "Baculoviral IAP repeat-containing protein 2"),
    ("AMP-activated protein kinase catalytic subunit alpha-1", "5'-AMP-activated protein kinase catalytic subunit alpha-2"),
    ("PP2A 56 kDa regulatory subunit epsilon isoform", "PP2A 56 kDa regulatory subunit alpha isoform"),
    ("HPV16 E7", "HPV11 E7"),
    ("pUL48", "pUL4"),
    ("BRO-C", "BRO-a"),
])
def test_another_member_of_the_family_clashes(ours, theirs):
    assert rx.member_clash(ours, theirs)


@pytest.mark.parametrize("ours, theirs", [
    ("topoisomerase IIb", "DNA topoisomerase II, beta isozyme"),
    ("DNA topoisomerase 2-alpha", "DNA topoisomerase II, alpha isozyme"),
    ("YAP", "Transcriptional coactivator YAP1"),  # no member stated: not a clash
    ("AMP-activated protein kinase catalytic subunit alpha-2", "5'-AMP-activated protein kinase catalytic subunit alpha-2"),
    ("class II transactivator", "MHC class II transactivator"),
])
def test_the_same_member_written_differently_does_not_clash(ours, theirs):
    assert not rx.member_clash(ours, theirs)


def test_match_score_skips_names_of_another_member():
    entry = {"full_name": "RAC-alpha serine/threonine-protein kinase", "name": "Akt1", "alt_names": "PKB alpha"}
    assert rx.name_score("Akt3", entry) >= 0.75  # spelling alone would match Akt3 to Akt1
    assert rx.match_score("Akt3", entry) < 0.75
    assert rx.match_score("Akt1", entry) == 1.0
