"""Tests for the mutation notations of backend/scripts/compare_with_nesdb_regex.py."""

import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("bs4")

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(_SCRIPTS))  # the script imports compare_with_nesdb_v2 by name
_spec = importlib.util.spec_from_file_location("compare_with_nesdb_regex", _SCRIPTS / "compare_with_nesdb_regex.py")
rx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rx)


@pytest.mark.parametrize("ours", ["Δ336-345", "∆336–345", "del336-345", "Delta 336-345", "deletion of 336-345",
                                  "Deletion of residues 336-345", "deletion of 336-LHGTMRPLSL-345",
                                  "336-345 deletion", "336-345aaDel"])
def test_deletion_matches_nesdb_deletion_however_written(ours):
    assert rx.mutation_set(ours) & rx.mutation_set("336 LHGTMRPLSL 345 by Deletion") == {("336-345", "Δ")}


def test_deleted_residues_next_to_each_other_are_one_deletion():
    nesdb = rx.mutation_set("T1035 by Deletion/V1036 by Deletion")
    assert nesdb == {("1035-1036", "Δ")}
    for ours in ["L1035del/K1036del", "L1035Δ/K1036Δ", "L1035/K1036 deleted", "Δ1035-1036"]:
        assert rx.mutation_set(ours) == nesdb


def test_list_of_deleted_ranges():
    assert rx.mutation_set("148-156/162-171/186-194 by Deletion") == {
        ("148-156", "Δ"), ("162-171", "Δ"), ("186-194", "Δ")}


@pytest.mark.parametrize("ours", ["335-344 deletion", "Δ336-344", "Δ336", "ΔC69", "NES2Δ5", "deletion of NES2",
                                  "336-345", "L336A/L338A"])
def test_other_deletions_and_bare_ranges_do_not_match(ours):
    assert not rx.mutation_set(ours) & rx.mutation_set("336 LHGTMRPLSL 345 by Deletion")


def test_stretch_mutated_to_one_residue():
    assert rx.mutation_set("82 LLM 84 mutated to A") == {("82", "A"), ("83", "A"), ("84", "A")}
    assert rx.mutation_set("82 LLM 84 mutated to A") == rx.mutation_set("L82A/L83A/M84A")


def test_stretch_whose_length_disagrees_with_its_range_is_not_read():
    assert rx.mutation_set("82 LLM 85 mutated to A") == set()


def test_substitution_is_not_a_deletion_of_the_same_residues():
    assert not rx.mutation_set("L440A/L441A/Q442A/L443A") & rx.mutation_set("440 LLQL 443 by Deletion")
