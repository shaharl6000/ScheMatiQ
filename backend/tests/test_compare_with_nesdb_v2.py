"""Tests for backend/scripts/compare_with_nesdb_v2.py (NESdb comparison, v2)."""

import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("bs4")

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "compare_with_nesdb_v2.py"
_spec = importlib.util.spec_from_file_location("compare_with_nesdb_v2", _SCRIPT)
cmp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cmp)

ARGS = SimpleNamespace(text_threshold=0.6)
AID = {
    "nes_id": "75", "name": "AID", "full_name": "Activation-induced cytidine deaminase", "alt_names": "AICDA",
    "organism": "Homo sapiens (Human)", "uniprot": "Q9GZX7", "pmids": ["19351893"],
    "signals": [{"start": 188, "seq": "DLRDAFRTLGL", "end": 198}], "signals_text": "188 DLRDAFRTLGL 198",
    "mutations_export": "Unknown", "mutations_crm1": "Unknown", "evidence": "LMB Sensitive",
    "secondary_structure": "Unknown", "other_residues": "Unknown", "sequence": "MDSLLMNRRKFLYQFKNVRWAKGRRETYLC",
    "set": "good",
}
MAPPING = {"Organism": "organism", "Nes Peptide Sequence": "signals", "Nes Coordinates": "signals",
           "Mutations": "mutations_export"}


def row(**cells):
    base = {"Organism": "", "Nes Peptide Sequence": "", "Nes Coordinates": "", "Mutations": ""}
    base.update(cells)
    return base


def test_entry_signals_parses_a_region_broken_by_spaces():
    entry = {"signals": [], "signals_text": "194 SLPPEPMMPMDQTMHP DLL 256"}
    assert cmp.entry_signals(entry) == [{"start": 194, "seq": "SLPPEPMMPMDQTMHPDLL", "end": 256}]


def test_entry_signals_takes_the_end_from_the_length_when_missing():
    entry = {"signals": [], "signals_text": "321 SMIEDVLGEG SVSAS"}
    assert cmp.entry_signals(entry) == [{"start": 321, "seq": "SMIEDVLGEGSVSAS", "end": 335}]


def test_entry_signals_undetermined_has_none():
    assert cmp.entry_signals({"signals": [], "signals_text": "Undetermined"}) == []


def test_signal_hit_by_sequence_and_by_location():
    s = AID["signals"][0]
    assert cmp.signal_hit("DLRDAFRTLGL", s) == "sequence"
    assert cmp.signal_hit("amino acids 190-200", s) == "location"
    assert cmp.signal_hit("amino acids 1-22", s) is None


def test_classify_row_gives_the_matching_signal():
    kind, s = cmp.classify_row(row(**{"Nes Peptide Sequence": "LRDAFRTLGL"}), AID, MAPPING, ARGS)
    assert kind == "sequence" and s["start"] == 188


def test_classify_row_coordinates_only_is_a_location_match():
    kind, _ = cmp.classify_row(row(**{"Nes Coordinates": "188-198"}), AID, MAPPING, ARGS)
    assert kind == "location"


def test_classify_row_other_nes_of_the_same_protein_is_extra():
    r = row(Organism="Human", **{"Nes Peptide Sequence": "ILVCSLMLLIRQLKI", "Nes Coordinates": "462-476"})
    assert cmp.classify_row(r, AID, MAPPING, ARGS) == (cmp.EXTRA_NES, None)


def test_classify_row_ortholog_is_another_species():
    r = row(Organism="Fugu", **{"Nes Peptide Sequence": "VRDALKLLGF"})
    assert cmp.classify_row(r, AID, MAPPING, ARGS) == (cmp.OTHER_PROTEIN, None)


def test_classify_row_without_any_nes():
    r = row(Organism="Human", **{"Nes Peptide Sequence": "LxxxVxxVxL"})
    assert cmp.classify_row(r, AID, MAPPING, ARGS) == (cmp.NO_NES_GIVEN, None)


def test_classify_row_when_nesdb_has_no_mapped_nes():
    entry = dict(AID, signals=[], signals_text="Undetermined")
    r = row(**{"Nes Peptide Sequence": "LRDAFRTLGL"})
    assert cmp.classify_row(r, entry, MAPPING, ARGS) == (cmp.NESDB_UNDETERMINED, None)


def test_in_text_ignores_spacing_but_not_case():
    letters = "TheNESLRDAFRTLGLwasmapped"
    assert cmp.in_text("LRDAFRTLGL", letters)
    assert not cmp.in_text("ALANINE", "thealaninewasmutated")


def test_is_external():
    assert cmp.is_external("sequence", "MDSLLMNRRKFLYQFKNVRWAKGRRETYLCYVVKRRDSATSFSLDFGYLRNKNGCHVELLFLRYISDWDLDPGRCYRVTWFTSWSPCYDCARHVADFLRGNPNLSLRIFTARLYFCEDRKAEPEGLRRLHRAGVQIAIMTFKDYFYCWNTFVENHERTFKAWEGLHENSVRLSRQLRRILLPLYEVDDLRDAFRTLGL")
    assert not cmp.is_external("sequence", "DLRDAFRTLGL")  # an NES in the sequence column
    assert cmp.is_external("uniprot", "Q9GZX7")
    assert not cmp.is_external("organism", "Human")


# A made-up 40-residue protein whose NES (DLRDAFRTLGL) sits at 21-31.
PROTEIN = "MSTNPKQAGEHYVRWCFILP" + "DLRDAFRTLGL" + "SEKQNAGHYV"
NES = {"start": 21, "seq": "DLRDAFRTLGL", "end": 31}


@pytest.mark.parametrize("values, expected", [
    (["DLRDAFRTLGL", "21-31"], cmp.PEPTIDE_AT_RESIDUES),
    (["DLRDAFRTLGL", "22-32"], cmp.PEPTIDE_AT_RESIDUES),       # one residue off: same NES
    (["DLRDAFRTLGL", "101-111"], cmp.NUMBERING_DIFFERS),       # another isoform's numbering
    (["DLRDAFRTLGL"], cmp.PEPTIDE_IN_PROTEIN),
    (["LRDAFKTLGL", "21-30"], cmp.PEPTIDE_NOT_IN_PROTEIN),     # ortholog / mutant
    (["35-80"], cmp.RANGE_OUTSIDE),
])
def test_uniprot_check_of_a_row(values, expected):
    assert cmp.uniprot_check(values, PROTEIN)[0] == expected


@pytest.mark.parametrize("values, expected", [
    (["amino acids 21-31"], cmp.RANGE_IS_NES),
    (["1-40"], cmp.RANGE_TOO_WIDE),
    (["29-35"], cmp.RANGE_SMALL_OVERLAP),
])
def test_uniprot_check_of_a_location_match(values, expected):
    assert cmp.uniprot_check(values, PROTEIN, NES)[0] == expected


def test_uniprot_check_reads_the_residues_at_an_unmatched_range():
    assert cmp.uniprot_check(["1-5"], PROTEIN) == (cmp.RANGE_RESIDUES, "1-5 MSTNP")


def test_protein_sequence_prefers_nesdb_and_does_not_fetch_without_need():
    store = {}
    assert cmp.protein_sequence({"sequence": "mstnp\nkq", "uniprot": "Q9GZX7"}, store, fetch=True) == "MSTNPKQ"
    assert cmp.protein_sequence({"sequence": "", "uniprot": "Q9GZX7"}, store, fetch=False) == ""
    assert store == {}


def _write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def test_main_per_signal_and_row_classes(tmp_path):
    cache = tmp_path / "nesdb.json"
    cache.write_text(json.dumps([AID]), encoding="utf-8")
    docs = tmp_path / "documents"
    (docs / "figures" / "AID_19351893_full").mkdir(parents=True)
    (docs / "figures" / "AID_19351893_full" / "fig001.png").write_bytes(b"")
    (docs / "AID_19351893_full.txt").write_text("The human NES DLRDAFRTLGL and the chicken NES LRDAFKTLGL.",
                                                encoding="utf-8")
    base = {"Full Name": "Activation-induced cytidine deaminase", "Source Document": "AID_19351893_full",
            "Organism": "Human", "Nes Peptide Sequence": "", "Nes Coordinates": "", "Sequence": ""}
    _write_csv(tmp_path / "export.csv", [
        dict(base, **{"Nes Coordinates": "188-198"}),                         # human NES, location only
        dict(base, Organism="Chicken", **{"Nes Peptide Sequence": "LRDAFKTLGL"}),  # ortholog
    ])
    cmp.main([str(tmp_path / "export.csv"), "--cache", str(cache), "--out-dir", str(tmp_path),
              "--docs-dir", str(docs), "--uniprot-cache", str(tmp_path / "uniprot.json"), "--no-uniprot-fetch"])

    with (tmp_path / "export_vs_nesdb_per_signal.csv").open(encoding="utf-8-sig") as f:
        signals = list(csv.DictReader(f))
    assert len(signals) == 1
    assert signals[0]["Status"] == "location"
    assert signals[0]["Why not by sequence"] == "peptide is in the paper text"

    with (tmp_path / "export_vs_nesdb.csv").open(encoding="utf-8-sig") as f:
        table = [r for r in csv.DictReader(f) if r["Source Document"]]
    assert table[0]["NES Match"] == "location: 188-198 DLRDAFRTLGL"
    assert table[1]["NES Match"] == cmp.OTHER_PROTEIN
    assert table[1]["Recovered"] == ""  # not scored

    with (tmp_path / "export_vs_nesdb_summary.csv").open(encoding="utf-8-sig") as f:
        summary = {(r["Section"], r["Metric"]): r["Value"] for r in csv.DictReader(f)}
    assert summary[("Rows", "CSV rows")] == "2"
    assert summary[("Per NESdb signal", "location")] == "1"
    assert summary[("Rows by NES match", cmp.OTHER_PROTEIN)] == "1"


@pytest.mark.parametrize("value, peptides", [
    ("signal at the C-terminal end", []),                      # lower-case words made of amino-acid letters
    ("Terminal NES, confirmed by mutagenesis", []),            # capitalised word; never part of a word ("NFIRMED")
    ("LMB-sensitive (RanGTP, NetNES)", []),                     # names with a few capitals
    ("NES: LQLPPLERLTL (residues 37-47)", ["LQLPPLERLTL"]),
    ("LDtLLdsLsL", ["LDTLLDSLSL"]),                             # non-consensus residues in lower case
    ("MtLgmIwtIiLr", ["MTLGMIWTIILR"]),
    ("STRADalpha binds HNSCCs", []),                            # a Greek letter spelled out; a plural acronym
    ("TOPFLASH assay", []),                                     # an upper-case word with a non-residue letter
    ("QDEGQESLEEELDVLVLDDEGGOVSYPSM", ["QDEGQESLEEELDVLVLDDEGG", "VSYPSM"]),  # a sequence with a typo
])
def test_find_peptides_reads_whole_peptide_words_only(value, peptides):
    assert cmp.find_peptides(value) == peptides
