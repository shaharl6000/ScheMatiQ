"""Regex patterns hardened in the regex audit: what they must match, what they must reject, and that a
hostile input can't make them hang."""

import importlib.util
import time
from pathlib import Path

import pytest

from app.services import row_filtering
from app.services.data_collection_service import DataCollectionService
from app.services.file_parser import FileParser
from app.services.pubmed_enrichment_service import _clean_source_doc_to_title, _extract_pmid_from_filename
from app.services.uniprot_lookup import extract_gene_symbols


# ---------------------------------------------------------------- excerpt fallback parse (file_parser)

@pytest.mark.parametrize("raw,text,source", [
    # Apostrophes and an "'}" inside the excerpt: not valid Python, so the regex fallback reads it.
    ("{'text': 'it's in the 'core'} region', 'source': 'a.pdf'}", "it's in the 'core'} region", "a.pdf"),
    ("{'text': 'Leu-to-Ala', 'source': 'x', so it's exported', 'source': 'b.pdf'}",
     "Leu-to-Ala', 'source': 'x', so it's exported", "b.pdf"),
    ("{'text': 'no source, it's fine'}", "no source, it's fine", "fallback.pdf"),
])
def test_excerpt_fallback_reads_the_whole_text(tmp_path, raw, text, source):
    data = {"Col": "value", "Col_excerpts": f"{raw} | {{'text': 'second', 'source': 'c.pdf'}}"}
    excerpts = FileParser(data_dir=str(tmp_path))._merge_excerpt_columns(data, "fallback.pdf")["Col"]["excerpts"]
    assert excerpts[0] == {"text": text, "source": source}
    assert excerpts[1] == {"text": "second", "source": "c.pdf"}


# ---------------------------------------------------------------- gene symbols for the UniProt lookup

@pytest.mark.parametrize("alt_names,symbols", [
    ("HTT; Huntingtin; HD", ["HTT", "HD"]),
    ("Bmal1, Mop3, Arnt3", ["Bmal1", "Mop3", "Arnt3"]),  # mouse symbols: mixed case with a digit
    ("Gln3; C9orf72; HLA-A", ["Gln3", "C9orf72", "HLA-A"]),
    ("Menin; Pericentrin; None", []),  # plain words are names, not symbols
    ("X; p53 protein; A1B2C3D4E5F6G7", []),  # too short, has a space, too long
])
def test_extract_gene_symbols(alt_names, symbols):
    assert extract_gene_symbols(alt_names) == symbols


# ---------------------------------------------------------------- row filtering: user-typed regex

def _regex_filter(value, pattern, case_sensitive=False):
    return row_filtering._evaluate_filter(value, "regex", pattern, case_sensitive)


@pytest.mark.parametrize("value,pattern,case_sensitive,expected", [
    ("Homo sapiens", r"^homo", False, True),
    ("Homo sapiens", r"^homo", True, False),
    ("L78A/L81A", r"L\d+A", True, True),
    ("CRM1", r"^XPO1$", False, False),
])
def test_regex_filter_matches_like_re(value, pattern, case_sensitive, expected):
    assert _regex_filter(value, pattern, case_sensitive) is expected


@pytest.mark.parametrize("pattern", ["(unclosed", "[a-", "*leading", "a" * 501])
def test_regex_filter_rejects_invalid_or_oversized_patterns(pattern):
    assert _regex_filter("anything", pattern) is False


def test_regex_filter_stops_catastrophic_backtracking():
    rows = ["a" * 30 + "!"] * 200
    start = time.perf_counter()
    assert not any(_regex_filter(v, r"^(a+)+$") for v in rows)
    # With `re` one row alone takes minutes; the timeout and the memo of timed-out patterns keep it short.
    assert time.perf_counter() - start < 2


# ---------------------------------------------------------------- data collection: secret redaction

@pytest.mark.parametrize("key", ["api_key", "API-KEY", "apiKey", "openai_api_keys", "credentials", "credential",
                                 "client_secret", "secrets", "password", "db_passwd", "access_token", "token",
                                 "private_key"])
def test_sanitize_config_redacts_secret_keys(key):
    assert DataCollectionService._sanitize_config({key: "value"})[key] == "***REDACTED***"


@pytest.mark.parametrize("key", ["max_tokens", "tokenizer", "model", "temperature", "secretary_name_length"])
def test_sanitize_config_keeps_ordinary_keys(key):
    assert DataCollectionService._sanitize_config({key: 7})[key] == 7


# ---------------------------------------------------------------- session id in the request path

def test_session_id_in_path_is_a_whole_segment():
    from app.main import _SESSION_ID_IN_PATH
    sid = "123e4567-e89b-12d3-a456-426614174000"
    assert _SESSION_ID_IN_PATH.search(f"/api/sessions/{sid}/data").group(1) == sid
    assert _SESSION_ID_IN_PATH.search(f"/api/sessions/{sid}").group(1) == sid
    assert _SESSION_ID_IN_PATH.search(f"/api/sessions/{sid}ff/data") is None
    assert _SESSION_ID_IN_PATH.search("/api/sessions/not-a-uuid/data") is None


# ---------------------------------------------------------------- PubMed: titles and PMIDs from file names

@pytest.mark.parametrize("source_doc,title", [
    ("co-A Saturated FG-Repeat Hydrogel", "A Saturated FG-Repeat Hydrogel"),
    ("amb-Capturing directed molecular motion in", "Capturing directed molecular motion in"),
    ("co-mRNA export requires CRM1", "mRNA export requires CRM1"),  # a display title: prefix always goes
    ("ATM_phosphorylates_PP2A_36434396_full.txt", "ATM phosphorylates PP2A"),
    ("beta-catenin_shuttles_15175654_full.txt", "beta-catenin shuttles"),  # the paper's own word stays
])
def test_clean_source_doc_to_title(source_doc, title):
    assert _clean_source_doc_to_title(source_doc) == title


@pytest.mark.parametrize("source_doc,pmid", [
    ("Bach1_15175654_full", "15175654"),
    ("CALM_24397609.txt", "24397609"),
    ("co-Bach1_15175654_full.pdf", "15175654"),
    ("Review_2004", None),           # a year, not a PMID
    ("Paper_1234567890_full", None),  # too long to be a PMID
])
def test_extract_pmid_from_filename(source_doc, pmid):
    assert _extract_pmid_from_filename(source_doc) == pmid


# ---------------------------------------------------------------- NESdb comparison script

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "compare_with_nesdb_v2.py"
pytest.importorskip("bs4")
_spec = importlib.util.spec_from_file_location("compare_with_nesdb_v2_regex_tests", _SCRIPT)
cmp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cmp)


@pytest.mark.parametrize("text,ranges", [
    ("188-198", [("188", "198")]),
    ("residues 74 to 88", [("74", "88")]),
    ("PMID 15175654-1", []),        # not a range inside a longer number
    ("12345-6789", []),
    ("NES 1..12, 30–44", [("1", "12"), ("30", "44")]),
])
def test_range_re_takes_whole_numbers(text, ranges):
    assert cmp.RANGE_RE.findall(text) == ranges


def test_mutation_list_re_reads_lists_once():
    assert cmp.mutation_set("L348/352A") == {("348", "A"), ("352", "A")}
    assert cmp.mutation_set("14,16L->A") == {("14", "A"), ("16", "A")}
    start = time.perf_counter()
    cmp.MUTATION_LIST_RE.findall("1/" * 5000 + "x")
    assert time.perf_counter() - start < 0.5
