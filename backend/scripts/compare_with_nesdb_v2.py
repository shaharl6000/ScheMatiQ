"""Compare a ScheMatiQ CSV export of NES proteins with the NESdb (LRNes) ground truth.

Version 2 of compare_with_nesdb.py (which is kept unchanged). Differences:
  * Each row is tied to one specific NES of the NESdb protein, by peptide or by
    coordinates (columns like "NES Coordinates" now count too). A row whose NES
    is none of NESdb's is reported as an extra NES, and a row about another
    species or protein (an ortholog, another virus' Rev) as such - neither is
    counted as a miss.
  * A per-signal table answers "which of the NESs NESdb cites this paper for did
    any row find?" - by sequence, by location only, or missed - and, with
    --docs-dir, why a missed one was missed (in the paper text or not, figures).
  * NESdb signals written as a long region broken by spaces are parsed too.
  * The full protein sequence and the UniProt ID come from UniProt, not from the
    paper, so they are reported but left out of the totals (--score-external to
    count them).
  * UniProt check: each row's NES is looked up in the protein's UniProt sequence
    (NESdb's copy, on which NESdb numbers its NESs; fetched from UniProt only when
    NESdb has none). A peptide is found at the row's residue numbers, elsewhere
    (another numbering or isoform), or not at all (an ortholog, a mutant or a wrong
    extraction); for a row with residue numbers only, the protein's residues there
    are read and, for a location match, compared with NESdb's NES.

Each CSV row is matched to an NESdb protein by its full-name column (also tried
against NESdb's short and alternative names). Every other column is then mapped
to an NESdb field (from its header, from --map, or by picking the field whose
values agree with it most often) and each cell is scored as recovered or not.
Cells whose NESdb value is Unknown/Undetermined are not counted.

Writes, next to the CSV (or to --out-dir):
  <name>_vs_nesdb.csv             the table + per-row "NES Match" and "Recovered" columns
                                  and a footer row with how many values each column recovered
  <name>_vs_nesdb_per_nes.csv     per NES ID: how many NESdb fields any row recovered
  <name>_vs_nesdb_per_signal.csv  per paper and NESdb signal: found by sequence / location / missed
  <name>_vs_nesdb_cells.csv       every compared cell, side by side with NESdb
  <name>_vs_nesdb_summary.csv     the printed summary, with a note on how each number was reached

NESdb pages are scraped once and cached in --cache.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\compare_with_nesdb_v2.py <export.csv>
        [--name-col "Full Name"] [--map "CSV column=field" ...] [--skip "CSV column" ...]
        [--docs-dir <project documents folder>] [--score-external]
        [--llm-judge] [--include-uncited] [--include-doubt] [--refresh]

--docs-dir is the project's documents folder (backend/data/<session id>/documents): every
paper in it is checked, including papers that produced no rows.

--llm-judge sends free-text cells the rules call a miss (evidence, secondary structure, other
residues, and mutations written in words) to Gemini (GEMINI_API_KEY from backend/.env) and saves each verdict, so re-runs are free.

Fields for --map: name, alt_names, organism, uniprot, evidence, mutations,
mutations_export, mutations_crm1, signals, secondary_structure, other_residues,
sequence, pmids.
"""

import argparse
import csv
import html
import json
import re
import statistics
import sys
import time
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://prodata.swmed.edu/LRNes/IndexFiles/"
PAGES = {"good": "namesGood.php", "doubt": "namesDoubt.php"}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"}
DEFAULT_CACHE = Path.home() / "Downloads" / "NES" / "nesdb_entries.json"

ID_COLUMNS = {"doc name", "row name", "observation unit", "unit name", "source document"}
NO_VALUE = {"", "unknown", "undetermined", "none", "n/a", "na", "-", "not reported", "not mentioned", "not specified", "null"}
HEAD_TO_FIELD = {
    "alternative names": "alt_names",
    "organism": "organism",
    "experimental evidence for crm1-mediated export": "evidence",
    "mutations that affect nuclear export": "mutations_export",
    "mutations that affect crm1 binding": "mutations_crm1",
    "functional export signals": "signals",
    "secondary structure of export signal": "secondary_structure",
    "other residues important for export": "other_residues",
}
# Header keyword rules, checked in order: the first rule whose keywords all appear wins.
HEADER_RULES = [
    (("uniprot",), "uniprot"), (("accession",), "uniprot"),
    (("organism",), "organism"), (("species",), "organism"),
    (("alternative",), "alt_names"), (("alias",), "alt_names"), (("synonym",), "alt_names"),
    (("gene",), "alt_names"), (("short", "name"), "alt_names"), (("abbrev",), "alt_names"),
    (("mutation", "crm1"), "mutations_crm1"), (("mutation", "binding"), "mutations_crm1"),
    (("mutation",), "mutations"), (("mutant",), "mutations"),
    (("secondary",), "secondary_structure"), (("helix",), "secondary_structure"),
    (("other", "residue"), "other_residues"), (("contact",), "other_residues"),
    (("full", "sequence"), "sequence"), (("protein", "sequence"), "sequence"), (("coordinat",), "signals"),
    (("nes", "sequence"), "signals"), (("export", "signal"), "signals"), (("nes", "motif"), "signals"),
    (("nes", "position"), "signals"), (("evidence",), "evidence"),
    (("pmid",), "pmids"), (("pubmed",), "pmids"),
]
# Columns named exactly like an NESdb field (as ScheMatiQ exports a schema seeded from NESdb).
EXACT_HEADERS = {"alternative names": "alt_names", "sequence": "sequence", "uniprot": "uniprot",
                 **{" ".join(re.sub(r"[^a-z0-9]+", " ", h).split()): f for h, f in HEAD_TO_FIELD.items()}}
GREEK = {"α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta", "ε": "epsilon", "κ": "kappa", "λ": "lambda", "μ": "mu", "σ": "sigma"}
STOPWORDS = {"the", "a", "an", "of", "and", "or", "in", "to", "with", "by", "for", "is", "was", "on", "at", "as", "protein"}
AA_RE = re.compile(r"[ACDEFGHIKLMNPQRSTVWY]{6,}")
THREE_LETTER = {"ala": "A", "arg": "R", "asn": "N", "asp": "D", "cys": "C", "gln": "Q", "glu": "E", "gly": "G",
                "his": "H", "ile": "I", "leu": "L", "lys": "K", "met": "M", "phe": "F", "pro": "P", "ser": "S",
                "thr": "T", "trp": "W", "tyr": "Y", "val": "V"}
_AA3 = "|".join(THREE_LETTER)
# Leu308Ala, Leu-544 to Ala, L619-A, L300->R, L172A
MUTATION3_RE = re.compile(rf"\b({_AA3})[\s\-]?(\d{{1,4}})\s*(?:to|->|→|>|-)?\s*({_AA3})\b", re.I)
MUTATION_RE = re.compile(r"\b([A-Z])-?(\d{1,4})\s*(?:->|→|>|-)?\s*([A-Z])\b")
# L348/352A, L348,352A and 14,16L->A: one substitution applied at several positions
MUTATION_LIST_RE = re.compile(r"\b([A-Z])?(\d{1,4}(?:\s*[/,]\s*\d{1,4})+)\s*([A-Z])?\s*(?:->|→|>|to)?\s*([A-Z])\b")
ORGANISM_ADJECTIVES = {"canine": "dog", "murine": "mouse", "porcine": "pig", "equine": "horse",
                       "feline": "cat", "ovine": "sheep", "simian": "monkey"}
GENERIC_NAME_WORDS = {"kinase", "factor", "receptor", "domain", "subunit", "homolog", "transcription factor",
                      "protein kinase", "nes", "complex", "enzyme", "helicase", "phosphatase"}
GENERIC_ORGANISM_WORDS = {"virus", "cells", "cell", "strain", "mammal", "mammalian", "in vitro", "yeast cells"}
RANGE_RE = re.compile(r"(\d{1,4})\s*(?:-|–|—|to|\.\.)\s*(\d{1,4})")
UNIPROT_RE = re.compile(r"\b([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})\b")
# A peptide as written in a paper or a cell: upper case only, so words like "signal" don't count.
PEPTIDE_RE = re.compile(r"[ACDEFGHIKLMNPQRSTVWY]{6,}")
# NESdb fields that describe one NES rather than the protein: not comparable for a row about another NES.
SIGNAL_FIELDS = {"signals", "secondary_structure", "other_residues", "mutations", "mutations_export", "mutations_crm1"}
# NESdb takes these from UniProt, not from the paper.
EXTERNAL_FIELDS = {"sequence", "uniprot"}
SOURCE_RE = re.compile(r"(.*)_(\d{5,9})_full")


# ---------------------------------------------------------------- NESdb scraping

def parse_details(page_html: str) -> dict:
    soup = BeautifulSoup(page_html, "html.parser")
    entry: dict = {"pmids": sorted(set(re.findall(r"list_uids=(\d+)", page_html)))}
    for head in soup.select("div.head"):
        content = head.find_next_sibling("div", class_="content")
        if content is None:
            continue
        key = " ".join(head.get_text(" ", strip=True).split()).lower()
        if key == "full name":
            link = content.find("a", class_="swiss")
            entry["uniprot"] = link["href"].strip().rstrip("/").split("/")[-1] if link else ""
        if key == "sequence":
            fasta = content.find("pre", id="toggleFasta")
            entry["sequence"] = "".join(fasta.get_text().splitlines()[1:]).strip() if fasta else ""
            continue
        for ref in content.select("sup:has(a.ref)"):
            ref.decompose()
        for note in content.select("div.notes"):
            note.decompose()
        if key == "functional export signals":
            entry["signals"] = [
                {"start": int(s), "seq": seq, "end": int(e)}
                for s, seq, e in re.findall(r"<sup>(\d+)</sup>\s*([A-Za-z]+)\s*<sup>(\d+)</sup>", str(content))
            ]
        text = content.get_text(" ", strip=True).replace("\xa0", " ")
        text = re.sub(r"\s*UniProt\s*$", "", " ".join(text.split()))
        if key == "full name":
            entry["full_name"] = text
        elif key in HEAD_TO_FIELD:
            entry[HEAD_TO_FIELD[key] + ("_text" if key == "functional export signals" else "")] = text
    return entry


def scrape_nesdb(include_doubt: bool) -> list[dict]:
    entries = []
    for kind in ["good", "doubt"] if include_doubt else ["good"]:
        soup = BeautifulSoup(requests.get(urljoin(BASE_URL, PAGES[kind]), headers=HEADERS, timeout=60).text, "html.parser")
        rows = {}
        for tr in soup.select("tr"):
            link = tr.select_one("td:nth-of-type(1) a")
            if link and "details.php" in link.get("href", ""):
                rows.setdefault(urljoin(BASE_URL, link["href"]), html.unescape(link.get_text(strip=True)))
        print(f"NESdb {kind}: {len(rows)} entries")
        for n, (url, name) in enumerate(rows.items(), 1):
            for attempt in range(3):
                try:
                    page = requests.get(url, headers=HEADERS, timeout=60).text
                    break
                except requests.RequestException as e:
                    if attempt == 2:
                        print(f"  skipped {url}: {e}")
                        page = None
                    time.sleep(2)
            if page is not None:
                entry = parse_details(page)
                entry.update(nes_id=url.split("=")[-1], name=name, set=kind, url=url)
                entries.append(entry)
            if n % 50 == 0:
                print(f"  {n}/{len(rows)}")
            time.sleep(0.2)
    return entries


def load_nesdb(cache: Path, include_doubt: bool, refresh: bool) -> list[dict]:
    if cache.exists() and not refresh:
        entries = json.loads(cache.read_text(encoding="utf-8"))
        if include_doubt and not any(e["set"] == "doubt" for e in entries):
            print("Cache has no 'in doubt' entries - re-scraping (or drop --include-doubt)")
        else:
            return [e for e in entries if include_doubt or e["set"] == "good"]
    entries = scrape_nesdb(include_doubt)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(entries, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"Cached {len(entries)} NESdb entries in {cache}")
    return entries


# ---------------------------------------------------------------- normalising

def norm(text) -> str:
    text = html.unescape(str(text or "")).lower()
    for g, word in GREEK.items():
        text = text.replace(g, f" {word} ")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def tokens(text) -> set[str]:
    return {t for t in norm(text).split() if t not in STOPWORDS}


def has_value(value) -> bool:
    n = norm(value)  # "unknown (NES not included in PDB)" records nothing to recover either
    return n not in NO_VALUE and not n.startswith("unknown ")


def entry_names(entry: dict) -> list[str]:
    names = [entry.get("full_name", ""), entry.get("name", "")]
    names += re.split(r"[,;]", entry.get("alt_names", ""))
    return [n for n in (norm(x) for x in names) if n and n not in NO_VALUE]


def name_score(csv_name: str, entry: dict) -> float:
    target = norm(csv_name)
    if not target:
        return 0.0
    best = 0.0
    for n in entry_names(entry):
        if n == target:
            return 1.0
        best = max(best, SequenceMatcher(None, target, n).ratio())
    return best


# ---------------------------------------------------------------- comparators
# Each returns (truth_text, result) where result is True/False, or None when NESdb has no value.

def text_match(csv_value: str, truth: str, threshold: float) -> bool:
    a, b = norm(csv_value), norm(truth)
    if not a:
        return False
    if (len(a) >= 3 and a in b) or (len(b) >= 3 and b in a):
        return True
    ta, tb = tokens(a), tokens(b)
    if tb and len(ta & tb) / len(tb) >= threshold:
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold


def cmp_text(field):
    def compare(value, entry, args):
        truth = entry.get(field, "")
        if not has_value(truth):
            return truth, None
        return truth, text_match(value, truth, args.text_threshold)
    return compare


def organism_text(text) -> str:
    v = norm(text)
    v = re.sub(r"\btype (\d)", r"\1", v)  # "herpes simplex virus type 1" = "... virus 1"
    return ORGANISM_ADJECTIVES.get(v, v)


def cmp_organism(value, entry, args):
    truth = entry.get("organism", "")
    if not has_value(truth):
        return truth, None
    parts = [p for p in (organism_text(x) for x in re.split(r"[()]", truth)) if p]
    v = organism_text(value)
    # The value may itself be "Name (abbrev)" or "Mouse / Human"; any substantial piece may carry the species.
    pieces = [v] + [p for p in (organism_text(x) for x in re.split(r"[()/,;]", value))
                    if len(p) >= 4 and p not in GENERIC_ORGANISM_WORDS]
    return truth, bool(v) and any(p in q or q in p for p in parts for q in pieces)


def compact_name(text) -> set[str]:
    """Spelling variants of a name with spaces dropped: Greek letters as words and as letters (IκBα = IkBa)."""
    raw = html.unescape(str(text or "")).lower()
    short = raw
    for g, word in GREEK.items():
        short = short.replace(g, word[0])
    return {re.sub(r"[^a-z0-9]+", "", norm(raw)), re.sub(r"[^a-z0-9]+", "", short)}


def cmp_names(value, entry, args):
    truth = "; ".join([entry.get("name", ""), entry.get("alt_names", "")]).strip("; ")
    raw_names = [entry.get("full_name", "")] + re.split(r"[,;/]", f"{entry.get('name', '')};{entry.get('alt_names', '')}")
    names = [n for n in raw_names if norm(n) and norm(n) not in NO_VALUE]
    variants = set().union(*(compact_name(n) for n in names)) if names else set()
    # NESdb runs several alternative names together ("Protein kinase B (PKB) Protein kinase B alpha ..."),
    # so a CSV name also counts when it appears as a whole phrase inside one of them - a single word only
    # when the rest of that name is generic ("APC" in "APC Protein"), so "ERK" does not match "ERK kinase 1".
    padded = [f" {norm(n)} " for n in names]
    generic = STOPWORDS | GENERIC_NAME_WORDS | {"protein", "transcription"}
    items = [x for x in re.split(r"[,;/|]", value) if norm(x)] + [value]
    for item in items:
        n = norm(item)
        if n in NO_VALUE or n in STOPWORDS or n in GENERIC_NAME_WORDS:
            continue
        if compact_name(item) & variants:
            return truth, True
        if " " in n and any(f" {n} " in p for p in padded):
            return truth, True
        if len(n) >= 3 and any(f" {n} " in p and set(p.replace(f" {n} ", " ", 1).split()) <= generic for p in padded):
            return truth, True
        # "Mek1 kinase" vs "MEK1": a one-word NESdb name inside the CSV name.
        if any(len(m) >= 3 and " " not in m and f" {m} " in f" {n} " for m in (norm(x) for x in names)):
            return truth, True
    return truth, False


def mutation_set(text: str) -> set[tuple[str, str]]:
    """(position, new residue) of each substitution in any common notation. The original residue is left out
    so that a residue NESdb or the paper names differently at the same position still counts."""
    text = text or ""
    found = {(pos, THREE_LETTER[new.lower()]) for _, pos, new in MUTATION3_RE.findall(text)}
    found |= {(pos, new) for _, pos, new in MUTATION_RE.findall(text)}
    for _, positions, _, new in MUTATION_LIST_RE.findall(text):
        found |= {(pos, new) for pos in re.findall(r"\d+", positions)}
    return found


def residues(value: str) -> str:
    """The value as one run of residues when it is nothing but a peptide - spaced ("L V V L"), numbered
    ("L165AGKM169SNLVL174"), starred or in three-letter code ("Cys-Gly-Gln") - else ''."""
    words = re.findall(r"[A-Za-z]+", value)
    if words and re.fullmatch(rf"(?:(?:{_AA3})[\s\-]*)+", value.strip(), re.I):
        return "".join(THREE_LETTER[w.lower()] for w in re.findall(_AA3, value, re.I))
    compact = re.sub(r"[\s\-\*\d]+", "", value)
    return compact if len(compact) >= 5 and re.fullmatch(r"[ACDEFGHIKLMNPQRSTVWY]+", compact) else ""


def cmp_mutations(fields):
    def compare(value, entry, args):
        truth = ", ".join(dict.fromkeys(entry.get(f, "") for f in fields if has_value(entry.get(f, ""))))
        expected = mutation_set(truth)
        if not expected:
            return truth, None
        return truth, bool(expected & mutation_set(value))
    return compare


def longest_common(a: str, b: str) -> int:
    return SequenceMatcher(None, a, b, autojunk=False).find_longest_match(0, len(a), 0, len(b)).size


def entry_signals(entry: dict) -> list[dict]:
    """NESdb's NESs for an entry. Falls back to the signal text when the page gave a long region whose
    sequence is broken by spaces ("194 SLPP...QDF DLL 256"), which the <sup>start</sup> seq <sup>end</sup>
    scrape misses; a missing end is taken from the sequence length."""
    if entry.get("signals"):
        return entry["signals"]
    parsed = []
    for piece in str(entry.get("signals_text", "")).split(","):
        m = re.fullmatch(r"\s*(\d{1,5})\s+([A-Za-z][A-Za-z\s]*?)\s*(\d{1,5})?\s*", piece)
        if not m:
            continue
        seq = re.sub(r"\s+", "", m.group(2)).upper()
        if not re.fullmatch(r"[ACDEFGHIKLMNPQRSTVWY]{4,}", seq):
            continue
        start = int(m.group(1))
        parsed.append({"start": start, "seq": seq, "end": int(m.group(3)) if m.group(3) else start + len(seq) - 1})
    return parsed


def signal_text(s: dict) -> str:
    return f"{s['start']}-{s['end']} {s['seq']}"


def signal_hit(value: str, s: dict) -> str | None:
    """'sequence' if the value gives this NES's peptide, 'location' if only an overlapping residue range."""
    whole = residues(value)
    for seq in [whole] if whole else AA_RE.findall(value.upper()):
        if longest_common(seq, s["seq"].upper()) >= min(8, len(seq), len(s["seq"])):
            return "sequence"
    for start, end in RANGE_RE.findall(value):
        if int(start) <= s["end"] and int(end) >= s["start"]:
            return "location"
    return None


def is_peptide(value: str) -> bool:
    """A short run of residues (an NES), as opposed to a whole protein sequence or no sequence."""
    seq = residues(value) or "".join(AA_RE.findall(value.upper()))
    return 0 < len(seq) < 50


def gives_signal(value: str) -> bool:
    """The value states some NES: a peptide or a residue range."""
    return bool(residues(value) or PEPTIDE_RE.search(value) or RANGE_RE.search(value))


def cmp_signals(value, entry, args):
    signals = entry_signals(entry)
    truth = ", ".join(signal_text(s) for s in signals) or entry.get("signals_text", "")
    if not signals:
        return truth, None
    return truth, any(signal_hit(value, s) for s in signals)


def cmp_sequence(value, entry, args):
    truth = entry.get("sequence", "")
    if not truth:
        return "", None
    if is_peptide(value):  # an NES, not the whole protein
        return cmp_signals(value, entry, args)
    seq = residues(value) or "".join(AA_RE.findall(value.upper()))
    return truth[:30] + "...", bool(seq) and (seq in truth or truth in seq)


def cmp_uniprot(value, entry, args):
    truth = entry.get("uniprot", "").strip()  # older caches kept the link's trailing newline
    if not truth:
        return truth, None
    return truth, truth.split("-")[0] in {u.split("-")[0] for u in UNIPROT_RE.findall(value.upper())}


def cmp_pmids(value, entry, args):
    truth = set(entry.get("pmids") or [])
    if not truth:
        return "", None
    return ", ".join(sorted(truth)), bool(truth & set(re.findall(r"\b\d{6,9}\b", value)))


COMPARATORS = {
    "name": cmp_names,
    "alt_names": cmp_names,
    "organism": cmp_organism,
    "uniprot": cmp_uniprot,
    "evidence": cmp_text("evidence"),
    "mutations": cmp_mutations(["mutations_export", "mutations_crm1"]),
    "mutations_export": cmp_mutations(["mutations_export"]),
    "mutations_crm1": cmp_mutations(["mutations_crm1"]),
    "signals": cmp_signals,
    "secondary_structure": cmp_text("secondary_structure"),
    "other_residues": cmp_text("other_residues"),
    "sequence": cmp_sequence,
    "pmids": cmp_pmids,
}


# ---------------------------------------------------------------- LLM judge

LLM_FIELDS = ("evidence", "secondary_structure", "other_residues", "mutations", "mutations_export", "mutations_crm1")
# Field-specific rules appended to the prompt.
LLM_HINTS = {
    "evidence": " A bare \"yes\", a list of numbers, or a value that only says the protein or signal is exported, "
                "without naming the kind of evidence NESdb records (e.g. LMB sensitivity, CRM1 binding, crm1/Xpo1 "
                "mutant, competition with other CRM1 cargos), is too vague.",
    "mutations": " It is a match only if the extracted value describes at least one of the same substitutions (same "
                 "residue position and the same replacement residue), in any notation or in words "
                 "(\"leucines 237 and 240 to alanine\" = L237A/L240A). Mutating \"the hydrophobic residues\" without "
                 "positions, a deletion, or other positions is not a match.",
}
LLM_HINTS["mutations_export"] = LLM_HINTS["mutations_crm1"] = LLM_HINTS["mutations"]
LLM_MODEL = "gemini-3.5-flash-lite"  # the model the backend uses for its own verification calls
LLM_PROMPT = """You are checking whether an automatic extraction from a scientific paper recovered the value that \
the curated NESdb database (nuclear export signals) records for the same protein.

Protein: {protein}
Field: {field}
NESdb value: {truth}
Extracted value: {value}

Answer "match": true if the extracted value states the same fact(s) as the NESdb value, or its main fact when the \
NESdb value lists several; wording, synonyms and abbreviations don't matter (e.g. "LMB sensitive" = "export blocked \
by leptomycin B", "alpha-helix" = "helical"). Extra information in the extracted value is fine. Answer false if it \
contradicts NESdb, is about something else, or is too vague to show the NESdb fact. Give a one-sentence reason."""
LLM_SCHEMA = {"type": "OBJECT", "properties": {"match": {"type": "BOOLEAN"}, "reason": {"type": "STRING"}},
              "required": ["match", "reason"]}


def llm_judge_cells(pending, rows, row_results, mapping, args) -> dict:
    """Return {(row index, column): (match, reason)} for each pending cell, using saved verdicts when present."""
    from concurrent.futures import ThreadPoolExecutor
    import hashlib
    import os

    from dotenv import load_dotenv
    from google import genai
    from google.genai import types

    saved = json.loads(args.llm_cache.read_text(encoding="utf-8")) if args.llm_cache.exists() else {}
    jobs = {}
    for i, col in pending:
        entry = row_results[i][2]
        prompt = LLM_PROMPT.format(protein=row_results[i][1].get("full_name") or row_results[i][1].get("name"),
                                   field=mapping[col].replace("_", " "), truth=entry[col][0], value=rows[i][col])
        prompt += LLM_HINTS.get(mapping[col], "")
        jobs[(i, col)] = (hashlib.sha1(f"{args.llm_model}\n{prompt}".encode()).hexdigest(), prompt)
    todo = {key: prompt for key, prompt in jobs.values() if key not in saved}
    print(f"\nLLM judge: {len(pending)} free-text cells the rules missed, {len(todo)} new calls to {args.llm_model}")

    if todo:
        load_dotenv(Path(__file__).resolve().parents[1] / ".env")
        if not os.environ.get("GEMINI_API_KEY"):
            sys.exit("GEMINI_API_KEY not found in backend/.env")
        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        config = types.GenerateContentConfig(temperature=0, response_mime_type="application/json", response_schema=LLM_SCHEMA)

        def ask(item):
            key, prompt = item
            for attempt in range(4):
                try:
                    reply = json.loads(client.models.generate_content(model=args.llm_model, contents=prompt, config=config).text)
                    return key, [bool(reply["match"]), reply.get("reason", "")]
                except Exception as e:  # rate limits and malformed replies: back off and retry
                    if attempt == 3:
                        print(f"  LLM call failed, cell left as missed: {e}")
                        return key, None
                    time.sleep(2 ** attempt * 2)

        with ThreadPoolExecutor(max_workers=8) as pool:
            for n, (key, verdict) in enumerate(pool.map(ask, todo.items()), 1):
                if verdict:
                    saved[key] = verdict
                if n % 25 == 0:
                    print(f"  {n}/{len(todo)}")
        args.llm_cache.parent.mkdir(parents=True, exist_ok=True)
        args.llm_cache.write_text(json.dumps(saved, indent=1, ensure_ascii=False), encoding="utf-8")

    return {cell: tuple(saved[key]) for cell, (key, _) in jobs.items() if key in saved}


# ---------------------------------------------------------------- matching

def find_entries(csv_name: str, by_name: dict, entries: list[dict], threshold: float) -> tuple[list[dict], float]:
    """Return every NESdb entry the name matches (one protein can have several NES IDs) and the score."""
    exact = by_name.get(norm(csv_name))
    if exact:
        return exact, 1.0
    scored = [(name_score(csv_name, e), e) for e in entries]
    best = max((s for s, _ in scored), default=0.0)
    if best < threshold:
        return [], best
    return [e for s, e in scored if s == best], best


def same_name(a: str, b: str) -> bool:
    """True if two protein names are equal ignoring spacing, or one appears as whole words in the other."""
    a, b = norm(a), norm(b)
    if not a or not b:
        return False
    return a.replace(" ", "") == b.replace(" ", "") or f" {a} " in f" {b} " or f" {b} " in f" {a} "


def match_row(row: dict, cols: dict, by_pmid: dict, by_name: dict, entries: list[dict], args) -> tuple[list[dict], float, str]:
    """Match a row to NESdb entries. Returns (entries, score, how).

    'cited': NESdb cites this row's paper (from '<name>_<pmid>_full' in Source Document) for the
    protein the row names - the paper is ground truth for it. 'name only': the protein is in NESdb
    but NESdb does not cite this paper for it (e.g. a control NES mentioned in passing).
    """
    names = [row.get(cols["name"], "")]
    if cols.get("alt"):
        names += re.split(r"[,;]", row[cols["alt"]])
    if cols.get("unit"):
        names.append(re.sub(r"\bnes\b.*$", "", row[cols["unit"]], flags=re.I))
    names = [n.strip() for n in names if has_value(n)]

    m = re.match(r"(.*)_(\d{5,9})_full", row.get(cols.get("source") or "", ""))
    if m:
        prefix, pmid = m.groups()
        scored = []
        for entry in by_pmid.get(pmid, []):
            prefix_hit = same_name(prefix.replace("_", " "), entry.get("name", "")) or \
                any(same_name(prefix, n) for n in entry_names(entry))
            for n in names:
                score = name_score(n, entry)
                if score >= args.name_threshold - 0.1 or any(same_name(n, en) for en in entry_names(entry)) \
                        or (prefix_hit and same_name(n, prefix)):
                    scored.append((max(score, 0.9), entry))
                    break
        if scored:
            best = max(s for s, _ in scored)
            return [e for s, e in scored if s == best], best, "cited"

    cands, score = find_entries(names[0] if names else "", by_name, entries, args.name_threshold)
    for n in names[1:]:
        if not cands and by_name.get(norm(n)):
            cands, score = by_name[norm(n)], 1.0
    if not cands and names:
        cands = [e for e in entries if any(same_name(n, en) for n in names for en in entry_names(e)[:2])]
        score = 0.9 if cands else score
    return cands, score, "name only" if cands else ""


def signal_values(row: dict, mapping: dict) -> list[str]:
    """The row's cells that state its NES: signal columns (peptide, coordinates) and short 'sequence' cells."""
    return [row[col] for col, field in mapping.items() if has_value(row[col])
            and (field == "signals" or (field == "sequence" and is_peptide(row[col])))]


def row_signal(row: dict, entry: dict, mapping: dict) -> tuple[str | None, dict | None]:
    """Which of the entry's NESs the row describes: ('sequence' | 'location', signal), or (None, None)."""
    values = signal_values(row, mapping)
    for kind in ("sequence", "location"):
        for s in entry_signals(entry):
            if any(signal_hit(v, s) == kind for v in values):
                return kind, s
    return None, None


# How a row relates to NESdb's NESs for its protein, as shown in the "NES Match" column.
EXTRA_NES = "extra NES - not in NESdb"
OTHER_PROTEIN = "other species/protein - not in NESdb"
NO_NES_GIVEN = "no NES given"
NESDB_UNDETERMINED = "NESdb: NES undetermined"


def classify_row(row: dict, entry: dict, mapping: dict, args) -> tuple[str, dict | None]:
    """('sequence' | 'location', signal) when the row gives one of the entry's NESs, else one of the
    labels above. A row giving another NES is an extra NES, or another species/protein (an ortholog,
    another virus' Rev) when its organism or UniProt ID contradicts NESdb's."""
    kind, s = row_signal(row, entry, mapping)
    if kind:
        return kind, s
    if not entry_signals(entry):
        return NESDB_UNDETERMINED, None
    if not any(gives_signal(v) for v in signal_values(row, mapping)):
        return NO_NES_GIVEN, None
    for col, field in mapping.items():
        if field in ("organism", "uniprot") and has_value(row[col]) \
                and COMPARATORS[field](row[col], entry, args)[1] is False:
            return OTHER_PROTEIN, None
    return EXTRA_NES, None


def is_external(field: str, value: str) -> bool:
    """NESdb took this value from UniProt, not from the paper (a short 'sequence' cell is an NES, though)."""
    return field in EXTERNAL_FIELDS and not (field == "sequence" and is_peptide(value))


def paper_text(docs_dir: Path | None, stem: str) -> str | None:
    """The paper's letters with spaces, digits and punctuation removed (case kept), or None if not found."""
    if not docs_dir:
        return None
    path = docs_dir / f"{stem}.txt"
    if not path.exists():
        return None
    return re.sub(r"[^A-Za-z]", "", path.read_text(encoding="utf-8", errors="ignore"))


def in_text(seq: str, letters: str) -> bool:
    """The NES peptide is written in the paper; a long region counts when any 12 residues of it are."""
    seq = seq.upper()
    if len(seq) <= 15:
        return seq in letters
    return any(seq[i:i + 12] in letters for i in range(len(seq) - 11))


def figure_count(docs_dir: Path | None, stem: str) -> int:
    folder = docs_dir / "figures" / stem if docs_dir else None
    return len(list(folder.glob("*.png"))) if folder and folder.is_dir() else 0


# ---------------------------------------------------------------- UniProt check

UNIPROT_FASTA_URL = "https://rest.uniprot.org/uniprotkb/{}.fasta"
# Results of the UniProt check, as shown in the "UniProt Check" column.
PEPTIDE_AT_RESIDUES = "peptide at the stated residues"
NUMBERING_DIFFERS = "peptide elsewhere in the protein - numbering differs"
PEPTIDE_IN_PROTEIN = "peptide in the protein"
PEPTIDE_NOT_IN_PROTEIN = "peptide not in the protein"
RANGE_IS_NES = "residues at the range are the NES"
RANGE_TOO_WIDE = "range much wider than the NES"
RANGE_SMALL_OVERLAP = "range barely overlaps the NES"
RANGE_RESIDUES = "residues at the stated range"
RANGE_OUTSIDE = "range beyond the protein's length"
NO_SEQUENCE = "no UniProt sequence"
# Results for a location-only match, best first.
LOCATION_ORDER = [PEPTIDE_AT_RESIDUES, RANGE_IS_NES, RANGE_TOO_WIDE, RANGE_SMALL_OVERLAP, NUMBERING_DIFFERS,
                  PEPTIDE_IN_PROTEIN, PEPTIDE_NOT_IN_PROTEIN, RANGE_RESIDUES, RANGE_OUTSIDE, NO_SEQUENCE, ""]


def protein_sequence(entry: dict, store: dict, fetch: bool) -> str:
    """The protein's UniProt sequence: NESdb's copy (NESdb numbers its NESs on it), else fetched from UniProt
    by the entry's accession and kept in store. '' when there is none."""
    seq = re.sub(r"\s+", "", entry.get("sequence") or "").upper()
    if seq:
        return seq
    acc = (entry.get("uniprot") or "").strip().split("-")[0]
    if not acc or not fetch:
        return ""
    if acc not in store:
        try:
            reply = requests.get(UNIPROT_FASTA_URL.format(acc), headers=HEADERS, timeout=30)
            reply.raise_for_status()
            store[acc] = "".join(reply.text.splitlines()[1:]).strip().upper()
        except requests.RequestException as e:
            print(f"  UniProt {acc}: {e}")
            return ""
    return store[acc]


def row_peptides(values: list[str]) -> list[str]:
    """Peptides the row states, upper case, in order."""
    found = []
    for v in values:
        whole = residues(v)
        found += [whole] if whole else PEPTIDE_RE.findall(v)
    return list(dict.fromkeys(p.upper() for p in found if len(p) >= 5))


def row_ranges(values: list[str]) -> list[tuple[int, int]]:
    ranges = [(int(a), int(b)) for v in values for a, b in RANGE_RE.findall(v)]
    return list(dict.fromkeys((a, b) for a, b in ranges if 1 <= a <= b))


def uniprot_check(values: list[str], sequence: str, signal: dict | None = None) -> tuple[str, str]:
    """Check the NES a row states against the protein's UniProt sequence. Returns (result, detail).

    A peptide is looked up in the protein: at the row's own residue numbers, elsewhere (the paper numbers
    residues differently, e.g. another isoform), or not at all (an ortholog, a mutant, or a wrong
    extraction). Without a peptide, the residues at the row's range are read from the protein and, for a
    row matched to NESdb's NES by location, compared with that NES."""
    if not sequence:
        return NO_SEQUENCE, ""
    ranges = row_ranges(values)
    for pep in row_peptides(values):
        i = sequence.find(pep)
        if i < 0:
            continue
        start, end = i + 1, i + len(pep)
        if not ranges:
            return PEPTIDE_IN_PROTEIN, f"{start}-{end}"
        # Papers are often a residue or two off (e.g. counting from after the initiator Met), and a peptide
        # may run a little past its range, so a range covering half of the peptide (or vice versa) is it.
        def overlap(r):
            return min(r[1], end) - max(r[0], start) + 1
        a, b = max(ranges, key=overlap)
        if overlap((a, b)) * 2 >= min(b - a + 1, end - start + 1):
            return PEPTIDE_AT_RESIDUES, f"{start}-{end}" + (f" (row says {a}-{b}, offset {a - start:+d})" if a != start else "")
        return NUMBERING_DIFFERS, f"peptide at {start}-{end}, row says {a}-{b} (offset {a - start:+d})"
    if row_peptides(values):
        return PEPTIDE_NOT_IN_PROTEIN, ", ".join(row_peptides(values))
    if not ranges:
        return "", ""
    if signal:
        overlapping = [(a, b) for a, b in ranges if a <= signal["end"] and b >= signal["start"]]
        ranges = overlapping or ranges
    a, b = ranges[0]
    if b > len(sequence):
        return RANGE_OUTSIDE, f"{a}-{b}, protein has {len(sequence)} residues"
    piece = sequence[a - 1:b]
    shown = f"{a}-{b} {piece if len(piece) <= 40 else piece[:40] + '...'}"
    if not signal:
        return RANGE_RESIDUES, shown
    nes_len = signal["end"] - signal["start"] + 1
    overlap = max(0, min(b, signal["end"]) - max(a, signal["start"]) + 1)
    if overlap < min(8, nes_len):
        return RANGE_SMALL_OVERLAP, f"{shown} ({overlap} of {nes_len} NES residues)"
    if b - a + 1 > 2 * nes_len + 10:
        return RANGE_TOO_WIDE, f"{shown} ({b - a + 1} residues for a {nes_len}-residue NES)"
    return RANGE_IS_NES, shown


def guess_field(header: str) -> str | None:
    h = norm(header)
    if h in EXACT_HEADERS:
        return EXACT_HEADERS[h]
    words = h.split()
    for keywords, field in HEADER_RULES:
        if all(any(w.startswith(k) for w in words) for k in keywords):
            return field
    return None


def best_field(column: str, rows: list[dict], matches: list[list[dict]], args) -> tuple[str | None, float]:
    """Pick the NESdb field whose values agree with this column most often."""
    best, best_rate = None, 0.0
    for field, compare in COMPARATORS.items():
        hits = total = 0
        for row, cands in zip(rows, matches):
            if not cands or not has_value(row[column]):
                continue
            results = [compare(row[column], e, args)[1] for e in cands]
            if all(r is None for r in results):
                continue
            total += 1
            hits += any(results)
        rate = hits / total if total else 0.0
        if total >= 3 and rate > best_rate:
            best, best_rate = field, rate
    return (best, best_rate) if best_rate >= args.auto_threshold else (None, best_rate)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv", type=Path, help="CSV exported from ScheMatiQ")
    parser.add_argument("--name-col", help="Column holding the protein full name (default: auto-detect 'Full Name')")
    parser.add_argument("--map", action="append", default=[], metavar="COLUMN=FIELD", help="Force a column to an NESdb field")
    parser.add_argument("--skip", action="append", default=[], metavar="COLUMN", help="Column not to compare")
    parser.add_argument("--include-doubt", action="store_true", help="Also use NESdb's 'NESs in doubt' entries")
    parser.add_argument("--include-uncited", action="store_true", help="Also score rows whose paper NESdb does not cite for that NES")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE, help="NESdb cache file (default: %(default)s)")
    parser.add_argument("--refresh", action="store_true", help="Re-scrape NESdb instead of using the cache")
    parser.add_argument("--out-dir", type=Path, help="Output folder (default: next to the CSV)")
    parser.add_argument("--name-threshold", type=float, default=0.85, help="Min name similarity to accept a protein match (default: %(default)s)")
    parser.add_argument("--text-threshold", type=float, default=0.6, help="Min similarity for free-text fields (default: %(default)s)")
    parser.add_argument("--llm-judge", action="store_true", help="Ask Gemini about free-text cells the rules call a miss")
    parser.add_argument("--llm-fields", nargs="+", default=list(LLM_FIELDS), choices=list(COMPARATORS),
                        help="NESdb fields the LLM judges (default: %(default)s)")
    parser.add_argument("--llm-model", default=LLM_MODEL, help="Gemini model (default: %(default)s)")
    parser.add_argument("--llm-cache", type=Path, default=DEFAULT_CACHE.with_name("nesdb_llm_judgments.json"),
                        help="Saved verdicts, so re-runs don't repeat calls (default: %(default)s)")
    parser.add_argument("--auto-map", action="store_true", help="Map unrecognised columns to the NESdb field they agree with most")
    parser.add_argument("--auto-threshold", type=float, default=0.3, help="Min agreement rate for --auto-map (default: %(default)s)")
    parser.add_argument("--docs-dir", type=Path, help="Project documents folder: check every paper in it (also those with "
                        "no rows) and whether a missed NES is written in the paper text")
    parser.add_argument("--uniprot-cache", type=Path, default=DEFAULT_CACHE.with_name("uniprot_sequences.json"),
                        help="Protein sequences fetched from UniProt when NESdb has none (default: %(default)s)")
    parser.add_argument("--no-uniprot-fetch", action="store_true",
                        help="Check only against the sequences NESdb already has; never call UniProt")
    parser.add_argument("--score-external", action="store_true",
                        help="Also count fields NESdb takes from UniProt (full sequence, UniProt ID) in the totals")
    args = parser.parse_args(argv)
    if args.docs_dir and not args.docs_dir.is_dir():
        sys.exit(f"--docs-dir {args.docs_dir} is not a folder")

    with args.csv.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = list(reader.fieldnames or [])
        rows = [{c: (r.get(c) or "").strip() for c in columns} for r in reader]
    if not rows:
        sys.exit("The CSV has no rows")

    name_col = args.name_col or next((c for c in columns if norm(c) == "full name"), None) \
        or next((c for c in columns if "full name" in norm(c)), None) \
        or next((c for c in columns if "protein" in norm(c) and "name" in norm(c)), None)
    if name_col not in columns:
        sys.exit(f"No full-name column found; pass --name-col. Columns: {columns}")

    entries = load_nesdb(args.cache, args.include_doubt, args.refresh)
    by_name, by_pmid = defaultdict(list), defaultdict(list)
    for e in entries:
        for n in entry_names(e)[:2]:  # full name and short name are unique enough for an exact hit
            by_name[n].append(e)
        for pmid in e.get("pmids") or []:
            by_pmid[pmid].append(e)

    def find_col(*names):
        return next((c for c in columns if norm(c) in names), None)
    cols = {"name": name_col, "alt": find_col("alternative names"), "unit": find_col("observation unit"),
            "source": find_col("source document")}
    found = [match_row(r, cols, by_pmid, by_name, entries, args) for r in rows]
    # Rows matched by name only are listed but not scored: NESdb's values come from other papers.
    matches = [cands if how == "cited" or args.include_uncited else [] for cands, _, how in found]
    hows = [how for _, _, how in found]
    print(f"\n{len(rows)} rows: {hows.count('cited')} matched to an NES that NESdb cites the row's paper for, "
          f"{hows.count('name only')} to an NES NESdb cites other papers for"
          f"{' (scored too)' if args.include_uncited else ' (not scored; --include-uncited to score)'}, "
          f"{hows.count('')} not in NESdb")

    # Decide which NESdb field each column is compared with.
    forced = {}
    for item in args.map:
        col, _, field = item.partition("=")
        if col.strip() not in columns or field.strip() not in COMPARATORS:
            sys.exit(f"Bad --map '{item}'. Columns: {columns}; fields: {list(COMPARATORS)}")
        forced[col.strip()] = field.strip()
    mapping = {}
    print("\nColumn -> NESdb field")
    for col in columns:
        if col == name_col or col in args.skip or norm(col) in ID_COLUMNS:
            continue
        if col in forced:
            mapping[col], how = forced[col], "--map"
        elif guess_field(col):
            mapping[col], how = guess_field(col), "header"
        elif not args.auto_map:
            print(f"  {col!r}: not compared (no NESdb field by that name - use --map or --auto-map)")
            continue
        else:
            field, rate = best_field(col, rows, matches, args)
            if not field:
                print(f"  {col!r}: not compared (no field agrees with it; best rate {rate:.0%} - use --map)")
                continue
            mapping[col], how = field, f"auto, {rate:.0%} agreement"
        print(f"  {col!r} -> {mapping[col]} ({how})")

    # Score every cell. A row matching several NES IDs is scored against the one whose NES it gives,
    # then the one it recovers most of. A row about another species or protein is not scored, and
    # for a row giving an NES that NESdb doesn't list, the NES-specific columns are not compared.
    cells, row_results, row_class, row_uniprot = [], [], [], []
    signal_rank = {"sequence": 2, "location": 1}
    uniprot_store = json.loads(args.uniprot_cache.read_text(encoding="utf-8")) if args.uniprot_cache.exists() else {}
    uniprot_known = len(uniprot_store)

    def sequence_of(entry: dict) -> str:
        return protein_sequence(entry, uniprot_store, fetch=not args.no_uniprot_fetch)

    for i, (row, cands) in enumerate(zip(rows, matches)):
        best = None
        for entry in cands:
            res = {col: COMPARATORS[field](row[col], entry, args) for col, field in mapping.items()}
            kind, _ = row_signal(row, entry, mapping)
            score = (signal_rank.get(kind, 0), sum(r is True for _, r in res.values()), -sum(r is False for _, r in res.values()))
            if best is None or score > best[0]:
                best = (score, entry, res)
        cls = classify_row(row, best[1], mapping, args) if best else (None, None)
        row_uniprot.append(uniprot_check(signal_values(row, mapping), sequence_of(best[1]), cls[1]) if best else ("", ""))
        if cls[0] == OTHER_PROTEIN:
            best = None
        elif cls[0] == EXTRA_NES:
            for col, (truth, r) in best[2].items():
                if mapping[col] in SIGNAL_FIELDS or (mapping[col] == "sequence" and is_peptide(row[col])):
                    best[2][col] = (truth, "extra")
        row_results.append(best)
        row_class.append(cls)

    def counted(col: str, value: str) -> bool:
        return args.score_external or not is_external(mapping[col], value)

    # Free-text cells the rules missed go to the LLM judge.
    judged = {}
    if args.llm_judge:
        pending = [(i, col) for i, best in enumerate(row_results) if best
                   for col, (_, r) in best[2].items()
                   if r is False and mapping[col] in args.llm_fields and has_value(rows[i][col])]
        judged = llm_judge_cells(pending, rows, row_results, mapping, args)
        for (i, col), (match, reason) in judged.items():
            truth, _ = row_results[i][2][col]
            row_results[i][2][col] = (truth, match)

    for i, (row, best) in enumerate(zip(rows, row_results)):
        if best:
            _, entry, res = best
            for col, (truth, result) in res.items():
                cells.append({
                    "row": i + 1, "protein": row[name_col], "nes_id": entry["nes_id"], "column": col,
                    "nesdb_field": mapping[col], "csv_value": row[col], "nesdb_value": truth,
                    "result": {True: "recovered", False: "missed", None: "no NESdb value",
                               "extra": "not compared - NES not in NESdb"}[result],
                    "counted": "yes" if result in (True, False) and counted(col, row[col]) else "no",
                    "judged_by": "llm" if (i, col) in judged else "rules",
                    "llm_reason": judged.get((i, col), (None, ""))[1],
                })

    out_dir = args.out_dir or args.csv.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = args.csv.stem + "_vs_nesdb"
    extra = ["NESdb ID", "NESdb Full Name", "Match", "Name Match", "NES Match", "UniProt Check", "UniProt Detail",
             "Recovered", "Recovered %"]

    def nes_match_text(cls) -> str:
        kind, s = cls
        return f"{kind}: {signal_text(s)}" if s else (kind or "")

    # Main table: original columns, per-row recovery, footer with per-column recovery.
    # External fields (from UniProt) are tallied per column but left out of the row and overall totals.
    col_hits = defaultdict(lambda: [0, 0])
    with (out_dir / f"{stem}.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns + extra)
        for row, best, (cands, score, how), cls, check in zip(rows, row_results, found, row_class, row_uniprot):
            if not best:
                ids = ", ".join(e["nes_id"] for e in cands)
                names = ", ".join(dict.fromkeys(e.get("full_name") or e.get("name", "") for e in cands)) or "not in NESdb"
                if cls[0] == OTHER_PROTEIN:
                    note = "other species/protein - not scored"
                else:
                    note = "name only - paper not cited for it, not scored" if how else "not in NESdb"
                w.writerow([row[c] for c in columns] + [ids, names, note, f"{score:.2f}", nes_match_text(cls), *check, "", ""])
                continue
            _, entry, res = best
            judged = [r for col, (_, r) in res.items() if r in (True, False) and counted(col, row[col])]
            for col, (_, r) in res.items():
                if r in (True, False):
                    col_hits[col][0] += r
                    col_hits[col][1] += 1
            pct = f"{sum(judged) / len(judged):.0%}" if judged else ""
            w.writerow([row[c] for c in columns] + [
                entry["nes_id"], entry.get("full_name", ""), how, f"{score:.2f}", nes_match_text(cls), *check,
                f"{sum(judged)}/{len(judged)}", pct])
        external_cols = {c for c in mapping if mapping[c] in EXTERNAL_FIELDS and not args.score_external}
        all_hits = sum(h for c, (h, _) in col_hits.items() if c not in external_cols)
        all_total = sum(t for c, (_, t) in col_hits.items() if c not in external_cols)
        field_row = [f"NESdb field: {mapping[c]}" + (" (from UniProt, not in total)" if c in external_cols else "")
                     if c in mapping else ("(name key)" if c == name_col else "") for c in columns]
        recovered_row = [f"{col_hits[c][0]}/{col_hits[c][1]} ({col_hits[c][0] / col_hits[c][1]:.0%})" if col_hits[c][1]
                         else ("0/0" if c in mapping else "") for c in columns]
        if columns[0] not in mapping and columns[0] != name_col:  # label the footer rows in the first (ID) column
            field_row[0], recovered_row[0] = "NESdb FIELD", "RECOVERED"
        total = f"{all_hits}/{all_total} ({all_hits / all_total:.0%})" if all_total else ""
        w.writerow([])
        w.writerow(field_row)
        w.writerow(recovered_row + ["", "", "", "", "", "", "", total, ""])

    # Per NES: a field counts as recovered if any row of that NES recovered it.
    per_nes = defaultdict(lambda: {"rows": 0, "fields": defaultdict(lambda: None), "entry": None})
    col_nes = defaultdict(dict)  # column -> NES ID -> did any row of that NES recover it
    for c in cells:
        nes = per_nes[c["nes_id"]]
        r = {"recovered": True, "missed": False}.get(c["result"]) if c["counted"] == "yes" else None
        if r is not None:
            nes["fields"][c["nesdb_field"]] = bool(nes["fields"][c["nesdb_field"]]) or r
            col_nes[c["column"]][c["nes_id"]] = col_nes[c["column"]].get(c["nes_id"], False) or r
    col_nes_hits = {col: (sum(d.values()), len(d)) for col, d in col_nes.items()}
    for best in row_results:
        if best:
            per_nes[best[1]["nes_id"]]["rows"] += 1
            per_nes[best[1]["nes_id"]]["entry"] = best[1]
    with (out_dir / f"{stem}_per_nes.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["NES ID", "Name", "Full Name", "CSV Rows", "Recovered", "Recovered %", "Recovered Fields", "Missed Fields"])
        nes_rates, nes_got, nes_total = [], 0, 0
        for nes_id, nes in sorted(per_nes.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0):
            got = sorted(k for k, v in nes["fields"].items() if v)
            missed = sorted(k for k, v in nes["fields"].items() if v is False)
            total = len(got) + len(missed)
            if total:
                nes_rates.append(len(got) / total)
                nes_got, nes_total = nes_got + len(got), nes_total + total
            w.writerow([nes_id, nes["entry"].get("name", ""), nes["entry"].get("full_name", ""), nes["rows"],
                        f"{len(got)}/{total}", f"{len(got) / total:.0%}" if total else "", "; ".join(got), "; ".join(missed)])
        if nes_rates:
            # Average: every NES weighs the same. Pooled: every NESdb field weighs the same.
            mean, median = sum(nes_rates) / len(nes_rates), statistics.median(nes_rates)
            w.writerow([])
            w.writerow(["AVERAGE per NES", "", f"over {len(nes_rates)} NES", "", "", f"{mean:.0%}", "", ""])
            w.writerow(["MEDIAN per NES", "", "", "", "", f"{median:.0%}", "", ""])
            w.writerow(["POOLED (all fields)", "", "", "", f"{nes_got}/{nes_total}", f"{nes_got / nes_total:.0%}", "", ""])

    with (out_dir / f"{stem}_cells.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["row", "protein", "nes_id", "column", "nesdb_field", "csv_value", "nesdb_value",
                                          "result", "counted", "judged_by", "llm_reason"])
        w.writeheader()
        w.writerows(cells)

    # Per signal: for every paper (the CSV's, plus --docs-dir's) and every NES NESdb cites that paper for,
    # did any row of the paper about that protein give it - by sequence, by location only, or not at all?
    papers = {}  # pmid -> document stem
    rows_by_pmid = defaultdict(list)
    for i, row in enumerate(rows):
        m = SOURCE_RE.match(row.get(cols.get("source") or "", ""))
        if m:
            papers.setdefault(m.group(2), m.group(0))
            rows_by_pmid[m.group(2)].append(i)
    if args.docs_dir:
        for path in args.docs_dir.glob("*.txt"):
            m = SOURCE_RE.match(path.stem)
            if m:
                papers.setdefault(m.group(2), path.stem)
    signal_rows, undetermined = [], []
    for pmid, doc in sorted(papers.items(), key=lambda kv: kv[1].lower()):
        letters = paper_text(args.docs_dir, doc)
        figures = figure_count(args.docs_dir, doc)
        for entry in by_pmid.get(pmid, []):
            signals = entry_signals(entry)
            if not signals:
                undetermined.append((doc, entry))
                continue
            own = [i for i in rows_by_pmid[pmid] if any(e is entry for e in found[i][0])]
            for s in signals:
                status, by_rows, location_checks = "missed", [], []
                for i in own:
                    values = signal_values(rows[i], mapping)
                    hits = {signal_hit(v, s) for v in values}
                    hit = "sequence" if "sequence" in hits else "location" if "location" in hits else None
                    if hit:
                        by_rows.append(str(i + 1))
                        if status != "sequence":
                            status = hit
                        if hit == "location":
                            location_checks.append(uniprot_check(values, sequence_of(entry), s)[0])
                # For a location-only match: the best UniProt result among the rows that gave it.
                location_check = min(location_checks, key=LOCATION_ORDER.index, default="") \
                    if status == "location" else ""
                if not own:
                    status = "missed - no row for this protein" if rows_by_pmid[pmid] else "missed - paper has no rows"
                reason = ""
                if status != "sequence" and args.docs_dir:
                    if letters is None:
                        reason = "paper not found in --docs-dir"
                    elif in_text(s["seq"], letters):
                        reason = "peptide is in the paper text"
                    else:
                        reason = f"peptide not in the paper text ({figures} extracted figures)" if figures \
                            else "peptide not in the paper text, no extracted figures"
                signal_rows.append([doc, pmid, entry["nes_id"], entry.get("name", ""), signal_text(s), status,
                                    ", ".join(by_rows), reason, location_check])
    with (out_dir / f"{stem}_per_signal.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Document", "PMID", "NES ID", "Name", "NESdb Signal", "Status", "CSV Rows", "Why not by sequence",
                    "UniProt Check (location matches)"])
        w.writerows(signal_rows)
        for doc, entry in undetermined:
            w.writerow([doc, SOURCE_RE.match(doc).group(2), entry["nes_id"], entry.get("name", ""),
                        "undetermined in NESdb", "not scored", "", "", ""])
    if len(uniprot_store) > uniprot_known:
        args.uniprot_cache.parent.mkdir(parents=True, exist_ok=True)
        args.uniprot_cache.write_text(json.dumps(uniprot_store, indent=1), encoding="utf-8")

    print("\nRecovered per column, per NES (an NES counts once, recovered if any of its rows got it; per row in brackets)")
    for col in mapping:
        hits, total = col_hits[col]
        col_got, col_n = col_nes_hits.get(col, (0, 0))
        note = " - from UniProt, not in totals" if mapping[col] in EXTERNAL_FIELDS and not args.score_external else ""
        print(f"  {col}: {col_got}/{col_n} NES" + (f" ({col_got / col_n:.0%})" if col_n else "")
              + f"  [rows {hits}/{total}" + (f" ({hits / total:.0%})" if total else "") + "]" + note)
    if nes_rates:
        print(f"\nPer NES ({len(nes_rates)} NES): average {mean:.0%} of fields recovered, median {median:.0%}, "
              f"pooled {nes_got}/{nes_total} ({nes_got / nes_total:.0%})")

    classes = defaultdict(int)
    for kind, _ in row_class:
        if kind:
            classes[kind] += 1
    print("\nRows by NES match: " + ", ".join(f"{k} {v}" for k, v in sorted(classes.items(), key=lambda kv: -kv[1])))
    statuses = defaultdict(int)
    reasons = defaultdict(int)
    for r in signal_rows:
        statuses[r[5]] += 1
        if r[7]:
            reasons[r[7].split(" (")[0]] += 1
    if signal_rows:
        print(f"\nPer NESdb signal ({len(signal_rows)} signals NESdb cites {len(papers)} papers for): "
              + ", ".join(f"{k} {v}" for k, v in sorted(statuses.items(), key=lambda kv: -kv[1])))
        if reasons:
            print("  Not found by sequence: " + ", ".join(f"{k} {v}" for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])))
    print(f"  {len(undetermined)} NESdb proteins cited for these papers have no mapped NES (undetermined) - not scored")
    row_checks = defaultdict(int)
    for check, _ in row_uniprot:
        if check:
            row_checks[check] += 1
    location_quality = defaultdict(int)
    for r in signal_rows:
        if r[8]:
            location_quality[r[8]] += 1
    print("\nUniProt check per row: " + ", ".join(f"{k} {v}" for k, v in sorted(row_checks.items(), key=lambda kv: -kv[1])))
    if location_quality:
        print("  Signals found by location only: "
              + ", ".join(f"{k} {v}" for k, v in sorted(location_quality.items(), key=lambda kv: -kv[1])))

    # The same summary as a table, with how each number was reached.
    summary = [["Rows", "CSV rows", len(rows), ""],
               ["Rows", "Matched to an NES NESdb cites the row's paper for", hows.count("cited"), ""],
               ["Rows", "Protein in NESdb, paper not cited for it", hows.count("name only"),
                "not scored" + (" (scored: --include-uncited)" if args.include_uncited else "")],
               ["Rows", "Not in NESdb", hows.count(""), ""]]
    for col in mapping:
        hits, total = col_hits[col]
        external = mapping[col] in EXTERNAL_FIELDS and not args.score_external
        col_got, col_n = col_nes_hits.get(col, (0, 0))
        summary.append(["Recovered per column, per NES", col,
                        f"{col_got}/{col_n}" + (f" ({col_got / col_n:.0%})" if col_n else ""),
                        "an NES counts once: recovered if any of its rows recovered it"])
        summary.append(["Recovered per column, per row", col, f"{hits}/{total}" + (f" ({hits / total:.0%})" if total else ""),
                        f"NESdb field: {mapping[col]}" + ("; from UniProt, not the paper - not in totals" if external else "")])
    if nes_rates:
        summary += [["Per NES", "NES IDs scored", len(nes_rates), ""],
                    ["Per NES", "Average share of fields recovered", f"{mean:.0%}", "every NES weighs the same"],
                    ["Per NES", "Median share of fields recovered", f"{median:.0%}", ""],
                    ["Per NES", "Pooled fields recovered", f"{nes_got}/{nes_total} ({nes_got / nes_total:.0%})",
                     "every NESdb field weighs the same"]]
    match_notes = {
        "sequence": "the row's peptide shares 8+ residues (or all of a shorter one) with this NESdb NES",
        "location": "only the row's residue range overlaps NESdb's start-end numbers (NESdb numbers residues on its "
                    "UniProt sequence) - see the UniProt check for what the protein has at that range",
        EXTRA_NES: "the row gives an NES that is none of NESdb's for this protein - NES-specific columns not compared",
        OTHER_PROTEIN: "the row's organism or UniProt ID contradicts NESdb's (e.g. an ortholog) - row not scored",
        NO_NES_GIVEN: "the row has no peptide or residue range",
        NESDB_UNDETERMINED: "NESdb has no mapped NES for this protein",
    }
    for kind, n in sorted(classes.items(), key=lambda kv: -kv[1]):
        summary.append(["Rows by NES match", kind, n, match_notes.get(kind, "")])
    summary.append(["Per NESdb signal", "Signals NESdb cites these papers for", len(signal_rows), f"{len(papers)} papers"])
    for status, n in sorted(statuses.items(), key=lambda kv: -kv[1]):
        summary.append(["Per NESdb signal", status, n, match_notes.get(status, "")])
    for reason, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
        summary.append(["Not found by sequence", reason, n, "letters of the paper text, ignoring spaces and line breaks"])
    summary.append(["Per NESdb signal", "Proteins with no mapped NES in NESdb", len(undetermined), "not scored"])
    check_notes = {
        PEPTIDE_AT_RESIDUES: "the row's peptide is in the protein at the residue numbers the row gives",
        NUMBERING_DIFFERS: "the row's peptide is in the protein but at other numbers (another isoform or numbering)",
        PEPTIDE_IN_PROTEIN: "the row's peptide is in the protein; the row gives no residue numbers",
        PEPTIDE_NOT_IN_PROTEIN: "the row's peptide is not in NESdb's protein: an ortholog, a mutant or a wrong extraction",
        RANGE_IS_NES: "no peptide; the protein's residues at the row's range are NESdb's NES",
        RANGE_TOO_WIDE: "no peptide; the range holds NESdb's NES but is more than twice as long",
        RANGE_SMALL_OVERLAP: "no peptide; the range shares fewer than 8 residues with NESdb's NES",
        RANGE_RESIDUES: "no peptide; the protein's residues at the row's range (an NES NESdb doesn't list)",
        RANGE_OUTSIDE: "the row's range goes past the end of NESdb's protein: numbering of another protein",
        NO_SEQUENCE: "neither NESdb nor UniProt gave a sequence",
    }
    for check, n in sorted(row_checks.items(), key=lambda kv: -kv[1]):
        summary.append(["UniProt check per row", check, n, check_notes.get(check, "")])
    for check, n in sorted(location_quality.items(), key=lambda kv: -kv[1]):
        summary.append(["UniProt check of signals found by location only", check, n, check_notes.get(check, "")])
    with (out_dir / f"{stem}_summary.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Section", "Metric", "Value", "Note"])
        w.writerows(summary)

    print(f"\n{len(per_nes)} NES IDs covered. Wrote {stem}.csv, {stem}_per_nes.csv, {stem}_per_signal.csv, "
          f"{stem}_cells.csv, {stem}_summary.csv to {out_dir}")


if __name__ == "__main__":
    main()
