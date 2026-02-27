#!/usr/bin/env python3
"""
ScheMatiQ vs. Gold-Table Evaluation
====================================
Compares judge names, decision dates, and court level (D/C/SCOTUS)
between a manually coded gold table and ScheMatiQ's output.

Precision / Recall / F1 are reported for each of the three fields.

Usage
-----
    python evaluate_schematiq.py

Expects the two CSV files in the same directory as this script (or update
GOLD_PATH / SCHEMATIQ_PATH below).
"""

import csv
import os
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# File paths  (update if files live elsewhere)
# ─────────────────────────────────────────────────────────────────────────────
GOLD_PATH = Path(__file__).parent / "Immigration_ground_truth.csv"
SCHEMATIQ_PATH = (
    Path(__file__).parent
    / "different Presidents ScheMatiQ meeting results_2026-02-23_01-35-28.csv"
)
# Directory that contains the raw .txt source documents.
# Only gold docs whose raw file exists here are included in the evaluation.
RAW_DOCS_DIR = Path(
    "/Users/ehabba/PycharmProjects/Legal_Schema_Generator/data/US_Immigration/USA/raw"
)

# ─────────────────────────────────────────────────────────────────────────────
# MANUAL TYPO / ALIAS CORRECTIONS
# These map non-canonical IDs → canonical IDs so both tables can be joined.
# Canonical form = Gold's Name value (possibly after applying GOLD_ID_FIXES).
# ─────────────────────────────────────────────────────────────────────────────

# Gold Name → canonical ID  (fix typos in the gold Name column)
GOLD_ID_FIXES: dict = {
    # Gold has a stray space; ScheMatiQ does not → normalise to no-space form
    "Immigrant Center2025-07-02DDC": "ImmigrantCenter2025-07-02DDC",
}

# ScheMatiQ Source Document → canonical ID  (fix typos in the ScheMatiQ column)
SCHEMATIQ_ID_FIXES: dict = {
    # "DColo" is a verbose abbreviation of the D.Colo. court; gold uses "DCO"
    "DBU2025-05-06DColo": "DBU2025-05-06DCO",
    # Date is off by one day in ScheMatiQ (Aug 2 → Aug 1)
    "ImmigrantRights2025-08-02DDC": "ImmigrantRights2025-08-01DDC",
    # ScheMatiQ prefixed these with "O" (likely "Original"); strip the prefix
    "ODoe2025-02-13DMA": "Doe2025-02-13DMA",
    "ODoe2025-07-03CA1": "Doe2025-07-03CA1",
}

# ─────────────────────────────────────────────────────────────────────────────
# Court-level mappings
# ─────────────────────────────────────────────────────────────────────────────
GOLD_COURT_MAP: dict = {
    "D": "district",
    "C": "circuit",
    "SCOTUS": "supreme",
}

# Substrings that identify each level in ScheMatiQ's free-text Court Level field
SCHEMATIQ_COURT_KEYWORDS: list = [
    ("supreme", "supreme"),
    ("circuit", "circuit"),
    ("court of appeals", "circuit"),
    ("district", "district"),
]


def normalize_court_gold(val: str) -> Optional[str]:
    if not val:
        return None
    return GOLD_COURT_MAP.get(val.strip().upper())


def normalize_court_schematiq(val: str) -> Optional[str]:
    if not val:
        return None
    v = val.strip().lower()
    for keyword, norm in SCHEMATIQ_COURT_KEYWORDS:
        if keyword in v:
            return norm
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Date normalisation
# ─────────────────────────────────────────────────────────────────────────────
_DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%B %d, %Y",
    "%b %d, %Y",
    "%m/%d/%y",
    "%B %Y",
    "%b %Y",
    "%Y",
]


def normalize_date(val: str) -> Optional[str]:
    if not val:
        return None
    val = val.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(val, fmt)
            # Reject obviously wrong years (e.g., "3035" typos in the gold table)
            if dt.year < 2000 or dt.year > 2099:
                return None
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Judge-name normalisation
# ─────────────────────────────────────────────────────────────────────────────

# Hard-coded fixes for gold judge codes that the regex cannot parse cleanly
_GOLD_JUDGE_OVERRIDES: dict = {
    "WalkerJustin": "walker",            # Justin Walker (full first name in code)
    "Martínez-OlguínA": "martinez-olguin",
    "DeAlbaA": "dealba",                 # "De Alba" treated as one hyphenated token
}

# Prefixes / suffixes to strip from ScheMatiQ's Doc Name before last-name extraction
_SCHEMATIQ_PREFIXES = re.compile(
    r"^(Chief\s+Justice|Circuit\s+Judge|District\s+Judge|Justice|Judge)\s+",
    re.IGNORECASE,
)
_SCHEMATIQ_SUFFIXES = re.compile(r"\b(II|III|IV|Jr\.?|Sr\.?)\b", re.IGNORECASE)
_COMMA_ROLE = re.compile(r",\s*(Circuit|District|Senior).*", re.IGNORECASE)

# Last-name tokens that are not real names (artifact from Doc Name = "Judge" alone)
_BOGUS_LASTNAMES = {"judge", "justice", "circuit", "district", "senior", "j", ""}


def _strip_accents(s: str) -> str:
    """Convert accented characters to their ASCII base (e.g. é → e)."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def extract_lastname_gold(code: str) -> Optional[str]:
    """
    Convert a gold 'Judge' code like "HellersteinA" to a normalised last name.
    Pattern: LastnameCamelInitial  (e.g. "HellersteinA" → "hellerstein")
    """
    if not code:
        return None
    code = code.strip()
    if code in _GOLD_JUDGE_OVERRIDES:
        return _GOLD_JUDGE_OVERRIDES[code]

    # Find the last transition from a lowercase letter to an uppercase letter.
    # Everything before that transition is the last name.
    last_boundary = -1
    for i in range(1, len(code)):
        if code[i - 1].islower() and code[i].isupper():
            last_boundary = i

    if last_boundary == -1:
        raw = code.lower()
    else:
        raw = code[:last_boundary].lower()
    return _strip_accents(raw)


# Manual overrides for ScheMatiQ Doc Names that confuse the generic extractor
_SCHEMATIQ_DOCNAME_OVERRIDES: dict = {
    "Judge DeAlba": "dealba",
    "Judge De Alba": "dealba",
    "De Alba": "dealba",
}


def extract_lastname_schematiq(doc_name: str) -> Optional[str]:
    """
    Extract a normalised last name from a ScheMatiQ Doc Name like:
      "Judge Stephanie L. Haines"  → "haines"
      "AMIR H. ALI"                → "ali"
      "Chief Justice Roberts"      → "roberts"
      "Henderson, Circuit Judge"   → "henderson"
      "RODOLFO A. RUIZ II"         → "ruiz"
    """
    if not doc_name:
        return None
    s = doc_name.strip()

    if s in _SCHEMATIQ_DOCNAME_OVERRIDES:
        return _SCHEMATIQ_DOCNAME_OVERRIDES[s]

    # Handle "Henderson, Circuit Judge" → take the part before the comma
    s = _COMMA_ROLE.sub("", s).strip()

    # Strip leading title
    s = _SCHEMATIQ_PREFIXES.sub("", s).strip()

    # Strip trailing honorific suffixes (II, Jr., etc.)
    s = _SCHEMATIQ_SUFFIXES.sub("", s).strip()

    # Split on whitespace
    parts = [p.rstrip(".,;") for p in s.split() if p.rstrip(".,;")]
    if not parts:
        return None

    # Handle compound last names with nobiliary particles (DE, VAN, VON, DEL …)
    # If the second-to-last token is a particle, join it with the last token.
    _PARTICLES = {"de", "van", "von", "del", "della", "di", "la", "le", "dos", "das"}
    if len(parts) >= 2 and parts[-2].lower() in _PARTICLES:
        candidate = _strip_accents((parts[-2] + parts[-1]).lower())
    else:
        candidate = _strip_accents(parts[-1].lower())

    # Reject tokens that are titles / role words, not actual surnames
    if candidate in _BOGUS_LASTNAMES:
        return None
    return candidate


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_gold(path: Path) -> dict:
    """
    Returns a dict keyed by canonical doc ID.
    Each value: {
        'judges':      set[str]   – normalised last names
        'date':        Optional[str] – "YYYY-MM-DD"
        'court':       Optional[str] – "district" / "circuit" / "supreme"
        'count':       int        – 0 or 1 (researcher's inclusion flag)
    }
    """
    docs: dict = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("Name", "").strip()
            if not name:
                continue
            name = GOLD_ID_FIXES.get(name, name)

            count = int(row.get("Count", "0") or "0")
            if name not in docs:
                docs[name] = {
                    "judges": set(),
                    "date": None,
                    "court": None,
                    "count": count,
                }
            else:
                # keep the most recently seen count (consistent within a doc)
                docs[name]["count"] = count

            judge = row.get("Judge", "").strip()
            if judge:
                ln = extract_lastname_gold(judge)
                if ln:
                    docs[name]["judges"].add(ln)

            if docs[name]["date"] is None:
                docs[name]["date"] = normalize_date(row.get("DecisionDate", ""))

            if docs[name]["court"] is None:
                docs[name]["court"] = normalize_court_gold(row.get("D/C/SCOTUS", ""))

    return docs


def extract_lastnames_from_judge_names_field(raw: str) -> set:
    """
    Extract all normalised last names from the free-text Judge Names cell,
    which may contain one or more full names separated by commas.
    e.g. "DAVID BRIONES" → {'briones'}
         "Judge Thacker, Judge Wilkinson, Judge King" → {'thacker','wilkinson','king'}
         "William K. Sessions, III" → {'sessions'}   (suffix 'III' stays attached)
    """
    if not raw:
        return set()
    # Split on commas; re-attach lone suffixes (II, III, Jr. etc.)
    parts  = [p.strip() for p in raw.split(",")]
    names_raw: list = []
    buf = ""
    for p in parts:
        if " " in p or not buf:          # new full name
            if buf:
                names_raw.append(buf)
            buf = p
        else:                            # suffix like 'III', 'Jr.'
            buf += ", " + p
    if buf:
        names_raw.append(buf)

    result: set = set()
    for name in names_raw:
        ln = extract_lastname_schematiq(name.strip())
        if ln:
            result.add(ln)
    return result


def load_schematiq(path: Path) -> dict:
    """
    Returns a dict keyed by canonical doc ID.
    Each value: {
        'judges':           set[str]  – union of last names from Doc Name AND Judge Names
        'judges_doc_name':  set[str]  – last names from Doc Name only
        'judges_jn_field':  set[str]  – last names from Judge Names field only
        'date':             Optional[str]
        'court':            Optional[str]
        'rows':             list[dict] – raw rows for diagnostics
    }

    Using the UNION of both fields gives ScheMatiQ the benefit of the doubt:
    if the judge name appears in *either* output field, it counts as found.
    """
    docs: dict = defaultdict(
        lambda: {"judges": set(), "judges_doc_name": set(),
                 "judges_jn_field": set(),
                 "date": None, "court": None,
                 "_dates": [], "rows": []}
    )
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            src = row.get("Source Document", "").strip()
            if not src:
                continue
            src = SCHEMATIQ_ID_FIXES.get(src, src)

            doc_name        = row.get("Doc Name", "").strip()
            judge_names_raw = row.get("Judge Names", "").strip()

            # Extract from Doc Name (primary observation-unit identifier)
            ln_dn = extract_lastname_schematiq(doc_name)
            if ln_dn:
                docs[src]["judges_doc_name"].add(ln_dn)
                docs[src]["judges"].add(ln_dn)

            # Extract from Judge Names field (the actual extracted value)
            for ln_jn in extract_lastnames_from_judge_names_field(judge_names_raw):
                docs[src]["judges_jn_field"].add(ln_jn)
                docs[src]["judges"].add(ln_jn)

            docs[src]["rows"].append(
                {"doc_name": doc_name, "judge_names_raw": judge_names_raw}
            )

            d = normalize_date(row.get("Decision Date", ""))
            if d:
                docs[src]["_dates"].append(d)

            if docs[src]["court"] is None:
                docs[src]["court"] = normalize_court_schematiq(
                    row.get("Court Level", "")
                )

    result = {}
    for src, info in docs.items():
        dates = info.pop("_dates")
        if dates:
            info["date"] = Counter(dates).most_common(1)[0][0]
        result[src] = info
    return result


def _count_names_in_cell(raw: str) -> int:
    """
    Heuristically count how many judge names are packed into one judge_names cell.
    Strategy: split on commas; tokens with spaces are standalone full names;
    tokens without spaces (like 'III', 'Jr.') are suffixes of the preceding name.
    """
    if not raw:
        return 0
    parts = [p.strip() for p in raw.split(",")]
    names = []
    buf = ""
    for p in parts:
        if " " in p or not buf:
            if buf:
                names.append(buf)
            buf = p
        else:
            buf += ", " + p   # suffix like 'III', 'Jr.'
    if buf:
        names.append(buf)
    return max(len(names), 1)


# ─────────────────────────────────────────────────────────────────────────────
# Raw-file existence check
# ─────────────────────────────────────────────────────────────────────────────

# Manual fuzzy mappings: gold canonical ID → raw filename stem
# (only needed when exact match fails)
_RAW_FILENAME_OVERRIDES: dict = {
    "DBU2025-05-06DCO":             "DBU2025-05-06DColo",
    "Doe2025-07-03CA1":             "ODoe2025-07-03CA1",
    "ImmigrantRights2025-08-01DDC": "ImmigrantRights2025-08-02DDC",
    "NewJersey2025-02-13DMA":       "NewJersey2025-02-13",
    # NewJersey2025-07-03CA1 has no raw file — intentionally not mapped
}


def build_raw_doc_set(raw_dir: Path) -> set:
    """
    Return the set of canonical gold IDs for which a raw .txt file exists.
    Applies manual fuzzy overrides for known filename mismatches.
    """
    if not raw_dir.exists():
        return set()   # if dir missing, skip filtering
    stems = {f.stem for f in raw_dir.glob("*.txt")}
    # Invert override map: raw stem → gold ID
    override_inv = {v: k for k, v in _RAW_FILENAME_OVERRIDES.items()}
    available: set = set()
    for stem in stems:
        if stem in override_inv:
            available.add(override_inv[stem])
        else:
            available.add(stem)
    return available


# ─────────────────────────────────────────────────────────────────────────────
# Metric helpers
# ─────────────────────────────────────────────────────────────────────────────

def prf(tp: int, fp: int, fn: int) -> tuple:
    prec = tp / (tp + fp) if tp + fp > 0 else 0.0
    rec  = tp / (tp + fn) if tp + fn > 0 else 0.0
    f1   = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0.0
    return prec, rec, f1


def accuracy_binary(correct: int, total: int) -> float:
    """Fraction of comparisons that were exactly right (for single-valued fields)."""
    return correct / total if total > 0 else 0.0


def jaccard(tp: int, fp: int, fn: int) -> float:
    """Element-level Jaccard = TP / (TP + FP + FN), the set-overlap accuracy."""
    return tp / (tp + fp + fn) if (tp + fp + fn) > 0 else 0.0


def _bar(val: float, width: int = 30) -> str:
    filled = round(val * width)
    return "█" * filled + "░" * (width - filled)


def _metrics_block(
    label: str,
    tp: int,
    fp: int,
    fn: int,
    correct: int,
    total: int,
    exact_match: Optional[int] = None,   # for judge exact-match (docs)
    total_docs: Optional[int] = None,
) -> None:
    """
    Print a full metrics block for one field.

    correct / total  → accuracy  (fraction of predictions that hit gold)
    tp / fp / fn     → precision / recall / F1
    exact_match      → (judges only) docs where every judge matched perfectly
    """
    prec, rec, f1 = prf(tp, fp, fn)
    acc = accuracy_binary(correct, total)
    jac = jaccard(tp, fp, fn)

    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    print(f"  TP={tp:>4}  FP={fp:>4}  FN={fn:>4}  "
          f"Correct={correct:>4}  Total={total:>4}")
    print(f"  Accuracy  : {acc:6.3f}  {_bar(acc)}"
          "  (correct / total comparisons)")
    print(f"  Precision : {prec:6.3f}  {_bar(prec)}")
    print(f"  Recall    : {rec:6.3f}  {_bar(rec)}")
    print(f"  F1        : {f1:6.3f}  {_bar(f1)}")
    if exact_match is not None and total_docs is not None:
        em = accuracy_binary(exact_match, total_docs)
        print(f"  Exact-match accuracy (all judges right per doc):")
        print(f"            : {em:6.3f}  {_bar(em)}"
              f"  ({exact_match}/{total_docs} docs)")
        print(f"  Jaccard   : {jac:6.3f}  {_bar(jac)}"
              "  (TP / TP+FP+FN, element-level)")


# ─────────────────────────────────────────────────────────────────────────────
# Main evaluation
# ─────────────────────────────────────────────────────────────────────────────

def evaluate() -> None:
    gold = load_gold(GOLD_PATH)
    sm   = load_schematiq(SCHEMATIQ_PATH)

    gold_ids  = set(gold)
    sm_ids    = set(sm)

    # Only count=1 docs are the real ground truth
    counted_all = {k: v for k, v in gold.items() if v["count"] == 1}

    # Further restrict to docs whose raw source file actually exists —
    # if the file was never in the corpus, SM could not have processed it
    # and it would be unfair to count it as a failure.
    raw_available = build_raw_doc_set(RAW_DOCS_DIR)
    if raw_available:
        counted = {k: v for k, v in counted_all.items() if k in raw_available}
        excluded = {k: v for k, v in counted_all.items() if k not in raw_available}
    else:
        counted  = counted_all
        excluded = {}

    matched_all     = gold_ids & sm_ids
    matched_counted = set(counted) & sm_ids
    missed_counted  = set(counted) - sm_ids  # gold docs ScheMatiQ never found

    print("=" * 60)
    print("  DOCUMENT COVERAGE")
    print("=" * 60)
    print(f"  Gold docs total                    : {len(gold_ids)}")
    print(f"  Gold docs (count=1)                : {len(counted_all)}")
    if excluded:
        print(f"  Excluded (raw file missing)        : {len(excluded)}")
        for d in sorted(excluded):
            print(f"    - {d}")
    print(f"  Evaluated (count=1 + raw exists)   : {len(counted)}")
    print(f"  ScheMatiQ docs                     : {len(sm_ids)}")
    print(f"  Matched (all gold)       : {len(matched_all)}")
    print(f"  Matched (count=1 gold)   : {len(matched_counted)}")
    print(f"  Gold-only (count=1 docs ScheMatiQ missed):")
    for d in sorted(missed_counted):
        print(f"    - {d}")
    sm_not_in_counted = sm_ids - set(counted)
    print(f"\n  ScheMatiQ docs not in count=1 gold ({len(sm_not_in_counted)}):")
    for d in sorted(sm_not_in_counted):
        print(f"    - {d}")

    eval_docs = sorted(matched_counted)
    print(f"\n  Evaluating on {len(eval_docs)} matched + counted documents.\n")

    # ─── OBSERVATION-UNIT ROW COUNT ANALYSIS ──────────────────────────────
    # The gold table has one row per judge; ScheMatiQ has one row per
    # observation unit (also one per judge). When ScheMatiQ produces fewer
    # rows than there are gold judges for a document, it structurally cannot
    # capture all judges regardless of name-matching quality.
    print("=" * 60)
    print("  OBSERVATION-UNIT ROW COUNT vs. GOLD JUDGES")
    print("=" * 60)
    print("  (matched + count=1 docs only; shows docs where counts differ)\n")
    hdr = "  " + "Document".ljust(45) + "  Gold  SM rows    Diff"
    print(hdr)
    print("  " + "-" * 62)

    total_gold_slots  = 0
    total_sm_slots    = 0
    missing_from_rows = 0   # judges structurally unreachable because SM had fewer rows
    extra_from_rows   = 0   # extra SM rows beyond gold judge count

    for doc in eval_docs:
        g_count = len(gold[doc]["judges"])
        s_count = len(sm[doc]["judges"])
        total_gold_slots += g_count
        total_sm_slots   += s_count
        diff = s_count - g_count
        if diff < 0:
            missing_from_rows += abs(diff)
            tag = "<<< SM too few rows"
        elif diff > 0:
            extra_from_rows += diff
            tag = ">>> SM extra rows"
        else:
            tag = ""
        if diff != 0:
            print("  %s  %5d  %7d  %+7d  %s" % (
                doc.ljust(45), g_count, s_count, diff, tag))

    # Also count missed docs (SM never extracted them → all gold judges missed)
    missed_slots = sum(len(counted[d]["judges"]) for d in missed_counted)
    net_row_deficit = missing_from_rows - extra_from_rows   # how many gold judges have no SM row at all

    print()
    print(f"  Gold judge-slots  (matched docs)           : {total_gold_slots}")
    print(f"  ScheMatiQ rows    (matched docs)           : {total_sm_slots}")
    print(f"    of which: docs where SM had fewer rows   : -{missing_from_rows} judge-slots unreachable")
    print(f"    of which: docs where SM had extra rows   : +{extra_from_rows} extra rows")
    print(f"    net row deficit (matched docs)           : {net_row_deficit}")
    print(f"  Judges missed because SM never found doc   : {missed_slots}")
    print()
    print(f"  CHECK: SM rows + net deficit + missed slots = gold total?")
    print(f"         {total_sm_slots} + {net_row_deficit} + {missed_slots} = {total_sm_slots + net_row_deficit + missed_slots}  (gold = {total_gold_slots + missed_slots})")
    print()
    total_unreachable = net_row_deficit + missed_slots
    print(f"  Total judges structurally unreachable      : {total_unreachable}")
    print(f"  (= net row deficit in matched docs + all judges in missed docs)\n")

    # ─── COMPLETE JUDGE IDENTIFICATION SUMMARY ────────────────────────────
    # Classify every FN judge into one of three failure categories:
    #   A  = document never processed by SM at all
    #   B  = doc found, but SM produced fewer rows than gold judges
    #        (only arises in multi-judge docs)
    #   C  = SM had a row for the doc but the extracted name was wrong
    print("=" * 60)
    print("  JUDGE IDENTIFICATION — COMPLETE SUMMARY")
    print("=" * 60)

    total_gold_all = sum(len(gold[d]["judges"]) for d in counted)
    tp_all         = sum(len(gold[d]["judges"] & sm[d]["judges"])
                         for d in matched_counted)

    # Category A: docs SM never touched → all their judges missed
    cat_a_docs   = []
    cat_a_judges = 0
    for doc in sorted(missed_counted, key=lambda d: -len(counted[d]["judges"])):
        n = len(counted[doc]["judges"])
        cat_a_judges += n
        cat_a_docs.append((doc, n, len(gold[doc]["judges"])))

    # Categories B and C: within matched docs
    cat_b_judges = 0   # structural row deficit in multi-judge docs
    cat_b_docs   = []
    cat_c_judges = 0   # naming / extraction errors
    cat_c_docs   = []

    for doc in sorted(eval_docs):
        g = gold[doc]["judges"]
        s = sm[doc]["judges"]
        fn = g - s
        fp = s - g
        if not fn:
            continue
        n_gold   = len(g)
        n_sm     = len(s)
        deficit  = max(0, n_gold - n_sm)   # judges with no SM row to absorb them
        naming   = len(fn) - deficit

        if deficit > 0:
            cat_b_judges += deficit
            cat_b_docs.append((doc, n_gold, n_sm, deficit))
        if naming > 0:
            cat_c_judges += naming
            cat_c_docs.append((doc, sorted(fn), sorted(fp), naming))

    fn_total = total_gold_all - tp_all

    print()
    print("  %-50s %s" % ("", "Judges   %"))
    print("  " + "-" * 60)
    print("  %-50s %6d  %5.1f%%" % (
        "Total gold judge rows (count=1 docs)", total_gold_all, 100))
    print("  %-50s %6d  %5.1f%%" % (
        "✓  Successfully identified", tp_all, 100*tp_all/total_gold_all))
    print("  %-50s %6d  %5.1f%%" % (
        "✗  Failed to identify  (total)", fn_total, 100*fn_total/total_gold_all))
    print()
    print("  Failure breakdown:")
    print()

    # ── A ──
    print("  A) Document never processed by SM")
    print("     (%d docs, all single or multi-judge)" % len(cat_a_docs))
    for doc, n, _ in cat_a_docs:
        label = "(%d judge%s)" % (n, "s" if n > 1 else " ")
        print("       %-45s  %s" % (doc, label))
    print("     Subtotal A : %d judges  (%.1f%% of all gold)" % (
        cat_a_judges, 100*cat_a_judges/total_gold_all))
    print()

    # ── B ──
    print("  B) Doc found, SM produced too few rows")
    print("     (only affects multi-judge docs)")
    for doc, ng, ns, nd in sorted(cat_b_docs, key=lambda x: -x[3]):
        print("       %-45s  gold=%d  sm_rows=%d  missed=%d" % (doc, ng, ns, nd))
    print("     Subtotal B : %d judges  (%.1f%% of all gold)" % (
        cat_b_judges, 100*cat_b_judges/total_gold_all))
    print()

    # ── C ──
    print("  C) SM had a row but extracted the wrong name")
    for doc, fn_j, fp_j, n in cat_c_docs:
        print("       %-45s  missed=%s  wrong=%s" % (doc, fn_j, fp_j))
    print("     Subtotal C : %d judges  (%.1f%% of all gold)" % (
        cat_c_judges, 100*cat_c_judges/total_gold_all))
    print()

    check = cat_a_judges + cat_b_judges + cat_c_judges
    print("  CHECK: A + B + C = %d  (FN total = %d)  %s" % (
        check, fn_total, "✓" if check == fn_total else "✗ MISMATCH"))
    print()

    # ─── COLLAPSED-CELL ANALYSIS ──────────────────────────────────────────
    # ScheMatiQ sometimes packs several judge names into one judge_names cell
    # instead of splitting them into separate observation-unit rows.
    # Those extra judges are invisible to the Doc Name extractor.
    print("=" * 60)
    print("  COLLAPSED JUDGE-NAMES CELLS")
    print("=" * 60)
    print("  (rows where judge_names field contains > 1 name)\n")
    print("  %-45s  %-35s  Names  Extra" % ("Source Doc", "Doc Name"))
    print("  " + "-" * 95)

    collapsed_extra_total = 0
    for doc in sorted(eval_docs):
        for r in sm[doc]["rows"]:
            n = _count_names_in_cell(r["judge_names_raw"])
            if n > 1:
                extra = n - 1
                collapsed_extra_total += extra
                print("  %-45s  %-35s  %5d  %5d  %s" % (
                    doc, r["doc_name"][:35], n, extra,
                    r["judge_names_raw"][:55]))

    print()
    print(f"  Total extra judges collapsed into multi-name cells : {collapsed_extra_total}")
    print(f"  (these judges exist in the judge_names field but not as separate rows)\n")

    # ─── WRONG-NAME DIAGNOSIS ─────────────────────────────────────────────
    # For mismatched docs where SM had enough rows (no structural deficit),
    # examine the raw Doc Name and Judge Names to classify the root cause.
    print("=" * 60)
    print("  WRONG-NAME FN DIAGNOSIS")
    print("=" * 60)
    print("  (docs where FN judges remain even after accounting for row deficit)\n")

    # Root-cause taxonomy
    CAUSE_SUFFIX_GARBAGE   = "suffix/title garbage extracted as name"
    CAUSE_WRONG_DOC_JUDGE  = "SM used a judge from a different document"
    CAUSE_ACCENT_TYPO      = "accent / Unicode normalisation mismatch"
    CAUSE_PARTICLE         = "compound surname particle split (De Alba → alba)"
    CAUSE_TRUNCATION       = "Doc Name too short / truncated"
    CAUSE_UNKNOWN          = "other / unclear"

    # Manual classification from inspection of raw data.
    # Note: with union evaluation (Doc Name ∪ Judge Names), many cases that
    # appeared as errors are now correctly resolved. Only genuine failures remain.
    _CAUSE_MAP: dict = {
        # (doc, gold_last_name) : cause
        # Doe2025-07-03CA1: 'barron' now found via Judge Names field (fixed by union)
        # Community2025-05-14CA9: 'fletcher' now found via Judge Names field (fixed)
        # Puentes2025-04-25WDTX: 'briones' now found via Judge Names field (fixed)
        ("Doe2025-07-03CA1",          "rikelman"):      CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "alito"):         CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "barrett"):       CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "gorsuch"):       CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "jackson"):       CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "kagan"):         CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "kavanaugh"):     CAUSE_TRUNCATION,
        ("Garcia2025-04-10SCt",       "thomas"):        CAUSE_TRUNCATION,
        ("Mahdawi2025-04-30DVT",      "sessions"):      CAUSE_WRONG_DOC_JUDGE,
    }

    cause_counts: dict = defaultdict(int)
    printed_any = False
    for doc in sorted(eval_docs):
        g_judges = gold[doc]["judges"]
        s_judges = sm[doc]["judges"]
        fn_judges = g_judges - s_judges
        fp_judges = s_judges - g_judges
        if not fn_judges:
            continue

        # Only show if this doc has "real" wrong-name FNs beyond structural deficit
        g_count = len(g_judges)
        s_count = len(s_judges)
        structural_fn = max(0, g_count - s_count)   # judges with no row at all
        naming_fn = len(fn_judges) - structural_fn
        if naming_fn <= 0:
            continue

        printed_any = True
        print("  ► %s" % doc)
        print("    Gold judges      : %s" % sorted(g_judges))
        print("    SM judges (DocName) : %s" % sorted(s_judges))
        print("    FP (wrong names) : %s" % sorted(fp_judges))
        print("    FN (missed names): %s" % sorted(fn_judges))
        print("    SM raw rows:")
        for r in sm[doc]["rows"]:
            print("      Doc Name: %-45s | Judge Names: %s" % (
                r["doc_name"][:45], r["judge_names_raw"][:60]))
        print("    Root causes:")
        for fn_j in sorted(fn_judges):
            cause = _CAUSE_MAP.get((doc, fn_j), CAUSE_UNKNOWN)
            cause_counts[cause] += 1
            print("      ✗ %-20s → %s" % (fn_j, cause))
        print()

    if not printed_any:
        print("  (none beyond structural row deficit)\n")

    print("  Summary of wrong-name FN causes:")
    for cause, cnt in sorted(cause_counts.items(), key=lambda x: -x[1]):
        print("    %3d  %s" % (cnt, cause))
    print()

    # ─── 1. JUDGES ────────────────────────────────────────────────────────
    j_tp = j_fp = j_fn = 0
    j_exact = 0          # docs where ScheMatiQ got every judge exactly right
    j_total_docs = 0     # docs used for exact-match denominator
    judge_details: list = []

    for doc in eval_docs:
        g_judges = gold[doc]["judges"]
        s_judges = sm[doc]["judges"]
        tp = g_judges & s_judges
        fp = s_judges - g_judges
        fn = g_judges - s_judges
        j_tp += len(tp)
        j_fp += len(fp)
        j_fn += len(fn)
        if g_judges:          # only count docs that have gold judges
            j_total_docs += 1
            if not fp and not fn:
                j_exact += 1
        if fp or fn:
            judge_details.append(
                {
                    "doc": doc,
                    "gold": sorted(g_judges),
                    "sm":   sorted(s_judges),
                    "tp":   sorted(tp),
                    "fp":   sorted(fp),
                    "fn":   sorted(fn),
                }
            )

    # Gold docs missed entirely → all their judges are FN; doc not exact-matched
    for doc in missed_counted:
        missed_judges = counted[doc]["judges"]
        j_fn += len(missed_judges)
        if missed_judges:
            j_total_docs += 1   # missed → definitely not exact match

    _metrics_block(
        "1. JUDGE COVERAGE (multi-label, micro-averaged)",
        j_tp, j_fp, j_fn,
        correct=j_tp,           # element-level "correct" = TP mentions
        total=j_tp + j_fp + j_fn,
        exact_match=j_exact,
        total_docs=j_total_docs,
    )

    if judge_details:
        print(f"\n  Mismatches in {len(judge_details)} document(s):")
        for m in judge_details:
            print(f"\n    ► {m['doc']}")
            print(f"       Gold      : {m['gold']}")
            print(f"       ScheMatiQ : {m['sm']}")
            if m["fp"]:
                print(f"       Extra (FP): {m['fp']}")
            if m["fn"]:
                print(f"       Missed(FN): {m['fn']}")

    # ─── 2. DECISION DATE ─────────────────────────────────────────────────
    d_tp = d_fp = d_fn = 0
    d_total = 0          # docs with a gold date (matched + missed)
    date_details: list = []

    for doc in eval_docs:
        g_date = gold[doc]["date"]
        s_date = sm[doc]["date"]
        if not g_date:
            continue  # no gold date to judge against
        d_total += 1
        if s_date == g_date:
            d_tp += 1
        elif s_date:
            d_fp += 1
            d_fn += 1
            date_details.append((doc, g_date, s_date, "wrong date"))
        else:
            d_fn += 1
            date_details.append((doc, g_date, None, "no date in ScheMatiQ"))

    # Missed count=1 docs → each counts as FN (if they have a gold date)
    for doc in missed_counted:
        if counted[doc]["date"]:
            d_fn += 1
            d_total += 1

    _metrics_block(
        "2. DECISION DATE",
        d_tp, d_fp, d_fn,
        correct=d_tp,
        total=d_total,
    )

    if date_details:
        print(f"\n  Mismatches ({len(date_details)}):")
        for doc, gd, sd, reason in date_details:
            print(f"    {doc:<45}  gold={gd}  sm={sd}  ({reason})")

    # ─── 3. COURT LEVEL ───────────────────────────────────────────────────
    c_tp = c_fp = c_fn = 0
    c_total = 0          # docs with a gold court level (matched + missed)
    court_details: list = []

    for doc in eval_docs:
        g_court = gold[doc]["court"]
        s_court = sm[doc]["court"]
        if not g_court:
            continue
        c_total += 1
        if s_court == g_court:
            c_tp += 1
        elif s_court:
            c_fp += 1
            c_fn += 1
            court_details.append((doc, g_court, s_court, "wrong level"))
        else:
            c_fn += 1
            court_details.append((doc, g_court, None, "no level in ScheMatiQ"))

    # Missed count=1 docs
    for doc in missed_counted:
        if counted[doc]["court"]:
            c_fn += 1
            c_total += 1

    _metrics_block(
        "3. COURT LEVEL  (D / C / SCOTUS)",
        c_tp, c_fp, c_fn,
        correct=c_tp,
        total=c_total,
    )

    if court_details:
        print(f"\n  Mismatches ({len(court_details)}):")
        for doc, gc, sc, reason in court_details:
            print(f"    {doc:<45}  gold={gc}  sm={sc}  ({reason})")

    # ─── SUMMARY TABLE ────────────────────────────────────────────────────
    j_acc_elem  = jaccard(j_tp, j_fp, j_fn)          # element-level Jaccard
    j_acc_exact = accuracy_binary(j_exact, j_total_docs)
    d_acc       = accuracy_binary(d_tp, d_total)
    c_acc       = accuracy_binary(c_tp, c_total)

    print("\n")
    print("=" * 72)
    print("  SUMMARY")
    print("=" * 72)
    hdr = f"  {'Field':<18}  {'Accuracy':>8}  {'Precision':>9}  {'Recall':>9}  {'F1':>9}"
    print(hdr)
    print(f"  {'-'*18}  {'-'*8}  {'-'*9}  {'-'*9}  {'-'*9}")

    p, r, f = prf(j_tp, j_fp, j_fn)
    print(f"  {'Judges (Jaccard)':<18}  {j_acc_elem:>8.3f}  {p:>9.3f}  {r:>9.3f}  {f:>9.3f}")
    print(f"  {'Judges (exact doc)':<18}  {j_acc_exact:>8.3f}  {'—':>9}  {'—':>9}  {'—':>9}")

    p, r, f = prf(d_tp, d_fp, d_fn)
    print(f"  {'Decision Date':<18}  {d_acc:>8.3f}  {p:>9.3f}  {r:>9.3f}  {f:>9.3f}")

    p, r, f = prf(c_tp, c_fp, c_fn)
    print(f"  {'Court Level':<18}  {c_acc:>8.3f}  {p:>9.3f}  {r:>9.3f}  {f:>9.3f}")

    print()
    print("  Notes:")
    print("  · Accuracy (Judges/Jaccard) = TP / (TP+FP+FN) — element-level overlap")
    print("  · Accuracy (Judges/exact)   = docs where ScheMatiQ got ALL judges right")
    print("  · Accuracy (Date/Court)     = correct / total docs with a gold value")
    print("  · Precision/Recall/F1 treat unmatched gold docs as FN")
    print()


if __name__ == "__main__":
    evaluate()
