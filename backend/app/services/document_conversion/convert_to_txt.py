"""Multi-format document to plain-text conversion.

Converts DOCX, DOC, RTF, and PDF files to UTF-8 text.

- DOCX: python-docx first, LibreOffice headless fallback
- DOC / RTF: LibreOffice headless
- PDF: pdfplumber first, OCR fallback (pdf2image + tesseract)

Vendored from Legal_Schema_Generator; adapted for use as a library inside
the ScheMatiQ backend (no CLI entry point, no process-wide signal handlers,
structured logging instead of print()).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

import pdfplumber
import pytesseract
from docx import Document
from pdf2image import convert_from_path
from pdfplumber.utils import DEFAULT_X_TOLERANCE, cluster_objects

logger = logging.getLogger(__name__)

LIBREOFFICE_EXTENSIONS = {".rtf", ".docx", ".doc"}
PDF_EXTENSION = ".pdf"
SUPPORTED_EXTENSIONS = LIBREOFFICE_EXTENSIONS | {PDF_EXTENSION}


# ---------------------------------------------------------------------------
# LibreOffice helpers
# ---------------------------------------------------------------------------

def get_libreoffice_path() -> str:
    """Return the LibreOffice soffice executable path.

    Raises FileNotFoundError when LibreOffice is not installed.
    """
    candidates = {
        "darwin": "/Applications/LibreOffice.app/Contents/MacOS/soffice",
        "linux": "/usr/bin/soffice",
        "win32": r"C:\Program Files\LibreOffice\program\soffice.exe",
    }
    default_path = candidates.get(sys.platform, "/usr/bin/soffice")
    if os.path.exists(default_path):
        return default_path

    found = shutil.which("soffice")
    if found:
        return found

    raise FileNotFoundError(
        "LibreOffice not found. Install it or add soffice to PATH."
    )


def _run_libreoffice(
    input_path: Path,
    output_dir: Path,
    soffice_path: str,
    *,
    profile_dir: Optional[Path] = None,
    max_retries: int = 3,
) -> Tuple[bool, str]:
    """Run LibreOffice headless to convert *input_path* to text.

    Each call uses its own user-profile directory (under *profile_dir*, or a
    fresh tempdir) so concurrent invocations don't collide.
    """
    cleanup_profile = False
    if profile_dir is None:
        profile_dir = Path(tempfile.mkdtemp(prefix="lo_profile_"))
        cleanup_profile = True

    cmd = [
        soffice_path,
        "--headless",
        f"-env:UserInstallation=file://{profile_dir}",
        "--convert-to", "txt:Text",
        "--outdir", str(output_dir),
        str(input_path),
    ]

    last_error = ""
    try:
        for attempt in range(max_retries):
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=120,
                )
                expected = output_dir / (input_path.stem + ".txt")
                if result.returncode == 0 and expected.exists():
                    return True, f"Converted: {expected}"

                last_error = (result.stderr.decode() if result.stderr else "Unknown error")
            except subprocess.TimeoutExpired:
                last_error = "Conversion timed out after 120 s"
            except Exception as e:
                last_error = str(e)

            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
    finally:
        if cleanup_profile:
            shutil.rmtree(profile_dir, ignore_errors=True)

    return False, f"LibreOffice conversion failed after {max_retries} attempts: {last_error}"


# ---------------------------------------------------------------------------
# DOCX
# ---------------------------------------------------------------------------

def convert_docx_to_txt(input_path: Path, output_dir: Path) -> Tuple[bool, str]:
    """Extract text from a DOCX using python-docx (paragraphs + tables)."""
    output_path = output_dir / (input_path.stem + ".txt")
    try:
        doc = Document(input_path)
        parts: list[str] = [p.text for p in doc.paragraphs]
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    t = cell.text.strip()
                    if t:
                        parts.append(t)
        if not parts:
            return False, "No text extracted from DOCX"
        output_path.write_text("\n".join(parts), encoding="utf-8")
        return True, f"Converted: {output_path}"
    except Exception as e:
        return False, f"DOCX extraction failed: {e}"


# ---------------------------------------------------------------------------
# PDF (pdfplumber → OCR fallback)
# ---------------------------------------------------------------------------

_CHARS_PER_PAGE_THRESHOLD = 200

# pdfplumber's default extract_text() reads a page in one top-to-bottom sweep
# across the full page width. On a two-column layout this interleaves the two
# columns word-by-word/line-by-line instead of reading one column fully
# before the other, scrambling sentences that span the interleaving.
# _detect_column_gutter/_extract_page_text_column_aware below detect a
# genuine column gutter (a consistent vertical gap in word positions, not
# just normal ragged-right word spacing) and, only when confident, read the
# left column fully then the right column fully instead.

_MIN_WORDS_FOR_COLUMN_DETECTION = 40
_LINE_CLUSTER_TOLERANCE_PT = 3
_MIN_RAW_GAP_PT = 12  # per-line candidate gap threshold (normal word spacing is ~2-3pt)
_MIN_FINAL_GUTTER_WIDTH_PT = 8  # width required after intersecting the gap across lines
_GUTTER_ZONE_FRACTION = (0.30, 0.70)  # gutter midpoint must fall in the page's middle band
_MIN_GAP_HEIGHT_FRACTION = 0.55  # gap's vertical span / page height
# lines-with-the-gap / lines-within-that-vertical-span. Calibrated against a
# corpus of real research-paper PDFs (not guessed): a genuine two-column page
# -- body prose beside a References-list-style column, or any page with
# short/ragged lines that don't all reach the gutter -- was found with real
# ratios as low as 0.203, and every low-ratio candidate found across the
# corpus was a real two-column page, never a false positive. This metric
# doesn't actually discriminate tables from text (see
# _MIN_AVG_WORDS_PER_LINE_PER_SIDE below for that); false positives are
# guarded against by _MIN_LINES_WITH_GAP, _MIN_GAP_HEIGHT_FRACTION,
# _MIN_SIDE_WORD_FRACTION, and the full-width-straddle veto instead.
_MIN_GAP_LINE_FRACTION = 0.15
_MIN_LINES_WITH_GAP = 8
_MIN_SIDE_WORD_FRACTION = 0.15  # each side must hold a real share of the page's words
# guards against two-column numeric tables (~1 word/line/side). Calibrated
# against the same real-paper corpus: genuine two-column text pages never
# dropped below ~2.4 words/line/side even on their sparsest (References-list)
# column, comfortably above a numeric table's ~1.
_MIN_AVG_WORDS_PER_LINE_PER_SIDE = 2

# ---------------------------------------------------------------------------
# Missing-literal-space-glyph word tolerance
# ---------------------------------------------------------------------------
# Some PDF producers (observed on certain Cell Press/Elsevier exports) place
# words via pure glyph positioning with no literal space character in the
# content stream at all. pdfplumber then decides word boundaries purely from
# the geometric gap between adjacent characters vs. x_tolerance (default 3pt,
# used everywhere in this file); when such a PDF's true inter-word gap is
# smaller than that, words are silently glued together
# ("wordsgluedlikethis"), which breaks anything downstream that needs the
# real source text (e.g. matching an excerpt back to its source for
# citation grounding).
#
# _derive_word_gap_threshold below detects this -- via two independent,
# confident, measured gates, exactly the same philosophy as
# _detect_column_gutter -- and derives a page-specific x_tolerance from that
# page's own character-gap distribution, instead of guessing a single global
# constant. Every gate is calibrated against the one confirmed real repro
# (kerning gaps ~0.0pt, real inter-word gap 1.2pt); treat as a starting point
# pending a larger corpus of real "glued" PDFs.
_MIN_CHARS_FOR_GAP_ANALYSIS = 200  # mirror _CHARS_PER_PAGE_THRESHOLD -- too little text, no reliable signal
# Real glued pages aren't uniformly glued -- parenthetical citations,
# accession-number lists, etc. keep literal spaces even on an otherwise-glued
# page. Measured directly against the real repro document's glued
# "Experimental Procedures" section: space characters are ~1.4-1.7% of text
# there, not 0%. Calibrated with margin above that (still ~3x) while staying
# far below normal English prose's ~15-17% space ratio, so
# _MIN_AVG_WORD_LEN_FOR_GLUING_SIGNAL remains the gate that protects normal
# pages from a false trigger.
_MAX_SPACE_CHAR_FRACTION = 0.05  # pages already using real space glyphs throughout must be left untouched
_MIN_AVG_WORD_LEN_FOR_GLUING_SIGNAL = 15  # default-tolerance "words" averaging longer than this look glued (English mean word length ~4.7 chars)
_MIN_GAP_SAMPLES = 60  # need enough adjacent-char gaps for the distribution to mean anything
_MIN_GAP_CLUSTER_FRACTION = 0.05  # each side of a candidate split must hold a real share of the gaps, not a lone outlier
_MAX_UPPER_GAP_CLUSTER_FRACTION = 0.5  # word-boundary gaps should be a minority of all adjacent-char gaps in real prose
_MIN_GAP_JUMP_PT = 0.4  # the jump between the two populations must be non-trivial in absolute terms
_MIN_GAP_JUMP_TO_SPREAD_RATIO = 3.0  # the jump must dwarf the lower (kerning) cluster's own spread, or it's not a real bimodal split


def _detect_column_gutter(
    page: "pdfplumber.page.Page", x_tolerance: float = DEFAULT_X_TOLERANCE
) -> Optional[Tuple[float, float]]:
    """Return (gutter_x0, gutter_x1) if *page* has a confident two-column
    layout, else None. A pure function of page geometry -- no I/O.
    """
    if getattr(page, "rotation", 0) not in (0, None):
        return None  # rotated pages: x/top axes don't map to visual columns reliably
    if not page.width or not page.height:
        return None  # degenerate/malformed page geometry -- nothing safe to measure

    words = [w for w in page.extract_words(x_tolerance=x_tolerance) if w.get("upright", True)]
    if len(words) < _MIN_WORDS_FOR_COLUMN_DETECTION:
        return None

    lines = cluster_objects(words, "top", _LINE_CLUSTER_TOLERANCE_PT)
    lines = [sorted(line, key=lambda w: w["x0"]) for line in lines]
    line_spans = [(min(w["top"] for w in line), max(w["bottom"] for w in line), line) for line in lines]

    # Per-line candidate gaps wide enough, and roughly centered, to plausibly
    # be a column gutter rather than ordinary word spacing.
    candidates = []  # (x0, x1, top, bottom)
    for top, bottom, line in line_spans:
        for a, b in zip(line, line[1:]):
            gap = b["x0"] - a["x1"]
            if gap < _MIN_RAW_GAP_PT:
                continue
            mid_frac = ((a["x1"] + b["x0"]) / 2) / page.width
            if _GUTTER_ZONE_FRACTION[0] <= mid_frac <= _GUTTER_ZONE_FRACTION[1]:
                candidates.append((a["x1"], b["x0"], top, bottom))

    if not candidates:
        return None

    # Merge candidates whose x-ranges mutually intersect, tracking the running
    # intersection -- this is what narrows ragged-edge per-line gaps down to
    # the one true, tight gutter shared across most lines.
    candidates.sort(key=lambda c: c[0])
    groups: list[dict] = []
    for c in candidates:
        merged = False
        for g in groups:
            new_x0, new_x1 = max(g["x0"], c[0]), min(g["x1"], c[1])
            if new_x1 - new_x0 >= _MIN_FINAL_GUTTER_WIDTH_PT:
                g.update(x0=new_x0, x1=new_x1, top=min(g["top"], c[2]),
                         bottom=max(g["bottom"], c[3]), count=g["count"] + 1)
                merged = True
                break
        if not merged:
            groups.append({"x0": c[0], "x1": c[1], "top": c[2], "bottom": c[3], "count": 1})

    if not groups:
        return None
    best = max(groups, key=lambda g: g["count"])

    if best["x1"] - best["x0"] < _MIN_FINAL_GUTTER_WIDTH_PT:
        return None
    if best["count"] < _MIN_LINES_WITH_GAP:
        return None
    if (best["bottom"] - best["top"]) / page.height < _MIN_GAP_HEIGHT_FRACTION:
        return None

    lines_in_span = [ls for ls in line_spans
                      if ls[0] >= best["top"] - 1 and ls[1] <= best["bottom"] + 1]
    if not lines_in_span or best["count"] / len(lines_in_span) < _MIN_GAP_LINE_FRACTION:
        return None

    mid = (best["x0"] + best["x1"]) / 2

    def _in_span(w: dict) -> bool:
        return w["top"] >= best["top"] - 1 and w["bottom"] <= best["bottom"] + 1

    left_words = [w for w in words if w["x1"] <= mid and _in_span(w)]
    right_words = [w for w in words if w["x0"] >= mid and _in_span(w)]
    total = len(left_words) + len(right_words)
    if total == 0:
        return None
    if (len(left_words) / total < _MIN_SIDE_WORD_FRACTION
            or len(right_words) / total < _MIN_SIDE_WORD_FRACTION):
        return None

    # Guard against two-column numeric tables (a wide gap but few words per
    # line on each side).
    n_lines = max(1, best["count"])
    if (len(left_words) / n_lines < _MIN_AVG_WORDS_PER_LINE_PER_SIDE
            or len(right_words) / n_lines < _MIN_AVG_WORDS_PER_LINE_PER_SIDE):
        return None

    # _MIN_GAP_LINE_FRACTION tolerates some lines within the gutter's
    # vertical span not having the gap -- but a line that's genuinely
    # full-width (a caption, table row, or equation spanning the whole
    # page) has a word straddling `mid` itself. within_bbox only keeps
    # objects FULLY inside a bbox, so such a word would fall in neither the
    # left nor the right crop and be silently dropped from the output. Bail
    # out to today's plain extract_text() rather than lose text.
    if any(w["x0"] < mid < w["x1"] for w in words if _in_span(w)):
        return None

    return best["x0"], best["x1"]


def _page_needs_gap_derived_tolerance(page: "pdfplumber.page.Page") -> bool:
    """True only when *page* independently shows both: (a) essentially no
    literal space glyphs despite substantial text, and (b) today's
    default-tolerance extraction already produces suspiciously long "words"
    (i.e. words are actually glued runs). Both must hold so a page that's
    merely low on spaces for a legitimate reason (e.g. a dense table of
    identifiers) doesn't get an unnecessary override --
    _find_bimodal_gap_threshold is a further, independent gate on top.
    """
    if getattr(page, "rotation", 0) not in (0, None):
        return False  # rotated pages: same axis-reliability caveat as _detect_column_gutter
    chars = getattr(page, "chars", None)
    if not chars:
        return False
    text_chars = [c for c in chars if c.get("upright", True)]
    if len(text_chars) < _MIN_CHARS_FOR_GAP_ANALYSIS:
        return False
    space_count = sum(1 for c in text_chars if c["text"].isspace())
    if space_count / len(text_chars) > _MAX_SPACE_CHAR_FRACTION:
        return False
    words = page.extract_words()
    if not words:
        return False
    avg_word_len = sum(len(w["text"]) for w in words) / len(words)
    return avg_word_len >= _MIN_AVG_WORD_LEN_FOR_GLUING_SIGNAL


def _collect_adjacent_char_gaps(page: "pdfplumber.page.Page") -> List[float]:
    """Adjacent same-line, non-space character x-gaps on *page*. Pure
    function of page.chars -- no I/O.
    """
    chars = [c for c in page.chars if c.get("upright", True) and not c["text"].isspace()]
    if len(chars) < 2:
        return []
    lines = cluster_objects(chars, "top", _LINE_CLUSTER_TOLERANCE_PT)
    gaps: list[float] = []
    for line in lines:
        line_sorted = sorted(line, key=lambda c: c["x0"])
        for a, b in zip(line_sorted, line_sorted[1:]):
            gaps.append(max(b["x0"] - a["x1"], 0.0))
    return gaps


def _find_bimodal_gap_threshold(gaps: List[float]) -> Optional[float]:
    """Look for a genuine two-population split in *gaps* -- a tight
    "kerning" population near 0 and a separate, larger "word-boundary"
    population -- and return a threshold between them, biased toward the
    lower cluster. Returns None when no confident split exists (e.g. a
    continuous/unimodal spread, as on normally-kerned or letter-spaced
    text).

    Scans candidate split indices in increasing order and returns the
    FIRST one that clears both confidence checks, rather than the single
    largest jump anywhere in the valid range. Taking the largest jump is
    wrong whenever a third population of legitimately wider (but still
    non-word-boundary) gaps -- e.g. around superscripts/subscripts or
    numbering runs -- reaches _MIN_GAP_CLUSTER_FRACTION of samples: the
    biggest jump then sits between the real word-gap cluster and that third
    population, not between kerning and real word-gaps, which silently
    picks a much-too-large threshold. Scanning left-to-right instead always
    finds the boundary of the tight low kerning cluster first.
    """
    if len(gaps) < _MIN_GAP_SAMPLES:
        return None
    s = sorted(gaps)
    n = len(s)
    min_side = max(1, int(n * _MIN_GAP_CLUSTER_FRACTION))
    max_upper = int(n * _MAX_UPPER_GAP_CLUSTER_FRACTION)

    for i in range(min_side, n - min_side + 1):
        if (n - i) > max_upper:
            continue
        jump = s[i] - s[i - 1]
        if jump < _MIN_GAP_JUMP_PT:
            continue
        lower_spread = s[i - 1] - s[0]
        if jump < _MIN_GAP_JUMP_TO_SPREAD_RATIO * max(lower_spread, 0.05):
            continue
        return s[i - 1] + jump / 2

    return None


def _derive_word_gap_threshold(page: "pdfplumber.page.Page") -> Optional[float]:
    """Page-specific x_tolerance derived from *page*'s own char-gap
    distribution, or None when pdfplumber's DEFAULT_X_TOLERANCE should be
    used unchanged (the case for the overwhelming majority of pages).
    """
    try:
        if not _page_needs_gap_derived_tolerance(page):
            return None
        threshold = _find_bimodal_gap_threshold(_collect_adjacent_char_gaps(page))
        if threshold is None:
            return None
        # Only ever tighten, never loosen, relative to pdfplumber's default --
        # this fix must never merge words on a page that wasn't glued.
        threshold = min(threshold, DEFAULT_X_TOLERANCE)
        logger.debug("Derived x_tolerance=%.2f for page with no literal space glyphs", threshold)
        return threshold
    except Exception:
        logger.debug("Word-gap tolerance derivation failed, using pdfplumber's default", exc_info=True)
        return None


def _effective_x_tolerance(page: "pdfplumber.page.Page") -> float:
    threshold = _derive_word_gap_threshold(page)
    return threshold if threshold is not None else DEFAULT_X_TOLERANCE


def _extract_page_text_column_aware(page: "pdfplumber.page.Page") -> str:
    """Extract a page's text, reading a confidently-detected two-column page
    column-major (full left column, then full right column) instead of
    pdfplumber's default row-major sweep. Single-column pages -- and any page
    where no confident column gutter is found -- are extracted exactly as
    `page.extract_text()` would today.

    Column detection and splitting run on arbitrary, uncontrolled real-world
    PDFs (malformed fonts/encodings, degenerate geometry); any unexpected
    failure there falls back to today's plain extraction rather than
    failing the whole document's conversion.
    """
    try:
        x_tolerance = _effective_x_tolerance(page)
        gutter = _detect_column_gutter(page, x_tolerance=x_tolerance)
        if gutter is None:
            return page.extract_text(x_tolerance=x_tolerance) or ""

        mid = (gutter[0] + gutter[1]) / 2
        left = page.within_bbox((0, 0, mid, page.height)).extract_text(x_tolerance=x_tolerance) or ""
        right = page.within_bbox((mid, 0, page.width, page.height)).extract_text(x_tolerance=x_tolerance) or ""

        # Cheap RTL accommodation: if most words on the page are RTL-directed,
        # the visually-first column is the right one.
        words = page.extract_words(x_tolerance=x_tolerance)
        if words and sum(1 for w in words if w.get("direction") == "rtl") / len(words) > 0.5:
            left, right = right, left

        return "\n\n".join(t for t in (left, right) if t)
    except Exception:
        # Silent-but-observable: falling back here is always safe (today's
        # exact behavior), but silently swallowing every exception with no
        # trace at all would let a real regression in the detection logic
        # run at effectively 100% failure rate, forever, with no signal
        # that the column-aware path is even being exercised.
        logger.debug("Column-aware extraction failed, falling back to plain extract_text()", exc_info=True)
        return page.extract_text() or ""


def convert_pdf_to_txt(input_path: Path, output_dir: Path) -> Tuple[bool, str]:
    """Convert a PDF to text via pdfplumber; fall back to OCR for scanned pages."""
    output_path = output_dir / (input_path.stem + ".txt")

    with pdfplumber.open(input_path) as pdf:
        page_count = max(1, len(pdf.pages))
        text_parts = [_extract_page_text_column_aware(p) for p in pdf.pages]

    total_chars = sum(len(t) for t in text_parts)

    if total_chars / page_count >= _CHARS_PER_PAGE_THRESHOLD:
        output_path.write_text(
            "\n\n".join(t for t in text_parts if t), encoding="utf-8",
        )
        return True, f"Converted: {output_path}"

    # Scanned PDF — OCR page-by-page to avoid loading all images into RAM.
    with tempfile.TemporaryDirectory(prefix="ocr_pages_") as tmpdir:
        try:
            page_images = convert_from_path(
                str(input_path),
                dpi=300,
                output_folder=tmpdir,
                fmt="png",
            )
        except Exception as e:
            return False, f"pdf2image failed: {e}"

        ocr_parts: list[str] = []
        for img in page_images:
            text = pytesseract.image_to_string(img)
            if text.strip():
                ocr_parts.append(text)
            # Let each PIL image be freed before the next page is processed.
            img.close()

    if not ocr_parts:
        return False, "No text extracted from PDF (tried pdfplumber and OCR)"

    output_path.write_text("\n\n".join(ocr_parts), encoding="utf-8")
    return True, f"Converted (OCR): {output_path}"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def convert_file(
    input_path: Path,
    output_dir: Path,
    soffice_path: str,
    worker_id: Optional[str] = None,
) -> Tuple[bool, str]:
    """Convert a single file to text based on its extension.

    For DOCX, tries python-docx first and falls back to LibreOffice.
    """
    ext = input_path.suffix.lower()

    if ext == ".docx":
        ok, msg = convert_docx_to_txt(input_path, output_dir)
        if ok:
            return ok, msg
        logger.debug("python-docx failed for %s, falling back to LibreOffice", input_path.name)
        return _run_libreoffice(input_path, output_dir, soffice_path)

    if ext in {".doc", ".rtf"}:
        return _run_libreoffice(input_path, output_dir, soffice_path)

    if ext == PDF_EXTENSION:
        return convert_pdf_to_txt(input_path, output_dir)

    return False, f"Unsupported file type: {ext}"
