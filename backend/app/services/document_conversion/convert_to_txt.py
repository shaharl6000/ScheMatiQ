"""Multi-format document to plain-text conversion.

Converts DOCX, DOC, RTF, and PDF files to UTF-8 text.

- DOCX: python-docx first, LibreOffice headless fallback
- DOC / RTF: LibreOffice headless
- PDF: pdfplumber first (in PyMuPDF's line order), OCR fallback (pdf2image + tesseract)

Vendored from Legal_Schema_Generator; adapted for use as a library inside
the ScheMatiQ backend (no CLI entry point, no process-wide signal handlers,
structured logging instead of print()).
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional, Tuple

import pdfplumber
import pytesseract
from docx import Document
from pdf2image import convert_from_path
from pdfplumber.utils import DEFAULT_X_TOLERANCE, cluster_objects, dedupe_chars, extract_text

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
# Many PDF producers place words via pure glyph positioning with no literal
# space character in the content stream at all. pdfplumber then decides word
# boundaries purely from the geometric gap between adjacent characters vs.
# x_tolerance (default 3pt, absolute); when such a PDF's true inter-word gap
# is smaller than that, words are silently glued together
# ("wordsgluedlikethis"), which breaks anything downstream that needs the
# real source text (e.g. matching an excerpt back to its source for citation
# grounding).
#
# Measured on 80 real papers (268 such pages), gaps scale with the font, so
# they are measured in em (gap / font size): within a word the gap is at most
# ~0.04 em, between words at least ~0.16 em, and almost nothing falls in
# between, whatever the font size. Word gaps of 1.6-2.8pt (a 6-10pt font)
# are below the absolute 3pt default, which glued 317 of 448 papers. So a
# page with no space glyphs splits words at 0.10 em; a page that has them
# keeps pdfplumber's default unchanged.
_MIN_CHARS_FOR_GAP_ANALYSIS = 200  # mirror _CHARS_PER_PAGE_THRESHOLD -- too little text, no reliable signal
# Real glued pages aren't uniformly glued -- parenthetical citations,
# accession-number lists, etc. keep literal spaces even on an otherwise-glued
# page (~1.4-1.7% of characters measured on one), while normal English prose
# is ~15-17% spaces.
_MAX_SPACE_CHAR_FRACTION = 0.05
_WORD_GAP_EM = 0.10


def _detect_column_gutter(
    page: "pdfplumber.page.Page",
    x_tolerance: float = DEFAULT_X_TOLERANCE,
    x_tolerance_ratio: Optional[float] = None,
) -> Optional[Tuple[float, float]]:
    """Return (gutter_x0, gutter_x1) if *page* has a confident two-column
    layout, else None. A pure function of page geometry -- no I/O.
    """
    if getattr(page, "rotation", 0) not in (0, None):
        return None  # rotated pages: x/top axes don't map to visual columns reliably
    if not page.width or not page.height:
        return None  # degenerate/malformed page geometry -- nothing safe to measure

    words = [w for w in page.extract_words(x_tolerance=x_tolerance, x_tolerance_ratio=x_tolerance_ratio)
             if w.get("upright", True)]
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


def _page_lacks_space_glyphs(page: "pdfplumber.page.Page") -> bool:
    """True when *page* has substantial text but essentially no literal
    space glyphs, so its word boundaries come from geometry alone."""
    if getattr(page, "rotation", 0) not in (0, None):
        return False  # rotated pages: same axis-reliability caveat as _detect_column_gutter
    text_chars = [c for c in (getattr(page, "chars", None) or []) if c.get("upright", True)]
    if len(text_chars) < _MIN_CHARS_FOR_GAP_ANALYSIS:
        return False
    space_count = sum(1 for c in text_chars if c["text"].isspace())
    return space_count / len(text_chars) <= _MAX_SPACE_CHAR_FRACTION


def _word_tolerance(page: "pdfplumber.page.Page") -> dict:
    """pdfplumber word-splitting arguments for *page*: 0.10 em on a page
    with no space glyphs, pdfplumber's default otherwise."""
    try:
        if _page_lacks_space_glyphs(page):
            return {"x_tolerance": DEFAULT_X_TOLERANCE, "x_tolerance_ratio": _WORD_GAP_EM}
    except Exception:
        logger.debug("Space-glyph check failed, using pdfplumber's default", exc_info=True)
    return {"x_tolerance": DEFAULT_X_TOLERANCE, "x_tolerance_ratio": None}


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
        tolerance = _word_tolerance(page)
        gutter = _detect_column_gutter(page, **tolerance)
        if gutter is None:
            return page.extract_text(**tolerance) or ""

        mid = (gutter[0] + gutter[1]) / 2
        # The page box need not start at (0, 0) (seen: (9, -9, 603, 774));
        # a crop reaching outside it raises, which used to silently drop the
        # page to the plain fallback below.
        x0, top, x1, bottom = page.bbox
        left = page.within_bbox((x0, top, mid, bottom)).extract_text(**tolerance) or ""
        right = page.within_bbox((mid, top, x1, bottom)).extract_text(**tolerance) or ""

        # Cheap RTL accommodation: if most words on the page are RTL-directed,
        # the visually-first column is the right one.
        words = page.extract_words(**tolerance)
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
        try:
            return page.extract_text(**_word_tolerance(page)) or ""
        except Exception:
            return page.extract_text() or ""


# ---------------------------------------------------------------------------
# Reading order from the PDF's own text lines
# ---------------------------------------------------------------------------
# The gutter search above needs one vertical gap through most of the page, so
# it misses the columns of any page with a full-width title, figure or table:
# of 4,636 pages in a 415-paper corpus, 71% were read straight across both
# columns ("Php4 acts as a neg- oxidative DNA damage"). PyMuPDF groups a
# page's text into blocks and lines in the order the PDF draws them, which
# for typeset papers is column by column. Its characters are not used,
# though: it decodes fonts without a Unicode map silently wrong ("importin
# a/b" for α/β, control characters for Greek), where pdfminer with the
# glyph-name fallbacks below reads them right. So PyMuPDF supplies only the
# lines and their order, and each line's text is built from the pdfplumber
# characters inside it. Building text line by line also keeps apart two
# lines that overlap vertically, which pdfplumber's own line grouping merges
# letter by letter ("phosphthorey lUatnediveNrrsfi2tyY").

_ALIGN_WINDOW = 12  # characters per run used to align the two libraries' coordinates
_MIN_ALIGNED_RUNS = 3
_MAX_ALIGN_SPREAD_PT = 1.5
# Upright characters no PyMuPDF line covers. Above this share the lines do not
# describe the page, and the page is read the old way.
_MAX_UNPLACED_FRACTION = 0.02
_LINE_SLACK_PT = 1.0
# A line this long with no space glyph is split at 0.10 em (_WORD_GAP_EM);
# a shorter one ("p-S74-Cdc6", "N1") may well be a single word.
_MIN_CHARS_FOR_LINE_GAP_ANALYSIS = 15

try:
    import pymupdf
except ImportError:  # optional: without it, every page is read the old way
    pymupdf = None

# MuPDF is not thread-safe, not even for separate documents, and conversions
# run in worker threads (asyncio.to_thread), so every PyMuPDF call -- here and
# in pdf_utils -- is made under this lock.
PYMUPDF_LOCK = threading.Lock()


def _open_layout_document(input_path: Path):
    """The PDF opened with PyMuPDF for its line layout, or None."""
    if pymupdf is None:
        return None
    try:
        with PYMUPDF_LOCK:
            return pymupdf.open(input_path)
    except Exception:
        logger.debug("PyMuPDF could not open %s; using the gutter search only", input_path.name, exc_info=True)
        return None


def _layout_page_count(layout_doc) -> Optional[int]:
    try:
        with PYMUPDF_LOCK:
            return layout_doc.page_count
    except Exception:
        return None


def _close_layout_document(layout_doc) -> None:
    """Close the PyMuPDF document; never raises (it runs in a finally, where an
    error would hide the conversion's own result or exception)."""
    if layout_doc is None:
        return
    try:
        with PYMUPDF_LOCK:
            layout_doc.close()
    except Exception:
        logger.debug("PyMuPDF could not close a document", exc_info=True)


def _page_line_layout(layout_doc, index: int) -> Optional[Tuple[list, list]]:
    """PyMuPDF's upright text lines on page *index* -- (line bboxes in reading
    order, their non-space characters) as plain data -- or None."""
    try:
        with PYMUPDF_LOCK:
            layout_page = layout_doc[index]
            if layout_page.rotation:
                return None
            blocks = layout_page.get_text("rawdict")["blocks"]
    except Exception:
        logger.debug("PyMuPDF could not lay out page %d", index + 1, exc_info=True)
        return None
    lines, layout_chars = [], []
    for block in blocks:
        if block.get("type") != 0:
            continue
        for line in block["lines"]:
            cos, sin = line["dir"]
            if cos <= 0 or abs(sin) > 1e-3:
                continue  # rotated text: left to the leftover pass of _extract_page_text_by_lines
            lines.append(line["bbox"])
            layout_chars.extend(ch for span in line["spans"] for ch in span["chars"] if not ch["c"].isspace())
    return lines, layout_chars


def _coordinate_offset(chars: list, layout_chars: list) -> Optional[Tuple[float, float]]:
    """(dx, dy) that turns a PyMuPDF position into pdfplumber's, or None.

    The two measure from different page boxes (seen: 9pt apart, either way
    vertically), so the offset is read off character runs both libraries
    decode the same, rather than derived from the boxes. A run that occurs
    more than once (repeated text) votes for each occurrence; only the true
    offset collects a vote from most runs.
    """
    def joined(items, key):
        # A character's text can be longer than one letter ("ﬃ", "(cid:12)").
        owner = [k for k, item in enumerate(items) for _ in item[key]]
        return "".join(item[key] for item in items), owner

    text, text_owner = joined(chars, "text")
    layout_text, layout_owner = joined(layout_chars, "c")
    step = max(_ALIGN_WINDOW, len(layout_text) // 30)
    candidates = []  # one list of (dx, dy) per run
    for i in range(0, len(layout_text) - _ALIGN_WINDOW, step):
        run = layout_text[i:i + _ALIGN_WINDOW]
        b = layout_chars[layout_owner[i]]
        found = []
        j = text.find(run)
        while j >= 0 and len(found) < 50:
            a = chars[text_owner[j]]
            found.append((a["x0"] - b["bbox"][0], (a["top"] + a["bottom"]) / 2 - (b["bbox"][1] + b["bbox"][3]) / 2))
            j = text.find(run, j + 1)
        if found:
            candidates.append(found)
    best, best_votes = None, 0
    for found in candidates:
        for dx, dy in found:
            votes = sum(any(abs(dx - x) <= _MAX_ALIGN_SPREAD_PT and abs(dy - y) <= _MAX_ALIGN_SPREAD_PT for x, y in other)
                        for other in candidates)
            if votes > best_votes:
                best, best_votes = (dx, dy), votes
    if best is None or best_votes < max(_MIN_ALIGNED_RUNS, len(candidates) // 2):
        return None
    return best


def _line_word_tolerance(line_chars: list, page_tolerance: dict) -> dict:
    """Word-splitting arguments for one line: 0.10 em when the line has no
    space glyphs, else the page's.

    The page-level check misses a page whose body text has no space glyphs
    but whose figure labels are full of them (Cdc6, p. 8: 16% spaces on the
    page, 0% in every body and caption line), so its 6-8pt text, with word
    gaps of ~0.25 em but under the 3pt default, came out glued
    ("thecytoplasm.Cytoplasmiclocation"). On 415 papers this rule split 367
    lines and cut no word in two.
    """
    if len(line_chars) >= _MIN_CHARS_FOR_LINE_GAP_ANALYSIS and not any(c["text"].isspace() for c in line_chars):
        return {**page_tolerance, "x_tolerance_ratio": _WORD_GAP_EM}
    return page_tolerance


def _extract_page_text_by_lines(page: "pdfplumber.page.Page", line_layout: Tuple[list, list]) -> Optional[str]:
    """The page's text in PyMuPDF's line order (from _page_line_layout), built
    from pdfplumber's characters; None when the lines cannot be trusted to
    cover the page."""
    if getattr(page, "rotation", 0) not in (0, None):
        return None
    lines, layout_chars = line_layout
    upright = [c for c in page.chars if c.get("upright", True)]
    visible = [c for c in upright if not c["text"].isspace()]
    if not visible:
        return None
    offset = _coordinate_offset(visible, layout_chars)
    if offset is None:
        return None
    dx, dy = offset
    lines = [(x0 + dx, y0 + dy, x1 + dx, y1 + dy) for x0, y0, x1, y1 in lines]

    by_row: dict = {}
    for k, (_, y0, _, y1) in enumerate(lines):
        for row in range(int(y0 - _LINE_SLACK_PT), int(y1 + _LINE_SLACK_PT) + 1):
            by_row.setdefault(row, []).append(k)
    placed: list[list] = [[] for _ in lines]
    leftover = [c for c in page.chars if not c.get("upright", True)]
    unplaced = 0
    for c in upright:
        cx, cy = (c["x0"] + c["x1"]) / 2, (c["top"] + c["bottom"]) / 2
        candidates = [k for k in by_row.get(int(cy), ())
                      if lines[k][0] - _LINE_SLACK_PT <= cx <= lines[k][2] + _LINE_SLACK_PT
                      and lines[k][1] - _LINE_SLACK_PT <= cy <= lines[k][3] + _LINE_SLACK_PT]
        if candidates:
            placed[min(candidates, key=lambda k: abs((lines[k][1] + lines[k][3]) / 2 - cy))].append(c)
        elif not c["text"].isspace():
            leftover.append(c)
            unplaced += 1
    if unplaced / len(visible) > _MAX_UNPLACED_FRACTION:
        return None

    tolerance = _word_tolerance(page)
    out = []
    for line_chars in placed:
        if line_chars:
            # Figure labels are often drawn twice in place; one line built from
            # both copies interleaves them ("pp--SS7744--CCddcc66"). The
            # tolerance stays under a narrow letter's width ("ll" at 5pt is
            # ~1.1pt apart), so real double letters are kept.
            line_chars = dedupe_chars(line_chars, tolerance=0.5)
            text = extract_text(line_chars, **_line_word_tolerance(line_chars, tolerance)).replace("\n", " ").strip()
            if text:
                out.append(text)
    if leftover:
        # Rotated or unplaced text (a sidebar, a stray label): kept, at the end.
        text = extract_text(leftover, **tolerance).strip()
        if text:
            out.append(text)
    return "\n".join(out)


def _extract_page_text(page: "pdfplumber.page.Page", line_layout: Optional[Tuple[list, list]] = None) -> str:
    """A page's text: in the PDF's own line order when PyMuPDF's layout covers
    the page, otherwise through the gutter search."""
    try:
        _decode_symbol_font_chars(page)
    except Exception:
        logger.debug("Symbol font decoding failed", exc_info=True)
    if line_layout is not None:
        try:
            text = _extract_page_text_by_lines(page, line_layout)
            if text is not None:
                return text
        except Exception:
            logger.debug("Line-order extraction failed, using the gutter search", exc_info=True)
    return _extract_page_text_column_aware(page)


# ---------------------------------------------------------------------------
# Symbol font characters
# ---------------------------------------------------------------------------
# A Symbol / SymbolGreek font with a ToUnicode map often maps its codes into
# the Private Use Area at U+F000 + code (Microsoft's convention), so "10 μm"
# came out as "10 U+F06Dm" and "α-actinin" as "U+F061-Actinin". Read such a
# character by Adobe's Symbol encoding instead. Codes for bracket and arrow
# pieces are left out.
_SYMBOL_ENCODING = {
    **{code: chr(code) for code in range(0x20, 0x7F)},
    0x22: "∀", 0x24: "∃", 0x27: "∋", 0x2A: "∗", 0x2D: "−", 0x40: "≅",
    **dict(zip(range(0x41, 0x5B), "ΑΒΧΔΕΦΓΗΙϑΚΛΜΝΟΠΘΡΣΤΥςΩΞΨΖ")),
    0x5C: "∴", 0x5E: "⊥",
    **dict(zip(range(0x61, 0x7B), "αβχδεφγηιϕκλμνοπθρστυϖωξψζ")),
    0x7E: "∼",
    0xA1: "ϒ", 0xA2: "′", 0xA3: "≤", 0xA4: "⁄", 0xA5: "∞", 0xA6: "ƒ", 0xA7: "♣", 0xA8: "♦", 0xA9: "♥",
    0xAA: "♠", 0xAB: "↔", 0xAC: "←", 0xAD: "↑", 0xAE: "→", 0xAF: "↓", 0xB0: "°", 0xB1: "±", 0xB2: "″",
    0xB3: "≥", 0xB4: "×", 0xB5: "∝", 0xB6: "∂", 0xB7: "•", 0xB8: "÷", 0xB9: "≠", 0xBA: "≡", 0xBB: "≈",
    0xBC: "…", 0xC0: "ℵ", 0xC4: "⊗", 0xC5: "⊕", 0xC6: "∅", 0xC7: "∩", 0xC8: "∪", 0xC9: "⊃", 0xCA: "⊇",
    0xCC: "⊂", 0xCD: "⊆", 0xCE: "∈", 0xCF: "∉", 0xD0: "∠", 0xD1: "∇", 0xD2: "®", 0xD3: "©", 0xD4: "™",
    0xD5: "∏", 0xD6: "√", 0xD7: "⋅", 0xD8: "¬", 0xD9: "∧", 0xDA: "∨", 0xDB: "⇔", 0xDC: "⇐", 0xDD: "⇑",
    0xDE: "⇒", 0xDF: "⇓", 0xE0: "◊", 0xE1: "〈", 0xE2: "®", 0xE3: "©", 0xE4: "™", 0xE5: "∑", 0xF1: "〉",
    0xF2: "∫",
}
_SYMBOL_FONT = re.compile(r"(?:^|\+)Symbol", re.IGNORECASE)


def _decode_symbol_font_chars(page: "pdfplumber.page.Page") -> None:
    """Replace a Symbol font's Private Use Area characters on *page* in place
    (pdfplumber caches the page's chars, so every later read sees them)."""
    for c in page.chars:
        text = c["text"]
        if len(text) == 1 and 0xF020 <= ord(text) <= 0xF0FF and _SYMBOL_FONT.search(c.get("fontname", "")):
            c["text"] = _SYMBOL_ENCODING.get(ord(text) - 0xF000, text)


# ---------------------------------------------------------------------------
# Unreadable text layer
# ---------------------------------------------------------------------------
# Some PDFs (typically older papers, e.g. made by Acrobat Distiller 4) embed
# subset fonts with a private encoding and no ToUnicode map. The page renders
# fine, but its text layer has no recoverable characters: pdfplumber returns
# "(cid:N)" placeholders, mixed with arbitrary punctuation where letters
# should be. Such a page looks full of text to the chars-per-page check, so it
# never reached OCR and the garbage was passed on as the paper's text.
# A normal paper has at most a handful of (cid:N) glyphs (an odd symbol), far
# below this fraction; an unmapped-font paper is mostly (cid:N).
#
# The common case is recoverable without OCR: such fonts name each glyph
# "C<n>" in their /Differences encoding, where n is the character's code in
# Adobe StandardEncoding ("C86" is "V", "C174" the "fi" ligature). pdfminer
# does not know these names and gives up; _install_glyph_name_fallbacks
# teaches it to read them. Whatever stays unreadable goes to OCR.
#
# The same gap loses Greek letters and math symbols in many otherwise
# readable papers: the Linotype Mathematical Pi / Universal Greek fonts used
# by several journals (JBC, MCB, PNAS) name their glyphs "H<n>", so "IκB"
# came out as "I(cid:1)B". _MATH_PI_GLYPHS maps the names seen in this
# corpus. Each entry was read off its context in real papers (H9253 in
# "[γ-32P]ATP", H11006 in "mean ± SD"); the Greek letters run in order from
# H9251 (α), with H9269 the final sigma. Names whose meaning was not clear
# from context are left out and stay (cid:N).

_CID_GLYPH = re.compile(r"\(cid:\d+\)")
_MAX_CID_GLYPH_FRACTION = 0.2
_C_NUMBER_GLYPH = re.compile(r"C(\d{2,3})")

_MATH_PI_GLYPHS = {
    # Greek capitals
    "H9004": "Δ", "H9021": "Φ", "H9023": "Ψ", "H9024": "Ω",
    # Greek lowercase, alphabetical from H9251
    **{f"H{9251 + i}": letter for i, letter in enumerate("αβγδεζηθικλμνξοπρσςτυφχψω")},
    "H9278": "ϕ", "H9280": "ϵ",
    # Math and typographic symbols
    "H11001": "+", "H11002": "−", "H11003": "×", "H11005": "=", "H11006": "±",
    "H11011": "∼", "H11015": "≈", "H11021": "<", "H11022": ">",
    "H11032": "′", "H11033": "″", "H11034": "°", "H11349": "≤", "H11350": "≥",
    "H11541": "′", "H11545": "+", "H11546": "−", "H11569": "*", "H11601": "∼",
    "H18528": "–", "H20841": "|", "H20862": "/", "H23041": "®",
}


# Elsevier's "Adv" math fonts name their glyphs "C<n>" with n below 32: the
# position in TeX's math symbol font (cmsy), not a character code. Read off
# context in this corpus ("−80 °C", "16000 × g", "***P", "n ≥ 3", "∼2 mg").
# AdvBMa1 also uses small C<n> names but for other glyphs, so it is not matched.
_CMSY_FONT = re.compile(r"Adv(?:P4C4E\d+|MT_SY|MacMthSy)$")
_CMSY_GLYPHS = {0: "−", 1: "·", 2: "×", 3: "∗", 6: "±", 14: "°", 20: "≤", 21: "≥", 24: "∼", 25: "≈"}

# Fonts whose codes carry no glyph name at all. The Mathematical Pi /
# Universal Greek fonts put Greek at the Latin letter's code, as Symbol does
# (97 "a" is α, 109 "m" is μ, 68 "D" is Δ), and the math signs from 46 up,
# matching their H11001.. names (49 is H11001 "+", 59 is H11011 "∼"). Each
# code was checked against its context in several papers ("importin α/β",
# "100 μg/ml", "−80 °C", "3′ portion", "Scale bar = 10 mm", ">90% pure").
_UNNAMED_CODE_GLYPHS = (
    (re.compile(r"MathematicalPi-One$|Universal-GreekwithMathPi"), {
        **{code: _SYMBOL_ENCODING[code] for code in (*range(0x41, 0x5B), *range(0x61, 0x7B))},
        46: ">", 49: "+", 50: "−", 51: "×", 53: "=", 54: "±", 56: "°", 57: "′", 59: "∼",
    }),
    (re.compile(r"AdvTimes(?:-\w+)?$"), {129: "ffi"}),  # "eﬃcient", "traﬃcking"
)
_SUBSET_PREFIX = re.compile(r"^[A-Z]{6}\+")


def _pdf_name(value) -> str:
    """A PDF name object's text; names that are not UTF-8 come as bytes
    (seen: a BaseFont of b"ABCDEE+\\xcb\\xce\\xcc\\xe5")."""
    name = getattr(value, "name", value)
    if isinstance(name, bytes):
        return name.decode("latin-1")
    return name if isinstance(name, str) else ""


def _install_glyph_name_fallbacks() -> None:
    """Make pdfminer read the glyph names it does not know: "C<n>" as
    StandardEncoding character n (or a cmsy position, in Elsevier's math
    fonts), the Mathematical Pi "H<n>" names, and the codes of a few fonts
    that name no glyphs at all.

    pdfminer skips a /Differences name it does not know, so the code keeps
    its base-encoding character ("#" for 35) or none at all ((cid:N)); these
    names override either. A font's ToUnicode map stays authoritative:
    pdfminer reads it first and falls back to these only for the codes it
    lacks (which used to come out as (cid:N)).
    """
    from pdfminer.encodingdb import EncodingDB
    from pdfminer.pdffont import PDFSimpleFont
    from pdfminer.pdftypes import resolve1

    original_init = PDFSimpleFont.__init__
    if getattr(original_init, "_reads_glyph_name_fallbacks", False):
        return

    def __init__(self, descriptor, widths, spec):
        original_init(self, descriptor, widths, spec)
        try:
            _add_glyph_fallbacks(self, spec)
        except Exception:
            # A malformed font is read as pdfminer reads it, never not at all.
            logger.debug("Glyph-name fallbacks skipped for a font", exc_info=True)

    def _add_glyph_fallbacks(font, spec):
        # Without /Differences pdfminer hands every font the same shared
        # encoding dict; writing to it would change all later fonts.
        font.cid2unicode = dict(font.cid2unicode)
        font_name = _SUBSET_PREFIX.sub("", _pdf_name(resolve1(spec.get("BaseFont"))))
        cmsy = bool(_CMSY_FONT.match(font_name))
        encoding = resolve1(spec.get("Encoding"))
        code = 0
        named = set()
        for item in (resolve1(encoding.get("Differences", [])) or []) if isinstance(encoding, dict) else []:
            if isinstance(item, int):
                code = item
                continue
            name = _pdf_name(item)
            named.add(code)
            match = _C_NUMBER_GLYPH.fullmatch(name) or (cmsy and re.fullmatch(r"C(\d)", name))
            if match and cmsy:
                # A cmsy position at any n ("C48" is ′, not "0"); only the
                # positions read off this corpus are mapped.
                if int(match[1]) in _CMSY_GLYPHS:
                    font.cid2unicode[code] = _CMSY_GLYPHS[int(match[1])]
            # Below 32 the number is not a character code ("C21" decoded
            # to a control character where the page shows "≈").
            elif match and 32 <= int(match[1]) < 256 and int(match[1]) != 127:
                n = int(match[1])
                font.cid2unicode[code] = EncodingDB.std2unicode.get(n) or chr(n)
            elif name in _MATH_PI_GLYPHS:
                font.cid2unicode[code] = _MATH_PI_GLYPHS[name]
            code += 1
        for font_re, glyphs in _UNNAMED_CODE_GLYPHS:
            if font_re.match(font_name):
                # Overrides the base encoding too: these fonts have no Latin
                # letters or digits, so StandardEncoding's "a" at 97 is wrong.
                font.cid2unicode.update({c: g for c, g in glyphs.items() if c not in named})

    __init__._reads_glyph_name_fallbacks = True
    PDFSimpleFont.__init__ = __init__


_install_glyph_name_fallbacks()


def looks_garbled(text: str) -> bool:
    """True when much of the extracted text is unmapped (cid:N) glyphs."""
    cid_count = len(_CID_GLYPH.findall(text))
    if not cid_count:
        return False
    other_glyphs = sum(1 for c in _CID_GLYPH.sub("", text) if not c.isspace())
    return cid_count / (cid_count + other_glyphs) >= _MAX_CID_GLYPH_FRACTION


def _ocr_pdf(input_path: Path) -> Tuple[Optional[str], str]:
    """OCR every page. Returns (text, "") or (None, why it failed)."""
    # Page-by-page to avoid loading all images into RAM.
    with tempfile.TemporaryDirectory(prefix="ocr_pages_") as tmpdir:
        try:
            page_images = convert_from_path(
                str(input_path),
                dpi=300,
                output_folder=tmpdir,
                fmt="png",
            )
        except Exception as e:
            return None, f"pdf2image failed: {e}"

        ocr_parts: list[str] = []
        try:
            for img in page_images:
                try:
                    text = pytesseract.image_to_string(img)
                finally:
                    # Let each PIL image be freed before the next page is processed.
                    img.close()
                if text.strip():
                    ocr_parts.append(text)
        except Exception as e:
            return None, f"OCR failed: {e}"

    if not ocr_parts:
        return None, "No text extracted from PDF (tried pdfplumber and OCR)"
    return "\n\n".join(ocr_parts), ""


def convert_pdf_to_txt(input_path: Path, output_dir: Path) -> Tuple[bool, str]:
    """Convert a PDF to text via pdfplumber; fall back to OCR for scanned
    pages or an unreadable text layer."""
    output_path = output_dir / (input_path.stem + ".txt")

    layout_doc = _open_layout_document(input_path)
    try:
        with pdfplumber.open(input_path) as pdf:
            page_count = max(1, len(pdf.pages))
            same_pages = layout_doc is not None and _layout_page_count(layout_doc) == len(pdf.pages)
            text_parts = [
                _extract_page_text(p, _page_line_layout(layout_doc, i) if same_pages else None)
                for i, p in enumerate(pdf.pages)
            ]
    finally:
        _close_layout_document(layout_doc)

    total_chars = sum(len(t) for t in text_parts)
    text = "\n\n".join(t for t in text_parts if t)
    garbled = looks_garbled(text)

    if total_chars / page_count >= _CHARS_PER_PAGE_THRESHOLD and not garbled:
        output_path.write_text(text, encoding="utf-8")
        return True, f"Converted: {output_path}"

    ocr_text, error = _ocr_pdf(input_path)
    if ocr_text is None:
        if not garbled:
            return False, error
        # Keep the document (its figures are still usable) rather than
        # rejecting a paper that was accepted before OCR was tried.
        logger.warning(
            "%s: text layer is unreadable (fonts without a Unicode map) and "
            "OCR failed, keeping the garbled text: %s", input_path.name, error,
        )
        output_path.write_text(text, encoding="utf-8")
        return True, f"Converted: {output_path}"

    output_path.write_text(ocr_text, encoding="utf-8")
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
