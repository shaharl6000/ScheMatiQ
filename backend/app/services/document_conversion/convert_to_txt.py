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
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional, Tuple

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


def _install_glyph_name_fallbacks() -> None:
    """Make pdfminer read the glyph names it does not know: "C<n>" as
    StandardEncoding character n, and the Mathematical Pi "H<n>" names.

    pdfminer skips a /Differences name it does not know, so the code keeps
    its base-encoding character ("#" for 35) or none at all ((cid:N)); these
    names override either. Fonts with a ToUnicode map (which, when present,
    is authoritative) are left alone.
    """
    from pdfminer.encodingdb import EncodingDB
    from pdfminer.pdffont import PDFSimpleFont
    from pdfminer.pdftypes import resolve1

    original_init = PDFSimpleFont.__init__
    if getattr(original_init, "_reads_glyph_name_fallbacks", False):
        return

    def __init__(self, descriptor, widths, spec):
        original_init(self, descriptor, widths, spec)
        if self.unicode_map is not None:
            return
        encoding = resolve1(spec.get("Encoding"))
        if not isinstance(encoding, dict):
            return
        code = 0
        for item in resolve1(encoding.get("Differences", [])) or []:
            if isinstance(item, int):
                code = item
                continue
            name = getattr(item, "name", "")
            match = _C_NUMBER_GLYPH.fullmatch(name)
            # Below 32 the number is not a character code ("C21" decoded
            # to a control character where the page shows "≈").
            if match and 32 <= int(match[1]) < 256 and int(match[1]) != 127:
                n = int(match[1])
                self.cid2unicode[code] = EncodingDB.std2unicode.get(n) or chr(n)
            elif name in _MATH_PI_GLYPHS:
                self.cid2unicode[code] = _MATH_PI_GLYPHS[name]
            code += 1

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

    with pdfplumber.open(input_path) as pdf:
        page_count = max(1, len(pdf.pages))
        text_parts = [_extract_page_text_column_aware(p) for p in pdf.pages]

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
