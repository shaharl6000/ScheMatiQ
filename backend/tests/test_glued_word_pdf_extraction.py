"""Tests for the missing-literal-space-glyph word-tolerance fix in
convert_to_txt.py.

Some PDF producers place words via pure glyph positioning with no literal
space character in the content stream at all (observed on certain Cell
Press/Elsevier exports). pdfplumber then decides word boundaries purely from
the geometric gap between adjacent characters vs. x_tolerance (default 3pt);
when such a PDF's true inter-word gap is smaller than that, words are
silently glued together ("wordsgluedlikethis"). These tests build small,
exact-geometry PDFs at test time (via reportlab) to prove the fix recovers
correct spacing for genuinely glued pages while leaving every normal page
(real space characters, sparse pages, or a dense numeric table) byte-for-byte
unchanged.
"""

import re

import pytest

pytest.importorskip("reportlab")

import pdfplumber

from app.services.document_conversion.convert_to_txt import (
    _derive_word_gap_threshold,
    _detect_column_gutter,
    _effective_x_tolerance,
    _extract_page_text_column_aware,
    _find_bimodal_gap_threshold,
    convert_pdf_to_txt,
)
from tests.helpers.pdf_fixtures import (
    build_glued_word_pdf,
    build_single_column_pdf,
    build_two_column_glued_pdf,
    build_two_column_pdf,
    build_two_column_with_full_width_line_pdf,
)

_SENTENCE = (
    "Like many other RPTPs this protein consists of an extracellular receptor "
    "like region a short transmembrane segment and a cytoplasmic region"
)
_RIGHT_SENTENCE = (
    "This sidebar describes most known signaling functions of the receptor "
    "phosphatase family across many cell types and tissues throughout the body"
)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower())


def _words(sentence: str, repeat: int) -> list[str]:
    return sentence.split() * repeat


def test_glued_page_with_no_space_chars_is_recovered(tmp_path):
    pdf_path = tmp_path / "glued.pdf"
    build_glued_word_pdf(pdf_path, _words(_SENTENCE, 6), inter_word_gap=1.2)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        raw = page.extract_text() or ""
        fixed = _extract_page_text_column_aware(page)
        threshold = _derive_word_gap_threshold(page)

    # Baseline proof the bug is real under today's default tolerance.
    assert " " not in raw
    # The fix recovers real word boundaries.
    assert _norm(_SENTENCE) in _norm(fixed)
    assert threshold is not None
    assert 0 < threshold < 1.2


def test_convert_pdf_to_txt_end_to_end_recovers_glued_words(tmp_path):
    pdf_path = tmp_path / "glued.pdf"
    build_glued_word_pdf(pdf_path, _words(_SENTENCE, 6), inter_word_gap=1.2)

    success, message = convert_pdf_to_txt(pdf_path, tmp_path)
    assert success, message

    text = (tmp_path / "glued.txt").read_text(encoding="utf-8")
    assert _norm(_SENTENCE) in _norm(text)


def test_normal_pages_are_unaffected_by_gap_derivation(tmp_path):
    single_path = tmp_path / "single.pdf"
    build_single_column_pdf(single_path, "A perfectly ordinary single-column paragraph of text. ")

    two_col_path = tmp_path / "two_col.pdf"
    build_two_column_pdf(two_col_path, _SENTENCE, _RIGHT_SENTENCE)

    with pdfplumber.open(single_path) as pdf:
        page = pdf.pages[0]
        assert _derive_word_gap_threshold(page) is None
        assert _effective_x_tolerance(page) == 3
        assert _extract_page_text_column_aware(page) == (page.extract_text() or "")

    with pdfplumber.open(two_col_path) as pdf:
        page = pdf.pages[0]
        assert _derive_word_gap_threshold(page) is None
        assert _effective_x_tolerance(page) == 3
        assert _detect_column_gutter(page) == _detect_column_gutter(page, x_tolerance=3)


def test_sparse_glued_page_below_char_threshold_is_not_touched(tmp_path):
    pdf_path = tmp_path / "sparse_glued.pdf"
    build_glued_word_pdf(pdf_path, ["Hi.", "Bye."], inter_word_gap=1.2)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        assert _derive_word_gap_threshold(page) is None


def test_word_gap_threshold_ignores_unimodal_gap_distribution():
    # A continuous spread with no real jump between populations -- e.g.
    # gradually increasing letter-spacing -- must not be mistaken for a
    # genuine kerning-vs-word-boundary split.
    gaps = [0.05 * i for i in range(120)]
    assert _find_bimodal_gap_threshold(gaps) is None


def test_word_gap_threshold_finds_genuine_bimodal_split():
    low = [0.0, 0.05, 0.1] * 40
    high = [1.2, 1.3, 1.1] * 20
    gaps = low + high
    threshold = _find_bimodal_gap_threshold(gaps)
    assert threshold is not None
    assert 0.1 < threshold < 1.1


def test_two_column_glued_page_is_split_and_word_spaced(tmp_path):
    pdf_path = tmp_path / "two_col_glued.pdf"
    build_two_column_glued_pdf(
        pdf_path,
        _words(_SENTENCE, 20),
        _words(_RIGHT_SENTENCE, 20),
        inter_word_gap=1.2,
    )

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        x_tolerance = _effective_x_tolerance(page)
        gutter = _detect_column_gutter(page, x_tolerance=x_tolerance)
        fixed = _extract_page_text_column_aware(page)

    assert gutter is not None, "a glued two-column page must still be detected as two-column"
    assert _norm(_SENTENCE) in _norm(fixed)
    assert _norm(_RIGHT_SENTENCE) in _norm(fixed)
    assert _norm(fixed).index(_norm(_SENTENCE).split()[0]) < _norm(fixed).index(
        _norm(_RIGHT_SENTENCE).split()[0]
    )


def test_mostly_glued_page_with_sparse_real_spaces_is_still_recovered(tmp_path):
    """Real glued pages aren't uniformly glued -- occasional runs (citations,
    accession-number lists) keep literal spaces. Measured directly against
    the real broken document that motivated this fix, its glued
    "Experimental Procedures" section has a space-character ratio of
    ~1.4-1.7%, not 0%. This fixture reproduces that (~1.5%) via
    `space_every_n_words`, proving the gate still fires on a realistic mix
    rather than only the idealized 0%-space fixtures above."""
    pdf_path = tmp_path / "mostly_glued.pdf"
    build_glued_word_pdf(pdf_path, _words(_SENTENCE, 8), inter_word_gap=1.2, space_every_n_words=10)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        chars = [c for c in page.chars if c.get("upright", True)]
        space_ratio = sum(1 for c in chars if c["text"].isspace()) / len(chars)
        threshold = _derive_word_gap_threshold(page)
        fixed = _extract_page_text_column_aware(page)

    assert 0.01 < space_ratio < 0.02, "fixture should match the real document's measured ~1.4-1.7% ratio"
    assert threshold is not None, "the gate must still fire on a realistic mixed-space page"
    assert _norm(_SENTENCE) in _norm(fixed)


def test_bimodal_split_not_hijacked_by_third_gap_population():
    """A third population of legitimately wider (but non-word-boundary)
    gaps -- e.g. from superscripts/numbering runs -- reaching just
    _MIN_GAP_CLUSTER_FRACTION (5%) of samples must not hijack the split
    away from the true kerning-vs-word-gap boundary."""
    kerning = [0.0] * 80 + [0.05] * 20  # 100 samples
    word_gaps = [1.2] * 55  # 55 samples -- the real word-boundary cluster
    third_population = [6.0] * 10  # 10 samples -- some other legitimately wider gap
    gaps = kerning + word_gaps + third_population

    threshold = _find_bimodal_gap_threshold(gaps)

    assert threshold is not None
    assert 0.05 < threshold < 1.2, (
        f"threshold {threshold} should split kerning from real word-gaps, "
        "not degrade toward the third population / DEFAULT_X_TOLERANCE"
    )


def test_bimodal_split_still_correct_with_few_outliers():
    """A couple of outliers below _MIN_GAP_CLUSTER_FRACTION must not be
    treated as a legitimate third cluster -- regression guard alongside the
    hijack test above."""
    kerning = [0.0] * 80 + [0.05] * 20
    word_gaps = [1.2] * 55
    few_outliers = [6.0, 6.5]  # below min_side, must be ignored as noise
    gaps = kerning + word_gaps + few_outliers

    threshold = _find_bimodal_gap_threshold(gaps)

    assert threshold is not None
    assert 0.05 < threshold < 1.2


def test_full_width_line_veto_unaffected_by_tighter_tolerance(tmp_path):
    """A full-width line straddling the gutter must still veto column
    splitting (and not lose text) even on a page that also needs a tighter,
    derived x_tolerance -- proves the two fixes don't interact badly."""
    pdf_path = tmp_path / "full_width.pdf"
    full_width_line = "This full width table caption spans the entire page width across both columns"
    build_two_column_with_full_width_line_pdf(pdf_path, _SENTENCE, _RIGHT_SENTENCE, full_width_line)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        default = page.extract_text() or ""

    assert _norm(full_width_line) in _norm(default)
