"""Tests for reading a PDF page in PyMuPDF's line order (convert_to_txt.py).

The gutter search needs one vertical gap through most of the page, so a
paper's first page -- a full-width title over two columns -- was read
straight across both columns. PyMuPDF's lines come in the order the PDF
draws them; the text of each line is still built from pdfplumber's
characters, which decode fonts PyMuPDF gets wrong. Also covers the Symbol
font's Private Use Area characters.
"""

import re

import pytest

pytest.importorskip("reportlab")
pytest.importorskip("pymupdf")

import pdfplumber
import pymupdf

from app.services.document_conversion import convert_to_txt
from app.services.document_conversion.convert_to_txt import (
    _decode_symbol_font_chars,
    _extract_page_text_by_lines,
    _extract_page_text_column_aware,
    convert_pdf_to_txt,
)
from tests.helpers.pdf_fixtures import build_single_column_pdf, build_titled_two_column_pdf

TITLE = "Nuclear export of Php4 is mediated by its leucine-rich signal and CRM1"
LEFT_SENTENCE = "Under low iron conditions Php4 acts as a negative regulatory subunit of the CCAAT binding factor. "
RIGHT_SENTENCE = "Iron is an essential nutrient yet excess iron causes oxidative damage to proteins and membranes. "


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _read(path):
    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        line_layout = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        return _extract_page_text_by_lines(page, line_layout), _extract_page_text_column_aware(page)


# Php4's JBC columns are 11.9pt apart, under the gutter search's 12pt.
@pytest.mark.parametrize("gutter", [30, 11.9])
def test_titled_two_column_page_is_read_column_by_column(tmp_path, gutter):
    path = tmp_path / "paper.pdf"
    build_titled_two_column_pdf(path, TITLE, LEFT_SENTENCE, RIGHT_SENTENCE, gutter=gutter)

    by_lines, _ = _read(path)

    assert by_lines.startswith(TITLE)
    assert _norm(LEFT_SENTENCE.strip()) in _norm(by_lines)
    assert _norm(RIGHT_SENTENCE.strip()) in _norm(by_lines)
    # The whole left column comes before the right one.
    assert _norm(by_lines).rindex("Php4 acts") < _norm(by_lines).index("Iron is an essential")


def test_page_box_not_at_origin(tmp_path):
    # Many journals' pages have a crop box inside the media box (seen: 9pt in);
    # the two libraries then measure from different corners.
    path = tmp_path / "paper.pdf"
    build_titled_two_column_pdf(path, TITLE, LEFT_SENTENCE, RIGHT_SENTENCE, crop_box=(9, 9, 603, 783))

    by_lines, _ = _read(path)

    assert by_lines is not None
    assert _norm(LEFT_SENTENCE.strip()) in _norm(by_lines)


def test_convert_uses_line_order(tmp_path):
    path = tmp_path / "paper.pdf"
    build_titled_two_column_pdf(path, TITLE, LEFT_SENTENCE, RIGHT_SENTENCE)

    ok, _ = convert_pdf_to_txt(path, tmp_path)

    text = (tmp_path / "paper.txt").read_text(encoding="utf-8")
    assert ok
    assert _norm(LEFT_SENTENCE.strip()) in _norm(text)


def test_text_drawn_twice_in_place_is_read_once(tmp_path):
    # Figure labels are often drawn twice at the same spot ("p-S74-Cdc6").
    from reportlab.pdfgen.canvas import Canvas

    path = tmp_path / "paper.pdf"
    c = Canvas(str(path))
    c.setFont("Helvetica", 10)
    for y in range(750, 400, -14):
        c.drawString(50, y, "Body text of the paper sits in ordinary single lines here.")
    c.drawString(50, 380, "p-S74-Cdc6")
    c.drawString(50, 380, "p-S74-Cdc6")
    c.showPage()
    c.save()

    by_lines, _ = _read(path)

    assert "p-S74-Cdc6" in by_lines
    assert "pp--" not in by_lines


def test_space_less_line_on_a_page_with_spaces_is_split(tmp_path):
    # Cdc6, p. 8: figure labels full of spaces, body text drawn word by word
    # with no space glyph and gaps (~0.25 em) under pdfplumber's 3pt default.
    from reportlab.pdfgen.canvas import Canvas

    from tests.helpers.pdf_fixtures import _draw_words_glued

    path = tmp_path / "paper.pdf"
    c = Canvas(str(path))
    c.setFont("Helvetica", 8)
    for y in range(750, 600, -12):
        c.drawString(50, y, "Ctrl     CyA     Ctrl     CyA     Ctrl     CyA")
    words = "expressed as means of three determinations obtained in each of three experiments".split()
    _draw_words_glued(c, words, 50, 560, 560, inter_word_gap=2.0, font_size=8)
    c.showPage()
    c.save()

    by_lines, _ = _read(path)

    assert " ".join(words) in _norm(by_lines)
    assert "Ctrl CyA" in _norm(by_lines)


def test_short_space_less_label_is_left_whole():
    chars = [{"text": ch} for ch in "p-S74-Cdc6"]

    assert convert_to_txt._line_word_tolerance(chars, {"x_tolerance": 3, "x_tolerance_ratio": None}) == {
        "x_tolerance": 3, "x_tolerance_ratio": None,
    }


def test_without_pymupdf_the_gutter_search_is_used(tmp_path, monkeypatch):
    monkeypatch.setattr(convert_to_txt, "pymupdf", None)
    path = tmp_path / "paper.pdf"
    build_single_column_pdf(path, "A perfectly ordinary single-column paragraph of text. ")

    ok, _ = convert_pdf_to_txt(path, tmp_path)

    assert ok
    assert "perfectly ordinary" in (tmp_path / "paper.txt").read_text(encoding="utf-8")


def test_a_failing_close_does_not_fail_the_conversion():
    class _Doc:
        def close(self):
            raise RuntimeError("already closed")

    convert_to_txt._close_layout_document(_Doc())


def test_pymupdf_is_only_called_under_the_lock():
    # MuPDF is not thread-safe and conversions run in worker threads.
    class _Doc:
        page_count = 1

        def __getitem__(self, index):
            assert convert_to_txt.PYMUPDF_LOCK.locked()
            return self

        rotation = 0

        def get_text(self, kind):
            assert convert_to_txt.PYMUPDF_LOCK.locked()
            return {"blocks": []}

        def close(self):
            assert convert_to_txt.PYMUPDF_LOCK.locked()

    doc = _Doc()
    assert convert_to_txt._layout_page_count(doc) == 1
    assert convert_to_txt._page_line_layout(doc, 0) == ([], [])
    convert_to_txt._close_layout_document(doc)


def test_the_layout_page_is_freed_under_the_lock():
    # Freeing a PyMuPDF Page calls into MuPDF too.
    freed_under_lock = []

    class _Page:
        rotation = 0

        def get_text(self, kind):
            return {"blocks": []}

        def __del__(self):
            freed_under_lock.append(convert_to_txt.PYMUPDF_LOCK.locked())

    class _Doc:
        def __getitem__(self, index):
            return _Page()

    assert convert_to_txt._page_line_layout(_Doc(), 0) == ([], [])
    assert freed_under_lock == [True]


def test_a_line_box_far_outside_the_page_is_clamped(tmp_path):
    # A malformed PDF can give a line a huge box; it must not build a row per point.
    path = tmp_path / "paper.pdf"
    build_titled_two_column_pdf(path, TITLE, LEFT_SENTENCE, RIGHT_SENTENCE)
    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        lines, layout_chars = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)
    x0, _, x1, _ = lines[-1]
    lines[-1] = (x0, -1e9, x1, 1e9)

    with pdfplumber.open(path) as pdf:
        text = _extract_page_text_by_lines(pdf.pages[0], (lines, layout_chars))

    assert text is None or TITLE in text


def test_a_page_pymupdf_cannot_lay_out_is_read_the_old_way(tmp_path):
    class _BrokenDoc:
        def __getitem__(self, index):
            raise RuntimeError("cannot load page")

    assert convert_to_txt._page_line_layout(_BrokenDoc(), 0) is None


class _FakePage:
    def __init__(self, chars):
        self.chars = chars


def test_symbol_font_private_use_characters_are_decoded():
    chars = [
        {"text": "", "fontname": "ABCDEF+Symbol"},
        {"text": "", "fontname": "SymbolGreek"},
        {"text": "", "fontname": "Symbol"},
        {"text": "", "fontname": "MathematicalPi-One"},  # not Symbol: left alone
        {"text": "a", "fontname": "Symbol"},
    ]

    _decode_symbol_font_chars(_FakePage(chars))

    assert [c["text"] for c in chars] == ["α", "μ", "=", "", "a"]


def test_math_pi_private_use_characters_are_decoded():
    # CID-keyed Math Pi fonts whose ToUnicode map gives U+E000 + code.
    chars = [
        {"text": "", "fontname": "HVKXKA+MathematicalPi-One"},  # "eEF1α"
        {"text": "", "fontname": "HVKXKA+MathematicalPi-One"},  # "5′"
        {"text": "", "fontname": "SWXLIE+MathematicalPi-Five"},  # "∼25%"
        {"text": "", "fontname": "ABCDEF+TimesNewRoman"},  # not Math Pi: left alone
    ]

    _decode_symbol_font_chars(_FakePage(chars))

    assert [c["text"] for c in chars] == ["α", "′", "∼", ""]


def test_calibri_ti_ligature_is_decoded():
    # CID-keyed Calibri's ToUnicode map leaves out glyph 415: "β-ac(cid:415)n".
    chars = [
        {"text": "(cid:415)", "fontname": "HBFBJA+Calibri-Bold"},
        {"text": "(cid:415)", "fontname": "ABCDEF+TimesNewRoman"},
    ]

    _decode_symbol_font_chars(_FakePage(chars))

    assert [c["text"] for c in chars] == ["ti", "(cid:415)"]


def test_rotated_label_is_read_in_its_own_direction(tmp_path):
    # Cdc6, p. 3: a y-axis label turned 90° counterclockwise and drawn twice
    # came out as "lloossoottyyCC".
    from reportlab.pdfgen.canvas import Canvas

    path = tmp_path / "paper.pdf"
    c = Canvas(str(path))
    c.setFont("Helvetica", 10)
    for y in range(750, 400, -14):
        c.drawString(50, y, "Body text of the paper sits in ordinary single lines here.")
    for _ in range(2):
        c.saveState()
        c.translate(400, 200)
        c.rotate(90)
        c.drawString(0, 0, "Cytosol")
        c.restoreState()
    c.saveState()
    c.translate(450, 300)
    c.rotate(-90)
    c.drawString(0, 0, "Nuclei")
    c.restoreState()
    c.showPage()
    c.save()

    by_lines, _ = _read(path)

    assert "Cytosol" in by_lines
    assert "Nuclei" in by_lines
    assert "CCyy" not in by_lines


def test_adobe_private_use_signs_and_soft_hyphens_are_decoded():
    chars = [
        {"text": "", "fontname": "ABCDEF+Helvetica"},  # "© The Rockefeller University Press"
        {"text": "", "fontname": "ABCDEF+Times"},  # "heterodimer — namely"
        {"text": "­", "fontname": "ABCDEF+Myriad"},  # "CRM1­dependent"
    ]

    _decode_symbol_font_chars(_FakePage(chars))

    assert [c["text"] for c in chars] == ["©", "—", "-"]


# ---------------------------------------------------------------------------
# Table rows
# ---------------------------------------------------------------------------
# PyMuPDF makes every table cell its own line; the branch first read a table
# one value per line (Attention: 10 of main's 45 rows intact).

def _build_table_pdf(path, *, label_first: bool):
    """Body text over a table drawn cell by cell, row by row, whose label
    "(C)" spans the middle three of five rows, drawn right before or right
    after them (as in Attention's Table 3)."""
    from reportlab.pdfgen.canvas import Canvas

    c = Canvas(str(path))
    c.setFont("Helvetica", 10)
    for y in range(750, 600, -14):
        c.drawString(50, y, "Body text of the paper sits in ordinary single lines here.")
    rows = [("base", "512", "4.92", "25.8"), ("", "256", "5.75", "24.5"), ("", "1024", "4.66", "26.0"),
            ("", "4096", "4.75", "26.2"), ("big", "1024", "4.33", "26.4")]
    for k, row in enumerate(rows):
        if (k == 1 and label_first) or (k == 4 and not label_first):
            c.drawString(60, 540, "(C)")
        y = 568 - 14 * k
        for x, cell in zip((60, 160, 260, 360), row):
            if cell:
                c.drawString(x, y, cell)
    c.showPage()
    c.save()


@pytest.mark.parametrize("label_first", [True, False])
def test_table_rows_are_read_one_per_line(tmp_path, label_first):
    path = tmp_path / "paper.pdf"
    _build_table_pdf(path, label_first=label_first)

    by_lines, _ = _read(path)
    lines = [_norm(line).strip() for line in by_lines.splitlines()]

    assert "base 512 4.92 25.8" in lines
    assert "256 5.75 24.5" in lines
    assert "(C) 1024 4.66 26.0" in lines  # the spanning label joins the row at its height
    assert "4096 4.75 26.2" in lines
    assert "big 1024 4.33 26.4" in lines
    assert lines.index("base 512 4.92 25.8") < lines.index("big 1024 4.33 26.4")


def _box(x0, top, x1, text, height=10):
    return (x0, top, x1, top + height), text


def test_two_columns_at_one_height_are_not_joined():
    # A column's lines are drawn one after another, so the left and right
    # lines at one height are never consecutive.
    left = [_box(50, 100 + 14 * k, 290, f"left line {k}") for k in range(3)]
    right = [_box(320, 100 + 14 * k, 560, f"right line {k}") for k in range(3)]

    assert convert_to_txt._join_rows(left + right) == [text for _, text in left + right]


def test_stacked_or_overlapping_lines_are_not_joined():
    built = [_box(50, 100, 300, "a line"), _box(50, 104, 300, "overlapping it"), _box(60, 118, 200, "below")]

    assert convert_to_txt._join_rows(built) == ["a line", "overlapping it", "below"]


def test_section_number_joins_its_title():
    built = [_box(50, 100, 62, "6.3"), _box(72, 100, 200, "English Constituency Parsing")]

    assert convert_to_txt._join_rows(built) == ["6.3 English Constituency Parsing"]


def test_short_line_next_to_split_body_text_is_not_taken_as_a_label():
    # Body text split at a wide gap forms rows of long cells; a short line
    # drawn next to them (a page range from the other column) stays alone.
    body = []
    for k in range(3):
        body += [_box(50, 100 + 14 * k, 230, "the long first part of a line of the paper's body text"),
                 _box(240, 100 + 14 * k, 290, "and its rest")]
    built = body + [_box(320, 114, 350, "68-77.")]

    assert convert_to_txt._join_rows(built)[-1] == "68-77."


def test_body_line_and_caption_across_the_gutter_are_not_joined():
    # An3: the right column's line and the left column's caption are drawn
    # one after another at one height, 12pt apart.
    built = [_box(308, 630, 554, "mutant in which 2 leucine residues at positions 19 and 21", height=8.5),
             _box(59, 634.5, 296, "FIG. 1. Biochemical characterization of GST-An3 ATPase ac-", height=7.5)]

    assert convert_to_txt._join_rows(built) == [text for _, text in built]


def test_glyph_just_past_its_line_edge_stays_on_its_line(tmp_path):
    # PyMuPDF can measure a glyph narrower than pdfplumber does (a bracket in
    # a Hebrew court decision's footnote marker), so the char sits just past
    # its line's box and used to be dumped at the end of the page.
    from reportlab.pdfgen.canvas import Canvas

    path = tmp_path / "paper.pdf"
    c = Canvas(str(path))
    c.setFont("Helvetica", 10)
    for y in range(750, 400, -14):
        c.drawString(50, y, "Body text of the paper sits in ordinary single lines here.")
    c.drawString(300, 380, "[19]")
    c.showPage()
    c.save()

    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        lines, layout_chars = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)
    # Narrow the marker's line box so "[" falls just outside it.
    k = min(range(len(lines)), key=lambda i: abs(lines[i][0] - 300))
    x0, y0, x1, y1 = lines[k]
    lines[k] = (x0 + 3.5, y0, x1, y1)
    with pdfplumber.open(path) as pdf:
        by_lines = _extract_page_text_by_lines(pdf.pages[0], (lines, layout_chars))

    assert "[19]" in by_lines.split("\n")
    assert "[" not in by_lines.replace("[19]", "")
