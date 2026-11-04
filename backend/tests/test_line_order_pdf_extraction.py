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
