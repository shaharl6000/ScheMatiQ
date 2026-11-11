"""Tests for Hebrew / RTL PDF text extraction (convert_to_txt.py).

pdfplumber sorts horizontal line characters left-to-right (visual order), so
Hebrew text came out reversed ("ןיד קספ" instead of "פסק דין"), and visual-stream
Hebrew PDFs failed coordinate alignment against PyMuPDF's logical-order characters.
"""

import pytest

pytest.importorskip("pymupdf")
pytest.importorskip("pdfplumber")

import pdfplumber
import pymupdf

from app.services.document_conversion import convert_to_txt
from app.services.document_conversion.convert_to_txt import (
    _coordinate_offset,
    _extract_page_text_by_lines,
    _extract_page_text_column_aware,
    _visual_line_to_logical,
    convert_pdf_to_txt,
)


def _hebrew_font_buffer() -> bytes:
    """Extract PyMuPDF's bundled Noto Hebrew font buffer for low-level insert_text calls."""
    tmp_doc = pymupdf.open()
    try:
        page = tmp_doc.new_page()
        page.insert_htmlbox(pymupdf.Rect(0, 0, 200, 50), "פסק דין של בית המשפט העליון")
        xref = page.get_fonts()[0][0]
        _, _, _, fontbuffer = tmp_doc.extract_font(xref)
        return fontbuffer
    finally:
        tmp_doc.close()


def test_visual_stream_hebrew_pdf_aligns_and_extracts_logical_order(tmp_path):
    """A Hebrew PDF whose content stream stores glyphs in visual left-to-right
    order must align in _coordinate_offset, use _extract_page_text_by_lines,
    and output logical Hebrew order ('פסק דין', not 'ןיד קספ')."""
    path = tmp_path / "ruling_visual.pdf"
    fontbuffer = _hebrew_font_buffer()

    doc = pymupdf.open()
    page = doc.new_page(width=595, height=842)
    page.insert_font(fontname="F0", fontbuffer=fontbuffer)
    visual_line = "ןוילעה טפשמה תיב לש ןיד קספ"
    for i in range(12):
        page.insert_text((80, 100 + i * 20), visual_line, fontname="F0", fontsize=12)
    doc.save(str(path))
    doc.close()

    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        line_layout = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)

    with pdfplumber.open(path) as pdf:
        pl_page = pdf.pages[0]
        visible = [c for c in pl_page.chars if not c["text"].isspace()]
        offset = _coordinate_offset(visible, line_layout[1])
        by_lines = _extract_page_text_by_lines(pl_page, line_layout)
        col_aware = _extract_page_text_column_aware(pl_page)

    assert offset is not None
    assert by_lines is not None
    assert by_lines.splitlines()[0] == "פסק דין של בית המשפט העליון"
    assert "ןיד קספ" not in by_lines
    assert col_aware.splitlines()[0] == "פסק דין של בית המשפט העליון"

    ok, _ = convert_pdf_to_txt(path, tmp_path)
    assert ok
    converted = (tmp_path / "ruling_visual.txt").read_text(encoding="utf-8")
    assert "פסק דין של בית המשפט העליון" in converted
    assert "ןיד קספ" not in converted


def test_hebrew_ruling_with_citations_numbers_brackets_and_english(tmp_path):
    """RTL lines with case numbers, dates, parentheses, footnote brackets, and
    embedded English phrases come out in logical order in both extraction paths."""
    path = tmp_path / "ruling_mixed.pdf"
    doc = pymupdf.open()
    page = doc.new_page(width=595, height=842)
    html = """
    <div dir="rtl" style="font-family: sans-serif; font-size: 12pt;">
    <p>פסק דין של בית המשפט העליון</p>
    <p>ע"א 1234/56 פלוני נ&#39; אלמוני (1995) [19]</p>
    <p>בעניין Axelrod v. State of Israel נגד פלוני</p>
    <p>ערעור על פסק דינו של בית המשפט המחוזי בירושלים מיום 12.05.2020.</p>
    </div>
    """
    page.insert_htmlbox(pymupdf.Rect(50, 50, 545, 500), html)
    doc.save(str(path))
    doc.close()

    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        line_layout = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)

    with pdfplumber.open(path) as pdf:
        pl_page = pdf.pages[0]
        by_lines = _extract_page_text_by_lines(pl_page, line_layout)
        col_aware = _extract_page_text_column_aware(pl_page)

    expected = [
        "פסק דין של בית המשפט העליון",
        'ע"א 1234/56 פלוני נ\' אלמוני (1995) [19]',
        "בעניין Axelrod v. State of Israel נגד פלוני",
        "ערעור על פסק דינו של בית המשפט המחוזי בירושלים מיום 12.05.2020.",
    ]
    assert by_lines is not None
    assert by_lines.splitlines() == expected
    assert col_aware.splitlines() == expected


def test_two_column_hebrew_page_reads_right_column_before_left_column(tmp_path):
    """On a two-column RTL page, the right column is read before the left column
    and all Hebrew lines are returned in logical order."""
    path = tmp_path / "two_col_hebrew.pdf"
    doc = pymupdf.open()
    page = doc.new_page(width=595, height=842)
    right_col_line = "העמודה הימנית מכילה את פסק הדין הראשון של בית המשפט"
    left_col_line = "העמודה השמאלית מכילה את הנימוקים המשלימים של השופטים"
    right_html = "<div dir=\"rtl\" style=\"font-size: 10pt;\">" + f"<p>{right_col_line}</p>" * 15 + "</div>"
    left_html = "<div dir=\"rtl\" style=\"font-size: 10pt;\">" + f"<p>{left_col_line}</p>" * 15 + "</div>"
    page.insert_htmlbox(pymupdf.Rect(315, 50, 545, 750), right_html)
    page.insert_htmlbox(pymupdf.Rect(50, 50, 280, 750), left_html)
    doc.save(str(path))
    doc.close()

    layout_doc = convert_to_txt._open_layout_document(path)
    try:
        line_layout = convert_to_txt._page_line_layout(layout_doc, 0)
    finally:
        convert_to_txt._close_layout_document(layout_doc)

    with pdfplumber.open(path) as pdf:
        pl_page = pdf.pages[0]
        by_lines = _extract_page_text_by_lines(pl_page, line_layout)
        col_aware = _extract_page_text_column_aware(pl_page)

    assert by_lines is not None
    norm_by_lines = " ".join(by_lines.split())
    assert right_col_line in norm_by_lines
    assert left_col_line in norm_by_lines
    assert norm_by_lines.index(right_col_line) < norm_by_lines.index(left_col_line)

    norm_col_aware = " ".join(col_aware.split())
    assert right_col_line in norm_col_aware
    assert left_col_line in norm_col_aware
    assert norm_col_aware.index(right_col_line) < norm_col_aware.index(left_col_line)


@pytest.mark.parametrize(
    "visual,expected",
    [
        ("Php4 acts as a negative regulatory subunit (1995) [19].", "Php4 acts as a negative regulatory subunit (1995) [19]."),
        ("ןיד קספ", "פסק דין"),
        ("ןוילעה טפשמה תיב לש ןיד קספ", "פסק דין של בית המשפט העליון"),
        ('[19] (1995) ינומלא \'נ ינולפ 1234/56 א"ע', 'ע"א 1234/56 פלוני נ\' אלמוני (1995) [19]'),
        ("קוחל 15 ,12 םיפיעס האר", "ראה סעיפים 12, 15 לחוק"),
        ('200 100 כ"הס', 'סה"כ 100 200'),
        ("דבלב 25% לש רועיש", "שיעור של 25% בלבד"),
        ("ינולפ דגנ Axelrod v. State of Israel ןיינעב", "בעניין Axelrod v. State of Israel נגד פלוני"),
        ("The ruling in ןיד קספ was affirmed.", "The ruling in פסק דין was affirmed."),
    ],
)
def test_visual_line_to_logical_cases(visual, expected):
    assert _visual_line_to_logical(visual) == expected
