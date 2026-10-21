"""Build small, exact-geometry PDFs for testing column-aware text extraction.

Uses reportlab's low-level `Canvas` (not Platypus) so column x-positions and
page margins are known constants the tests can assert against, and
`simpleSplit` for accurate per-column line wrapping (naive word-chunking can
silently let a line overflow into the gutter and defeat column detection).
"""

from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import simpleSplit
from reportlab.pdfgen.canvas import Canvas

_FONT_NAME = "Helvetica"
_FONT_SIZE = 10
_LINE_HEIGHT = 14
_TOP_MARGIN = 50
_BOTTOM_MARGIN = 50


def _wrapped_lines(text: str, col_width: float, *, repeat: int = 20) -> list[str]:
    """Wrap `text` (repeated so it's long enough to fill a full page) to `col_width`."""
    return simpleSplit(text * repeat, _FONT_NAME, _FONT_SIZE, col_width)


def _draw_column(c: Canvas, lines: list[str], x: float, page_height: float) -> None:
    """Draw `lines` top-down starting near the top margin, stopping at the bottom margin."""
    y = page_height - _TOP_MARGIN
    for line in lines:
        if y < _BOTTOM_MARGIN:
            break
        c.drawString(x, y, line)
        y -= _LINE_HEIGHT


def build_two_column_pdf(
    path: Path,
    left_text: str,
    right_text: str,
    *,
    page_size=letter,
) -> None:
    """A single page with two genuine, full-height text columns."""
    width, height = page_size
    left_x, right_x = 50, width / 2 + 15
    col_width = width / 2 - 65

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column(c, _wrapped_lines(left_text, col_width), left_x, height)
    _draw_column(c, _wrapped_lines(right_text, col_width), right_x, height)
    c.showPage()
    c.save()


def build_single_column_pdf(path: Path, text: str, *, page_size=letter) -> None:
    """A single page of ordinary full-width single-column prose."""
    width, height = page_size
    x = 72
    col_width = width - 144

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column(c, _wrapped_lines(text, col_width), x, height)
    c.showPage()
    c.save()


def build_figure_page_pdf(path: Path, caption_text: str, *, page_size=letter) -> None:
    """A page with one full-height column of prose and nothing on the other
    half (simulating a page mostly occupied by a figure/image)."""
    width, height = page_size
    x = 50
    col_width = width / 2 - 65

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column(c, _wrapped_lines(caption_text, col_width), x, height)
    c.showPage()
    c.save()


def build_two_column_table_pdf(path: Path, *, rows: int = 40, page_size=letter) -> None:
    """A page of two sparse numeric columns (1-2 short tokens per line, wide
    gap between) -- geometrically similar to a two-column gutter but with far
    too little content per line to be real prose."""
    width, height = page_size
    left_x, right_x = 60, width / 2 + 20

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    y = height - _TOP_MARGIN
    for i in range(rows):
        if y < _BOTTOM_MARGIN:
            break
        c.drawString(left_x, y, f"{i}.{i}")
        c.drawString(right_x, y, f"{i * 10}.{i}")
        y -= _LINE_HEIGHT
    c.showPage()
    c.save()


def build_two_column_prose_with_ragged_reference_column_pdf(
    path: Path,
    dense_text: str,
    *,
    page_size=letter,
) -> None:
    """A two-column page shaped like the end of a Methods section running
    beside the start of a References list: the left column is dense,
    full-height wrapped prose (like build_two_column_pdf's left column), but
    the right column has short, few-word citation-style fragments and --
    critically -- fewer total lines than the left column over the same
    vertical span (References-list line spacing doesn't align 1:1 with the
    left column's line grid). This reproduces the geometric shape that
    caused a genuine two-column research-paper page to be misdetected as
    not-two-column: low line-coverage fraction and low average
    words-per-line on the sparser side, despite being real two-column
    running text, not a numeric table.
    """
    width, height = page_size
    left_x, right_x = 50, width / 2 + 15
    col_width = width / 2 - 65

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)

    left_lines = _wrapped_lines(dense_text, col_width)
    _draw_column(c, left_lines, left_x, height)

    ref_fragments = ["Smith J.", "Jones AB, 1998.", "Lee C."]
    y = height - _TOP_MARGIN
    row = 0
    frag_i = 0
    while y >= _BOTTOM_MARGIN and row < len(left_lines):
        if row % 2 == 0:
            c.drawString(right_x, y, ref_fragments[frag_i % len(ref_fragments)])
            frag_i += 1
        y -= _LINE_HEIGHT
        row += 1

    c.showPage()
    c.save()


def build_short_gap_heading_pdf(path: Path, heading: str, body_text: str, *, page_size=letter) -> None:
    """A single-column page whose heading has a wide letter/word-spaced gap on
    only its first couple of lines, followed by ordinary single-column body
    text -- should not be mistaken for a two-column layout."""
    width, height = page_size
    x = 72
    col_width = width - 144

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, 14)
    left_half, right_half = heading[: len(heading) // 2], heading[len(heading) // 2 :]
    y = height - _TOP_MARGIN
    c.drawString(x, y, left_half)
    c.drawString(x + width / 2, y, right_half)
    y -= 20

    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column_from_y(c, _wrapped_lines(body_text, col_width), x, y)
    c.showPage()
    c.save()


def _draw_column_from_y(c: Canvas, lines: list[str], x: float, start_y: float) -> None:
    y = start_y
    for line in lines:
        if y < _BOTTOM_MARGIN:
            break
        c.drawString(x, y, line)
        y -= _LINE_HEIGHT


def build_mixed_layout_pdf(
    path: Path,
    title: str,
    left_text: str,
    right_text: str,
    *,
    page_size=letter,
) -> None:
    """A two-page PDF: page 1 single-column (title/abstract), page 2 two-column body."""
    width, height = page_size
    c = Canvas(str(path), pagesize=page_size)

    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column(c, _wrapped_lines(title, width - 144, repeat=1), 72, height)
    c.showPage()

    left_x, right_x = 50, width / 2 + 15
    col_width = width / 2 - 65
    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_column(c, _wrapped_lines(left_text, col_width), left_x, height)
    _draw_column(c, _wrapped_lines(right_text, col_width), right_x, height)
    c.showPage()
    c.save()


def build_two_column_with_full_width_line_pdf(
    path: Path,
    left_text: str,
    right_text: str,
    full_width_line: str,
    *,
    page_size=letter,
) -> None:
    """A two-column page with one line, partway down, that spans the FULL
    page width (a caption/table-row/equation straddling the gutter) --
    exercises the guard against silently dropping that line's text."""
    width, height = page_size
    left_x, right_x = 50, width / 2 + 15
    col_width = width / 2 - 65

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    left_lines = _wrapped_lines(left_text, col_width)
    right_lines = _wrapped_lines(right_text, col_width)
    mid_row = len(left_lines) // 2

    y = height - _TOP_MARGIN
    for i, line in enumerate(left_lines):
        if y < _BOTTOM_MARGIN:
            break
        if i == mid_row:
            c.drawString(left_x, y, full_width_line)
        else:
            c.drawString(left_x, y, line)
        y -= _LINE_HEIGHT

    y = height - _TOP_MARGIN
    for i, line in enumerate(right_lines):
        if y < _BOTTOM_MARGIN:
            break
        if i != mid_row:  # the full-width line already occupies this row
            c.drawString(right_x, y, line)
        y -= _LINE_HEIGHT

    c.showPage()
    c.save()


def build_sparse_pdf(path: Path, text: str = "Hi.", *, page_size=letter) -> None:
    """A near-blank page with only a couple of words."""
    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    c.drawString(72, page_size[1] - 72, text)
    c.showPage()
    c.save()


def _draw_words_glued(
    c: Canvas,
    words: list[str],
    x0: float,
    max_x: float,
    y0: float,
    *,
    inter_word_gap: float,
    space_every_n_words: int | None = None,
) -> None:
    """Draw `words` left-to-right via individual `drawString` calls, cursor
    advanced by each word's rendered width plus `inter_word_gap` points --
    with NO literal space character placed in the content stream, EXCEPT:
    when `space_every_n_words` is set, every Nth word is joined to the word
    that follows it with a real ' ' character inside the same `drawString`
    call, producing a fixture with a realistic sparse-but-nonzero space
    ratio (real glued documents aren't uniformly glued -- occasional runs
    keep literal spaces) instead of the default's strict all-or-nothing 0%.
    Wraps to a new line when a chunk would cross `max_x`.
    """
    x, y = x0, y0
    i, count, n = 0, 0, len(words)
    while i < n:
        chunk = words[i]
        i += 1
        if space_every_n_words and count > 0 and count % space_every_n_words == 0 and i < n:
            chunk = chunk + " " + words[i]
            i += 1
        count += 1
        chunk_width = c.stringWidth(chunk, _FONT_NAME, _FONT_SIZE)
        if x > x0 and x + chunk_width > max_x:
            x, y = x0, y - _LINE_HEIGHT
            if y < _BOTTOM_MARGIN:
                break
        c.drawString(x, y, chunk)
        x += chunk_width + inter_word_gap


def build_glued_word_pdf(
    path: Path,
    words: list[str],
    *,
    inter_word_gap: float = 1.2,
    space_every_n_words: int | None = None,
    page_size=letter,
) -> None:
    """A page where every word is drawn via its own `drawString` call,
    positioned edge-to-edge plus a small explicit `inter_word_gap` pt --
    with no literal space character anywhere in the content stream (unless
    `space_every_n_words` is set -- see `_draw_words_glued`).
    Reproduces PDF producers that place words via pure glyph positioning
    (observed on certain Cell Press/Elsevier exports), which is the
    precondition for pdfplumber's default x_tolerance=3 to glue adjacent
    words together whenever the true gap is smaller than that.
    """
    width, height = page_size
    x_margin, max_x = 72, width - 72
    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    _draw_words_glued(
        c,
        words,
        x_margin,
        max_x,
        height - _TOP_MARGIN,
        inter_word_gap=inter_word_gap,
        space_every_n_words=space_every_n_words,
    )
    c.showPage()
    c.save()


def build_two_column_glued_pdf(
    path: Path,
    left_words: list[str],
    right_words: list[str],
    *,
    inter_word_gap: float = 1.2,
    page_size=letter,
) -> None:
    """A genuine two-column page where words within each column are placed
    via `drawString` with no literal space glyphs -- exercises composition
    of the column-gutter detection with the word-gap-derived tolerance fix.
    Uses the same column geometry as `build_two_column_pdf`.
    """
    width, height = page_size
    left_x, right_x = 50, width / 2 + 15
    max_left_x, max_right_x = width / 2 - 15, width - 50

    c = Canvas(str(path), pagesize=page_size)
    c.setFont(_FONT_NAME, _FONT_SIZE)
    y0 = height - _TOP_MARGIN
    _draw_words_glued(c, left_words, left_x, max_left_x, y0, inter_word_gap=inter_word_gap)
    _draw_words_glued(c, right_words, right_x, max_right_x, y0, inter_word_gap=inter_word_gap)
    c.showPage()
    c.save()
