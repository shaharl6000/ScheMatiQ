"""Tests for PDFFigures 2.0-based figure extraction.

Split by what each layer needs:
- Heading/paragraph matching and JSON reshaping are pure functions, tested
  directly against hand-built dicts shaped like PDFFigures2's verified JSON
  output — no subprocess, no Java.
- The commit_document_to_documents_dir() wiring (extract-before-delete,
  persist-after-move, tmp-dir cleanup on failure) is tested via monkeypatched
  extract_figures/persist_figures — no subprocess, no Java.
- The real end-to-end subprocess run is gated behind _pdffigures2_available()
  and skips cleanly when the jar/java aren't installed (the default in dev).
"""

import shutil
from pathlib import Path

import pymupdf
import pytest

from app.models.figures import ExtractedFigure, FigureBBox
from app.services import document_preprocessor as dp
from app.services.figure_extraction_service import (
    _FigureBuildResult,
    _build_figure_records,
    _fallback_crop_rect,
    _flatten_positioned_items,
    _is_region_degenerate,
    _match_figure_context,
    _render_fallback_crops,
    extract_figures,
)

LEGAL_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "legal_corpus"
FIGURE_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "figures"


def _pdffigures2_available() -> bool:
    from app.core.config import PDFFIGURES2_JAR_PATH

    return shutil.which("java") is not None and Path(PDFFIGURES2_JAR_PATH).is_file()


@pytest.fixture
def pending_dir(tmp_path):
    work = tmp_path / "pending_documents"
    work.mkdir()
    return work


# ---------------------------------------------------------------------------
# Pure logic: heading/paragraph matching + JSON reshaping
# ---------------------------------------------------------------------------

def _box(x1, y1, x2, y2):
    return {"x1": x1, "y1": y1, "x2": x2, "y2": y2}


def test_flatten_positioned_items_sorts_by_page_then_y():
    sections = [
        {
            "title": {"text": "2. Discussion", "page": 1, "region": _box(0, 50, 100, 60)},
            "paragraphs": [{"text": "second page para", "page": 1, "region": _box(0, 70, 100, 90)}],
        },
        {
            "title": {"text": "1. Results", "page": 0, "region": _box(0, 100, 100, 110)},
            "paragraphs": [{"text": "first page para", "page": 0, "region": _box(0, 120, 100, 140)}],
        },
    ]
    items = _flatten_positioned_items(sections)
    assert [it["text"] for it in items] == [
        "1. Results", "first page para", "2. Discussion", "second page para",
    ]


def test_match_figure_context_finds_nearest_heading_and_windowed_paragraphs():
    items = _flatten_positioned_items([
        {
            "title": {"text": "1. Results", "page": 0, "region": _box(0, 100, 100, 110)},
            "paragraphs": [
                {"text": "para before A", "page": 0, "region": _box(0, 120, 100, 140)},
                {"text": "para before B", "page": 0, "region": _box(0, 150, 100, 170)},
                {"text": "para after A", "page": 0, "region": _box(0, 300, 100, 320)},
            ],
        },
    ])
    # Figure sits between "para before B" (y1=150) and "para after A" (y1=300).
    heading, context = _match_figure_context(page=0, y1=200, items=items)
    assert heading == "1. Results"
    before = [c for c in context if c.position == "before"]
    after = [c for c in context if c.position == "after"]
    assert [c.text for c in before] == ["para before B", "para before A"]
    assert before[0].distance == 1
    assert [c.text for c in after] == ["para after A"]


def test_match_figure_context_no_heading_before_any_title():
    items = _flatten_positioned_items([
        {
            "title": {"text": "1. Results", "page": 0, "region": _box(0, 500, 100, 510)},
            "paragraphs": [],
        },
    ])
    heading, context = _match_figure_context(page=0, y1=10, items=items)
    assert heading is None
    assert context == []


def test_build_figure_records_reshapes_savedfigure_json():
    raw = {
        "figures": [
            {
                "name": "1",
                "figType": "Figure",
                "page": 2,
                "caption": "Figure 1: A sample chart.",
                "imageText": ["A", "B"],
                "captionBoundary": _box(50, 400, 400, 420),
                "regionBoundary": _box(50, 200, 400, 390),
                "renderURL": "/tmp/xyz/paper-Figure1-1.png",
                "renderDpi": 150,
            },
        ],
        "abstractText": None,
        "sections": [
            {
                "title": {"text": "1. Results", "page": 2, "region": _box(0, 100, 100, 110)},
                "paragraphs": [
                    {"text": "intro paragraph", "page": 2, "region": _box(0, 120, 100, 140)},
                ],
            },
        ],
    }
    figures = _build_figure_records(raw, source_document="paper.pdf")
    assert len(figures) == 1
    fig = figures[0]
    assert fig.figure_id == "fig001"
    assert fig.source_document == "paper.pdf"
    assert fig.figure_label == "Figure"
    assert fig.page_no == 2
    assert fig.caption == "Figure 1: A sample chart."
    assert fig.image_text == ["A", "B"]
    assert fig.image_filename == "paper-Figure1-1.png"
    assert fig.region_bbox.left == 50 and fig.region_bbox.page_no == 2
    assert fig.nearby_heading == "1. Results"
    assert fig.context_paragraphs[0].text == "intro paragraph"
    # A well-formed region (comparable width to its caption) is trusted as-is.
    assert fig.region_source == "detected"
    # Forward-compat fields start empty for a future vision-LLM step.
    assert fig.vision_description is None


def test_build_figure_records_empty_figures_list():
    assert _build_figure_records({"figures": [], "sections": []}, source_document="x.pdf") == []


# ---------------------------------------------------------------------------
# Degenerate region detection + caption-anchored fallback
#
# Regression coverage for a real bug found in production: an NIH-PA "Author
# Manuscript" PDF where PDFFigures2's region_bbox for every figure was a
# narrow vertical sliver (matching the page's rotated watermark sidebar, not
# the actual figure) while caption_bbox stayed correctly detected. The saved
# PNG ended up showing ~1/4 of the real figure.
# ---------------------------------------------------------------------------

def test_is_region_degenerate_flags_real_nihms_sliver():
    # Exact numbers from the manifest that surfaced this bug (fig002, page 12).
    region = FigureBBox(left=510.24, top=61.92, right=541.44, bottom=338.4, page_no=12)
    caption = FigureBBox(left=150.0, top=352.93, right=520.2, bottom=496.12, page_no=12)
    assert _is_region_degenerate(region, caption) is True


def test_is_region_degenerate_accepts_plausible_region():
    region = FigureBBox(left=50, top=200, right=400, bottom=390, page_no=2)
    caption = FigureBBox(left=50, top=400, right=400, bottom=420, page_no=2)
    assert _is_region_degenerate(region, caption) is False


def test_is_region_degenerate_true_when_region_missing_but_caption_present():
    caption = FigureBBox(left=50, top=400, right=400, bottom=420, page_no=2)
    assert _is_region_degenerate(None, caption) is True


def test_is_region_degenerate_false_when_both_missing():
    assert _is_region_degenerate(None, None) is False


def test_fallback_crop_rect_uses_padded_caption_width_and_preceding_item_as_top_bound():
    items = _flatten_positioned_items([
        {
            "title": {"text": "Results", "page": 12, "region": _box(0, 300, 100, 310)},
            "paragraphs": [],
        },
    ])
    caption = FigureBBox(left=150.0, top=352.93, right=520.2, bottom=496.12, page_no=12)
    left, top, right, bottom = _fallback_crop_rect(page=12, caption=caption, items=items)
    assert (left, right) == (caption.left - 20.0, caption.right + 20.0)  # padded, not exact caption edges
    assert top == 300  # bounded by the preceding heading's y1, not the page top
    assert bottom == caption.top - 4.0


def test_fallback_crop_rect_honors_distant_preceding_item_without_clamping():
    """Regression test: a real figure taller than _MAX_FALLBACK_HEIGHT (650pt)
    must not be clamped just because it's tall — only the "no preceding item
    at all" case should ever use the height cap. The bug this guards against:
    the original implementation applied the cap unconditionally via
    max(preceding_y, caption.top - _MAX_FALLBACK_HEIGHT), silently cropping
    the top off any figure taller than the cap even when the real preceding
    boundary was known and much further away.
    """
    items = _flatten_positioned_items([
        {
            "title": {"text": "Results", "page": 12, "region": _box(0, 50, 100, 60)},
            "paragraphs": [],
        },
    ])
    caption = FigureBBox(left=150.0, top=750.0, right=520.0, bottom=780.0, page_no=12)
    left, top, right, bottom = _fallback_crop_rect(page=12, caption=caption, items=items)
    assert top == 50  # the true preceding_y (700pt away), not caption.top - 650


def test_fallback_crop_rect_caps_height_when_no_preceding_item():
    caption = FigureBBox(left=150.0, top=1000.0, right=520.0, bottom=1050.0, page_no=0)
    left, top, right, bottom = _fallback_crop_rect(page=0, caption=caption, items=[])
    assert top == caption.top - 650.0  # capped by _MAX_FALLBACK_HEIGHT, not pulled to page top


# ---------------------------------------------------------------------------
# Cross-column region/caption mismatch
#
# Regression coverage for a second, harder real bug: a two-column paper
# (Earl et al., "CD45 Glycosylation...") where PDFFigures2 cross-wired a
# Table's caption (right column) to a region actually sitting in the left
# column (near an unrelated Figure's diagram), and vice versa for that
# Figure. Both regions individually passed the width/aspect check (neither
# is a sliver), so a second signal — the region and caption disagreeing on
# which column they're in — was needed to catch this.
# ---------------------------------------------------------------------------

def test_is_region_degenerate_flags_cross_column_region():
    # Exact numbers from the manifest that surfaced this bug: Table 1's region
    # sits in the left column while its caption is in the right column.
    region = FigureBBox(left=43.2, top=69.6, right=277.44, bottom=248.16, page_no=1)
    caption = FigureBBox(left=295.88, top=72.92, right=543.99, bottom=87.21, page_no=1)
    items = _flatten_positioned_items([
        {"title": {"text": "L1", "page": 1, "region": _box(40, 100, 200, 110)}, "paragraphs": [
            {"text": "left para", "page": 1, "region": _box(40, 400, 200, 410)},
        ]},
        {"title": {"text": "R1", "page": 1, "region": _box(300, 95, 500, 105)}, "paragraphs": [
            {"text": "right para", "page": 1, "region": _box(300, 300, 500, 310)},
        ]},
    ])
    assert _is_region_degenerate(region, caption, page=1, items=items) is True


def test_is_region_degenerate_trusts_plausible_region_in_same_column():
    # Same shape/ratio as a real case, but region and caption agree on column
    # this time — the cross-column signal must not false-positive here.
    region = FigureBBox(left=300, top=90, right=520, bottom=280, page_no=1)
    caption = FigureBBox(left=295.88, top=72.92, right=543.99, bottom=87.21, page_no=1)
    items = _flatten_positioned_items([
        {"title": {"text": "L1", "page": 1, "region": _box(40, 100, 200, 110)}, "paragraphs": [
            {"text": "left para", "page": 1, "region": _box(40, 400, 200, 410)},
        ]},
        {"title": {"text": "R1", "page": 1, "region": _box(300, 95, 500, 105)}, "paragraphs": [
            {"text": "right para", "page": 1, "region": _box(300, 300, 500, 310)},
        ]},
    ])
    assert _is_region_degenerate(region, caption, page=1, items=items) is False


def test_is_region_degenerate_skips_column_check_when_items_not_given():
    # Backward-compat: callers that don't pass page/items (e.g. the simple
    # tests above) only get the width/aspect checks, same as before this.
    region = FigureBBox(left=43.2, top=69.6, right=277.44, bottom=248.16, page_no=1)
    caption = FigureBBox(left=295.88, top=72.92, right=543.99, bottom=87.21, page_no=1)
    assert _is_region_degenerate(region, caption) is False


def test_fallback_crop_rect_table_anchors_below_caption_in_same_column():
    """Tables have their caption ABOVE the content (opposite of Figures), and
    on a multi-column page the "next item" must come from the caption's own
    column — a left-column item positioned right after the caption in raw
    y-order must not be used just because it's numerically next.
    """
    items = _flatten_positioned_items([
        {"title": {"text": "Results", "page": 1, "region": _box(40, 100, 200, 110)}, "paragraphs": [
            {"text": "left column text, ignore", "page": 1, "region": _box(40, 90, 200, 100)},
            {"text": "more left column text", "page": 1, "region": _box(40, 600, 200, 610)},
        ]},
        {"title": {"text": "Next section", "page": 1, "region": _box(300, 500, 500, 510)}, "paragraphs": []},
    ])
    caption = FigureBBox(left=295.88, top=72.92, right=543.99, bottom=87.21, page_no=1)
    left, top, right, bottom = _fallback_crop_rect(page=1, caption=caption, items=items, label="Table")
    assert top == caption.bottom + 4.0
    assert bottom == 500 - 4.0  # the right-column heading, not the left-column text at y=90


def test_build_figure_records_fixes_cross_column_table_and_figure():
    """End-to-end regression using the real coordinates from the Earl et al.
    two-column paper: PDFFigures2 cross-wired Table 1's region to the left
    column (near Figure 1's own diagram) and Figure 1's region to the right
    column, even though both individually looked plausible-shaped.
    """
    raw = {
        "figures": [
            {
                "name": "1", "figType": "Table", "page": 1,
                "caption": "Table 1 Expression of CD45 isoforms and selected glycans on T-cell subsets",
                "imageText": [],
                "captionBoundary": _box(295.88, 72.92, 543.99, 87.21),
                "regionBoundary": _box(43.2, 69.6, 277.44, 248.16),  # wrong column
                "renderURL": "/tmp/xyz/input-Table1-1.png",
                "renderDpi": 150,
            },
            {
                "name": "2", "figType": "Figure", "page": 1,
                "caption": "Figure 1 Model of CD45RABC, the largest isoform of CD45.",
                "imageText": [],
                "captionBoundary": _box(35.9, 258.36, 284.0, 309.1),
                "regionBoundary": _box(302.4, 308.64, 579.84, 335.52),  # wrong column
                "renderURL": "/tmp/xyz/input-Figure1-1.png",
                "renderDpi": 150,
            },
        ],
        "sections": [
            {
                "title": {"text": "Results", "page": 1, "region": _box(40, 100, 200, 110)},
                "paragraphs": [
                    {"text": "left column body text", "page": 1, "region": _box(40, 350, 200, 360)},
                ],
            },
            {
                "paragraphs": [
                    {"text": "O-glycans N-glycans", "page": 1, "region": _box(300, 400, 500, 410)},
                    {"text": "CD45 isoform", "page": 1, "region": _box(300, 600, 500, 610)},
                ],
            },
        ],
    }
    figures = _build_figure_records(raw, source_document="earl-cd45.pdf")
    assert len(figures) == 2
    table, figure = figures

    assert table.region_source == "caption_fallback"
    # Table convention: content below the caption, in the caption's own (right) column.
    assert table.region_bbox.left == 295.88 - 20.0
    assert table.region_bbox.top == 87.21 + 4.0
    assert table.region_bbox.bottom == 400 - 4.0

    assert figure.region_source == "caption_fallback"
    # Figure convention: content above the caption, in the caption's own (left) column.
    assert figure.region_bbox.left == 35.9 - 20.0
    assert figure.region_bbox.bottom == 258.36 - 4.0
    assert figure.region_bbox.top == 100.0


def test_build_figure_records_falls_back_for_degenerate_region():
    raw = {
        "figures": [
            {
                "name": "2",
                "figType": "Figure",
                "page": 12,
                "caption": "Figure 2. T cells decorate their glycoproteins...",
                "imageText": [],
                "captionBoundary": _box(150.0, 352.93, 520.2, 496.12),
                "regionBoundary": _box(510.24, 61.92, 541.44, 338.4),
                "renderURL": "/tmp/xyz/input-Figure2-1.png",
                "renderDpi": 150,
            },
        ],
        "sections": [
            {
                "title": {"text": "Glycosylation", "page": 12, "region": _box(0, 300, 100, 310)},
                "paragraphs": [],
            },
        ],
    }
    figures = _build_figure_records(raw, source_document="nihms-537414.pdf")
    assert len(figures) == 1
    fig = figures[0]
    assert fig.region_source == "caption_fallback"
    assert fig.image_filename == "input-Figure2-1.png"  # same filename PDFFigures2 used, so it gets overwritten
    assert (fig.region_bbox.left, fig.region_bbox.right) == (150.0 - 20.0, 520.2 + 20.0)
    assert fig.region_bbox.bottom == 352.93 - 4.0
    # Heading/context matched against the caption's position, not the bad region's.
    assert fig.nearby_heading == "Glycosylation"


def test_render_fallback_crops_overwrites_image_for_flagged_figures(tmp_path):
    pdf_path = tmp_path / "doc.pdf"
    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.draw_rect(pymupdf.Rect(100, 100, 400, 300), color=(1, 0, 0), fill=(1, 0, 0))
    doc.save(str(pdf_path))
    doc.close()

    fig = ExtractedFigure(
        figure_id="fig001",
        source_document="doc.pdf",
        figure_label="Figure",
        page_no=0,
        region_bbox=FigureBBox(left=100, top=100, right=400, bottom=300, page_no=0),
        image_filename="fallback-fig001.png",
        region_source="caption_fallback",
    )

    tmp_dir = tmp_path / "staging"
    tmp_dir.mkdir()
    _render_fallback_crops([fig], pdf_path, tmp_dir)

    out = tmp_dir / "fallback-fig001.png"
    assert out.exists() and out.stat().st_size > 0


def test_render_fallback_crops_noop_when_nothing_flagged(tmp_path):
    fig = ExtractedFigure(
        figure_id="fig001",
        source_document="doc.pdf",
        figure_label="Figure",
        page_no=0,
        image_filename="whatever.png",
        region_source="detected",
    )
    tmp_dir = tmp_path / "staging"
    tmp_dir.mkdir()
    # Would raise if it tried to open a nonexistent PDF — proves it's a no-op.
    _render_fallback_crops([fig], tmp_path / "does-not-exist.pdf", tmp_dir)
    assert list(tmp_dir.iterdir()) == []


# ---------------------------------------------------------------------------
# extract_figures(): disabled / unavailable degrades to None without a subprocess
# ---------------------------------------------------------------------------

def test_extract_figures_returns_none_when_disabled(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.figure_extraction_service.ENABLE_FIGURE_EXTRACTION", False)
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"
    assert extract_figures(pdf, documents_dir, source_document="doc.pdf") is None


def test_extract_figures_returns_none_when_jar_missing(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.figure_extraction_service.ENABLE_FIGURE_EXTRACTION", True)
    monkeypatch.setattr("app.services.figure_extraction_service._jar_resolved", False)
    # _jar_path itself must also be monkeypatched (not just _jar_resolved): once
    # _get_jar_path() re-resolves below, it sets the real module-level _jar_path
    # to None as a side effect (PDFFIGURES2_JAR_PATH points nowhere). monkeypatch
    # only restores attributes it was explicitly told to track, so without this
    # line _jar_path stays permanently None for the rest of the test session —
    # a real bug that silently broke every later real end-to-end test in a full
    # suite run (they passed individually only because nothing before them had
    # forced a re-resolution).
    monkeypatch.setattr("app.services.figure_extraction_service._jar_path", None)
    monkeypatch.setattr("app.services.figure_extraction_service.PDFFIGURES2_JAR_PATH", str(tmp_path / "missing.jar"))
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"
    assert extract_figures(pdf, documents_dir, source_document="doc.pdf") is None


# ---------------------------------------------------------------------------
# preprocess_uploaded_file() wiring, via monkeypatched extract/persist
#
# This is the function every upload path actually funnels through — including
# the initial-upload route (add_documents -> _preprocess_and_record ->
# preprocess_uploaded_file directly), which never calls
# commit_document_to_documents_dir() at all. That was a real bug found during
# development: the hook originally lived only in commit_document_to_documents_dir(),
# so fresh uploads silently never got figures (the PDF was already converted
# to .txt and deleted by the time that function ever saw the file).
# ---------------------------------------------------------------------------

def test_preprocess_uploaded_file_extracts_before_convert_and_persists_after(pending_dir, tmp_path, monkeypatch):
    calls = {}

    def fake_extract(pdf_path, documents_dir, *, source_document):
        calls["extract"] = (pdf_path.name, documents_dir, source_document)
        return "SENTINEL_BUILD_RESULT"

    def fake_persist(build_result, documents_dir, doc_stem, *, source_document):
        calls["persist"] = (build_result, documents_dir, doc_stem, source_document)
        return documents_dir / "figures" / doc_stem

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", fake_extract)
    monkeypatch.setattr("app.services.figure_extraction_service.persist_figures", fake_persist)

    source = LEGAL_FIXTURES_DIR / "seattle_homeland.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")
    dest = pending_dir / "seattle_homeland.pdf"
    shutil.copy2(source, dest)

    documents_dir = tmp_path / "documents"
    result = dp.preprocess_uploaded_file(dest, original_filename="seattle_homeland.pdf", documents_dir=documents_dir)

    assert result.success
    assert calls["extract"] == ("seattle_homeland.pdf", documents_dir, "seattle_homeland.pdf")
    assert calls["persist"] == ("SENTINEL_BUILD_RESULT", documents_dir, result.output_path.stem, "seattle_homeland.pdf")


def test_preprocess_uploaded_file_defaults_figures_dir_to_source_parent(pending_dir, monkeypatch):
    """When no documents_dir is given (defensive fallback — every real caller
    now passes one), figures stage next to the source file instead."""
    calls = {}

    def fake_extract(pdf_path, documents_dir, *, source_document):
        calls["documents_dir"] = documents_dir
        return None  # disabled/unavailable — nothing further to mock

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", fake_extract)

    source = LEGAL_FIXTURES_DIR / "seattle_homeland.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")
    dest = pending_dir / "seattle_homeland.pdf"
    shutil.copy2(source, dest)

    dp.preprocess_uploaded_file(dest, original_filename="seattle_homeland.pdf")
    assert calls["documents_dir"] == pending_dir


def test_preprocess_uploaded_file_cleans_up_figure_tmp_dir_when_conversion_fails(pending_dir, tmp_path, monkeypatch):
    fake_tmp = tmp_path / "fake_pdffigures2_tmp"
    fake_tmp.mkdir()

    def fake_extract(pdf_path, documents_dir, *, source_document):
        return _FigureBuildResult(figures=[], tmp_dir=fake_tmp, status="ok", error=None)

    def fake_convert_file(*a, **k):
        return False, "forced failure for test"

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", fake_extract)
    monkeypatch.setattr(dp, "convert_file", fake_convert_file)

    dest = pending_dir / "whatever.pdf"
    dest.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"

    result = dp.preprocess_uploaded_file(dest, original_filename="whatever.pdf", documents_dir=documents_dir)

    assert not result.success
    assert not fake_tmp.exists()  # cleaned up rather than leaked


def test_commit_pdf_forwards_documents_dir_to_preprocess(pending_dir, tmp_path, monkeypatch):
    """commit_document_to_documents_dir() itself no longer calls
    extract_figures/persist_figures directly — it just needs to forward its
    documents_dir through to preprocess_uploaded_file(), which does."""
    captured = {}

    def fake_preprocess(source_path, *, worker_id=None, original_filename=None, documents_dir=None):
        captured["documents_dir"] = documents_dir
        return dp.ExtractionResult(
            output_path=source_path,
            display_name=source_path.name,
            method="pdf",
            status="extracted from pdf",
            success=True,
            original_filename=original_filename,
        )

    monkeypatch.setattr(dp, "preprocess_uploaded_file", fake_preprocess)

    dest = pending_dir / "whatever.pdf"
    dest.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"

    dp.commit_document_to_documents_dir(dest, documents_dir)
    assert captured["documents_dir"] == documents_dir


def test_commit_non_pdf_never_touches_figure_extraction(pending_dir, tmp_path, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("extract_figures must not be called for non-PDF uploads")

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", boom)

    dest = pending_dir / "notes.txt"
    dest.write_text("hello world", encoding="utf-8")
    documents_dir = tmp_path / "documents"

    result = dp.commit_document_to_documents_dir(dest, documents_dir)
    assert result is not None


# ---------------------------------------------------------------------------
# Real end-to-end run (skipped when pdffigures2 isn't installed — the default
# in this dev environment; see backend/docs/figure-extraction.md for setup)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _pdffigures2_available(), reason="pdffigures2 jar/java not available")
def test_figure_extraction_end_to_end_produces_manifest(pending_dir, tmp_path):
    source = FIGURE_FIXTURES_DIR / "sample_with_figure.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")
    dest = pending_dir / "sample_with_figure.pdf"
    shutil.copy2(source, dest)

    documents_dir = tmp_path / "documents"
    result = dp.commit_document_to_documents_dir(dest, documents_dir)
    assert result is not None

    manifest_path = documents_dir / "figures" / result.stem / "manifest.json"
    assert manifest_path.exists()

    import json
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ok"
    assert manifest["figure_count"] >= 1
    assert "Figure 1" in manifest["figures"][0]["caption"]


@pytest.mark.skipif(not _pdffigures2_available(), reason="pdffigures2 jar/java not available")
def test_figure_extraction_handles_non_ascii_filename(pending_dir, tmp_path):
    """Regression test: on Windows, the JVM decodes argv/filenames using the
    system's legacy codepage, not UTF-8. A non-ASCII character in the PDF's
    name (e.g. a unicode hyphen U+2010, as opposed to plain ASCII "-") used
    to get mangled before Java could find the file, so pdffigures2 exited 1
    with "is not a PDF file" even though the file existed. Fixed by staging
    the PDF under a fixed ASCII-only filename before invoking the jar.
    """
    source = FIGURE_FIXTURES_DIR / "sample_with_figure.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")
    tricky_name = "Structural study of the O‐linked sugar chains.pdf"  # U+2010, not "-"
    dest = pending_dir / tricky_name
    shutil.copy2(source, dest)

    documents_dir = tmp_path / "documents"
    result = dp.commit_document_to_documents_dir(dest, documents_dir)
    assert result is not None

    manifest_path = documents_dir / "figures" / result.stem / "manifest.json"
    assert manifest_path.exists()

    import json
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ok"
    assert manifest["figure_count"] >= 1


@pytest.mark.skipif(not _pdffigures2_available(), reason="pdffigures2 jar/java not available")
def test_figure_extraction_handles_typographic_output_characters(tmp_path):
    """Regression test: PDFFigures2 writes its output JSON via `new
    PrintWriter(file)` with no explicit charset (FigureRenderer.scala), so it
    defaults to the JVM's platform charset — on Windows, the system codepage,
    not UTF-8. Any typographic character in the extracted text (smart quotes,
    en/em dashes — common in real papers, e.g. "model's" vs "model's")
    then got written as non-UTF-8 bytes, and read_text(encoding="utf-8")
    raised UnicodeDecodeError, surfacing as status="failed" with a "codec
    can't decode" error. Fixed via -Dfile.encoding=UTF-8 on the java
    invocation. Confirmed against real arXiv papers during development.

    Calls extract_figures() directly rather than routing through
    commit_document_to_documents_dir(): whether PDFFigures2's caption-detector
    recognizes a given caption is a separate, font/layout-sensitive concern
    (flaky on a hand-built PDF) from what this test actually guards — that a
    non-ASCII JSON *parses* without crashing. The fixture's body text already
    contains the typographic characters (en dash, right single quote) either
    way, so parsing exercises the fixed code path regardless of whether a
    figure region happens to be detected.
    """
    source = FIGURE_FIXTURES_DIR / "sample_with_typographic_chars.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")

    documents_dir = tmp_path / "documents"
    result = extract_figures(source, documents_dir, source_document=source.name)
    assert result is not None
    assert result.status == "ok", result.error
    assert result.error is None


@pytest.mark.skipif(not _pdffigures2_available(), reason="pdffigures2 jar/java not available")
def test_figure_extraction_captionless_image_yields_zero_figures(pending_dir, tmp_path):
    source = LEGAL_FIXTURES_DIR / "seattle_homeland.pdf"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")
    dest = pending_dir / "seattle_homeland.pdf"
    shutil.copy2(source, dest)

    documents_dir = tmp_path / "documents"
    result = dp.commit_document_to_documents_dir(dest, documents_dir)
    assert result is not None
    assert not (documents_dir / "figures" / result.stem).exists()
