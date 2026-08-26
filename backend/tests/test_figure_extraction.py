"""Tests for Docling-based figure extraction.

Split by what each layer needs:
- Docling-document -> ExtractedFigure record mapping (_build_figure_records)
  is tested against a fake document object exposing iterate_items() with
  fake PictureItem/TableItem-like elements — no real Docling models loaded,
  fast and offline.
- The commit_document_to_documents_dir() wiring (extract-before-delete,
  persist-after-move, tmp-dir cleanup on failure) is tested via monkeypatched
  extract_figures/persist_figures — no Docling models loaded.
- The real end-to-end conversion is gated behind RUN_DOCLING_MODEL_TESTS=1
  (opt-in, not auto-detected) since Docling being importable does not mean
  its multi-hundred-MB model weights are already downloaded/cached — running
  it unconditionally would make the suite non-deterministically slow/networked.
"""

import json
import os
import shutil
from pathlib import Path

import pytest
from docling_core.types.doc import BoundingBox, CoordOrigin
from docling_core.types.doc.base import Size

from app.services import document_preprocessor as dp
from app.services import figure_extraction_service as svc
from app.services.figure_extraction_service import (
    _PANEL_MERGE_GAP_PT,
    _FigureBuildResult,
    _build_figure_records,
    extract_figures,
)


def _bbox(l, t, r, b):
    return BoundingBox(l=l, t=t, r=r, b=b, coord_origin=CoordOrigin.BOTTOMLEFT)

LEGAL_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "legal_corpus"
FIGURE_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "figures"


def _docling_models_enabled() -> bool:
    return os.environ.get("RUN_DOCLING_MODEL_TESTS") == "1"


@pytest.fixture
def pending_dir(tmp_path):
    work = tmp_path / "pending_documents"
    work.mkdir()
    return work


# ---------------------------------------------------------------------------
# _build_figure_records(): Docling document -> ExtractedFigure mapping
#
# Fakes stand in for docling_core's PictureItem/TableItem so this exercises
# the real isinstance()-based branching in _build_figure_records() without
# needing a real converted document.
# ---------------------------------------------------------------------------

_next_default_bbox_offset = [0]


class _FakeProv:
    def __init__(self, page_no, bbox=None):
        self.page_no = page_no
        if bbox is not None:
            self.bbox = bbox
        else:
            # Each unspecified bbox gets a unique, far-apart default so
            # unrelated fake items in tests that don't care about merging
            # never accidentally read as adjacent panels of one figure.
            _next_default_bbox_offset[0] += 100_000
            off = _next_default_bbox_offset[0]
            self.bbox = _bbox(0, 100 + off, 50, 0 + off)


class _FakeImage:
    def __init__(self, fail=False):
        self.fail = fail

    def save(self, path, fmt=None):
        if self.fail:
            raise OSError("simulated image save failure")
        Path(path).write_bytes(b"PNGDATA")


class _FakeDoclingItem:
    """Base for the fakes below. NOT shared via subclassing between
    _FakePictureItem/_FakeTableItem — they must be siblings, not parent/child,
    since the real PictureItem/TableItem aren't in an isinstance() relationship
    with each other either, and _build_figure_records() branches on isinstance
    order (Picture checked first)."""

    def __init__(self, *, caption="", image=None, page_no=1, has_prov=True, self_ref=None, bbox=None):
        self.prov = [_FakeProv(page_no, bbox=bbox)] if has_prov else []
        self._caption = caption
        self._image = image if image is not None else _FakeImage()
        self.self_ref = self_ref

    def caption_text(self, doc):
        return self._caption

    def get_image(self, doc):
        return self._image


class _FakePictureItem(_FakeDoclingItem):
    pass


class _FakeTableItem(_FakeDoclingItem):
    pass


class _FakePilImage:
    """Stands in for a rendered page bitmap. .crop() records the box it was
    asked to crop and returns a saveable fake image, mirroring what
    element.get_image() returns elsewhere in these fakes."""

    def __init__(self):
        self.crop_calls = []

    def crop(self, box):
        self.crop_calls.append(box)
        return _FakeImage()


class _FakePageImage:
    def __init__(self, pil_image, size):
        self.pil_image = pil_image
        self.size = size


class _FakePage:
    def __init__(self, size, image):
        self.size = size
        self.image = image


class _FakeDocument:
    def __init__(self, items, pages=None):
        self._items = items
        self.pages = pages or {}

    def iterate_items(self):
        for item in self._items:
            yield item, 0


def _fake_page_with_image(page_size=(600, 800), image_size=(600, 800)):
    """A page whose full-bitmap crop path is exercised (generate_page_images=True
    in production). Returns (page, pil_image) so tests can inspect crop_calls."""
    size = Size(width=page_size[0], height=page_size[1])
    pil_image = _FakePilImage()
    page_image = _FakePageImage(pil_image, Size(width=image_size[0], height=image_size[1]))
    return _FakePage(size, page_image), pil_image


@pytest.fixture
def fake_docling_items(monkeypatch):
    """Point figure_extraction_service's isinstance checks at the fakes above."""
    monkeypatch.setattr(svc, "PictureItem", _FakePictureItem)
    monkeypatch.setattr(svc, "TableItem", _FakeTableItem)


def test_build_figure_records_builds_captioned_figure_and_table(fake_docling_items, tmp_path):
    picture = _FakePictureItem(
        caption="Figure 1: A sample chart.", page_no=2, self_ref="#/pictures/0",
    )
    table = _FakeTableItem(
        caption="Table 1: Results summary.", page_no=3, self_ref="#/tables/0",
    )
    document = _FakeDocument([picture, table])

    figures = _build_figure_records(document, tmp_path, source_document="paper.pdf")

    assert len(figures) == 2
    fig, tbl = figures

    assert fig.figure_id == "fig001"
    assert fig.source_document == "paper.pdf"
    assert fig.figure_label == "Figure"
    assert fig.page_no == 2
    assert fig.caption == "Figure 1: A sample chart."
    assert fig.image_filename == "fig001.png"
    assert fig.origin_name == "#/pictures/0"
    assert (tmp_path / "fig001.png").exists()

    assert tbl.figure_id == "fig002"
    assert tbl.figure_label == "Table"
    assert tbl.page_no == 3
    assert tbl.image_filename == "fig002.png"
    assert (tmp_path / "fig002.png").exists()


def test_build_figure_records_skips_captionless_items(fake_docling_items, tmp_path):
    picture = _FakePictureItem(caption="", page_no=1)
    figures = _build_figure_records(_FakeDocument([picture]), tmp_path, source_document="x.pdf")
    assert figures == []
    assert list(tmp_path.iterdir()) == []  # nothing saved for a skipped item


def test_build_figure_records_skips_items_without_provenance(fake_docling_items, tmp_path):
    picture = _FakePictureItem(caption="Figure 1", has_prov=False)
    figures = _build_figure_records(_FakeDocument([picture]), tmp_path, source_document="x.pdf")
    assert figures == []


def test_build_figure_records_skips_items_whose_image_fails_to_save(fake_docling_items, tmp_path):
    picture = _FakePictureItem(caption="Figure 1", image=_FakeImage(fail=True), page_no=1)
    figures = _build_figure_records(_FakeDocument([picture]), tmp_path, source_document="x.pdf")
    assert figures == []


def test_build_figure_records_ignores_non_figure_items(fake_docling_items, tmp_path):
    figures = _build_figure_records(_FakeDocument([object()]), tmp_path, source_document="x.pdf")
    assert figures == []


def test_build_figure_records_empty_document(fake_docling_items, tmp_path):
    assert _build_figure_records(_FakeDocument([]), tmp_path, source_document="x.pdf") == []


def test_build_figure_records_sequences_ids_only_for_emitted_figures(fake_docling_items, tmp_path):
    """A skipped captionless item must not consume a sequence number — the
    next real figure should still be fig001, not fig002."""
    skipped = _FakePictureItem(caption="", page_no=1)
    kept = _FakePictureItem(caption="Figure 1", page_no=1)
    figures = _build_figure_records(_FakeDocument([skipped, kept]), tmp_path, source_document="x.pdf")
    assert len(figures) == 1
    assert figures[0].figure_id == "fig001"
    assert figures[0].image_filename == "fig001.png"


# ---------------------------------------------------------------------------
# Multi-panel merge: an uncaptioned PictureItem adjacent to a captioned one
# (Docling sometimes detects panel A and panel B of one figure as separate
# regions, associating the caption with only one) gets absorbed into a merged
# crop instead of being silently dropped.
# ---------------------------------------------------------------------------

def test_build_figure_records_merges_adjacent_uncaptioned_panel(fake_docling_items, tmp_path):
    page, pil_image = _fake_page_with_image()
    # panel_a directly above panel_b with a small (< _PANEL_MERGE_GAP_PT) gap,
    # mirroring the real-world case this fix targets.
    panel_a_bbox = _bbox(l=50, t=700, r=300, b=500)
    panel_b_bbox = _bbox(l=50, t=500 - (_PANEL_MERGE_GAP_PT - 1), r=300, b=200)
    panel_a = _FakePictureItem(caption="Fig. 1. Two panels.", page_no=1, bbox=panel_a_bbox)
    panel_b = _FakePictureItem(caption="", page_no=1, bbox=panel_b_bbox)
    document = _FakeDocument([panel_a, panel_b], pages={1: page})

    figures = _build_figure_records(document, tmp_path, source_document="x.pdf")

    assert len(figures) == 1  # panel_b did not become (or stay) its own dropped item
    assert figures[0].caption == "Fig. 1. Two panels."
    assert len(pil_image.crop_calls) == 1  # merged crop came from the page bitmap, not element.get_image()


def test_build_figure_records_does_not_merge_distant_uncaptioned_picture(fake_docling_items, tmp_path):
    page, pil_image = _fake_page_with_image()
    panel_a_bbox = _bbox(l=50, t=700, r=300, b=500)
    far_bbox = _bbox(l=50, t=500 - (_PANEL_MERGE_GAP_PT + 50), r=300, b=100)
    panel_a = _FakePictureItem(caption="Fig. 1.", page_no=1, bbox=panel_a_bbox)
    far = _FakePictureItem(caption="", page_no=1, bbox=far_bbox)
    document = _FakeDocument([panel_a, far], pages={1: page})

    figures = _build_figure_records(document, tmp_path, source_document="x.pdf")

    assert len(figures) == 1
    assert pil_image.crop_calls == []  # no merge -> fell back to element.get_image()


def test_build_figure_records_does_not_double_merge_shared_neighbor(fake_docling_items, tmp_path):
    """An uncaptioned picture between two captioned ones, close enough to both,
    is absorbed by only one — not duplicated into both figures' crops."""
    page, pil_image = _fake_page_with_image()
    top_bbox = _bbox(l=50, t=700, r=300, b=500)
    middle_bbox = _bbox(l=50, t=490, r=300, b=300)
    bottom_bbox = _bbox(l=50, t=290, r=300, b=100)
    top = _FakePictureItem(caption="Fig. 1.", page_no=1, bbox=top_bbox)
    middle = _FakePictureItem(caption="", page_no=1, bbox=middle_bbox)
    bottom = _FakePictureItem(caption="Fig. 2.", page_no=1, bbox=bottom_bbox)
    document = _FakeDocument([top, middle, bottom], pages={1: page})

    figures = _build_figure_records(document, tmp_path, source_document="x.pdf")

    assert len(figures) == 2
    assert len(pil_image.crop_calls) == 1  # exactly one figure absorbed the middle panel


def test_build_figure_records_merges_multi_panel_chain(fake_docling_items, tmp_path):
    """Three stacked panels, only the top one captioned — all three should
    merge via iterative growth, not just the immediate neighbor."""
    page, pil_image = _fake_page_with_image()
    top_bbox = _bbox(l=50, t=700, r=300, b=500)
    middle_bbox = _bbox(l=50, t=490, r=300, b=300)
    bottom_bbox = _bbox(l=50, t=290, r=300, b=100)
    top = _FakePictureItem(caption="Fig. 1.", page_no=1, bbox=top_bbox)
    middle = _FakePictureItem(caption="", page_no=1, bbox=middle_bbox)
    bottom = _FakePictureItem(caption="", page_no=1, bbox=bottom_bbox)
    document = _FakeDocument([top, middle, bottom], pages={1: page})

    figures = _build_figure_records(document, tmp_path, source_document="x.pdf")

    assert len(figures) == 1
    assert len(pil_image.crop_calls) == 1
    merged_box = pil_image.crop_calls[0]
    # union of top_bbox (t=700) and bottom_bbox (b=100) in BOTTOMLEFT coords,
    # converted to top-left-origin pixel coords on an 800pt-tall page:
    # new_t = 800-700=100, new_b = 800-100=700 (page/image are the same size
    # here, so no additional scaling).
    assert merged_box == (50.0, 100.0, 300.0, 700.0)


# ---------------------------------------------------------------------------
# DocumentConverter singleton: built once per process, cached (incl. failure)
# ---------------------------------------------------------------------------

def test_get_doc_converter_builds_once_and_caches(monkeypatch):
    monkeypatch.setattr(svc, "_converter_resolved", False)
    monkeypatch.setattr(svc, "_doc_converter", None)
    calls = {"n": 0}

    def fake_build():
        calls["n"] += 1
        return "FAKE_CONVERTER"

    monkeypatch.setattr(svc, "_build_converter", fake_build)

    assert svc._get_doc_converter() == "FAKE_CONVERTER"
    assert svc._get_doc_converter() == "FAKE_CONVERTER"
    assert calls["n"] == 1


def test_get_doc_converter_caches_failure(monkeypatch):
    monkeypatch.setattr(svc, "_converter_resolved", False)
    monkeypatch.setattr(svc, "_doc_converter", None)
    calls = {"n": 0}

    def fake_build():
        calls["n"] += 1
        raise RuntimeError("model weights missing")

    monkeypatch.setattr(svc, "_build_converter", fake_build)

    assert svc._get_doc_converter() is None
    assert svc._get_doc_converter() is None
    assert calls["n"] == 1  # not retried once resolution has happened


# ---------------------------------------------------------------------------
# extract_figures(): disabled / unavailable degrades to None, no conversion attempted
# ---------------------------------------------------------------------------

def test_extract_figures_returns_none_when_disabled(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "ENABLE_FIGURE_EXTRACTION", False)
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"
    assert extract_figures(pdf, documents_dir, source_document="doc.pdf") is None


def test_extract_figures_returns_none_when_converter_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "ENABLE_FIGURE_EXTRACTION", True)
    monkeypatch.setattr(svc, "_converter_resolved", True)
    monkeypatch.setattr(svc, "_doc_converter", None)
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"
    assert extract_figures(pdf, documents_dir, source_document="doc.pdf") is None


def test_extract_figures_degrades_on_conversion_exception(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "ENABLE_FIGURE_EXTRACTION", True)
    monkeypatch.setattr(svc, "_converter_resolved", True)

    class _BoomConverter:
        def convert(self, path):
            raise RuntimeError("malformed PDF")

    monkeypatch.setattr(svc, "_doc_converter", _BoomConverter())
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    documents_dir = tmp_path / "documents"

    result = extract_figures(pdf, documents_dir, source_document="doc.pdf")
    assert result is not None
    assert result.status == "failed"
    assert "malformed PDF" in result.error


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
    fake_tmp = tmp_path / "fake_docling_tmp"
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
# Real end-to-end run (opt-in only: set RUN_DOCLING_MODEL_TESTS=1). Skipped by
# default since Docling being importable doesn't mean its model weights are
# already downloaded — running this unconditionally would make the suite
# non-deterministically slow/networked. See backend/docs/figure-extraction.md.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _docling_models_enabled(), reason="set RUN_DOCLING_MODEL_TESTS=1 to run (uses real Docling models)")
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

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ok"
    assert manifest["figure_count"] >= 1
    assert "Figure 1" in manifest["figures"][0]["caption"]


@pytest.mark.skipif(not _docling_models_enabled(), reason="set RUN_DOCLING_MODEL_TESTS=1 to run (uses real Docling models)")
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
