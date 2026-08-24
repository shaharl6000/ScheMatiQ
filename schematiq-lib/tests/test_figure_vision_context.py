"""Figure images reach value extraction alongside the columns being filled.

Two properties matter here, both motivated by avoiding data loss on cell
values that only appear inside a graph (an axis label, a legend entry):

1. _load_document_figures() loads every figure's image once per document
   (no relevance filtering) and records an attach-audit marker on the
   manifest — never a text description standing in for the image.
2. _generate() attaches that same set of images to every per-unit /
   per-column call, so an image is never sent to the model except
   alongside the <REQUESTED_COLUMNS> it's meant to help fill.

Follows the repo pattern of driving methods unbound with a MagicMock
``self`` (see test_extract_values_for_unit_narrowing.py), so no real LLM
or on-disk PDF pipeline is needed.
"""
import json
from unittest.mock import MagicMock

from schematiq.value_extraction.core import paper_processor as pp
from schematiq.value_extraction.core.paper_processor import PaperProcessor


def _make_self(is_gemini=True):
    s = MagicMock()
    s._is_gemini_backend.return_value = is_gemini
    s.llm.model = "gemini-test-model"
    s._active_context_cache = None
    s._active_figure_images = []
    return s


def _write_manifest(figures_dir, figures):
    figures_dir.mkdir(parents=True, exist_ok=True)
    manifest = {"source_document": "doc", "figure_count": len(figures), "figures": figures, "status": "ok"}
    (figures_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    for fig in figures:
        (figures_dir / fig["image_filename"]).write_bytes(b"fake-image-bytes-" + fig["figure_id"].encode())


class TestLoadDocumentFigures:
    def test_loads_every_figure_no_filtering(self, tmp_path):
        figures_dir = tmp_path / "figures" / "doc"
        _write_manifest(figures_dir, [
            {"figure_id": "doc_fig001", "image_filename": "doc-Figure1-1.png"},
            {"figure_id": "doc_fig002", "image_filename": "doc-Table1-1.png"},
        ])
        s = _make_self()

        PaperProcessor._load_document_figures(s, figures_dir)

        assert len(s._active_figure_images) == 2
        for data, mime in s._active_figure_images:
            assert mime == "image/png"
            assert data.startswith(b"fake-image-bytes-")

    def test_marks_attach_audit_trail_on_manifest(self, tmp_path):
        figures_dir = tmp_path / "figures" / "doc"
        _write_manifest(figures_dir, [
            {"figure_id": "doc_fig001", "image_filename": "doc-Figure1-1.png"},
        ])
        s = _make_self()

        PaperProcessor._load_document_figures(s, figures_dir)

        manifest = json.loads((figures_dir / "manifest.json").read_text(encoding="utf-8"))
        fig = manifest["figures"][0]
        assert fig["vision_model"] == "gemini-test-model"
        assert fig["vision_extracted_at"] is not None
        # Audit marker only — never a substitute description.
        assert "vision_description" not in fig or fig.get("vision_description") is None

    def test_noop_when_no_manifest(self, tmp_path):
        s = _make_self()
        PaperProcessor._load_document_figures(s, tmp_path / "figures" / "missing")
        assert s._active_figure_images == []

    def test_noop_when_figures_dir_is_none(self):
        s = _make_self()
        PaperProcessor._load_document_figures(s, None)
        assert s._active_figure_images == []

    def test_noop_when_not_gemini_backend(self, tmp_path):
        figures_dir = tmp_path / "figures" / "doc"
        _write_manifest(figures_dir, [
            {"figure_id": "doc_fig001", "image_filename": "doc-Figure1-1.png"},
        ])
        s = _make_self(is_gemini=False)

        PaperProcessor._load_document_figures(s, figures_dir)

        assert s._active_figure_images == []
        # No audit trail written when the feature didn't run.
        manifest = json.loads((figures_dir / "manifest.json").read_text(encoding="utf-8"))
        assert "vision_model" not in manifest["figures"][0]

    def test_noop_when_feature_flag_disabled(self, tmp_path, monkeypatch):
        figures_dir = tmp_path / "figures" / "doc"
        _write_manifest(figures_dir, [
            {"figure_id": "doc_fig001", "image_filename": "doc-Figure1-1.png"},
        ])
        monkeypatch.setattr(pp, "ENABLE_FIGURE_VISION_CONTEXT", False)
        s = _make_self()

        PaperProcessor._load_document_figures(s, figures_dir)

        assert s._active_figure_images == []


class TestGenerateAttachesImages:
    def test_attaches_images_to_cached_call(self):
        s = _make_self()
        s._active_context_cache = MagicMock()
        s._active_figure_images = [(b"img", "image/png")]
        s.llm.generate_with_cache.return_value = "response"

        result = PaperProcessor._generate(s, "extract Column X")

        assert result == "response"
        _, kwargs = s.llm.generate_with_cache.call_args
        assert kwargs["images"] == [(b"img", "image/png")]

    def test_attaches_images_to_uncached_call(self):
        s = _make_self()
        s._active_context_cache = None
        s._active_figure_images = [(b"img", "image/png")]
        s.llm.generate.return_value = "response"

        result = PaperProcessor._generate(s, "extract Column X")

        assert result == "response"
        _, kwargs = s.llm.generate.call_args
        assert kwargs["images"] == [(b"img", "image/png")]

    def test_no_images_loaded_means_no_images_kwarg(self):
        s = _make_self()
        s._active_context_cache = None
        s._active_figure_images = []
        s.llm.generate.return_value = "response"

        PaperProcessor._generate(s, "extract Column X")

        _, kwargs = s.llm.generate.call_args
        assert "images" not in kwargs

    def test_explicit_images_kwarg_is_not_overridden(self):
        s = _make_self()
        s._active_context_cache = None
        s._active_figure_images = [(b"doc-wide-image", "image/png")]
        s.llm.generate.return_value = "response"

        PaperProcessor._generate(s, "extract Column X", images=[(b"caller-override", "image/jpeg")])

        _, kwargs = s.llm.generate.call_args
        assert kwargs["images"] == [(b"caller-override", "image/jpeg")]


class TestDeleteDocumentCacheClearsFigures:
    def test_clears_active_figure_images(self):
        s = _make_self()
        s._active_context_cache = None
        s._active_figure_images = [(b"img", "image/png")]

        PaperProcessor._delete_document_cache(s)

        assert s._active_figure_images == []
