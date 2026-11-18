"""Tests for the per-project enable_figure_extraction toggle in backend."""

from types import SimpleNamespace
from pathlib import Path

from app.api.routes.load import _preprocess_and_record
from app.models.schematiq import LLMConfig, ScheMatiQConfig
from app.models.session import SessionMetadata
from app.services import document_preprocessor as dp
from app.services.pipeline.config_handler import convert_config_to_schematiq_format


def _make_fake_convert(output_text: str = "Converted plain text content"):
    def fake_convert(source_path: Path, output_dir: Path, *_args):
        out = output_dir / f"{source_path.stem}.txt"
        out.write_text(output_text, encoding="utf-8")
        return True, "pdf"

    return fake_convert


def test_preprocess_uploaded_file_skips_figures_when_disabled(tmp_path, monkeypatch):
    pdf_path = tmp_path / "ruling.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 dummy")
    monkeypatch.setattr(dp, "convert_file", _make_fake_convert())

    def forbid_extract(*_args, **_kwargs):
        raise AssertionError("extract_figures must not run when enable_figure_extraction=False")

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", forbid_extract)

    res = dp.preprocess_uploaded_file(
        pdf_path,
        original_filename="ruling.pdf",
        documents_dir=tmp_path,
        enable_figure_extraction=False,
    )
    assert res.success is True
    assert res.output_path.name == "ruling.txt"

    docs_dir = tmp_path / "committed"
    docs_dir.mkdir()
    pdf_commit = tmp_path / "ruling_commit.pdf"
    pdf_commit.write_bytes(b"%PDF-1.4 dummy")
    committed = dp.commit_document_to_documents_dir(
        pdf_commit,
        docs_dir,
        enable_figure_extraction=False,
    )
    assert committed is not None and committed.name == "ruling_commit.txt"


def test_preprocess_uploaded_file_runs_figures_when_enabled(tmp_path, monkeypatch):
    pdf_path = tmp_path / "ruling.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 dummy")
    monkeypatch.setattr(dp, "convert_file", _make_fake_convert())

    calls = []

    def fake_extract(source_path, figures_dir, *, source_document):
        calls.append((source_path.name, figures_dir, source_document))
        return None

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", fake_extract)

    res = dp.preprocess_uploaded_file(
        pdf_path,
        original_filename="ruling.pdf",
        documents_dir=tmp_path,
        enable_figure_extraction=True,
    )
    assert res.success is True
    assert calls == [("ruling.pdf", tmp_path, "ruling.pdf")]


def test_preprocess_and_record_respects_session_figure_toggle(tmp_path, monkeypatch):
    monkeypatch.setattr(dp, "convert_file", _make_fake_convert())

    calls = []

    def fake_extract(source_path, _figures_dir, *, source_document):
        calls.append(source_document)
        return None

    monkeypatch.setattr("app.services.figure_extraction_service.extract_figures", fake_extract)

    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()

    # Session with figure extraction disabled skips extract_figures.
    pdf_off = tmp_path / "case_off.pdf"
    pdf_off.write_bytes(b"%PDF-1.4 dummy")
    sess_off = SimpleNamespace(
        metadata=SessionMetadata(source="test", enable_figure_extraction=False)
    )
    doc_meta_off = {}
    name, err = _preprocess_and_record(pdf_off, "case_off.pdf", docs_dir, sess_off, doc_meta_off)
    assert err is None
    assert name == "case_off.txt"
    assert calls == []

    # Session with figure extraction enabled runs extract_figures.
    pdf_on = tmp_path / "case_on.pdf"
    pdf_on.write_bytes(b"%PDF-1.4 dummy")
    sess_on = SimpleNamespace(
        metadata=SessionMetadata(source="test", enable_figure_extraction=True)
    )
    doc_meta_on = {}
    name, err = _preprocess_and_record(pdf_on, "case_on.pdf", docs_dir, sess_on, doc_meta_on)
    assert err is None
    assert name == "case_on.txt"
    assert calls == ["case_on.pdf"]


def test_config_and_metadata_preserve_and_default_enable_figure_extraction(tmp_path):
    default_cfg = ScheMatiQConfig(
        query="q",
        docs_path="docs",
        schema_creation_backend=LLMConfig(provider="gemini", model="gemini-3.8-flash"),
        value_extraction_backend=LLMConfig(provider="gemini", model="gemini-3.5-flash-lite"),
        output_path="out.json",
    )
    assert default_cfg.enable_figure_extraction is True

    off_cfg = default_cfg.model_copy(update={"enable_figure_extraction": False})
    converted = convert_config_to_schematiq_format(off_cfg, "sess-1", tmp_path, [str(tmp_path)])
    assert converted["enable_figure_extraction"] is False

    meta = SessionMetadata(source="test")
    assert meta.enable_figure_extraction is True
    assert dp.session_figures_enabled(None) is True
    assert dp.session_figures_enabled(SimpleNamespace(metadata=meta)) is True
    assert dp.session_figures_enabled(
        SimpleNamespace(metadata=SessionMetadata(source="test", enable_figure_extraction=False))
    ) is False

