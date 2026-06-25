"""Regression tests for documents/ ingestion (pending → documents conversion)."""

from pathlib import Path

from app.services.document_preprocessor import (
    LIB_READABLE_STORAGE_EXTENSIONS,
    commit_document_to_documents_dir,
    commit_bytes_to_documents_dir,
)
from app.services.schematiq_runner import ScheMatiQRunner


def _mock_pdf_convert(monkeypatch, text: str):
    def _fake_convert(input_path, output_dir, soffice_path, worker_id):
        out = output_dir / f"{input_path.stem}.txt"
        out.write_text(text, encoding="utf-8")
        if input_path != out:
            input_path.unlink(missing_ok=True)
        return True, "extracted from pdf"

    monkeypatch.setattr(
        "app.services.document_preprocessor.convert_file",
        _fake_convert,
    )


def test_commit_pdf_becomes_txt_in_documents(tmp_path, monkeypatch):
    pending = tmp_path / "pending_documents"
    documents = tmp_path / "documents"
    pending.mkdir()
    (pending / "judges.pdf").write_bytes(b"%PDF-1.4 fake")

    _mock_pdf_convert(monkeypatch, "Federal judges roster text")

    pdf_path = pending / "judges.pdf"
    dest = commit_document_to_documents_dir(pdf_path, documents)

    assert dest == documents / "judges.txt"
    assert dest.read_text(encoding="utf-8") == "Federal judges roster text"
    assert not pdf_path.exists()
    assert not any(p.suffix.lower() == ".pdf" for p in documents.iterdir())
    assert set(LIB_READABLE_STORAGE_EXTENSIONS) >= {".txt", ".md", ".html", ".htm"}


def test_commit_plain_txt_moves_without_double_convert(tmp_path):
    pending = tmp_path / "pending_documents"
    documents = tmp_path / "documents"
    pending.mkdir()
    source = pending / "notes.txt"
    source.write_text("already plain", encoding="utf-8")

    dest = commit_document_to_documents_dir(source, documents)

    assert dest == documents / "notes.txt"
    assert dest.read_text(encoding="utf-8") == "already plain"
    assert not source.exists()


def test_commit_conversion_failure_leaves_no_pdf_in_documents(tmp_path, monkeypatch):
    pending = tmp_path / "pending_documents"
    documents = tmp_path / "documents"
    pending.mkdir()
    pdf_path = pending / "broken.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    monkeypatch.setattr(
        "app.services.document_preprocessor.convert_file",
        lambda *args, **kwargs: (False, "corrupt pdf"),
    )

    dest = commit_document_to_documents_dir(pdf_path, documents)

    assert dest is None
    assert not any(documents.iterdir())
    assert pdf_path.exists()


def test_move_pending_documents_converts_pdf(tmp_path, monkeypatch):
    session_id = "sess-ingest"
    data_dir = tmp_path / "data" / session_id
    pending = data_dir / "pending_documents"
    documents = data_dir / "documents"
    pending.mkdir(parents=True)
    (pending / "judges.pdf").write_bytes(b"%PDF-1.4 fake")

    _mock_pdf_convert(monkeypatch, "INGESTED TEXT")

    runner = ScheMatiQRunner()
    monkeypatch.chdir(tmp_path)
    runner._move_pending_documents(session_id)

    assert (documents / "judges.txt").read_text(encoding="utf-8") == "INGESTED TEXT"
    assert not list(pending.iterdir())
    assert not any(f.suffix.lower() == ".pdf" for f in documents.iterdir())


def test_commit_bytes_pdf_becomes_txt(tmp_path, monkeypatch):
    # Downloaded/uploaded PDF bytes should land in documents/ as readable .txt,
    # with no temp staging or .pdf left behind.
    documents = tmp_path / "data" / "sess" / "documents"
    _mock_pdf_convert(monkeypatch, "CLOUD PDF TEXT")

    dest = commit_bytes_to_documents_dir(b"%PDF-1.4 fake", "judges.pdf", documents)

    assert dest == documents / "judges.txt"
    assert dest.read_text(encoding="utf-8") == "CLOUD PDF TEXT"
    assert not any(f.suffix.lower() == ".pdf" for f in documents.iterdir())


def test_commit_bytes_plain_txt_passes_through(tmp_path):
    documents = tmp_path / "data" / "sess" / "documents"

    dest = commit_bytes_to_documents_dir(b"plain bytes", "notes.txt", documents)

    assert dest == documents / "notes.txt"
    assert dest.read_text(encoding="utf-8") == "plain bytes"


def test_commit_bytes_conversion_failure_returns_none(tmp_path, monkeypatch):
    documents = tmp_path / "data" / "sess" / "documents"
    monkeypatch.setattr(
        "app.services.document_preprocessor.convert_file",
        lambda *args, **kwargs: (False, "corrupt pdf"),
    )

    dest = commit_bytes_to_documents_dir(b"%PDF-1.4", "broken.pdf", documents)

    assert dest is None
    assert not documents.exists() or not any(documents.iterdir())
