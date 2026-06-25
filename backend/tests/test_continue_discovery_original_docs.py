"""Regression tests for original-document loading in continue discovery.

Background: a session's source PDF is moved verbatim into data/{session}/documents/
by schematiq_runner._move_pending_documents. The original-source loader used to
accept only .txt/.md, so a PDF-only session yielded 0 documents and continue
discovery failed with "No documents available for schema discovery" — while the
detector (get_available_documents) counted the same PDF and reported
can_use_original=True. The two sides now share ORIGINAL_DOC_EXTENSIONS.

These tests lock that contract so the mismatch cannot silently return.
"""

import pytest

from app.services.continue_discovery_service import (
    ContinueDiscoveryService,
    ORIGINAL_DOC_EXTENSIONS,
)


@pytest.fixture
def service(tmp_path, monkeypatch):
    """A service whose data/work dirs point at an isolated tmp location."""
    svc = ContinueDiscoveryService(None, None)
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    data_dir.mkdir()
    work_dir.mkdir()
    monkeypatch.setattr(svc, "_get_data_dir", lambda: data_dir)
    monkeypatch.setattr(svc, "_get_schematiq_work_dir", lambda: work_dir)
    return svc


def _documents_dir(service, session_id):
    docs = service._get_data_dir() / session_id / "documents"
    docs.mkdir(parents=True, exist_ok=True)
    return docs


def test_original_extensions_cover_critical_formats():
    """The shared set must include the formats the pipeline actually persists.

    A PDF is what triggered the original bug; plain text is the baseline. If a
    future edit drops either, this fails before it reaches the detector/loader.
    """
    assert ".pdf" in ORIGINAL_DOC_EXTENSIONS
    assert ".txt" in ORIGINAL_DOC_EXTENSIONS


@pytest.mark.asyncio
async def test_original_loader_reads_pdf_and_skips_unsupported(service, monkeypatch):
    """The loader converts a PDF to text and ignores unsupported / empty files."""
    session_id = "sess-pdf"
    docs = _documents_dir(service, session_id)

    (docs / "judges.pdf").write_bytes(b"%PDF-1.4 fake bytes")
    (docs / "notes.txt").write_text("plain text doc", encoding="utf-8")
    (docs / "meta.json").write_text('{"k": "v"}', encoding="utf-8")
    (docs / "readme.md").write_text("# heading", encoding="utf-8")
    (docs / "server.log").write_text("unsupported extension", encoding="utf-8")
    (docs / "empty.txt").write_text("   ", encoding="utf-8")  # whitespace-only

    # Avoid a real PDF/pymupdf dependency: the loader imports this at call time.
    monkeypatch.setattr(
        "app.services.pdf_utils.extract_text_from_pdf",
        lambda path: "EXTRACTED PDF TEXT",
    )

    _docs_dir, documents, filenames = await service._prepare_documents(
        session_id, "original", bypass_limit=True
    )

    # Unsupported (.log) and whitespace-only (.txt) are excluded; the rest load.
    assert set(filenames) == {"judges.pdf", "notes.txt", "meta.json", "readme.md"}
    # The PDF was converted to text via the extractor, not read as raw bytes.
    pdf_content = documents[filenames.index("judges.pdf")]
    assert pdf_content == "EXTRACTED PDF TEXT"


@pytest.mark.asyncio
async def test_original_loader_survives_pdf_extraction_failure(service, monkeypatch):
    """A failing PDF extractor is logged and skipped, not fatal to the batch."""
    session_id = "sess-bad-pdf"
    docs = _documents_dir(service, session_id)
    (docs / "good.txt").write_text("usable", encoding="utf-8")
    (docs / "broken.pdf").write_bytes(b"%PDF-1.4 fake bytes")

    def _boom(path):
        raise RuntimeError("corrupt pdf")

    monkeypatch.setattr("app.services.pdf_utils.extract_text_from_pdf", _boom)

    _docs_dir, documents, filenames = await service._prepare_documents(
        session_id, "original", bypass_limit=True
    )

    assert filenames == ["good.txt"]
    assert documents == ["usable"]
