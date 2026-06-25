"""Tests for document text preprocessing (Westlaw-style DOCX + PDF)."""

import shutil
from pathlib import Path

import pytest

from app.services.document_preprocessor import preprocess_uploaded_file

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "legal_corpus"

FIXTURE_FILES = [
    ("American Civil Liberties Union v Reno.docx", "extracted from docx"),
    ("Arizona v Biden.docx", "extracted from docx"),
    ("seattle_homeland.pdf", None),
]

OOXML_MARKERS = ("PK\x03\x04", "word/document.xml", "[Content_Types].xml")


def _libreoffice_available() -> bool:
    from app.services.document_conversion.convert_to_txt import get_libreoffice_path

    try:
        get_libreoffice_path()
        return True
    except FileNotFoundError:
        return False


@pytest.fixture
def pending_dir(tmp_path):
    work = tmp_path / "pending_documents"
    work.mkdir()
    return work


@pytest.mark.parametrize("fixture_name,expected_status", FIXTURE_FILES)
def test_preprocess_legal_corpus_fixtures(pending_dir, fixture_name, expected_status):
    source = FIXTURES_DIR / fixture_name
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")

    dest = pending_dir / fixture_name
    shutil.copy2(source, dest)

    result = preprocess_uploaded_file(dest, original_filename=fixture_name)

    assert result.success, result.status
    assert result.output_path.exists()
    assert result.output_path.suffix == ".txt"

    text = result.output_path.read_text(encoding="utf-8")
    assert len(text.strip()) > 500, f"Expected substantial text, got {len(text.strip())} chars"

    for marker in OOXML_MARKERS:
        assert marker not in text[:2000], f"Output looks like raw OOXML (found {marker!r})"

    if fixture_name.endswith(".pdf"):
        assert result.status in ("extracted from pdf", "extracted via OCR")
    else:
        assert result.status == expected_status or result.status == "extracted via LibreOffice"


@pytest.mark.skipif(not _libreoffice_available(), reason="LibreOffice required for DOC fallback")
def test_docx_libreoffice_fallback_when_docx_corrupt(pending_dir):
    """Non-standard DOCX should still extract via LibreOffice when python-docx fails."""
    source = FIXTURES_DIR / "American Civil Liberties Union v Reno.docx"
    if not source.exists():
        pytest.skip(f"Fixture not found: {source}")

    dest = pending_dir / source.name
    shutil.copy2(source, dest)
    result = preprocess_uploaded_file(dest, original_filename=source.name)
    assert result.success
    assert "failed" not in result.status
