"""Project bundle unpacking refuses oversized archives (CWE-409)."""

import json
import zipfile

import pytest

from app.services import file_parser
from app.services.file_parser import FileParser


def _bundle(path, entries):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return path


def test_bundle_unpacks_project_and_documents(tmp_path):
    session_dir = tmp_path / "sid"
    session_dir.mkdir()
    zip_path = _bundle(
        session_dir / "p.zip",
        {"project.json": json.dumps({"rows": []}), "documents/a.txt": "hello"},
    )

    result = FileParser()._extract_project_bundle(zip_path, session_dir)

    assert result == session_dir / "project.json"
    assert (session_dir / "pending_documents" / "a.txt").read_text() == "hello"
    assert not zip_path.exists()


@pytest.mark.parametrize(
    "limit_name, message",
    [("_BUNDLE_MAX_ENTRY_BYTES", "file that is too large"), ("_BUNDLE_MAX_TOTAL_BYTES", "too large")],
)
def test_bundle_over_limit_is_refused_before_writing(tmp_path, monkeypatch, limit_name, message):
    monkeypatch.setattr(file_parser, limit_name, 1000)
    session_dir = tmp_path / "sid"
    session_dir.mkdir()
    # Highly compressible: tiny on disk, large once unpacked.
    zip_path = _bundle(
        session_dir / "p.zip",
        {"project.json": "{}", "documents/big.txt": "A" * 5000},
    )

    with pytest.raises(ValueError, match=message):
        FileParser()._extract_project_bundle(zip_path, session_dir)

    assert not (session_dir / "project.json").exists()
    assert not (session_dir / "pending_documents" / "big.txt").exists()


def test_bundle_skips_dot_dot_document_names(tmp_path):
    session_dir = tmp_path / "sid"
    session_dir.mkdir()
    zip_path = _bundle(session_dir / "p.zip", {"project.json": "{}", "documents/..": "x"})

    FileParser()._extract_project_bundle(zip_path, session_dir)

    assert list((session_dir / "pending_documents").iterdir()) == []
