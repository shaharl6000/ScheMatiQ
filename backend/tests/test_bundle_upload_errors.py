"""A refused project bundle is a 400 and leaves no session dir behind."""

import io
import zipfile

import pytest
from fastapi.testclient import TestClient

from app.api.routes import load as load_routes
from app.main import app
from app.services import file_parser
from app.services.file_parser import FileParser


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    class _TmpParser(FileParser):
        def __init__(self):
            super().__init__(str(tmp_path))

    monkeypatch.setattr(load_routes, "FileParser", _TmpParser)
    monkeypatch.setattr(load_routes.session_manager, "create_session", lambda session: None)
    return tmp_path


def _bundle_bytes(entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def test_refused_bundle_is_400_and_cleaned_up(data_dir, monkeypatch):
    monkeypatch.setattr(file_parser, "_BUNDLE_MAX_ENTRY_BYTES", 100)
    content = _bundle_bytes({"project.json": "{}", "documents/big.txt": "A" * 5000})

    response = TestClient(app).post(
        "/api/load/file", files={"file": ("p.zip", content, "application/zip")}
    )

    assert response.status_code == 400, response.text
    assert "too large" in response.json()["detail"]
    assert list(data_dir.iterdir()) == []


def test_corrupt_bundle_entry_is_400(data_dir):
    raw = bytearray(_bundle_bytes({"project.json": "{}", "documents/b.txt": "B" * 10_000}))
    # Flip a byte inside b.txt's compressed data so its CRC check fails.
    with zipfile.ZipFile(io.BytesIO(bytes(raw))) as zf:
        info = zf.getinfo("documents/b.txt")
    data_start = info.header_offset + 30 + len(info.filename) + len(info.extra)
    raw[data_start + 5] ^= 0xFF

    response = TestClient(app).post(
        "/api/load/file", files={"file": ("p.zip", bytes(raw), "application/zip")}
    )

    assert response.status_code == 400, response.text
    assert list(data_dir.iterdir()) == []
