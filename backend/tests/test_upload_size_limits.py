"""Uploads are size-capped before they are read into memory (CWE-400)."""

import io
import json
import zipfile

import pytest
from fastapi.testclient import TestClient

from app.api.routes import load as load_routes
from app.main import app
from app.services import file_parser
from app.services.file_parser import FileParser


@pytest.fixture
def client():
    return TestClient(app)


def _bundle_bytes(entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def test_max_upload_bytes_by_kind():
    assert FileParser.max_upload_bytes("table.csv") == file_parser.MAX_FILE_SIZE
    assert FileParser.max_upload_bytes("table.json") == file_parser.MAX_FILE_SIZE
    assert FileParser.max_upload_bytes("bundle.ZIP") == file_parser._BUNDLE_MAX_TOTAL_BYTES
    assert FileParser.max_upload_bytes(None) == file_parser.MAX_FILE_SIZE


def test_oversized_table_is_413(client, monkeypatch):
    monkeypatch.setattr(file_parser, "MAX_FILE_SIZE", 10)

    response = client.post(
        "/api/load/file", files={"file": ("t.json", b'[{"a": 1}, {"a": 2}]', "application/json")}
    )

    assert response.status_code == 413, response.text


def test_oversized_bundle_is_413(client, monkeypatch):
    monkeypatch.setattr(file_parser, "_BUNDLE_MAX_TOTAL_BYTES", 10)
    content = _bundle_bytes({"project.json": "{}"})

    response = client.post("/api/load/file", files={"file": ("p.zip", content, "application/zip")})

    assert response.status_code == 413, response.text


def test_bundle_within_limit_is_saved_and_unpacked(client, tmp_path, monkeypatch):
    class _TmpParser(FileParser):
        def __init__(self):
            super().__init__(str(tmp_path))

    monkeypatch.setattr(load_routes, "FileParser", _TmpParser)
    monkeypatch.setattr(load_routes.session_manager, "create_session", lambda session: None)
    content = _bundle_bytes({"project.json": json.dumps({"data": []}), "documents/a.txt": "hello"})

    response = client.post("/api/load/file", files={"file": ("p.zip", content, "application/zip")})

    assert response.status_code == 200, response.text
    session_dir = tmp_path / response.json()["session_id"]
    assert (session_dir / "project.json").exists()
    assert (session_dir / "pending_documents" / "a.txt").read_text() == "hello"
    assert not (session_dir / "p.zip").exists()
