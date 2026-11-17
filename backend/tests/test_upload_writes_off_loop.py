"""Upload writes and bundle unpacking run off the event loop (#691)."""

import asyncio
import io
import json
import zipfile

import pytest
from fastapi import UploadFile

from app.api.routes import load
from app.models.session import (
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.file_parser import FileParser


@pytest.fixture
def thread_calls(monkeypatch):
    """Record the name of every function run through asyncio.to_thread."""
    calls = []
    real_to_thread = asyncio.to_thread

    async def spy(func, *args, **kwargs):
        calls.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", spy)
    return calls


@pytest.fixture
def upload_session(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    session = VisualizationSession(
        id="sid",
        type=SessionType.UPLOAD,
        metadata=SessionMetadata(source="x"),
        status=SessionStatus.COMPLETED,
    )
    monkeypatch.setattr(load.session_manager, "get_session", lambda sid: session)
    monkeypatch.setattr(load.session_manager, "update_session", lambda *a, **k: None)
    monkeypatch.setattr(
        load.FileParser, "__init__", lambda self, *a, **k: setattr(self, "data_dir", data_dir)
    )
    monkeypatch.setattr(load, "_preprocess_and_record", lambda path, name, *a, **k: (name, None))
    return data_dir


async def test_add_documents_writes_through_a_thread(upload_session, thread_calls):
    upload = UploadFile(file=io.BytesIO(b"hello"), filename="a.txt", size=5)

    await load.add_documents("sid", files=[upload])

    assert (upload_session / "sid" / "pending_documents" / "a.txt").read_bytes() == b"hello"
    assert "write_bytes" in thread_calls


async def test_add_cloud_documents_writes_through_a_thread(
    upload_session, thread_calls, monkeypatch
):
    class FakeDataset:
        name = "ds"

    class FakeStorage:
        async def list_datasets(self):
            return [FakeDataset()]

        async def download_dataset_file(self, dataset, filename):
            return b"cloud"

    monkeypatch.setattr(load, "get_storage", lambda: FakeStorage())
    request = load.CloudDocumentRequest(dataset="ds", files=["b.txt"])

    await load.add_cloud_documents("sid", request)

    assert (upload_session / "sid" / "pending_documents" / "b.txt").read_bytes() == b"cloud"
    assert "write_bytes" in thread_calls


async def test_project_bundle_is_unpacked_in_a_thread(tmp_path, thread_calls):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("project.json", json.dumps({"rows": []}))
        zf.writestr("documents/a.txt", "doc")
    buf.seek(0)
    parser = FileParser()
    parser.data_dir = tmp_path

    result = await parser.save_uploaded_file("sid", UploadFile(file=buf, filename="p.zip"))

    assert result == tmp_path / "sid" / "project.json"
    assert (tmp_path / "sid" / "pending_documents" / "a.txt").read_text() == "doc"
    assert "_extract_project_bundle" in thread_calls
