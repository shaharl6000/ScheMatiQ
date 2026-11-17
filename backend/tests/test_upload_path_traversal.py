"""Client-supplied file names cannot write or read outside the session (CWE-22)."""

import io
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, UploadFile

from app.api.routes import load
from app.core.safe_paths import is_within, safe_upload_name
from app.models.session import (
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)

# "ABSOLUTE" is replaced in the test by an absolute path under tmp_path, so a
# regression never writes outside the pytest temp dir.
TRAVERSAL_NAMES = ["../../../escaped.txt", "ABSOLUTE", "..\\..\\escaped.txt"]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("paper.pdf", "paper.pdf"),
        ("../../escaped.txt", "escaped.txt"),
        ("/etc/escaped.txt", "escaped.txt"),
        ("..\\..\\escaped.txt", "escaped.txt"),
        ("folder/sub/paper.pdf", "paper.pdf"),
        ("my paper .pdf", "my paper .pdf"),
    ],
)
def test_safe_upload_name_keeps_only_the_last_component(raw, expected):
    assert safe_upload_name(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", ".", "..", "../", "a/..", "bad\x00.txt"])
def test_safe_upload_name_rejects_unusable_names(raw):
    with pytest.raises(ValueError):
        safe_upload_name(raw)


def test_is_within(tmp_path):
    assert is_within(tmp_path, tmp_path / "a" / "b.txt")
    assert not is_within(tmp_path / "a", tmp_path / "a" / ".." / "b.txt")


@pytest.fixture
def upload_session(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    session = VisualizationSession(
        id="sid",
        type=SessionType.UPLOAD,
        metadata=SessionMetadata(source="x"),
        status=SessionStatus.DOCUMENTS_UPLOADED,
    )
    monkeypatch.setattr(load.session_manager, "get_session", lambda sid: session)
    monkeypatch.setattr(load.session_manager, "update_session", lambda *a, **k: None)
    monkeypatch.setattr(
        load.FileParser, "__init__", lambda self, *a, **k: setattr(self, "data_dir", data_dir)
    )
    monkeypatch.setattr(load, "_preprocess_and_record", lambda path, name, *a, **k: (name, None))
    return session, data_dir


def _files_outside(root, allowed):
    return [p for p in root.rglob("*") if p.is_file() and not is_within(allowed, p)]


@pytest.mark.parametrize("evil", TRAVERSAL_NAMES)
async def test_add_documents_writes_inside_pending_dir(upload_session, tmp_path, evil):
    _, data_dir = upload_session
    if evil == "ABSOLUTE":
        evil = str(tmp_path / "escaped.txt")
    upload = UploadFile(file=io.BytesIO(b"pwned"), filename=evil, size=5)

    result = await load.add_documents("sid", files=[upload])

    pending = data_dir / "sid" / "pending_documents"
    assert (pending / "escaped.txt").read_bytes() == b"pwned"
    assert result["uploaded_files"] == ["escaped.txt"]
    assert _files_outside(tmp_path, data_dir / "sid") == []


async def test_add_cloud_documents_refuses_traversal_keys(upload_session, monkeypatch):
    session, _ = upload_session
    session.status = SessionStatus.COMPLETED
    downloads = []

    class FakeStorage:
        async def list_datasets(self):
            return [SimpleNamespace(name="ds")]

        async def download_dataset_file(self, dataset, filename):
            downloads.append(filename)
            return b"content"

    monkeypatch.setattr(load, "get_storage", lambda: FakeStorage())
    request = load.CloudDocumentRequest(dataset="ds", files=["../../secret.txt", "/etc/secret", "\\etc\\secret"])

    with pytest.raises(HTTPException):  # nothing valid was added
        await load.add_cloud_documents("sid", request)
    assert downloads == []


async def test_remove_uploaded_document_refuses_traversal(upload_session):
    session, _ = upload_session
    session.metadata.uploaded_documents = ["../outside.txt"]
    request = load.RemoveDocumentRequest(filename="../outside.txt")

    with pytest.raises(HTTPException) as err:
        await load.remove_uploaded_document("sid", request)
    assert err.value.status_code == 400


async def test_save_uploaded_file_stays_in_session_dir(tmp_path):
    from app.services.file_parser import FileParser

    parser = FileParser()
    parser.data_dir = tmp_path / "data"
    parser.data_dir.mkdir()
    upload = UploadFile(file=io.BytesIO(b"a,b\n1,2\n"), filename="../../escaped.csv")

    path = await parser.save_uploaded_file("sid", upload)

    assert path == parser.data_dir / "sid" / "escaped.csv"
    assert _files_outside(tmp_path, parser.data_dir / "sid") == []


async def test_local_backend_refuses_dataset_reads_outside_dataset(tmp_path, monkeypatch):
    from app.storage.local_backend import LocalStorageBackend

    datasets = tmp_path / "datasets"
    (datasets / "ds").mkdir(parents=True)
    (datasets / "ds" / "ok.txt").write_bytes(b"ok")
    (tmp_path / "secret.txt").write_bytes(b"secret")

    backend = LocalStorageBackend.__new__(LocalStorageBackend)
    monkeypatch.setattr(backend, "_resolve_datasets_dir", lambda: datasets, raising=False)

    assert await backend.download_dataset_file("ds", "ok.txt") == b"ok"
    assert await backend.download_dataset_file("ds", "../../secret.txt") is None
    assert await backend.download_dataset_file("..", "secret.txt") is None


async def test_add_documents_refuses_same_stem_in_one_upload(upload_session):
    _, data_dir = upload_session
    first = UploadFile(file=io.BytesIO(b"first"), filename="a/paper.pdf", size=5)
    second = UploadFile(file=io.BytesIO(b"second"), filename="b/paper.docx", size=6)

    result = await load.add_documents("sid", files=[first, second])

    pending = data_dir / "sid" / "pending_documents"
    assert (pending / "paper.pdf").read_bytes() == b"first"
    assert not (pending / "paper.docx").exists()
    assert any("same name" in e for e in result["errors"])


async def test_add_cloud_documents_refuses_same_stem_keys(upload_session, monkeypatch):
    session, data_dir = upload_session
    session.status = SessionStatus.COMPLETED
    downloads = []

    class FakeStorage:
        async def list_datasets(self):
            return [SimpleNamespace(name="ds")]

        async def download_dataset_file(self, dataset, filename):
            downloads.append(filename)
            return filename.encode()

    monkeypatch.setattr(load, "get_storage", lambda: FakeStorage())
    request = load.CloudDocumentRequest(dataset="ds", files=["a/x.pdf", "b/x.pdf", "folder/"])

    result = await load.add_cloud_documents("sid", request)

    assert downloads == ["a/x.pdf"]
    assert (data_dir / "sid" / "pending_documents" / "x.pdf").read_bytes() == b"a/x.pdf"
    statuses = {f["filename"]: f["status"] for f in result["failed_files"]}
    assert statuses["b/x.pdf"] == "failed: duplicate name in upload"
    assert statuses["folder/"] == "failed: invalid file name"
