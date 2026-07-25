"""Tests for storing reference document text in the storage backend."""

import pytest

from app.models.session import (
    ReferenceDocument,
    SessionMetadata,
    SessionType,
    VisualizationSession,
)
from app.services import reference_document_service as refsvc
from app.services.reference_context import build_reference_context
from app.storage.factory import reset_storage
from app.storage.local_backend import LocalStorageBackend


@pytest.fixture
def local_storage(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    reset_storage()
    backend = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(tmp_path / "data"),
        schematiq_work_dir=str(tmp_path / "work"),
    )
    monkeypatch.setattr("app.storage.factory.get_storage", lambda: backend)
    monkeypatch.setattr("app.storage.get_storage", lambda: backend)
    yield backend
    reset_storage()


def _session(refs):
    return VisualizationSession(
        id="store-session",
        type=SessionType.SCHEMATIQ,
        metadata=SessionMetadata(source="test"),
        reference_documents=refs,
    )


@pytest.mark.asyncio
async def test_store_then_load_roundtrip(local_storage):
    csv = b"judge,appointing_president\nAcker,Reagan\nSmith,Obama\n"
    ref = await refsvc.store_reference_document("store-session", "judges.csv", csv)
    # Metadata only; text is not inline on the session.
    assert ref.content is None
    assert ref.char_count > 0

    text = await refsvc.load_reference_text("store-session", ref)
    assert "Acker,Reagan" in text and "Smith,Obama" in text


@pytest.mark.asyncio
async def test_build_reference_context_loads_from_storage(local_storage):
    ref = await refsvc.store_reference_document(
        "store-session", "judges.csv", b"judge,pres\nAcker,Reagan\n"
    )
    ctx = await build_reference_context(_session([ref]))
    assert ctx and "judges.csv" in ctx and "Acker,Reagan" in ctx


@pytest.mark.asyncio
async def test_delete_removes_stored_text(local_storage):
    ref = await refsvc.store_reference_document(
        "store-session", "judges.csv", b"judge,pres\nAcker,Reagan\n"
    )
    await refsvc.delete_reference_storage("store-session", ref.id)
    # After deletion the text is gone; load returns empty rather than raising.
    assert await refsvc.load_reference_text("store-session", ref) == ""


@pytest.mark.asyncio
async def test_load_prefers_inline_content_for_legacy_docs(local_storage):
    # Legacy documents carry inline content and must not require storage.
    legacy = ReferenceDocument(id="legacy", filename="old.csv", content="a,b\n1,2", char_count=7)
    assert await refsvc.load_reference_text("store-session", legacy) == "a,b\n1,2"
