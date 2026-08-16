"""Tests for the synchronous document precheck in start_continue_discovery.

Previously, when the chosen document source had no documents, the operation was
created and the background task later raised "No documents available for schema
discovery" — surfacing to the caller as an async operation failure rather than
an immediate error. start_continue_discovery now checks availability up front and
raises synchronously, before creating an operation.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.continue_discovery_service import ContinueDiscoveryService


@pytest.fixture
def service(tmp_path, monkeypatch):
    session_manager = MagicMock()
    session_manager.get_session.return_value = SimpleNamespace(
        columns=[], schema_query="q", metadata=SimpleNamespace(cloud_dataset=None)
    )
    svc = ContinueDiscoveryService(None, session_manager)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(svc, "_get_data_dir", lambda: data_dir)
    return svc


@pytest.mark.asyncio
async def test_upload_source_without_documents_raises_synchronously(service):
    # No pending_documents dir at all -> immediate error, no operation created.
    with pytest.raises(ValueError, match="No uploaded documents"):
        await service.start_continue_discovery(
            session_id="s1", document_source="upload", llm_config={}
        )
    assert not service.active_operations


@pytest.mark.asyncio
async def test_upload_source_with_empty_pending_dir_raises(service):
    pending = service._get_data_dir() / "s1" / "pending_documents"
    pending.mkdir(parents=True)
    (pending / ".keep").write_text("")  # dotfile must be ignored
    with pytest.raises(ValueError, match="No uploaded documents"):
        await service.start_continue_discovery(
            session_id="s1", document_source="upload", llm_config={}
        )
    assert not service.active_operations


@pytest.mark.asyncio
async def test_original_source_unavailable_raises_synchronously(service, monkeypatch):
    monkeypatch.setattr(
        service,
        "get_available_documents",
        AsyncMock(return_value={"can_use_original": False}),
    )
    with pytest.raises(ValueError, match="No source documents are available"):
        await service.start_continue_discovery(
            session_id="s1", document_source="original", llm_config={}
        )
    assert not service.active_operations
