"""Tests for UploadDocumentProcessor explicit UploadRunState lifecycle."""

from unittest.mock import AsyncMock

import pytest

from app.models.session import (
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.upload_document_processor import (
    UploadDocumentProcessor,
    UploadRunState,
)


def test_upload_processor_run_state_transitions(session_manager_fixture):
    ws = AsyncMock()
    processor = UploadDocumentProcessor(ws, session_manager_fixture)

    session_id = "upload-state-1"
    assert processor.is_running(session_id) is False
    assert processor.is_stop_requested(session_id) is False
    # Worker abort predicate must return True when session is not registered as RUNNING.
    assert processor.should_abort_worker(session_id) is True
    assert processor.request_stop(session_id)["accepted"] is False

    processor.mark_running(session_id)
    assert processor.running_sessions[session_id] == UploadRunState.RUNNING
    assert processor.is_running(session_id) is True
    assert processor.is_stop_requested(session_id) is False
    assert processor.should_abort_worker(session_id) is False

    res = processor.request_stop(session_id)
    assert res["accepted"] is True
    assert processor.running_sessions[session_id] == UploadRunState.STOP_REQUESTED
    assert processor.is_running(session_id) is True
    assert processor.is_stop_requested(session_id) is True
    assert processor.should_abort_worker(session_id) is True

    processor.clear_running(session_id)
    assert processor.is_running(session_id) is False
    assert processor.is_stop_requested(session_id) is False
    assert processor.should_abort_worker(session_id) is True


@pytest.mark.asyncio
async def test_finalize_stopped_run_merges_partial_rows_and_updates_status(
    tmp_path, session_manager_fixture
):
    ws = AsyncMock()
    processor = UploadDocumentProcessor(ws, session_manager_fixture)
    processor._merge_extracted_data = AsyncMock(return_value=3)

    session_id = "upload-stop-merge"
    session = VisualizationSession(
        id=session_id,
        type=SessionType.UPLOAD,
        status=SessionStatus.PROCESSING_DOCUMENTS,
        metadata=SessionMetadata(source="test"),
    )
    session_manager_fixture.create_session(session)

    output_path = tmp_path / "additional_data.jsonl"
    output_path.write_text('{"row_name": "r1"}\n')

    await processor._finalize_stopped_run(session_id, output_path, post_task=True)

    updated = session_manager_fixture.get_session(session_id)
    assert updated.status == SessionStatus.STOPPED
    assert updated.metadata.additional_rows_added == 3
    processor._merge_extracted_data.assert_awaited_once()
