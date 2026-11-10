"""Tests for session status translation and orphaned state reconciliation in data_query.get_status."""

import threading

import pytest

from app.models.session import (
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import concurrency_limiter
from app.services.pipeline.data_query import get_status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "orphaned_status",
    [SessionStatus.PROCESSING, SessionStatus.SCHEMA_READY],
)
async def test_orphaned_running_status_reconciles_to_stopped(
    tmp_path, session_manager_fixture, orphaned_status
):
    session = VisualizationSession(
        id=f"orphan-{orphaned_status.value}",
        type=SessionType.SCHEMATIQ,
        status=orphaned_status,
        metadata=SessionMetadata(source="test", schema_discovery_completed=True),
    )
    session_manager_fixture.create_session(session)

    result = await get_status(
        session.id,
        session_manager_fixture,
        running_sessions={},
        state_lock=threading.Lock(),
        work_dir=tmp_path,
    )

    assert result.status == "stopped"
    assert result.progress == 1.0
    updated = session_manager_fixture.get_session(session.id)
    assert updated.status == SessionStatus.STOPPED


@pytest.mark.asyncio
async def test_processing_with_active_slot_stays_processing(
    tmp_path, session_manager_fixture
):
    session = VisualizationSession(
        id="slot-active-processing",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(source="test"),
    )
    session_manager_fixture.create_session(session)

    await concurrency_limiter.acquire(session.id, "schematiq")
    try:
        result = await get_status(
            session.id,
            session_manager_fixture,
            running_sessions={},
            state_lock=threading.Lock(),
            work_dir=tmp_path,
        )
        assert result.status == "processing"
        assert session_manager_fixture.get_session(session.id).status == SessionStatus.PROCESSING
    finally:
        await concurrency_limiter.release(session.id)


@pytest.mark.asyncio
async def test_error_status_propagates_error_message(
    tmp_path, session_manager_fixture
):
    session = VisualizationSession(
        id="errored-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.ERROR,
        error_message="API key invalid",
        metadata=SessionMetadata(source="test"),
    )
    session_manager_fixture.create_session(session)

    result = await get_status(
        session.id,
        session_manager_fixture,
        running_sessions={},
        state_lock=threading.Lock(),
        work_dir=tmp_path,
    )

    assert result.status == "error"
    assert result.error_message == "API key invalid"


@pytest.mark.asyncio
async def test_schema_extracted_status_translated_explicitly(
    tmp_path, session_manager_fixture
):
    session = VisualizationSession(
        id="schema-extracted-session",
        type=SessionType.UPLOAD,
        status=SessionStatus.SCHEMA_EXTRACTED,
        metadata=SessionMetadata(source="test"),
    )
    session_manager_fixture.create_session(session)

    result = await get_status(
        session.id,
        session_manager_fixture,
        running_sessions={},
        state_lock=threading.Lock(),
        work_dir=tmp_path,
    )

    assert result.status == "schema_extracted"
    assert result.progress == 1.0
