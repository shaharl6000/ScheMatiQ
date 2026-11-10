"""Tests for the observation-unit rediscovery state machine on SessionMetadata and ScheMatiQRunner."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.session import (
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.schematiq_runner import ScheMatiQRunner


def test_mark_observation_unit_edited_transitions():
    meta = SessionMetadata(source="test")

    # Initial review pause should never flag rediscovery even if columns exist.
    meta.schema_discovery_completed = True
    meta.mark_observation_unit_edited(
        status=SessionStatus.OBSERVATION_UNIT_REVIEW,
        has_columns=True,
    )
    assert meta.pending_observation_unit_rediscovery is False

    # Post-schema edit marks pending rediscovery.
    meta.mark_observation_unit_edited(
        status=SessionStatus.COMPLETED,
        has_columns=False,
    )
    assert meta.pending_observation_unit_rediscovery is True

    # Pre-schema edit with no columns does not mark pending rediscovery.
    meta.schema_discovery_completed = False
    meta.mark_observation_unit_edited(
        status=SessionStatus.CREATED,
        has_columns=False,
    )
    assert meta.pending_observation_unit_rediscovery is False


def test_rediscovery_run_lifecycle_helpers():
    meta = SessionMetadata(
        source="test",
        pending_observation_unit_rediscovery=True,
        skip_stop_data_collection=True,
    )
    meta.begin_observation_unit_rediscovery_run()
    assert meta.observation_unit_rediscovery_run is True

    assert meta.consume_skip_stop_data_collection() is True
    assert meta.skip_stop_data_collection is False
    assert meta.consume_skip_stop_data_collection() is False

    # Aborted/stopped run clears per-run flags but preserves pending_observation_unit_rediscovery
    meta.skip_stop_data_collection = True
    was_rediscovery = meta.finish_observation_unit_rediscovery_run(completed=False)
    assert was_rediscovery is True
    assert meta.pending_observation_unit_rediscovery is True
    assert meta.observation_unit_rediscovery_run is False
    assert meta.skip_stop_data_collection is False

    # Completed run clears all three flags
    meta.begin_observation_unit_rediscovery_run()
    meta.skip_stop_data_collection = True
    was_rediscovery = meta.finish_observation_unit_rediscovery_run(completed=True)
    assert was_rediscovery is True
    assert meta.pending_observation_unit_rediscovery is False
    assert meta.observation_unit_rediscovery_run is False
    assert meta.skip_stop_data_collection is False


@pytest.mark.asyncio
async def test_stop_after_schema_clears_per_run_flags_and_preserves_pending_and_upload_type(
    tmp_path, session_manager_fixture
):
    ws = MagicMock()
    ws.broadcast_to_session = AsyncMock()
    runner = ScheMatiQRunner(
        websocket_manager=ws,
        session_manager=session_manager_fixture,
        work_dir=str(tmp_path),
    )

    session = VisualizationSession(
        id="stop-schema-rediscover",
        type=SessionType.UPLOAD,
        status=SessionStatus.SCHEMA_READY,
        metadata=SessionMetadata(
            source="test",
            pending_observation_unit_rediscovery=True,
            observation_unit_rediscovery_run=True,
            skip_stop_data_collection=True,
        ),
    )
    session_manager_fixture.create_session(session)

    await runner._handle_stop_after_schema(session.id)

    updated = session_manager_fixture.get_session(session.id)
    assert updated.status == SessionStatus.STOPPED
    assert updated.type == SessionType.UPLOAD
    assert updated.metadata.pending_observation_unit_rediscovery is True
    assert updated.metadata.observation_unit_rediscovery_run is False
    assert updated.metadata.skip_stop_data_collection is False


@pytest.mark.asyncio
async def test_stop_after_extraction_clears_per_run_flags_preserves_pending_and_honors_skip_archive(
    tmp_path, session_manager_fixture
):
    ws = MagicMock()
    ws.broadcast_to_session = AsyncMock()
    data_collection = MagicMock()
    data_collection.trigger_archive = AsyncMock()
    runner = ScheMatiQRunner(
        websocket_manager=ws,
        session_manager=session_manager_fixture,
        work_dir=str(tmp_path),
        data_collection_service=data_collection,
    )

    session_id = "stop-extract-rediscover"
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "extracted_data.jsonl").write_text('{"row_name": "r1", "data": {}}\n')

    session = VisualizationSession(
        id=session_id,
        type=SessionType.UPLOAD,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(
            source="test",
            pending_observation_unit_rediscovery=True,
            observation_unit_rediscovery_run=True,
            skip_stop_data_collection=True,
        ),
    )
    session_manager_fixture.create_session(session)

    await runner._handle_stop_after_extraction(session_id)

    updated = session_manager_fixture.get_session(session_id)
    assert updated.status == SessionStatus.STOPPED
    assert updated.type == SessionType.UPLOAD
    assert updated.metadata.pending_observation_unit_rediscovery is True
    assert updated.metadata.observation_unit_rediscovery_run is False
    assert updated.metadata.skip_stop_data_collection is False
    data_collection.trigger_archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_schematiq_error_clears_per_run_flags_and_preserves_pending(
    tmp_path, session_manager_fixture
):
    ws = AsyncMock()
    runner = ScheMatiQRunner(
        websocket_manager=ws,
        session_manager=session_manager_fixture,
        work_dir=str(tmp_path),
    )

    session_id = "error-rediscover"
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "config.json").write_text(
        json.dumps(
            {
                "query": "test query",
                "output_path": "outputs/out.json",
                "schema_creation_backend": {"provider": "gemini", "model": "gemini-3.8-flash", "temperature": 0},
                "value_extraction_backend": {"provider": "gemini", "model": "gemini-3.5-flash-lite", "temperature": 0},
            }
        )
    )

    session = VisualizationSession(
        id=session_id,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(
            source="test",
            pending_observation_unit_rediscovery=True,
            observation_unit_rediscovery_run=True,
            skip_stop_data_collection=True,
        ),
    )
    session_manager_fixture.create_session(session)

    runner._execute_schematiq = AsyncMock(side_effect=RuntimeError("boom"))

    await runner.run_schematiq(session_id)

    updated = session_manager_fixture.get_session(session_id)
    assert updated.status == SessionStatus.ERROR
    assert updated.metadata.pending_observation_unit_rediscovery is True
    assert updated.metadata.observation_unit_rediscovery_run is False
    assert updated.metadata.skip_stop_data_collection is False


@pytest.mark.asyncio
async def test_run_schematiq_cancelled_error_clears_per_run_flags_and_preserves_pending(
    tmp_path, session_manager_fixture
):
    ws = AsyncMock()
    runner = ScheMatiQRunner(
        websocket_manager=ws,
        session_manager=session_manager_fixture,
        work_dir=str(tmp_path),
    )

    session_id = "cancelled-rediscover"
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "config.json").write_text(
        json.dumps(
            {
                "query": "test query",
                "output_path": "outputs/out.json",
                "schema_creation_backend": {"provider": "gemini", "model": "gemini-3.8-flash", "temperature": 0},
                "value_extraction_backend": {"provider": "gemini", "model": "gemini-3.5-flash-lite", "temperature": 0},
            }
        )
    )

    session = VisualizationSession(
        id=session_id,
        type=SessionType.UPLOAD,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(
            source="test",
            pending_observation_unit_rediscovery=True,
            observation_unit_rediscovery_run=True,
            skip_stop_data_collection=True,
        ),
    )
    session_manager_fixture.create_session(session)

    runner._execute_schematiq = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await runner.run_schematiq(session_id)

    updated = session_manager_fixture.get_session(session_id)
    assert updated.type == SessionType.UPLOAD
    assert updated.metadata.pending_observation_unit_rediscovery is True
    assert updated.metadata.observation_unit_rediscovery_run is False
    assert updated.metadata.skip_stop_data_collection is False


@pytest.mark.asyncio
async def test_prepare_resume_clears_skip_stop_data_collection_if_stop_did_not_consume(
    tmp_path, session_manager_fixture
):
    ws = MagicMock()
    ws.broadcast_to_session = AsyncMock()
    runner = ScheMatiQRunner(
        websocket_manager=ws,
        session_manager=session_manager_fixture,
        work_dir=str(tmp_path),
    )

    session_id = "prepare-resume-cleanup"
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "config.json").write_text(
        json.dumps(
            {
                "query": "test query",
                "schema_creation_backend": {"provider": "gemini", "model": "gemini-3.8-flash", "temperature": 0},
                "value_extraction_backend": {"provider": "gemini", "model": "gemini-3.5-flash-lite", "temperature": 0},
            }
        )
    )

    session = VisualizationSession(
        id=session_id,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        observation_unit=ObservationUnitInfo(name="Case", definition="A court ruling"),
        metadata=SessionMetadata(
            source="test",
            pending_observation_unit_rediscovery=True,
        ),
    )
    session_manager_fixture.create_session(session)

    # Simulate an active run whose stop_execution returns without consuming skip_stop_data_collection.
    runner.stop_flags[session_id] = True
    runner.stop_execution = AsyncMock(return_value={"stopped": True})

    await runner.prepare_resume(session_id)

    from app.services import concurrency_limiter
    await concurrency_limiter.release(session_id)

    updated = session_manager_fixture.get_session(session_id)
    assert updated.metadata.skip_stop_data_collection is False
    assert updated.metadata.observation_unit_rediscovery_run is True
