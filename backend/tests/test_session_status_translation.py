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


@pytest.mark.parametrize(
    "orphaned_status",
    [SessionStatus.PROCESSING, SessionStatus.SCHEMA_READY],
)
def test_startup_reconciles_orphaned_sessions_before_status_poll(
    tmp_path, orphaned_status
):
    from app.services.session_manager import SessionManager
    from app.storage.local_backend import LocalStorageBackend

    storage = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(tmp_path / "data"),
        schematiq_work_dir=str(tmp_path / "work"),
    )
    session_id = f"startup-orphan-{orphaned_status.value}"
    storage.save_session_sync(
        session_id,
        VisualizationSession(
            id=session_id,
            type=SessionType.SCHEMATIQ,
            status=orphaned_status,
            metadata=SessionMetadata(source="test", schema_discovery_completed=True),
        ).model_dump(mode="json"),
    )

    manager = SessionManager(storage=storage)
    reconciled = manager.reconcile_orphaned_sessions(
        running_sessions={}, state_lock=threading.Lock()
    )

    assert reconciled == 1
    loaded = manager.get_session(session_id)
    assert loaded is not None
    assert loaded.status == SessionStatus.STOPPED
    persisted = storage.get_session_sync(session_id)
    assert persisted is not None
    assert persisted["status"] == SessionStatus.STOPPED.value


@pytest.mark.asyncio
async def test_startup_reconcile_preserves_active_task_and_slot_sessions(tmp_path):
    from app.services.session_manager import SessionManager
    from app.storage.local_backend import LocalStorageBackend

    storage = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(tmp_path / "data"),
        schematiq_work_dir=str(tmp_path / "work"),
    )
    manager = SessionManager(storage=storage)

    task_session = VisualizationSession(
        id="active-task-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(source="test"),
    )
    slot_session = VisualizationSession(
        id="active-slot-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.SCHEMA_READY,
        metadata=SessionMetadata(source="test"),
    )
    orphan_session = VisualizationSession(
        id="orphan-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(source="test"),
    )
    manager.create_session(task_session)
    manager.create_session(slot_session)
    manager.create_session(orphan_session)

    await concurrency_limiter.acquire(slot_session.id, "schematiq")
    try:
        reconciled = manager.reconcile_orphaned_sessions(
            running_sessions={task_session.id: object()},
            state_lock=threading.Lock(),
        )
        assert reconciled == 1
        assert manager.get_session(task_session.id).status == SessionStatus.PROCESSING
        assert manager.get_session(slot_session.id).status == SessionStatus.SCHEMA_READY
        assert manager.get_session(orphan_session.id).status == SessionStatus.STOPPED
    finally:
        await concurrency_limiter.release(slot_session.id)


@pytest.mark.parametrize(
    "orphaned_status",
    [SessionStatus.PROCESSING, SessionStatus.SCHEMA_READY],
)
def test_lazy_load_reconciles_orphaned_session_on_first_get_session(
    tmp_path, orphaned_status
):
    from app.services.session_manager import SessionManager
    from app.storage.local_backend import LocalStorageBackend

    storage = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(tmp_path / "data"),
        schematiq_work_dir=str(tmp_path / "work"),
    )
    session_id = f"lazy-orphan-{orphaned_status.value}"
    storage.save_session_sync(
        session_id,
        VisualizationSession(
            id=session_id,
            type=SessionType.SCHEMATIQ,
            status=orphaned_status,
            metadata=SessionMetadata(source="test"),
        ).model_dump(mode="json"),
    )

    manager = SessionManager(storage=storage)
    loaded = manager.get_session(session_id)

    assert loaded is not None
    assert loaded.status == SessionStatus.STOPPED
    persisted = storage.get_session_sync(session_id)
    assert persisted is not None
    assert persisted["status"] == SessionStatus.STOPPED.value


@pytest.mark.asyncio
async def test_supabase_list_sessions_paginates_past_default_100_limit():
    from app.storage.supabase_backend import SupabaseStorageBackend

    calls = []

    class _FakeBucket:
        def list(self, path="", options=None):
            calls.append((path, options))
            limit = (options or {}).get("limit", 100)
            offset = (options or {}).get("offset", 0)
            total = 1250
            end = min(offset + limit, total)
            return [{"name": f"session-{i}.json", "id": str(i)} for i in range(offset, end)]

    class _FakeStorage:
        def from_(self, bucket):
            assert bucket == "sessions"
            return _FakeBucket()

    class _FakeClient:
        storage = _FakeStorage()

    backend = SupabaseStorageBackend.__new__(SupabaseStorageBackend)
    backend.client = _FakeClient()

    sync_ids = backend.list_sessions_sync()
    async_ids = await backend.list_sessions()

    assert len(sync_ids) == 1250
    assert sync_ids[0] == "session-0"
    assert sync_ids[-1] == "session-1249"
    assert async_ids == sync_ids
    assert calls[:2] == [
        ("", {"limit": 1000, "offset": 0}),
        ("", {"limit": 1000, "offset": 1000}),
    ]


