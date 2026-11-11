"""Tests for skipping unnecessary Supabase data-file hydration (Issue #643)."""

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from app.models.session import (
    ColumnInfo,
    DataStatistics,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import data_utils
from app.services.data_utils import (
    clear_missing_session_data_cache,
    ensure_session_data_file_local,
    persist_session_data_file,
    resolve_session_data_files,
)
from app.services.pipeline.data_query import get_data, load_all_rows


class CountingStorage:
    """In-memory storage stub that records every ``download_file`` call."""

    def __init__(self, files: Optional[Dict[str, bytes]] = None):
        self.files: Dict[str, bytes] = dict(files or {})
        self.download_calls: List[Tuple[str, str]] = []

    async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
        self.download_calls.append((bucket, path))
        return self.files.get(f"{bucket}/{path}")

    async def file_exists(self, bucket: str, path: str) -> bool:
        return f"{bucket}/{path}" in self.files

    async def upload_file(self, bucket: str, path: str, content: bytes, **kwargs) -> None:
        self.files[f"{bucket}/{path}"] = content

    async def delete_file(self, bucket: str, path: str) -> bool:
        return self.files.pop(f"{bucket}/{path}", None) is not None


def _jsonl_bytes(rows: List[dict]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")


def _make_session(
    session_id: str,
    *,
    session_type: SessionType = SessionType.SCHEMATIQ,
    status: SessionStatus = SessionStatus.PROCESSING,
    schema_discovery_completed: bool = False,
    statistics: Optional[DataStatistics] = None,
    additional_rows_added: int = 0,
    row_count: Optional[int] = None,
    original_row_count: Optional[int] = None,
) -> VisualizationSession:
    return VisualizationSession(
        id=session_id,
        type=session_type,
        status=status,
        metadata=SessionMetadata(
            source="test",
            schema_discovery_completed=schema_discovery_completed,
            additional_rows_added=additional_rows_added,
            row_count=row_count,
            original_row_count=original_row_count,
        ),
        columns=[ColumnInfo(name="Title", definition="Paper title")],
        statistics=statistics,
    )


def _zero_row_stats() -> DataStatistics:
    return DataStatistics(
        total_rows=0,
        total_columns=1,
        total_documents=0,
        completeness=0.0,
        column_stats=[
            ColumnInfo(
                name="Title",
                definition="Paper title",
                non_null_count=0,
                unique_count=0,
            )
        ],
    )


def _nonzero_row_stats(total_rows: int = 5) -> DataStatistics:
    return DataStatistics(
        total_rows=total_rows,
        total_columns=1,
        total_documents=total_rows,
        completeness=100.0,
        column_stats=[
            ColumnInfo(
                name="Title",
                definition="Paper title",
                non_null_count=total_rows,
                unique_count=total_rows,
            )
        ],
    )


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """Isolate work/data dirs, session_manager, storage, and negative cache."""
    clear_missing_session_data_cache()
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    # Ensure candidate_*_dirs don't pick up real repo directories.
    monkeypatch.setattr(data_utils, "candidate_work_dirs", lambda: [work_dir])
    monkeypatch.setattr(data_utils, "candidate_data_dirs", lambda: [data_dir])
    monkeypatch.setattr(data_utils, "get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(data_utils, "get_data_dir", lambda: data_dir)

    sessions: Dict[str, VisualizationSession] = {}

    class FakeSessionManager:
        def get_session(self, session_id: str) -> Optional[VisualizationSession]:
            return sessions.get(session_id)

    monkeypatch.setattr("app.services.session_manager", FakeSessionManager())

    storage = CountingStorage()
    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    yield work_dir, data_dir, sessions, storage

    clear_missing_session_data_cache()


@pytest.mark.asyncio
async def test_schema_discovery_in_progress_skips_storage_downloads(isolated_env):
    """While schema discovery runs, polling must make 0 storage.download_file calls."""
    work_dir, data_dir, sessions, storage = isolated_env
    sid = "sess-discovery"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.PROCESSING,
        schema_discovery_completed=False,
        statistics=None,
    )

    for _ in range(5):
        files = await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir)
        assert files == []
        hydrated = await ensure_session_data_file_local(
            sid, work_dir / sid / "extracted_data.jsonl", storage=storage
        )
        assert hydrated is False

    assert storage.download_calls == []


@pytest.mark.asyncio
async def test_observation_unit_review_skips_storage_downloads(isolated_env):
    """During observation-unit review pause, polling makes 0 storage.download_file calls."""
    work_dir, data_dir, sessions, storage = isolated_env
    sid = "sess-ou-review"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.OBSERVATION_UNIT_REVIEW,
        schema_discovery_completed=False,
        statistics=None,
    )

    page = await get_data(sid, work_dir=work_dir)
    rows = await load_all_rows(sid, work_dir=work_dir)

    assert page.total_count == 0
    assert rows == []
    assert storage.download_calls == []


@pytest.mark.asyncio
async def test_value_extraction_in_progress_uses_local_file_without_storage_downloads(
    isolated_env,
):
    """During SCHEMA_READY value extraction, polls never download from remote storage."""
    work_dir, data_dir, sessions, storage = isolated_env
    sid = "sess-extracting"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.SCHEMA_READY,
        schema_discovery_completed=True,
        statistics=None,
    )

    # Before incremental extraction writes extracted_data.jsonl:
    assert await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir) == []
    assert storage.download_calls == []

    # Once incremental extraction writes rows to local disk:
    local_extracted = work_dir / sid / "extracted_data.jsonl"
    local_extracted.parent.mkdir(parents=True, exist_ok=True)
    local_extracted.write_bytes(
        _jsonl_bytes([{"_row_name": "Row 1", "_papers": ["doc1.txt"], "Title": {"answer": "T1"}}])
    )

    resolved = await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir)
    assert [p.resolve() for p in resolved] == [local_extracted.resolve()]
    assert storage.download_calls == []


@pytest.mark.asyncio
async def test_completed_schema_only_skips_until_deferred_extraction_persists_rows(
    isolated_env,
):
    """Completed schema-only sessions skip downloads, then hydrate after deferred extraction."""
    work_dir, data_dir, sessions, storage = isolated_env
    sid = "sess-schema-only"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        schema_discovery_completed=True,
        statistics=_zero_row_stats(),
    )

    # Repeated polls after schema-only completion make 0 storage calls.
    for _ in range(10):
        page = await get_data(sid, work_dir=work_dir)
        rows = await load_all_rows(sid, work_dir=work_dir)
        assert page.total_count == 0
        assert rows == []

    assert storage.download_calls == []

    # Simulate deferred extraction writing 5 rows, persisting to storage, and updating stats.
    local_extracted = work_dir / sid / "extracted_data.jsonl"
    local_extracted.parent.mkdir(parents=True, exist_ok=True)
    extracted_rows = [
        {"_row_name": f"Row {i}", "_papers": [f"doc{i}.txt"], "Title": {"answer": f"Paper {i}"}}
        for i in range(5)
    ]
    local_extracted.write_bytes(_jsonl_bytes(extracted_rows))
    await persist_session_data_file(sid, local_extracted, storage=storage)
    sessions[sid].statistics = _nonzero_row_stats(5)

    # Simulate Railway container redeploy wiping local disk.
    local_extracted.unlink()
    assert not local_extracted.exists()

    resolved = await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir)
    assert [p.resolve() for p in resolved] == [local_extracted.resolve()]
    assert local_extracted.exists()
    assert ("data", f"{sid}/extracted_data.jsonl") in storage.download_calls

    page_after = await get_data(sid, work_dir=work_dir)
    assert page_after.total_count == 5


@pytest.mark.asyncio
async def test_session_with_recorded_rows_is_never_negative_cached_on_transient_outage(
    isolated_env,
):
    """Sessions with total_rows > 0 must retry storage immediately after a transient None."""
    work_dir, data_dir, sessions, _storage = isolated_env
    sid = "sess-outage"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        schema_discovery_completed=True,
        statistics=_nonzero_row_stats(2),
    )

    payload = _jsonl_bytes(
        [
            {"_row_name": "Row 1", "Title": {"answer": "A"}},
            {"_row_name": "Row 2", "Title": {"answer": "B"}},
        ]
    )

    class FlakyStorage(CountingStorage):
        def __init__(self):
            super().__init__({f"data/{sid}/extracted_data.jsonl": payload})
            self.fail_next_extracted = True

        async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
            self.download_calls.append((bucket, path))
            if path.endswith("extracted_data.jsonl") and self.fail_next_extracted:
                self.fail_next_extracted = False
                return None
            return self.files.get(f"{bucket}/{path}")

    flaky_storage = FlakyStorage()
    local_extracted = work_dir / sid / "extracted_data.jsonl"

    # Attempt 1: transient storage outage returns None -> hydration fails (caller returns 503).
    first = await resolve_session_data_files(
        sid, work_dir=work_dir, data_dir=data_dir, storage=flaky_storage
    )
    assert first == []
    assert not local_extracted.exists()
    calls_after_first = len(flaky_storage.download_calls)
    assert calls_after_first == 2  # extracted_data.jsonl + data.jsonl

    # Attempt 2 ("Retry now" 1s later): MUST NOT be blocked by negative cache!
    second = await resolve_session_data_files(
        sid, work_dir=work_dir, data_dir=data_dir, storage=flaky_storage
    )
    assert [p.resolve() for p in second] == [local_extracted.resolve()]
    assert local_extracted.exists()
    assert len(flaky_storage.download_calls) > calls_after_first


@pytest.mark.asyncio
async def test_ambiguous_stopped_session_uses_negative_cache_and_invalidates_on_persist(
    isolated_env,
):
    """Ambiguous STOPPED session negative-caches misses within TTL and invalidates on persist."""
    work_dir, data_dir, sessions, storage = isolated_env
    sid = "sess-stopped"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.STOPPED,
        schema_discovery_completed=True,
        statistics=None,
    )

    # First poll checks storage (extracted_data.jsonl + data.jsonl) and finds nothing.
    assert await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir) == []
    assert storage.download_calls == [
        ("data", f"{sid}/extracted_data.jsonl"),
        ("data", f"{sid}/data.jsonl"),
    ]

    # Subsequent polls within TTL hit the negative cache -> 0 new download_file calls.
    for _ in range(5):
        assert await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir) == []
    assert len(storage.download_calls) == 2

    # Persisting a data file invalidates the negative cache immediately.
    local_extracted = work_dir / sid / "extracted_data.jsonl"
    local_extracted.parent.mkdir(parents=True, exist_ok=True)
    local_extracted.write_bytes(_jsonl_bytes([{"_row_name": "Recovered", "Title": {"answer": "X"}}]))
    await persist_session_data_file(sid, local_extracted, storage=storage)

    # Wipe local copy to verify hydration immediately hits storage and succeeds.
    local_extracted.unlink()
    resolved = await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir)
    assert [p.resolve() for p in resolved] == [local_extracted.resolve()]
    assert local_extracted.exists()
    assert len(storage.download_calls) > 2


@pytest.mark.asyncio
async def test_created_sessions_skip_storage_downloads(isolated_env):
    """Freshly CREATED sessions (both UPLOAD and SCHEMATIQ) make 0 storage downloads."""
    work_dir, data_dir, sessions, storage = isolated_env
    for idx, stype in enumerate((SessionType.SCHEMATIQ, SessionType.UPLOAD)):
        sid = f"sess-created-{idx}"
        sessions[sid] = _make_session(
            sid,
            session_type=stype,
            status=SessionStatus.CREATED,
        )
        assert await resolve_session_data_files(sid, work_dir=work_dir, data_dir=data_dir) == []

    assert storage.download_calls == []


@pytest.mark.asyncio
async def test_download_exception_is_not_negative_cached(isolated_env):
    """If storage.download_file raises an exception, do not cache it as a missing file."""
    work_dir, _data_dir, sessions, _storage = isolated_env
    sid = "sess-exc"
    sessions[sid] = _make_session(
        sid,
        session_type=SessionType.SCHEMATIQ,
        status=SessionStatus.STOPPED,
        schema_discovery_completed=True,
        statistics=None,
    )

    payload = _jsonl_bytes([{"_row_name": "R1", "Title": {"answer": "OK"}}])
    attempts = {"count": 0}

    class RaisingThenWorkingStorage(CountingStorage):
        async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
            self.download_calls.append((bucket, path))
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("transient network failure")
            return payload

    custom_storage = RaisingThenWorkingStorage()
    target = work_dir / sid / "extracted_data.jsonl"

    assert await ensure_session_data_file_local(sid, target, storage=custom_storage) is False
    assert await ensure_session_data_file_local(sid, target, storage=custom_storage) is True
    assert target.exists()
    assert len(custom_storage.download_calls) == 2
