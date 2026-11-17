"""Data-file writers share one lock, write atomically, keep rows in their own
file, and persist every change to storage (so edits survive a redeploy)."""

import asyncio
import json
import threading
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from app.models.unit import MergeUnitsRequest
from app.services import session_data_store
from app.services.data_editor import _get_session_write_lock
from app.services.schema_manager import SchemaManager
from app.services.session_data_store import session_write_lock
from app.services.unit_view_service import UnitViewService


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


@pytest.fixture
def persisted(monkeypatch):
    """Record every persist call instead of uploading."""
    calls: list[Path] = []

    async def fake_persist(session_id, local_path, storage=None):
        calls.append(Path(local_path))

    monkeypatch.setattr(session_data_store, "persist_session_data_file", fake_persist)
    return calls


@pytest.fixture
def unit_svc(tmp_path, monkeypatch):
    svc = UnitViewService(data_dir=str(tmp_path / "data"))
    svc.work_dir = tmp_path / "work"
    svc._row_cache.clear()

    async def no_hydrate(*args, **kwargs):
        return []

    monkeypatch.setattr(
        "app.services.unit_view_service.resolve_session_data_files", no_hydrate
    )
    yield svc
    svc._row_cache.clear()


def test_data_editor_uses_the_shared_lock():
    assert _get_session_write_lock("s") is session_write_lock("s")


def test_reextraction_merges_use_the_shared_lock():
    from app.services.reextraction_service import ReextractionService

    svc = ReextractionService.__new__(ReextractionService)
    assert svc._get_incremental_merge_lock("s-rx") is session_write_lock("s-rx")


async def test_merge_units_keeps_rows_in_their_own_file_and_persists(
    tmp_path, unit_svc, persisted
):
    sid = "multi_file"
    primary = tmp_path / "work" / sid / "extracted_data.jsonl"
    extra = tmp_path / "data" / sid / "data.jsonl"
    _write_jsonl(primary, [
        {"_row_name": "a", "_unit_name": "GPT 4", "_source_document": "d1", "acc": "1"},
        {"_row_name": "b", "_unit_name": "Llama", "_source_document": "d1", "acc": "2"},
    ])
    _write_jsonl(extra, [
        {"_row_name": "c", "_unit_name": "gpt-4", "_source_document": "d2", "data": {"acc": "3"}},
    ])

    res = await unit_svc.merge_units_async(
        sid,
        MergeUnitsRequest(source_units=["GPT 4", "gpt-4"], target_unit="GPT-4", strategy="rename"),
    )

    assert res.success is True
    assert res.rows_affected == 2
    primary_rows = _read_jsonl(primary)
    extra_rows = _read_jsonl(extra)
    # Rows stay in the file they came from; nothing is copied into the primary.
    assert [r["_row_name"] for r in primary_rows] == ["a", "b"]
    assert [r["_row_name"] for r in extra_rows] == ["c"]
    assert primary_rows[0]["_unit_name"] == "GPT-4"
    assert primary_rows[1]["_unit_name"] == "Llama"
    assert extra_rows[0]["_unit_name"] == "GPT-4"
    # The transient view index never reaches disk.
    assert all("_row_index" not in r for r in primary_rows + extra_rows)
    assert sorted(persisted) == sorted([primary, extra])


async def test_rename_unit_persists_only_changed_files(tmp_path, unit_svc, persisted):
    sid = "rename"
    primary = tmp_path / "work" / sid / "extracted_data.jsonl"
    extra = tmp_path / "data" / sid / "data.jsonl"
    _write_jsonl(primary, [{"_row_name": "Judge A", "_source_document": "d1"}])
    _write_jsonl(extra, [{"_row_name": "Judge B", "_source_document": "d2"}])

    res = await unit_svc.rename_unit_async(sid, "Judge A", "Judge Alpha")

    assert res.success is True
    assert _read_jsonl(primary)[0]["_row_name"] == "Judge Alpha"
    assert persisted == [primary]


async def test_unit_mutation_waits_for_the_session_lock(tmp_path, unit_svc, persisted):
    sid = "locked"
    primary = tmp_path / "work" / sid / "extracted_data.jsonl"
    _write_jsonl(primary, [{"_row_name": "x", "_source_document": "d"}])

    lock = session_write_lock(sid)
    await lock.acquire()
    try:
        task = asyncio.create_task(unit_svc.rename_unit_async(sid, "x", "y"))
        await asyncio.sleep(0.05)
        assert not task.done()
        assert _read_jsonl(primary)[0]["_row_name"] == "x"
    finally:
        lock.release()
    await task
    assert _read_jsonl(primary)[0]["_row_name"] == "y"


async def test_cancelled_unit_mutation_holds_lock_until_thread_finishes(
    tmp_path, unit_svc, persisted
):
    sid = "cancelled"
    primary = tmp_path / "work" / sid / "extracted_data.jsonl"
    _write_jsonl(primary, [{"_row_name": "x", "_source_document": "d"}])
    started, release = threading.Event(), threading.Event()

    def slow_rewrite(session_id):
        started.set()
        release.wait(5)
        _write_jsonl(primary, [{"_row_name": "renamed", "_source_document": "d"}])
        return "done"

    task = asyncio.create_task(unit_svc._run_locked_mutation(sid, slow_rewrite))
    while not started.is_set():
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The worker thread is still writing: the lock must stay held.
    lock = session_write_lock(sid)
    assert lock.locked()
    release.set()
    async with lock:
        pass
    assert _read_jsonl(primary)[0]["_row_name"] == "renamed"
    assert persisted == [primary]


@pytest.fixture
def schema_mgr(monkeypatch, tmp_path):
    files: list[Path] = []

    async def fake_resolve(session_id, *args, **kwargs):
        return [f for f in files if f.exists()]

    monkeypatch.setattr("app.services.schema_manager.resolve_session_data_files", fake_resolve)
    mgr = SchemaManager(AsyncMock(), AsyncMock())
    return mgr, files


async def test_remove_column_data_handles_both_row_formats_in_every_file(
    tmp_path, schema_mgr, persisted
):
    mgr, files = schema_mgr
    flat = tmp_path / "work" / "s" / "extracted_data.jsonl"
    nested = tmp_path / "data" / "s" / "data.jsonl"
    _write_jsonl(flat, [{"_row_name": "a", "judge": "X", "judge_excerpt": "e", "court": "C"}])
    _write_jsonl(nested, [{"row_name": "b", "data": {"judge": "Y", "court": "D"}}])
    files.extend([flat, nested])

    await mgr.remove_column_data("s", "judge")

    assert _read_jsonl(flat) == [{"_row_name": "a", "court": "C"}]
    assert _read_jsonl(nested) == [{"row_name": "b", "data": {"court": "D"}}]
    assert sorted(persisted) == sorted([flat, nested])


async def test_merge_column_data_flat_rows_and_target_equal_to_source(
    tmp_path, schema_mgr, persisted
):
    mgr, files = schema_mgr
    flat = tmp_path / "work" / "s" / "extracted_data.jsonl"
    _write_jsonl(flat, [{"_row_name": "a", "first": "A", "last": "B"}])
    files.append(flat)

    await mgr.merge_column_data("s", ["first", "last"], "first", "concatenate", " ")

    assert _read_jsonl(flat) == [{"_row_name": "a", "first": "A B"}]
    assert persisted == [flat]

