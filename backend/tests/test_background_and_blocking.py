"""Background tasks are kept alive and logged; slow per-request work runs off
the event loop; persistence failures are reported."""

import asyncio
import gc
import logging
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import background_tasks
from app.core.background_tasks import spawn_background


async def test_spawn_background_keeps_a_reference_until_done():
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        return 42

    task = spawn_background(work(), name="kept")
    del task
    gc.collect()
    await started.wait()
    assert any(t.get_name() == "kept" for t in background_tasks._background_tasks)
    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not any(t.get_name() == "kept" for t in background_tasks._background_tasks)


async def test_spawn_background_logs_failures(caplog):
    async def boom():
        raise RuntimeError("kaput")

    with caplog.at_level(logging.ERROR, logger="app.core.background_tasks"):
        task = spawn_background(boom(), name="boom-task")
        with pytest.raises(RuntimeError):
            await task
        await asyncio.sleep(0)
    assert any("boom-task" in r.getMessage() for r in caplog.records)


async def test_attach_source_documents_converts_off_the_event_loop(tmp_path, monkeypatch):
    from app.api.routes import units

    loop_thread = threading.get_ident()
    seen_threads: list[int] = []

    def fake_commit(written: Path, documents_dir: Path, enable_figure_extraction: bool = True):
        seen_threads.append(threading.get_ident())
        raise ValueError("conversion failed")  # original must still be kept

    monkeypatch.setattr(units, "commit_document_to_documents_dir", fake_commit)
    monkeypatch.setattr(units, "get_data_dir", lambda: tmp_path)
    monkeypatch.setattr(units.session_manager, "get_session", lambda sid: SimpleNamespace())

    upload = SimpleNamespace(filename="a.pdf")

    async def read():
        return b"%PDF-1.4 test"

    upload.read = read

    result = await units.attach_source_documents("sid", files=[upload])

    assert result["attached"] == ["a.pdf"]
    assert seen_threads and seen_threads[0] != loop_thread
    assert (tmp_path / "sid" / "pending_documents" / "a.pdf").exists()


async def test_persist_session_data_file_reports_failure(tmp_path, caplog, monkeypatch):
    from app.services import data_utils

    local = tmp_path / "data" / "sid" / "data.jsonl"
    local.parent.mkdir(parents=True)
    local.write_text('{"a": 1}\n')

    class FailingStorage:
        async def upload_file(self, *args, **kwargs):
            raise ConnectionError("storage down")

    class OkStorage:
        async def upload_file(self, *args, **kwargs):
            return "ok"

    monkeypatch.setattr(
        data_utils, "storage_path_for_data_file", lambda p, sid: f"{sid}/data.jsonl"
    )
    with caplog.at_level(logging.ERROR, logger=data_utils.logger.name):
        assert await data_utils.persist_session_data_file("sid", local, FailingStorage()) is False
    assert any("storage down" in r.getMessage() for r in caplog.records)
    assert await data_utils.persist_session_data_file("sid", local, OkStorage()) is True

