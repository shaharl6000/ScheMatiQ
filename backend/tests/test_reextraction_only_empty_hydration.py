"""The only_empty narrowing scan must hydrate data files from storage first.

_resolve_reextraction_data_files / _build_only_empty_targets read
extracted_data.jsonl off local disk. On a fresh worker (post-redeploy, or a
worker that did not run the original extraction) the file lives only in Supabase.
Without hydration the scan finds nothing, silently skips only_empty narrowing,
and runs a full extraction. These tests pin that _hydrate_reextraction_data_files
pulls the file down so the scan sees the current cell values.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services.reextraction_service import ReextractionService


class InMemoryStorage:
    def __init__(self, files: dict[str, bytes] | None = None):
        self.files = dict(files or {})

    async def download_file(self, bucket: str, path: str) -> bytes | None:
        return self.files.get(f"{bucket}/{path}")

    async def file_exists(self, bucket: str, path: str) -> bool:
        return f"{bucket}/{path}" in self.files


def _make_service() -> ReextractionService:
    return ReextractionService(websocket_manager=MagicMock(), session_manager=MagicMock())


def _jsonl(rows: list[dict]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode()


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "schematiq_work").mkdir(parents=True)
    (tmp_path / "data").mkdir(parents=True)
    return tmp_path


@pytest.mark.asyncio
async def test_hydrates_extracted_data_from_storage_when_local_absent(isolated_dirs, monkeypatch):
    tmp = isolated_dirs
    session_id = "sess-hydrate-1"
    rows = [{"_row_name": "Row A", "_papers": ["p1"], "col1": {"answer": "x", "excerpts": []}}]
    storage = InMemoryStorage({f"data/{session_id}/extracted_data.jsonl": _jsonl(rows)})
    monkeypatch.setattr("app.storage.get_storage", lambda: storage, raising=False)

    local = tmp / "schematiq_work" / session_id / "extracted_data.jsonl"
    assert not local.exists()  # fresh worker: nothing on disk

    svc = _make_service()
    await svc._hydrate_reextraction_data_files(session_id)

    assert local.exists(), "extracted_data.jsonl was not hydrated from storage"
    # And the sync scan resolver now finds it, so only_empty narrowing can run.
    resolved = svc._resolve_reextraction_data_files(session_id)
    assert local.resolve() in [p.resolve() for p in resolved]


@pytest.mark.asyncio
async def test_hydration_is_noop_when_local_file_present(isolated_dirs, monkeypatch):
    tmp = isolated_dirs
    session_id = "sess-hydrate-2"
    local = tmp / "schematiq_work" / session_id / "extracted_data.jsonl"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(_jsonl([{"_row_name": "Row A", "col1": {"answer": "local", "excerpts": []}}]))

    # Storage has different content; hydration must NOT overwrite the local copy.
    storage = InMemoryStorage(
        {f"data/{session_id}/extracted_data.jsonl": _jsonl([{"_row_name": "Row A", "col1": {"answer": "remote", "excerpts": []}}])}
    )
    monkeypatch.setattr("app.storage.get_storage", lambda: storage, raising=False)

    svc = _make_service()
    await svc._hydrate_reextraction_data_files(session_id)

    with open(local, encoding="utf-8") as f:
        row = json.loads(f.readline())
    assert row["col1"]["answer"] == "local"


@pytest.mark.asyncio
async def test_hydration_survives_storage_error(isolated_dirs, monkeypatch):
    session_id = "sess-hydrate-3"

    class BoomStorage:
        async def download_file(self, bucket, path):
            raise RuntimeError("storage down")

    monkeypatch.setattr("app.storage.get_storage", lambda: BoomStorage(), raising=False)

    svc = _make_service()
    # Must not raise: a storage hiccup falls back to the local (empty) view.
    await svc._hydrate_reextraction_data_files(session_id)
