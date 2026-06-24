"""Tests that data reads hydrate session JSONL from storage when needed."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services.pipeline.data_query import get_data
from app.services.reextraction_service import ReextractionService


class InMemoryStorage:
    """Minimal storage mock for data-bucket JSONL hydration tests."""

    def __init__(self, files: dict[str, bytes] | None = None):
        self.files = dict(files or {})

    async def download_file(self, bucket: str, path: str) -> bytes | None:
        return self.files.get(f"{bucket}/{path}")

    async def file_exists(self, bucket: str, path: str) -> bool:
        return f"{bucket}/{path}" in self.files

    async def upload_file(self, bucket: str, path: str, content: bytes, **kwargs) -> None:
        self.files[f"{bucket}/{path}"] = content


def _jsonl_lines(rows: list[dict]) -> bytes:
    return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode()


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir()
    data_dir.mkdir()
    monkeypatch.setattr(
        "app.services.data_utils.get_schematiq_work_dir",
        lambda: work_dir,
    )
    monkeypatch.setattr(
        "app.services.data_utils.get_data_dir",
        lambda: data_dir,
    )
    return work_dir, data_dir


@pytest.mark.asyncio
async def test_get_data_hydrates_from_storage_when_local_missing(isolated_dirs, monkeypatch):
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-read-1"
    storage_key = f"data/{session_id}/extracted_data.jsonl"
    rows = [
        {
            "_row_name": "Row A",
            "_source_document": "doc1",
            "Title": {"answer": "Paper A"},
            "Year": {"answer": "2020"},
        },
        {
            "_row_name": "Row B",
            "_source_document": "doc2",
            "Title": {"answer": "Paper B"},
            "Year": {"answer": "2021"},
        },
    ]
    storage = InMemoryStorage({storage_key: _jsonl_lines(rows)})
    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    result = await get_data(session_id, work_dir=work_dir, page=0, page_size=50)

    assert result.total_count == 2
    assert len(result.rows) == 2
    row_names = {r.row_name for r in result.rows}
    assert row_names == {"Row A", "Row B"}
    assert (work_dir / session_id / "extracted_data.jsonl").exists()


@pytest.mark.asyncio
async def test_get_data_after_reextraction_merge_reads_from_storage_only(
    isolated_dirs, monkeypatch
):
    """Merge persists to storage; subsequent read must hydrate even if local is gone."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-read-2"
    storage = InMemoryStorage()
    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    async def _capture_persist(sid, path, storage_obj=None):
        from app.services.data_utils import storage_path_for_data_file

        storage_path = storage_path_for_data_file(path, sid)
        if storage_path:
            storage.files[f"data/{storage_path}"] = path.read_bytes()

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _capture_persist,
    )

    existing_rows = [
        {
            "_row_name": "Row A",
            "_source_document": "doc1",
            "Title": {"answer": "Original A"},
            "Year": {"answer": "2020"},
        },
    ]
    storage.files[f"data/{session_id}/extracted_data.jsonl"] = _jsonl_lines(existing_rows)

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.parent.mkdir(parents=True)
    extraction_file.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Row A",
                    "_source_document": "doc1",
                    "Title": {"answer": "Updated A"},
                },
            ]
        )
    )

    service = ReextractionService(MagicMock(), MagicMock())
    await service._merge_reextracted_data(
        session_id,
        columns=["Title"],
        extraction_file=extraction_file,
    )

    local_path = work_dir / session_id / "extracted_data.jsonl"
    assert local_path.exists()
    local_path.unlink()

    result = await get_data(session_id, work_dir=work_dir, page=0, page_size=50)

    assert result.total_count == 1
    assert result.rows[0].row_name == "Row A"
    title = result.rows[0].data.get("Title") if result.rows[0].data else None
    assert title == {"answer": "Updated A"}
    year = result.rows[0].data.get("Year") if result.rows[0].data else None
    assert year == {"answer": "2020"}
