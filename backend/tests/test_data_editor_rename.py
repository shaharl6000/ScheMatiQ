"""Tests for storage-aware column rename across session data files."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services.data_editor import DataEditor
from app.services.data_utils import enumerate_session_data_files
from app.services.reextraction_service import ReextractionService
from app.storage.factory import reset_storage
from app.storage.local_backend import LocalStorageBackend


@pytest.fixture
def session_dirs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    return work_dir, data_dir


@pytest.fixture
def local_storage(session_dirs, tmp_path, monkeypatch):
    work_dir, data_dir = session_dirs
    reset_storage()
    backend = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    monkeypatch.setattr("app.storage.factory.get_storage", lambda: backend)
    monkeypatch.setattr("app.storage.get_storage", lambda: backend)
    yield backend
    reset_storage()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


@pytest.mark.asyncio
async def test_rename_column_updates_all_data_files(session_dirs, local_storage):
    work_dir, data_dir = session_dirs
    session_id = "test-session"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))

    row = {
        "_row_name": "row-1",
        "appointing_president": {"answer": "Obama", "excerpts": []},
    }
    _write_jsonl(work_dir / session_id / "extracted_data.jsonl", [row])
    _write_jsonl(
        data_dir / session_id / "data.jsonl",
        [{"row_name": "row-1", "data": {"appointing_president": {"answer": "Biden", "excerpts": []}}}],
    )

    result = await editor.rename_column(session_id, "appointing_president", "judge_name")

    assert result["files_updated"] == 2
    assert result["rows_updated"] == 2

    for path in enumerate_session_data_files(session_id, work_dir=work_dir, data_dir=data_dir):
        with open(path, encoding="utf-8") as f:
            for line in f:
                row_data = json.loads(line)
                cells = row_data.get("data", row_data)
                assert "appointing_president" not in cells
                assert "judge_name" in cells


@pytest.mark.asyncio
async def test_rename_column_hydrates_from_storage_when_local_missing(
    session_dirs, local_storage
):
    work_dir, data_dir = session_dirs
    session_id = "hydrate-session"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))

    remote_rows = [
        {
            "row_name": "row-1",
            "data": {"appointing_president": {"answer": "Roberts", "excerpts": []}},
        }
    ]
    remote_content = "\n".join(json.dumps(r) for r in remote_rows) + "\n"
    await local_storage.upload_file(
        "data",
        f"{session_id}/data.jsonl",
        remote_content.encode("utf-8"),
    )

    result = await editor.rename_column(session_id, "appointing_president", "judge_name")

    assert result["files_updated"] == 1
    local_file = data_dir / session_id / "data.jsonl"
    assert local_file.exists()
    with open(local_file, encoding="utf-8") as f:
        row_data = json.loads(f.readline())
    assert "appointing_president" not in row_data["data"]
    assert "judge_name" in row_data["data"]

    downloaded = await local_storage.download_file("data", f"{session_id}/data.jsonl")
    stored_row = json.loads(downloaded.decode("utf-8").splitlines()[0])
    assert "appointing_president" not in stored_row["data"]
    assert "judge_name" in stored_row["data"]


@pytest.mark.asyncio
async def test_rename_column_raises_when_no_data_files(session_dirs, local_storage):
    work_dir, data_dir = session_dirs
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))

    with pytest.raises(FileNotFoundError):
        await editor.rename_column("missing-session", "old_col", "new_col")


@pytest.mark.asyncio
async def test_rename_then_reextract_merge_removes_stale_key(
    session_dirs, local_storage, monkeypatch
):
    """Repro path: schema rename + re-extract must not leave the old column key."""
    work_dir, data_dir = session_dirs

    session_id = "rename-reextract-session"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))

    extracted_path = work_dir / session_id / "extracted_data.jsonl"
    load_path = data_dir / session_id / "data.jsonl"
    _write_jsonl(
        extracted_path,
        [
            {
                "_row_name": "row-1",
                "appointing_president": {"answer": "Obama", "excerpts": []},
            }
        ],
    )
    _write_jsonl(
        load_path,
        [
            {
                "row_name": "row-1",
                "data": {
                    "appointing_president": {"answer": "Biden", "excerpts": []},
                },
            }
        ],
    )

    await editor.rename_column(session_id, "appointing_president", "judge_name")

    # Simulate the original bug: stale old key survives in one file after re-extract
    # would have added judge_name alongside appointing_president.
    with open(extracted_path, encoding="utf-8") as f:
        row = json.loads(f.readline())
    row["appointing_president"] = {"answer": "stale", "excerpts": []}
    with open(extracted_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")

    extraction_output = work_dir / session_id / "reextract_output_test.jsonl"
    _write_jsonl(
        extraction_output,
        [
            {
                "_row_name": "row-1",
                "judge_name": {"answer": "Roberts", "excerpts": []},
            }
        ],
    )

    service = ReextractionService(MagicMock(), MagicMock())
    await service._merge_reextracted_data(
        session_id,
        ["judge_name"],
        extraction_output,
        renamed_from={"judge_name": "appointing_president"},
    )

    for path in enumerate_session_data_files(session_id, work_dir=work_dir, data_dir=data_dir):
        with open(path, encoding="utf-8") as f:
            for line in f:
                row_data = json.loads(line)
                cells = row_data.get("data", row_data)
                assert "appointing_president" not in cells
                assert cells["judge_name"]["answer"] == "Roberts"
