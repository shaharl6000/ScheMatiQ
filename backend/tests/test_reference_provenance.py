"""Tests for external-reference provenance marking on cell updates."""

import json
from pathlib import Path

import pytest

from app.services.data_editor import DataEditor
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


def _read_row(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.loads(f.readline())


@pytest.mark.asyncio
async def test_reference_source_marks_cell_external(session_dirs, local_storage):
    work_dir, data_dir = session_dirs
    session_id = "prov-nested"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))
    data_file = data_dir / session_id / "data.jsonl"
    _write_jsonl(
        data_file,
        [{"row_name": "Acker", "data": {"president": {"answer": "", "excerpts": []}}}],
    )

    await editor.update_cell(
        session_id, "Acker", "president", "Ronald Reagan", reference_source="FJC.xlsx"
    )

    row = _read_row(data_file)
    assert row["_cell_status"]["president"] == "external_source"
    cell = row["data"]["president"]
    assert cell["answer"] == "Ronald Reagan"
    assert cell["excerpts"] and cell["excerpts"][0]["source"] == "FJC.xlsx"


@pytest.mark.asyncio
async def test_reference_source_marks_flat_format(session_dirs, local_storage):
    work_dir, data_dir = session_dirs
    session_id = "prov-flat"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))
    data_file = data_dir / session_id / "data.jsonl"
    _write_jsonl(data_file, [{"row_name": "Acker", "president": {"answer": "", "excerpts": []}}])

    await editor.update_cell(
        session_id, "Acker", "president", "Ronald Reagan", reference_source="FJC.xlsx"
    )

    row = _read_row(data_file)
    assert row["_cell_status"]["president"] == "external_source"
    assert row["president"]["excerpts"][0]["source"] == "FJC.xlsx"


@pytest.mark.asyncio
async def test_no_reference_source_leaves_plain_manual_edit(session_dirs, local_storage):
    work_dir, data_dir = session_dirs
    session_id = "prov-plain"
    editor = DataEditor(work_dir=str(work_dir), data_dir=str(data_dir))
    data_file = data_dir / session_id / "data.jsonl"
    _write_jsonl(
        data_file,
        [{"row_name": "Acker", "data": {"president": {"answer": "", "excerpts": []}}}],
    )

    await editor.update_cell(session_id, "Acker", "president", "Ronald Reagan")

    row = _read_row(data_file)
    assert "_cell_status" not in row
    assert row["data"]["president"]["excerpts"] == []
    assert row["data"]["president"]["manually_edited"] is True
