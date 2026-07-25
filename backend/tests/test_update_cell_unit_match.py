"""Tests that update_cell can address observation-unit rows by their unit name.

Rows produced per observation unit (multiple units per source document) are
identified by ``_unit_name`` and have no ``_row_name``. update_cell must match on
the unit name so each unit's cell can be filled individually.
"""

import json
from pathlib import Path

import pytest

from app.services.data_editor import DataEditor
from app.storage.factory import reset_storage
from app.storage.local_backend import LocalStorageBackend


@pytest.fixture
def editor_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "work"
    data_dir = tmp_path / "data"
    work_dir.mkdir()
    data_dir.mkdir()
    reset_storage()
    backend = LocalStorageBackend(
        sessions_dir=str(tmp_path / "sessions"),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    monkeypatch.setattr("app.storage.factory.get_storage", lambda: backend)
    monkeypatch.setattr("app.storage.get_storage", lambda: backend)
    yield DataEditor(work_dir=str(work_dir), data_dir=str(data_dir)), data_dir
    reset_storage()


def _write(data_dir: Path, session_id: str, rows: list[dict]) -> Path:
    path = data_dir / session_id / "data.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return path


def _rows(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


@pytest.mark.asyncio
async def test_update_cell_matches_unit_name_when_no_row_name(editor_env):
    editor, data_dir = editor_env
    sid = "units"
    # Three observation units from the SAME source document, identified by unit
    # name, with no _row_name (as multi-unit extraction produces).
    path = _write(data_dir, sid, [
        {"appointee": {"answer": "", "excerpts": []}, "_unit_name": "Judge Canby",
         "_source_document": "docA"},
        {"appointee": {"answer": "", "excerpts": []}, "_unit_name": "Judge Forrest",
         "_source_document": "docA"},
        {"appointee": {"answer": "", "excerpts": []}, "_unit_name": "Judge Smith",
         "_source_document": "docA"},
    ])

    await editor.update_cell(sid, "Judge Canby", "appointee", "Democratic")
    await editor.update_cell(sid, "Judge Forrest", "appointee", "Trump")
    await editor.update_cell(sid, "Judge Smith", "appointee", "Other Republican")

    answers = [r["appointee"]["answer"] for r in _rows(path)]
    assert answers == ["Democratic", "Trump", "Other Republican"]


@pytest.mark.asyncio
async def test_update_cell_unit_name_nested_format(editor_env):
    editor, data_dir = editor_env
    sid = "units-nested"
    path = _write(data_dir, sid, [
        {"_unit_name": "Judge Canby", "_source_document": "docA",
         "data": {"appointee": {"answer": "", "excerpts": []}}},
    ])
    await editor.update_cell(sid, "Judge Canby", "appointee", "Democratic")
    assert _rows(path)[0]["data"]["appointee"]["answer"] == "Democratic"


@pytest.mark.asyncio
async def test_update_cell_still_matches_row_name(editor_env):
    editor, data_dir = editor_env
    sid = "rowname"
    path = _write(data_dir, sid, [
        {"appointee": {"answer": "", "excerpts": []}, "_row_name": "Coughenour"},
    ])
    await editor.update_cell(sid, "Coughenour", "appointee", "Other Republican")
    assert _rows(path)[0]["appointee"]["answer"] == "Other Republican"
