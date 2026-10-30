"""Tests for batched cell updates (PUT /schematiq/cells).

A column clear used to send one request per cell; each request rewrote and
re-uploaded the whole data file behind the session lock, so a large clear ran
past the client timeout and most cells were reported as failed. The batch path
applies every update in memory and writes each file once.
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


def _write(data_dir: Path, sid: str, rows: list[dict]) -> Path:
    p = data_dir / sid / "data.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return p


def _rows(p: Path) -> list[dict]:
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


@pytest.mark.asyncio
async def test_batch_clears_whole_column_with_one_write(editor_env, monkeypatch):
    editor, data_dir = editor_env
    sid = "s-batch"
    rows = [
        {"_row_name": f"u{i}", "court": {"answer": "District Court", "excerpts": []}}
        for i in range(200)
    ]
    path = _write(data_dir, sid, rows)

    import app.services.data_editor as de
    writes = []
    real_write = de.write_jsonl_atomic
    monkeypatch.setattr(
        de, "write_jsonl_atomic",
        lambda p, r, **kw: (writes.append(p), real_write(p, r, **kw)),
    )

    results = await editor.update_cells(
        sid, [{"row_name": f"u{i}", "column": "court", "value": ""} for i in range(200)],
    )

    assert all(r["ok"] for r in results)
    assert len(writes) == 1
    assert all(r["court"]["answer"] == "" for r in _rows(path))


@pytest.mark.asyncio
async def test_missing_row_fails_only_its_entry(editor_env):
    editor, data_dir = editor_env
    sid = "s-partial"
    path = _write(data_dir, sid, [
        {"_row_name": "a", "col": {"answer": "1", "excerpts": []}},
        {"_row_name": "b", "col": {"answer": "2", "excerpts": []}},
    ])

    results = await editor.update_cells(sid, [
        {"row_name": "a", "column": "col", "value": "x"},
        {"row_name": "ghost", "column": "col", "value": "y"},
        {"row_name": "b", "column": "col", "value": "z"},
    ])

    assert [r["ok"] for r in results] == [True, False, True]
    assert "ghost" in results[1]["error"]
    stored = _rows(path)
    assert stored[0]["col"]["answer"] == "x"
    assert stored[1]["col"]["answer"] == "z"


@pytest.mark.asyncio
async def test_batch_restore_writes_value_verbatim(editor_env):
    editor, data_dir = editor_env
    sid = "s-restore"
    original = {"answer": "orig", "excerpts": [{"text": "e"}]}
    path = _write(data_dir, sid, [{"_row_name": "a", "col": {"answer": "", "excerpts": []}}])

    results = await editor.update_cells(sid, [
        {"row_name": "a", "column": "col", "value": "", "is_restore": True, "restore": original},
    ])

    assert results[0]["ok"]
    assert _rows(path)[0]["col"] == original


@pytest.mark.asyncio
async def test_single_update_cell_still_raises_on_missing_row(editor_env):
    editor, data_dir = editor_env
    sid = "s-single"
    _write(data_dir, sid, [{"_row_name": "a", "col": {"answer": "1", "excerpts": []}}])

    with pytest.raises(ValueError):
        await editor.update_cell(sid, "ghost", "col", "x")
