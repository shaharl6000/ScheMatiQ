"""Tests for undo/redo cell restores (PUT /schematiq/cell with a `restore`
body) versus plain manual edits.

A restore must never stamp manually_edited=True and must not go through the
plain answer/excerpts write path -- it exists specifically so undo/redo of a
bulk server-side rewrite (re-extraction, a deleted column's restored values)
does not misrepresent an automated replay as something the user typed
(SpreadsheetSurface.tsx's applyCellUpdates / helpers.ts's diffPaginatedData).

Restoring a cell that had NO value at all before the operation being undone
(restore=None) is the tricky case: `restore` being None on its own is
ambiguous with "no restore requested" (a plain edit's default). The route
distinguishes them by whether a body was sent at all (see
app.api.routes.schematiq.update_cell's is_restore derivation), and
DataEditor._apply_cell_update pops the key entirely rather than writing an
empty answer object, so the cell reads exactly as it did before the
operation being undone.
"""

import json
from pathlib import Path

import pytest

from app.api.routes.schematiq import CellRestoreBody, update_cell
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
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_plain_edit_stamps_manually_edited(editor_env):
    """Regression guard: a genuine edit (no restore) keeps today's behavior."""
    editor, data_dir = editor_env
    sid = "s-plain-edit"
    path = _write(
        data_dir, sid,
        [{"row_name": "r1", "col": {"answer": "old", "excerpts": [{"text": "x"}]}}],
    )

    await editor.update_cell(sid, "r1", "col", "typed by user")

    row = _rows(path)[0]
    assert row["col"] == {"answer": "typed by user", "excerpts": [], "manually_edited": True}


@pytest.mark.asyncio
async def test_restore_with_value_writes_verbatim_no_manually_edited(editor_env):
    editor, data_dir = editor_env
    sid = "s-restore-value"
    path = _write(
        data_dir, sid,
        [{"row_name": "r1", "col": {"answer": "new", "excerpts": [], "manually_edited": True}}],
    )

    original = {"answer": "original", "excerpts": [{"text": "grounded"}]}
    await editor.update_cell(sid, "r1", "col", "unused", restore=original, is_restore=True)

    row = _rows(path)[0]
    assert row["col"] == original


@pytest.mark.asyncio
async def test_restore_to_none_pops_the_key_without_stamping(editor_env):
    """The bug this guards: restoring a cell that had no value before the
    operation being undone must not fall through to the plain-edit path
    (which would stamp manually_edited=True on a value nobody typed)."""
    editor, data_dir = editor_env
    sid = "s-restore-empty"
    path = _write(
        data_dir, sid,
        [{"row_name": "r1", "col": {"answer": "filled by reextraction", "excerpts": []}}],
    )

    await editor.update_cell(sid, "r1", "col", "unused", restore=None, is_restore=True)

    row = _rows(path)[0]
    assert "col" not in row


@pytest.mark.asyncio
async def test_restore_to_none_on_flat_row_shape_pops_the_key(editor_env):
    """Same as above but for the flat (no `data` dict) runtime row shape that
    _apply_cell_update also handles."""
    editor, data_dir = editor_env
    sid = "s-restore-empty-flat"
    path = _write(
        data_dir, sid,
        [{"row_name": "r1", "col": "filled by reextraction"}],
    )

    await editor.update_cell(sid, "r1", "col", "unused", restore=None, is_restore=True)

    row = _rows(path)[0]
    assert "col" not in row


@pytest.mark.asyncio
async def test_route_derives_is_restore_from_body_presence(monkeypatch):
    """is_restore must come from whether a body was sent at all, not from
    whether `restore` is None -- both a plain edit (no body) and a
    restore-to-nothing (body={"restore": null}) pass restore=None to
    DataEditor, and only the route can tell them apart."""
    import app.api.routes.schematiq as schematiq_routes

    captured = {}

    async def fake_update_cell(session_id, row_name, column, value, **kwargs):
        captured["is_restore"] = kwargs.get("is_restore")
        captured["restore"] = kwargs.get("restore")
        return {"status": "success"}

    monkeypatch.setattr(schematiq_routes.session_manager, "get_session", lambda sid: object(), raising=True)
    monkeypatch.setattr(schematiq_routes.data_editor, "update_cell", fake_update_cell, raising=True)

    await update_cell("sid", "col", "value", row_name="r1", body=None)
    assert captured["is_restore"] is False

    await update_cell("sid", "col", "value", row_name="r1", body=CellRestoreBody(restore=None))
    assert captured["is_restore"] is True
    assert captured["restore"] is None

    await update_cell("sid", "col", "value", row_name="r1", body=CellRestoreBody(restore={"answer": "x"}))
    assert captured["is_restore"] is True
    assert captured["restore"] == {"answer": "x"}
