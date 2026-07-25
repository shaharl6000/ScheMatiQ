"""Tests that same-named observation units in different documents are addressable.

A unit is uniquely identified by (unit name + source document). When two rows
share a unit name, update_cell must be able to target the right one via
source_document, and the chat agent must be able to supply it.
"""

import json
from pathlib import Path

import pytest

import app.services.chat.tool_executor as te
from app.services.chat.tool_executor import ToolExecutor
from app.services.chat.tool_registry import get_tools_for_context
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


def _same_name_rows(col="appointee"):
    return [
        {col: {"answer": "", "excerpts": []}, "_row_name": "John Smith",
         "_unit_name": "John Smith", "_source_document": "caseA"},
        {col: {"answer": "", "excerpts": []}, "_row_name": "John Smith",
         "_unit_name": "John Smith", "_source_document": "caseB"},
    ]


@pytest.mark.asyncio
async def test_source_document_disambiguates_same_name(editor_env):
    editor, data_dir = editor_env
    sid = "collide"
    path = _write(data_dir, sid, _same_name_rows())

    await editor.update_cell(sid, "John Smith", "appointee", "Reagan", source_document="caseA")
    await editor.update_cell(sid, "John Smith", "appointee", "Obama", source_document="caseB")

    by_doc = {r["_source_document"]: r["appointee"]["answer"] for r in _rows(path)}
    assert by_doc == {"caseA": "Reagan", "caseB": "Obama"}


def test_update_cell_tool_exposes_source_document_to_agent():
    tools = {t.name: t for t in get_tools_for_context("s1", "schematiq")}
    props = tools["update_cell"].parameters["properties"]
    assert "source_document" in props
    # session_id is injected by the server, not asked of the agent.
    assert "source_document" not in tools["update_cell"].server_injects


@pytest.mark.asyncio
async def test_executor_forwards_agent_source_document(monkeypatch):
    """An explicit source_document from the agent is passed through (and the
    ambiguous server-side guess is not used)."""
    captured = {}

    async def fake_update_cell(session_id, row_name, column, value, **kwargs):
        captured["row"] = row_name
        captured["source_document"] = kwargs.get("source_document")
        return {"status": "success"}

    async def fail_resolve(*a, **k):  # must not be called when agent supplies it
        raise AssertionError("server resolution should be skipped")

    monkeypatch.setattr(te.data_editor, "update_cell", fake_update_cell)
    monkeypatch.setattr(ToolExecutor, "_resolve_source_document", fail_resolve)

    ex = ToolExecutor()
    await ex.execute(
        "update_cell", "sid", "schematiq",
        {"row": "John Smith", "column": "appointee", "value": "Reagan",
         "source_document": "caseA"},
    )
    assert captured["source_document"] == "caseA"
