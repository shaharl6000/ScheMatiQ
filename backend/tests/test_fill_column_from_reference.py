"""Tests for fill_column_from_reference: per-row model fill from a reference.

The single LLM touchpoint (_extract_value_from_reference) is stubbed so the loop,
row iteration, write/stream, quota counting, and early-stop are verified
deterministically. Model-output quality is out of scope here.
"""

import pytest

import app.services.chat.tool_executor as te
from app.models.session import (
    ColumnInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.chat.tool_executor import ToolExecutor
from schematiq.core.llm_call_tracker import QuotaExceededError


class _Ref:
    id = "ref-1"
    filename = "FJC.csv"
    content = "judge,appointing_president\nCanby,Carter"  # small -> full context path


@pytest.fixture
def fill_session(session_manager_fixture):
    session = VisualizationSession(
        id="fill-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[ColumnInfo(name="appointee", definition="Appointing president")],
        schema_query="q",
    )
    session_manager_fixture.create_session(session)
    return session


def _wire(monkeypatch, executor, rows, values, quota_raises_after=None):
    """Stub IO + the LLM call. Returns (updates, broadcasts, recorded)."""
    updates: list[tuple] = []
    broadcasts: list[dict] = []
    recorded: list[dict] = []

    async def fake_load_all_rows(self, session_id):
        return rows

    async def fake_extract(self, client, unit, column, definition, context):
        return values[unit]

    async def fake_update_cell(session_id, row_name, column, value, **kw):
        updates.append((row_name, column, value, kw.get("reference_source")))
        return {"status": "success"}

    async def fake_broadcast(session_id, message):
        broadcasts.append(message)

    monkeypatch.setattr(te, "get_gemini_api_key", lambda: "test-key")
    monkeypatch.setattr(ToolExecutor, "_get_fill_client", lambda self: object())
    monkeypatch.setattr(ToolExecutor, "_load_all_rows", fake_load_all_rows)
    monkeypatch.setattr(ToolExecutor, "_extract_value_from_reference", fake_extract)
    monkeypatch.setattr(te.data_editor, "update_cell", fake_update_cell)
    monkeypatch.setattr(te.websocket_manager, "broadcast_to_session", fake_broadcast)
    monkeypatch.setattr(
        te, "get_reference_document", None, raising=False
    )

    import app.services.reference_document_service as refsvc

    monkeypatch.setattr(refsvc, "get_reference_document", lambda session, rid: _Ref())

    async def fake_load_text(session_id, ref):
        return _Ref.content

    monkeypatch.setattr(refsvc, "load_reference_text", fake_load_text)

    call_counter = {"n": 0}

    def fake_check(limit):
        call_counter["n"] += 1
        if quota_raises_after is not None and call_counter["n"] > quota_raises_after:
            raise QuotaExceededError(used=999, limit=limit)

    monkeypatch.setattr(te.schematiq_runner, "check_global_quota", fake_check)
    monkeypatch.setattr(
        te.schematiq_runner, "record_external_usage",
        lambda source_id, counts: recorded.append(counts),
    )
    return updates, broadcasts, recorded


@pytest.mark.asyncio
async def test_fill_runs_per_row_and_skips_missing(fill_session, monkeypatch):
    executor = ToolExecutor()
    rows = [
        {"unit_name": "Canby", "source_document": "docA"},
        {"unit_name": "Forrest", "source_document": "docA"},
        {"unit_name": "Unknown", "source_document": "docB"},
    ]
    values = {"Canby": "Democratic", "Forrest": "Trump", "Unknown": "N/A"}
    updates, broadcasts, recorded = _wire(monkeypatch, executor, rows, values)

    result = await executor.execute(
        "fill_column_from_reference", fill_session.id, "schematiq",
        {"column": "appointee", "reference_id": "ref-1"},
    )

    assert result["filled"] == 2 and result["skipped"] == 1 and result["total"] == 3
    assert [u[0] for u in updates] == ["Canby", "Forrest"]  # N/A row not written
    assert all(u[3] == "FJC.csv" for u in updates)  # attributed to the reference
    assert len(broadcasts) == 2 and all(b["type"] == "cell_extracted" for b in broadcasts)
    assert recorded == [{"chat": 3}]  # one model call per row, counted toward quota


@pytest.mark.asyncio
async def test_fill_stops_when_quota_reached(fill_session, monkeypatch):
    executor = ToolExecutor()
    rows = [
        {"unit_name": "Canby", "source_document": "docA"},
        {"unit_name": "Forrest", "source_document": "docA"},
    ]
    values = {"Canby": "Democratic", "Forrest": "Trump"}
    # Allow the first row's check, raise on the second.
    updates, broadcasts, recorded = _wire(
        monkeypatch, executor, rows, values, quota_raises_after=1
    )

    result = await executor.execute(
        "fill_column_from_reference", fill_session.id, "schematiq",
        {"column": "appointee", "reference_id": "ref-1"},
    )

    assert result["stopped"] is True
    assert result["filled"] == 1
    assert [u[0] for u in updates] == ["Canby"]


@pytest.mark.asyncio
async def test_fill_rejects_unknown_column(fill_session, monkeypatch):
    executor = ToolExecutor()
    _wire(monkeypatch, executor, [], {})
    with pytest.raises(ValueError):
        await executor.execute(
            "fill_column_from_reference", fill_session.id, "schematiq",
            {"column": "does_not_exist", "reference_id": "ref-1"},
        )
