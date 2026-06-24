"""Tests for chat tool executor read handlers."""

import pytest

from app.models.session import ColumnInfo, SessionMetadata, SessionStatus, SessionType, VisualizationSession
from app.services.chat.tool_executor import ToolExecutor


@pytest.fixture
def executor() -> ToolExecutor:
    return ToolExecutor()


@pytest.fixture
def sample_session(session_manager_fixture):
    session_manager = session_manager_fixture
    session = VisualizationSession(
        id="chat-test-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[
            ColumnInfo(name="Title", definition="Paper title"),
            ColumnInfo(name="Year", definition="Publication year"),
        ],
        schema_query="test query",
    )
    session_manager.create_session(session)
    return session


@pytest.mark.asyncio
async def test_get_schema(executor, sample_session):
    result = await executor.execute(
        "get_schema",
        sample_session.id,
        "schematiq",
        {},
    )
    assert result["query"] == "test query"
    assert len(result["schema"]) == 2


@pytest.mark.asyncio
async def test_get_validation_flags_missing_definitions(executor, sample_session):
    sample_session.columns.append(ColumnInfo(name="X", definition=""))
    from app.services import session_manager
    session_manager.update_session(sample_session)

    result = await executor.execute(
        "get_validation",
        sample_session.id,
        "schematiq",
        {},
    )
    assert result["is_valid"] is True
    assert "X" in result["missing_definitions"]


@pytest.mark.asyncio
async def test_unknown_tool_raises(executor, sample_session):
    with pytest.raises(ValueError, match="Unknown tool"):
        await executor.execute("not_a_tool", sample_session.id, "schematiq", {})


@pytest.fixture
def patched_reextraction(monkeypatch):
    """Stub the expensive re-extraction plumbing and capture the scoped columns."""
    from app.services.chat import tool_executor as te

    captured: dict[str, list] = {}

    async def fake_start(session_id, columns, renamed_from=None):
        captured["columns"] = list(columns)
        return {"status": "started", "columns": list(columns)}

    async def noop(*args, **kwargs):
        return None

    async def fake_precheck(*args, **kwargs):
        return {"can_proceed": True, "missing_documents": []}

    monkeypatch.setattr(te.reextraction_service, "start_reextraction", fake_start)
    monkeypatch.setattr(te.reextraction_service, "capture_and_save_baseline", noop)
    monkeypatch.setattr(te.reextraction_service, "precheck_document_availability", fake_precheck)
    monkeypatch.setattr(te.concurrency_limiter, "acquire", noop)
    monkeypatch.setattr(te.concurrency_limiter, "release", noop)
    return te, captured


@pytest.mark.asyncio
async def test_reextract_scopes_to_explicit_columns(executor, sample_session, patched_reextraction):
    _, captured = patched_reextraction
    result = await executor.execute(
        "reextract", sample_session.id, "schematiq", {"columns": ["Title"]}
    )
    assert captured["columns"] == ["Title"]
    assert result["columns"] == ["Title"]


@pytest.mark.asyncio
async def test_reextract_rejects_unknown_columns(executor, sample_session, patched_reextraction):
    with pytest.raises(ValueError, match="exist in the schema"):
        await executor.execute(
            "reextract", sample_session.id, "schematiq", {"columns": ["Nope"]}
        )


@pytest.mark.asyncio
async def test_reextract_edited_only_does_not_widen_to_all(executor, sample_session, patched_reextraction):
    te, _ = patched_reextraction
    te.reextraction_service.detect_schema_changes = lambda session: {}
    with pytest.raises(ValueError, match="No edited or new columns"):
        await executor.execute(
            "reextract", sample_session.id, "schematiq", {"scope": "edited_only"}
        )
