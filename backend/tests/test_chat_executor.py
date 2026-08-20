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

    async def fake_start(
        session_id,
        columns,
        renamed_from=None,
        paper_discovery=None,
        documents=None,
        rows=None,
        only_empty=False,
        only_empty_targets=None,
        retry_confirmed_empty=False,
    ):
        captured["columns"] = list(columns)
        return {"status": "started", "columns": list(columns)}

    async def noop(*args, **kwargs):
        return None

    async def fake_discover(*args, **kwargs):
        return {
            "total_rows": 0,
            "rows_with_papers": 0,
            "available_papers": ["doc1.txt"],
            "missing_papers": [],
            "paper_to_rows": {},
            "cloud_papers": {},
            "local_papers": ["doc1.txt"],
            "session_document_count": 1,
        }

    async def fake_precheck(*args, **kwargs):
        return {"can_proceed": True, "missing_documents": []}

    monkeypatch.setattr(te.reextraction_service, "discover_papers", fake_discover)
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


@pytest.mark.asyncio
async def test_reprocess_scopes_to_explicit_columns(executor, sample_session, patched_reextraction):
    _, captured = patched_reextraction
    result = await executor.execute(
        "reprocess", sample_session.id, "schematiq", {"columns": ["Title"]}
    )
    assert captured["columns"] == ["Title"]
    assert result["columns"] == ["Title"]


@pytest.mark.asyncio
async def test_reprocess_rejects_unknown_columns(executor, sample_session, patched_reextraction):
    with pytest.raises(ValueError, match="exist in the schema"):
        await executor.execute(
            "reprocess", sample_session.id, "schematiq", {"columns": ["Nope"]}
        )


@pytest.mark.asyncio
async def test_reprocess_rejects_excerpt_only_columns(
    executor, sample_session, patched_reextraction, session_manager_fixture,
):
    sample_session.columns.append(ColumnInfo(name="Title_excerpt", definition="Supporting text"))
    session_manager_fixture.update_session(sample_session)

    with pytest.raises(ValueError, match="No columns available for re-extraction"):
        await executor.execute(
            "reprocess",
            sample_session.id,
            "schematiq",
            {"columns": ["Title_excerpt"]},
        )


@pytest.mark.asyncio
async def test_reprocess_strips_excerpt_from_mixed_column_list(
    executor, sample_session, patched_reextraction, session_manager_fixture,
):
    sample_session.columns.append(ColumnInfo(name="Title_excerpt", definition="Supporting text"))
    session_manager_fixture.update_session(sample_session)
    _, captured = patched_reextraction

    result = await executor.execute(
        "reprocess",
        sample_session.id,
        "schematiq",
        {"columns": ["Title", "Title_excerpt"]},
    )

    assert captured["columns"] == ["Title"]
    assert result["columns"] == ["Title"]


@pytest.mark.asyncio
async def test_fill_tool_delegates_to_background_service(executor, sample_session, monkeypatch):
    """The chat tool starts the background fill service (returns immediately)
    rather than looping synchronously in the chat turn."""
    import app.services.chat.tool_executor as te

    captured: dict = {}

    async def fake_start(session_id, column, reference_id, rows=None, only_empty=False):
        captured.update(
            {
                "session_id": session_id,
                "column": column,
                "reference_id": reference_id,
                "only_empty": only_empty,
            }
        )
        return {"status": "started", "fill_id": "f1", "total": 3}

    monkeypatch.setattr(te.reference_fill_service, "start_fill", fake_start)
    result = await executor.execute(
        "fill_column_from_reference", sample_session.id, "schematiq",
        {"column": "Year", "reference_id": "r1"},
    )
    assert result["status"] == "started"
    assert captured == {
        "session_id": sample_session.id,
        "column": "Year",
        "reference_id": "r1",
        "only_empty": False,
    }
