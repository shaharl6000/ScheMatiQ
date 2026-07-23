"""Tests for external reference documents: model, conversion service, chat tools."""

import io

import pytest

from app.models.session import (
    ColumnInfo,
    ReferenceDocument,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import reference_document_service as refsvc
from app.services.chat.tool_executor import ToolExecutor
from app.services.chat.tool_registry import get_tools_for_context


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def test_session_reference_documents_roundtrip():
    """Reference docs survive model_dump() -> VisualizationSession(**) persistence."""
    ref = ReferenceDocument(id="r1", filename="judges.csv", content="a,b\n1,2", char_count=7)
    session = VisualizationSession(
        id="ref-model-roundtrip",
        type=SessionType.SCHEMATIQ,
        metadata=SessionMetadata(source="test"),
        reference_documents=[ref],
    )
    restored = VisualizationSession(**session.model_dump())
    assert len(restored.reference_documents) == 1
    assert restored.reference_documents[0].filename == "judges.csv"
    assert restored.reference_documents[0].content == "a,b\n1,2"


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def test_extract_text_csv_passthrough():
    raw = b"judge,president\r\nSmith,Trump\r\nJones,Obama\r\n"
    text, truncated = refsvc.extract_text("judges.csv", raw)
    assert "judge,president" in text
    assert "Smith,Trump" in text
    assert "\r" not in text  # CRLF normalised
    assert truncated is False


def test_extract_text_xlsx():
    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    df = pd.DataFrame({"judge": ["Smith", "Jones"], "president": ["Trump", "Obama"]})
    buf = io.BytesIO()
    df.to_excel(buf, index=False, sheet_name="mapping")
    text, truncated = refsvc.extract_text("judges.xlsx", buf.getvalue())
    assert "Sheet: mapping" in text
    assert "judge" in text and "president" in text
    assert "Smith" in text and "Trump" in text
    assert truncated is False


def test_extract_text_unsupported_extension():
    with pytest.raises(refsvc.UnsupportedReferenceFormat):
        refsvc.extract_text("archive.zip", b"PK\x03\x04")


def test_extract_text_empty_raises():
    with pytest.raises(refsvc.ReferenceExtractionError):
        refsvc.extract_text("empty.txt", b"   \n  ")


def test_extract_text_truncates_at_cap(monkeypatch):
    monkeypatch.setattr(refsvc, "MAX_REFERENCE_CHARS", 10)
    text, truncated = refsvc.extract_text("big.txt", b"x" * 50)
    assert len(text) == 10
    assert truncated is True


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def _session(sid: str) -> VisualizationSession:
    return VisualizationSession(
        id=sid, type=SessionType.SCHEMATIQ, metadata=SessionMetadata(source="test")
    )


def test_add_list_get_remove_reference():
    session = _session("ref-helpers")
    ref = refsvc.build_reference_document("judges.csv", b"judge,president\nSmith,Trump")
    refsvc.add_reference_document(session, ref)

    assert len(refsvc.list_reference_documents(session)) == 1
    assert refsvc.get_reference_document(session, ref.id) is ref
    assert refsvc.get_reference_document(session, "nope") is None

    assert refsvc.remove_reference_document(session, ref.id) is True
    assert refsvc.list_reference_documents(session) == []
    assert refsvc.remove_reference_document(session, ref.id) is False


# ---------------------------------------------------------------------------
# Chat tools
# ---------------------------------------------------------------------------

def test_reference_tools_present_in_schematiq_and_load():
    for mode in ("schematiq", "load"):
        names = {t.name for t in get_tools_for_context("s1", mode)}
        assert "list_reference_sources" in names
        assert "read_reference_source" in names


@pytest.fixture
def executor() -> ToolExecutor:
    return ToolExecutor()


@pytest.fixture
def session_with_reference(session_manager_fixture):
    session_manager = session_manager_fixture
    session = VisualizationSession(
        id="ref-chat-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[ColumnInfo(name="Judge", definition="Judge name")],
    )
    ref = refsvc.build_reference_document(
        "appointments.csv", b"judge,president\nSmith,Trump\nJones,Obama"
    )
    refsvc.add_reference_document(session, ref)
    session_manager.create_session(session)
    return session, ref


@pytest.mark.asyncio
async def test_list_reference_sources_tool(executor, session_with_reference):
    session, ref = session_with_reference
    result = await executor.execute("list_reference_sources", session.id, "schematiq", {})
    assert result["count"] == 1
    entry = result["reference_sources"][0]
    assert entry["id"] == ref.id
    assert entry["filename"] == "appointments.csv"
    assert "content" not in entry  # listing must not dump full bodies


@pytest.mark.asyncio
async def test_read_reference_source_tool(executor, session_with_reference):
    session, ref = session_with_reference
    result = await executor.execute(
        "read_reference_source", session.id, "schematiq", {"reference_id": ref.id}
    )
    assert result["filename"] == "appointments.csv"
    assert "Smith,Trump" in result["content"]


@pytest.mark.asyncio
async def test_read_reference_source_missing_id(executor, session_with_reference):
    session, _ = session_with_reference
    with pytest.raises(ValueError, match="reference_id is required"):
        await executor.execute("read_reference_source", session.id, "schematiq", {})


@pytest.mark.asyncio
async def test_read_reference_source_large_content_survives_truncation(
    executor, session_manager_fixture
):
    """A reference bigger than the chat budget must still return usable content.

    Regression: truncate_result drops an oversized string value outright, so the
    handler must pre-clip content to survive."""
    from app.services.chat.tool_executor import READ_REFERENCE_CHAT_BUDGET

    session_manager = session_manager_fixture
    big = "judge_{i},president_{i}\n".format(i="X") * 2000  # well over 8 KB
    session = VisualizationSession(
        id="ref-large-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
    )
    ref = refsvc.build_reference_document("big.csv", big.encode("utf-8"))
    refsvc.add_reference_document(session, ref)
    session_manager.create_session(session)

    result = await executor.execute(
        "read_reference_source", session.id, "schematiq", {"reference_id": ref.id}
    )
    assert result.get("content"), "content must not be dropped by truncation"
    assert len(result["content"]) <= READ_REFERENCE_CHAT_BUDGET
    assert result["content_clipped"] is True


@pytest.mark.asyncio
async def test_read_reference_source_unknown_id(executor, session_with_reference):
    session, _ = session_with_reference
    with pytest.raises(ValueError, match="not found"):
        await executor.execute(
            "read_reference_source", session.id, "schematiq", {"reference_id": "nope"}
        )
