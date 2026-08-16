"""Integration test: document availability unlocks extraction tools in load mode.

This wires the real pieces together — an imported (UPLOAD) session, real source
files on disk, the real ChatAgentService._extraction_capable signal, and the real
tool registry — to prove the end-to-end gating decision. The merged unit tests
either mock the capability signal or pass extraction_capable directly; this test
exercises the whole chain against the actual filesystem, so the availability gate
can't silently regress.

The pipeline itself is not run (that needs an LLM); this covers the gating
decision, which is the part that determines whether the chat can even offer to
re-extract an imported project.
"""

import pytest

from app.models.session import (
    ColumnInfo,
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.chat.agent_service import ChatAgentService
from app.services.chat.tool_registry import get_tools_for_context

# The four document-backed extraction tools that share the availability gate.
EXTRACTION_TOOLS = {"reextract", "extract_cells", "rediscover", "continue_discovery"}


def _create_upload_session(session_manager, session_id: str) -> VisualizationSession:
    session = VisualizationSession(
        id=session_id,
        type=SessionType.UPLOAD,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="import"),
        columns=[ColumnInfo(name="case_name", definition="Case name")],
        schema_query="judges voting on immigration policy",
        observation_unit=ObservationUnitInfo(name="Judge", definition="A single judge"),
    )
    session_manager.create_session(session)
    return session


def _tool_names(session_id: str):
    capable = ChatAgentService._extraction_capable(session_id, "load")
    tools = get_tools_for_context(session_id, "load", extraction_capable=capable)
    return capable, {t.name for t in tools}


@pytest.mark.asyncio
async def test_imported_session_with_local_docs_offers_extraction_tools(
    tmp_path, monkeypatch, session_manager_fixture
):
    monkeypatch.chdir(tmp_path)
    session_id = "import-with-docs"
    _create_upload_session(session_manager_fixture, session_id)

    # A real source file on disk, as a bundle import or "Show source document"
    # re-attach would leave it.
    docs = tmp_path / "data" / session_id / "documents"
    docs.mkdir(parents=True)
    (docs / "CASA2025-06-27SCt.txt").write_text("opinion text", encoding="utf-8")

    capable, names = _tool_names(session_id)

    # The real local-disk signal makes the imported session extraction-capable...
    assert capable is True
    # ...and the real registry therefore offers the document-backed tools.
    assert EXTRACTION_TOOLS <= names


@pytest.mark.asyncio
async def test_imported_session_without_docs_hides_extraction_tools(
    tmp_path, monkeypatch, session_manager_fixture
):
    monkeypatch.chdir(tmp_path)
    session_id = "import-no-docs"
    _create_upload_session(session_manager_fixture, session_id)

    # Documents dir exists but is empty (no source files, no cloud dataset).
    (tmp_path / "data" / session_id / "documents").mkdir(parents=True)

    capable, names = _tool_names(session_id)

    # No documents -> not capable -> extraction tools stay hidden. This is the
    # strict no-behavior-change guarantee for existing doc-less imports.
    assert capable is False
    assert EXTRACTION_TOOLS.isdisjoint(names)
    # reprocess remains available in load mode regardless (unchanged baseline).
    assert "reprocess" in names
