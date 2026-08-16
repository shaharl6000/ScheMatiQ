"""Tests for the chat `rediscover` handler.

`rediscover` triggers a full schema/observation-unit rebuild from source
documents (the same operation as POST /load/rediscover and the workspace
"Rediscover schema" button), re-evaluating previously-skipped documents. These
tests drive the handler with mocked I/O to assert the gate and the
per-session-type config handling, without running the real pipeline.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.session import (
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.chat import tool_executor as te_module
from app.services.chat.tool_executor import ToolExecutor


@pytest.fixture
def executor() -> ToolExecutor:
    return ToolExecutor()


def _make_session(session_manager, session_type: SessionType) -> VisualizationSession:
    session = VisualizationSession(
        id=f"rediscover-{session_type.value}",
        type=session_type,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[],
        schema_query="judges voting on immigration policy",
        observation_unit=ObservationUnitInfo(name="Judge", definition="A single judge"),
    )
    session_manager.create_session(session)
    return session


@pytest.fixture
def wired(monkeypatch, executor):
    """Mock the heavy deps the handler pulls from .deps and never run a pipeline."""
    reext = MagicMock()
    reext.discover_papers = AsyncMock(return_value={})
    reext.precheck_document_availability = AsyncMock(
        return_value={"can_proceed": True}
    )
    runner = MagicMock()
    runner.check_global_quota = MagicMock()
    runner.save_config = AsyncMock()
    runner.prepare_resume = AsyncMock()
    monkeypatch.setattr(te_module, "reextraction_service", reext)
    monkeypatch.setattr(te_module, "schematiq_runner", runner)
    # Do not spawn the real pipeline task.
    monkeypatch.setattr(executor, "_run_schematiq_task", AsyncMock())
    # Force the quota path to run (DEVELOPER_MODE off) so check_global_quota is exercised.
    monkeypatch.setattr(te_module, "DEVELOPER_MODE", False)
    return reext, runner


@pytest.mark.asyncio
async def test_rediscover_upload_synthesizes_config(
    executor, wired, session_manager_fixture
):
    reext, runner = wired
    session = _make_session(session_manager_fixture, SessionType.UPLOAD)

    result = await executor.execute("rediscover", session.id, "load", {})

    assert result["status"] == "started"
    # UPLOAD has no runnable config.json, so the handler synthesizes and saves one.
    runner.save_config.assert_awaited_once()
    runner.prepare_resume.assert_awaited_once()
    executor._run_schematiq_task.assert_awaited_once_with(session.id)


@pytest.mark.asyncio
async def test_rediscover_schematiq_preserves_existing_config(
    executor, wired, session_manager_fixture
):
    reext, runner = wired
    session = _make_session(session_manager_fixture, SessionType.SCHEMATIQ)

    result = await executor.execute("rediscover", session.id, "schematiq", {})

    assert result["status"] == "started"
    # SCHEMATIQ already has a config.json from /schematiq/configure; it must not
    # be overwritten with RELEASE_CONFIG defaults.
    runner.save_config.assert_not_awaited()
    runner.prepare_resume.assert_awaited_once()


@pytest.mark.asyncio
async def test_rediscover_rejected_without_documents(
    executor, wired, session_manager_fixture
):
    reext, runner = wired
    reext.precheck_document_availability = AsyncMock(
        return_value={"can_proceed": False}
    )
    session = _make_session(session_manager_fixture, SessionType.UPLOAD)

    with pytest.raises(ValueError, match="No source documents are available"):
        await executor.execute("rediscover", session.id, "load", {})

    runner.prepare_resume.assert_not_awaited()
    executor._run_schematiq_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_rediscover_requires_observation_unit(
    executor, wired, session_manager_fixture
):
    reext, runner = wired
    session = VisualizationSession(
        id="rediscover-no-ou",
        type=SessionType.UPLOAD,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[],
        schema_query="q",
        observation_unit=None,
    )
    session_manager_fixture.create_session(session)

    with pytest.raises(ValueError, match="observation unit"):
        await executor.execute("rediscover", session.id, "load", {})
