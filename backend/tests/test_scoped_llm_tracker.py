"""Verify per-run ContextVar LLMCallTracker isolation across concurrent backend tasks."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.session import (
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import schematiq_thread_pool
from schematiq.core.llm_call_tracker import LLMCallTracker


@pytest.mark.asyncio
async def test_concurrent_tasks_on_thread_pool_have_isolated_trackers():
    """Two overlapping runs on schematiq_thread_pool run concurrently and never mix counts."""
    LLMCallTracker.get_instance().reset()
    both_inside_pool = asyncio.Barrier(2)

    async def run_job(initial_stage: str, inner_stage: str, calls: int) -> tuple[int, dict]:
        tracker, token = LLMCallTracker.activate(initial_stage)
        try:
            loop = asyncio.get_running_loop()

            def blocking_work():
                t = LLMCallTracker.get_instance()
                t.set_stage(inner_stage)
                for _ in range(calls):
                    t.increment()

            await both_inside_pool.wait()
            await loop.run_in_executor(schematiq_thread_pool, blocking_work)
            if initial_stage == "run_a":
                # Resetting inside run_a must not wipe run_b or affect anything outside.
                tracker.reset()
                LLMCallTracker.get_instance().set_stage("after_reset")
                LLMCallTracker.get_instance().increment()
            return tracker.get_total(), tracker.get_counts()
        finally:
            LLMCallTracker.deactivate(token)

    res_a, res_b = await asyncio.wait_for(
        asyncio.gather(
            run_job("run_a", "value_extraction", 4),
            run_job("run_b", "schema_discovery", 7),
        ),
        timeout=5.0,
    )

    assert res_a == (1, {"after_reset": 1})
    assert res_b == (7, {"schema_discovery": 7})
    assert LLMCallTracker.get_instance().get_total() == 0


@pytest.mark.asyncio
async def test_reextraction_counts_calls_after_stage_overwrite(monkeypatch):
    """run_tracker.get_total() captures calls even when PaperProcessor sets value_extraction."""
    from app.services.reextraction_service import ReextractionOperation, ReextractionService
    import app.services.orchestration as orch_mod

    sid = "sess-reext-tracker"
    session = VisualizationSession(
        id=sid,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        schema_query="q",
        columns=[],
        metadata=SessionMetadata(source="test"),
    )
    session_mgr = MagicMock()
    session_mgr.get_session.return_value = session

    recorded: list[tuple[str, dict]] = []
    fake_runner = MagicMock()
    fake_runner.record_external_usage.side_effect = (
        lambda rec_id, counts: recorded.append((rec_id, counts))
    )
    monkeypatch.setattr(orch_mod, "schematiq_runner", fake_runner, raising=False)

    svc = ReextractionService(
        websocket_manager=AsyncMock(),
        session_manager=session_mgr,
    )

    op_id = "op-tracker-1"
    op = ReextractionOperation(
        operation_id=op_id,
        session_id=sid,
        columns=["colA"],
        status="pending",
    )
    svc.active_operations[op_id] = op

    async def fake_discover_papers(*args, **kwargs):
        # Simulate inner LLM calls under a different stage.
        t = LLMCallTracker.get_instance()
        t.set_stage("value_extraction")
        t.increment()
        t.increment()
        t.increment()
        raise RuntimeError("abort after counting")

    monkeypatch.setattr(svc, "discover_papers", fake_discover_papers)
    monkeypatch.setattr(svc, "broadcast_event", AsyncMock())
    monkeypatch.setattr(svc, "_release_slot_once", AsyncMock())

    with pytest.raises(RuntimeError, match="abort after counting"):
        await svc._run_reextraction(op_id)

    assert len(recorded) == 1
    assert recorded[0][1] == {"reextraction": 3}
    assert LLMCallTracker.get_instance().get_total() == 0

