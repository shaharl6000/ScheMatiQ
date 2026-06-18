"""Unit tests for ScheMatiQRunner.request_stop optimistic 'stopped' broadcast.

These verify the fix for the bug where the monitor stayed in the
"Wrapping up current operation..." spinner state after Stop was clicked:
request_stop must now emit an immediate 'stopped' broadcast (with current
schema/row counts read from disk) so the UI leaves the spinner state right
away, instead of waiting for the pipeline task to reach its next checkpoint.

The runner module pulls in a heavy dependency chain (fastapi, the schematiq
library, sentence-transformers, ...) that is not needed to exercise this
logic, so those modules are stubbed in sys.modules before import. Run with:

    pytest backend/test_request_stop_broadcast.py
"""
import sys
import types
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


# ──────────────────────────────────────────────────────────────────
# Stub the heavy / environment-specific imports so we can import the
# ScheMatiQRunner class without installing the full backend stack.
# ──────────────────────────────────────────────────────────────────
def _install_import_stubs():
    class _AutoModule(types.ModuleType):
        """Module whose every attribute access returns a fresh MagicMock.

        Lets arbitrary `from schematiq.core.x import Y` succeed without us having
        to enumerate every imported name the backend pulls at import time.
        """
        def __getattr__(self, name):
            if name in ("__path__", "__all__", "__spec__", "__loader__"):
                raise AttributeError(name)
            val = MagicMock(name=f"{self.__name__}.{name}")
            setattr(self, name, val)
            return val

    def stub(name, is_pkg=False, auto=False):
        if name not in sys.modules:
            mod = _AutoModule(name) if auto else types.ModuleType(name)
            if is_pkg:
                mod.__path__ = []  # mark as a package so submodule imports resolve
            sys.modules[name] = mod
        return sys.modules[name]

    # schematiq library surface used at import time across the backend.
    core = stub("schematiq", is_pkg=True, auto=True)
    stub("schematiq.core", is_pkg=True, auto=True)
    stub("schematiq.core.model_specs", auto=True)
    stub("schematiq.core.retrievers", auto=True)
    stub("schematiq.core.schema", auto=True)
    stub("schematiq.core.llm_backends", auto=True)
    stub("schematiq.core.cost_estimator", auto=True)
    # Exception types must be real classes (used in `except`/`raise`).
    tracker_mod = stub("schematiq.core.llm_call_tracker", auto=True)
    tracker_mod.QuotaExceededError = type("QuotaExceededError", (Exception,), {})
    core.ObservationUnitDiscoveryError = type("ObservationUnitDiscoveryError", (Exception,), {})

    # value_extraction subpackage (pulled in via the pipeline package __init__).
    stub("schematiq.value_extraction", is_pkg=True, auto=True)
    stub("schematiq.value_extraction.main", auto=True)
    stub("schematiq.value_extraction.core", is_pkg=True, auto=True)
    stub("schematiq.value_extraction.core.table_builder", auto=True)


@pytest.fixture(scope="module")
def runner_cls():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _install_import_stubs()
    try:
        from app.services.schematiq_runner import ScheMatiQRunner
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Could not import ScheMatiQRunner in this environment: {exc}")
    return ScheMatiQRunner


def _make_runner(runner_cls, tmp_path):
    """Build a runner without running __init__ (avoids real WS/session managers)."""
    runner = runner_cls.__new__(runner_cls)
    runner.work_dir = Path(tmp_path)
    runner.running_sessions = {}
    runner.stop_flags = {}
    runner._state_lock = threading.Lock()
    runner.broadcast_stopped = AsyncMock()
    return runner


# ──────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_no_running_session_does_not_broadcast(runner_cls, tmp_path):
    runner = _make_runner(runner_cls, tmp_path)

    result = await runner.request_stop("missing-session")

    assert result["accepted"] is False
    runner.broadcast_stopped.assert_not_called()
    assert "missing-session" not in runner.stop_flags


@pytest.mark.asyncio
async def test_running_session_sets_flag_and_broadcasts_immediately(runner_cls, tmp_path):
    runner = _make_runner(runner_cls, tmp_path)
    session_id = "sess-1"
    runner.running_sessions[session_id] = MagicMock()  # pretend a task is running

    result = await runner.request_stop(session_id)

    assert result["accepted"] is True
    assert runner.stop_flags[session_id] is True
    # The optimistic broadcast must happen during request_stop, not later.
    runner.broadcast_stopped.assert_awaited_once()
    sid_arg, payload = runner.broadcast_stopped.await_args.args
    assert sid_arg == session_id
    # No artifacts on disk yet → conservative defaults.
    assert payload["schema_saved"] is False
    assert payload["data_rows_saved"] == 0


@pytest.mark.asyncio
async def test_broadcast_reflects_partial_artifacts_on_disk(runner_cls, tmp_path):
    runner = _make_runner(runner_cls, tmp_path)
    session_id = "sess-2"
    runner.running_sessions[session_id] = MagicMock()

    # Simulate a partially-completed run: schema saved + 3 extracted rows.
    session_dir = Path(tmp_path) / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "discovered_schema.json").write_text('{"schema": []}')
    (session_dir / "extracted_data.jsonl").write_text(
        '{"row": 1}\n{"row": 2}\n{"row": 3}\n'
    )

    await runner.request_stop(session_id)

    runner.broadcast_stopped.assert_awaited_once()
    _sid, payload = runner.broadcast_stopped.await_args.args
    assert payload["schema_saved"] is True
    assert payload["data_rows_saved"] == 3


@pytest.mark.asyncio
async def test_broadcast_failure_does_not_break_stop(runner_cls, tmp_path):
    """A failed broadcast must not prevent the stop flag from being honored."""
    runner = _make_runner(runner_cls, tmp_path)
    session_id = "sess-3"
    runner.running_sessions[session_id] = MagicMock()
    runner.broadcast_stopped = AsyncMock(side_effect=RuntimeError("ws down"))

    result = await runner.request_stop(session_id)

    assert result["accepted"] is True
    assert runner.stop_flags[session_id] is True
