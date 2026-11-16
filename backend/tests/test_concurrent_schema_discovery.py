"""Concurrency tests for run_schema_discovery saturating schematiq_thread_pool."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pytest

from app.core.config import SCHEMATIQ_THREAD_POOL_SIZE
from app.services.pipeline.schema_discovery import (
    run_callback_on_loop,
    run_schema_discovery,
)
from schematiq.core import schema as schema_mod
from schematiq.core.llm_backends import LLMInterface


class _FakeEmbeddingModel:
    """Fast deterministic CPU embedding model for unit-testing concurrency without weights."""

    device = "cpu"

    def encode(
        self,
        text: str,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
    ) -> np.ndarray:
        seed = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")
        vec = np.random.default_rng(seed).standard_normal(64).astype(np.float32)
        if normalize_embeddings:
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
        return vec


class ConcurrentFakeLLM(LLMInterface):
    """Thread-safe deterministic fake LLM for concurrent discovery testing."""

    _provider = "openai"

    def __init__(self):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = 16384

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        if isinstance(prompt, list):
            system = prompt[0].get("content", "") if len(prompt) > 0 else ""
            user = prompt[1].get("content", "") if len(prompt) > 1 else ""
        else:
            system = ""
            user = str(prompt)

        if "ObservationUnitLLM" in system:
            return json.dumps(
                {
                    "observation_unit": {
                        "name": "Court Ruling",
                        "definition": "A single judicial decision",
                        "example_names": ["Case 1"],
                    },
                    "reasoning": "Test unit",
                }
            )

        if "SchemaLLM" in system:
            match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
            passages_text = match.group(1) if match else user
            if "DOC_SPLIT_PAIR" in passages_text and "DOC_ALPHA" in passages_text and "DOC_BETA" in passages_text:
                raise RuntimeError("400 INVALID_ARGUMENT: maximum context length exceeded")
            columns: List[Dict[str, Any]] = []
            if "DOC_ALPHA" in passages_text:
                columns.append(
                    {
                        "name": "case_identifier",
                        "definition": "Unique docket identifier",
                        "rationale": "Identifies each ruling",
                        "value_kind": "identifier",
                    }
                )
            if "DOC_BETA" in passages_text:
                columns.append(
                    {
                        "name": "court_level",
                        "definition": "Court level",
                        "rationale": "Institutional tier",
                        "value_kind": "category",
                        "allowed_values": ["district", "appellate"],
                    }
                )
            if "DOC_GAMMA" in passages_text:
                columns.append(
                    {
                        "name": "ruling_outcome",
                        "definition": "Appeal outcome",
                        "rationale": "Primary outcome",
                        "value_kind": "category",
                        "allowed_values": ["granted", "denied"],
                    }
                )
            return json.dumps(
                {
                    "document_helpful": True,
                    "columns": columns,
                    "suggested_value_additions": [],
                }
            )

        raise AssertionError(f"Unexpected prompt: {system[:100]}")


class AsyncRecordingWSManager:
    """Async WebSocket manager simulating yielding to the event loop on every callback."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []

    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]) -> None:
        await asyncio.sleep(0)
        self.events.append({"session_id": session_id, "type": message.get("type")})

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        await asyncio.sleep(0)
        self.logs.append({"session_id": session_id, "level": log_data.get("level")})


async def _run_six_concurrent_discoveries(tmp_path: Path) -> None:
    """Execute 6 concurrent discoveries (matching pool size 6) and verify all results."""
    num_concurrent = max(6, SCHEMATIQ_THREAD_POOL_SIZE)

    async def _run_one(idx: int):
        session_id = f"concurrent_sess_{idx}"
        (tmp_path / session_id).mkdir(parents=True, exist_ok=True)
        ws_manager = AsyncRecordingWSManager()
        progress_events: List[tuple] = []

        async def progress_callback(stage: str, fraction: float, info: Dict[str, Any]) -> None:
            await asyncio.sleep(0)
            progress_events.append((stage, fraction, info))

        config = {
            "query": "What factors drive asylum appeal outcomes?",
            "max_keys_schema": 20,
            "documents_batch_size": 2 if idx == 0 else 1,
            "batch_strategy": "fixed",
            "convergence_threshold": 5,
            "schema_creation_backend": {
                "provider": "openai",
                "model": "fake-deterministic",
                "context_window_size": 16384,
            },
        }
        docs = [
            "DOC_SPLIT_PAIR DOC_ALPHA: First document." if idx == 0 else "DOC_ALPHA: First document.",
            "DOC_SPLIT_PAIR DOC_BETA: Second document." if idx == 0 else "DOC_BETA: Second document.",
            "DOC_GAMMA: Third document.",
        ]
        schema, evolution = await run_schema_discovery(
            session_id=session_id,
            documents=docs,
            filenames=["alpha.txt", "beta.txt", "gamma.txt"],
            schematiq_config=config,
            llm=ConcurrentFakeLLM(),
            retriever=None,
            progress_callback=progress_callback,
            is_stop_requested=lambda _sid: False,
            ws_manager=ws_manager,
            work_dir=tmp_path,
        )
        return idx, schema, evolution, progress_events, ws_manager

    results = await asyncio.wait_for(
        asyncio.gather(*(_run_one(i) for i in range(num_concurrent))),
        timeout=30.0,
    )

    assert len(results) == num_concurrent
    for idx, schema, evolution, progress_events, ws_manager in results:
        assert {c.name for c in schema.columns} == {
            "case_identifier",
            "court_level",
            "ruling_outcome",
        }
        assert len(progress_events) == (2 if idx == 0 else 3)
        assert len(evolution.snapshots) == (2 if idx == 0 else 3)
        assert len(ws_manager.events) == (2 if idx == 0 else 3)
        assert (tmp_path / f"concurrent_sess_{idx}" / "discovered_schema.json").exists()
        if idx == 0:
            assert len(ws_manager.logs) == 1


@pytest.mark.asyncio
async def test_six_concurrent_discoveries_complete_without_deadlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Running 6 discoveries concurrently from cold start serializes slow model loading cleanly."""
    counter_lock = threading.Lock()
    load_calls = 0
    active_loads = 0
    max_active_loads = 0

    def fake_slow_loader(_model_name: str, **_kwargs: Any) -> _FakeEmbeddingModel:
        nonlocal load_calls, active_loads, max_active_loads
        with counter_lock:
            load_calls += 1
            active_loads += 1
            max_active_loads = max(max_active_loads, active_loads)
        try:
            time.sleep(0.05)
            return _FakeEmbeddingModel()
        finally:
            with counter_lock:
                active_loads -= 1

    monkeypatch.setattr(schema_mod, "_EMB_MODEL", None)
    monkeypatch.setattr(schema_mod, "load_sentence_transformer", fake_slow_loader)

    await _run_six_concurrent_discoveries(tmp_path)
    assert load_calls == 1
    assert max_active_loads == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_six_concurrent_discoveries_real_model_integration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Opt-in integration test exercising real cold SentenceTransformer load across 6 threads."""
    markexpr = request.config.getoption("-m") or ""
    if "integration" not in markexpr and os.environ.get("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip(
            "integration test skipped by default (pass -m integration or set RUN_INTEGRATION_TESTS=1)"
        )

    monkeypatch.setattr(schema_mod, "_EMB_MODEL", None)
    await _run_six_concurrent_discoveries(tmp_path)


@pytest.mark.asyncio
async def test_cancelled_discovery_does_not_wedge_worker_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Cancelling an in-flight discovery task unblocks run_callback_on_loop, logs drop, and frees workers."""
    monkeypatch.setattr(schema_mod, "_EMB_MODEL", _FakeEmbeddingModel())
    caplog.set_level(logging.DEBUG, logger="app.services.pipeline.schema_discovery")
    callback_started = asyncio.Event()

    async def slow_progress(_stage: str, _fraction: float, _info: Dict[str, Any]) -> None:
        callback_started.set()
        await asyncio.sleep(30.0)

    slow_sess = "cancelled_sess"
    (tmp_path / slow_sess).mkdir(parents=True, exist_ok=True)
    config = {
        "query": "What factors drive asylum appeal outcomes?",
        "max_keys_schema": 20,
        "documents_batch_size": 1,
        "batch_strategy": "fixed",
        "convergence_threshold": 5,
        "schema_creation_backend": {
            "provider": "openai",
            "model": "fake-deterministic",
            "context_window_size": 16384,
        },
    }
    docs = [
        "DOC_ALPHA: First document.",
        "DOC_BETA: Second document.",
        "DOC_GAMMA: Third document.",
    ]

    task = asyncio.create_task(
        run_schema_discovery(
            session_id=slow_sess,
            documents=docs,
            filenames=["alpha.txt", "beta.txt", "gamma.txt"],
            schematiq_config=config,
            llm=ConcurrentFakeLLM(),
            retriever=None,
            progress_callback=slow_progress,
            is_stop_requested=lambda _sid: False,
            ws_manager=None,
            work_dir=tmp_path,
        )
    )
    await asyncio.wait_for(callback_started.wait(), timeout=5.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    for _ in range(40):
        if "caller cancelled" in caplog.text:
            break
        await asyncio.sleep(0.05)

    assert "Dropping" in caplog.text
    assert "caller cancelled" in caplog.text

    # Now verify another discovery still acquires workers and completes quickly.
    follow_sess = "followup_sess"
    (tmp_path / follow_sess).mkdir(parents=True, exist_ok=True)

    async def fast_progress(_stage: str, _fraction: float, _info: Dict[str, Any]) -> None:
        await asyncio.sleep(0)

    schema, _evolution = await asyncio.wait_for(
        run_schema_discovery(
            session_id=follow_sess,
            documents=["DOC_ALPHA: First document."],
            filenames=["alpha.txt"],
            schematiq_config=config,
            llm=ConcurrentFakeLLM(),
            retriever=None,
            progress_callback=fast_progress,
            is_stop_requested=lambda _sid: False,
            ws_manager=None,
            work_dir=tmp_path,
        ),
        timeout=10.0,
    )
    assert {c.name for c in schema.columns} == {"case_identifier"}


def test_run_callback_on_loop_logs_when_loop_stopped_or_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """run_callback_on_loop logs debug messages when dropping callbacks on stopped/closed loops."""
    caplog.set_level(logging.DEBUG, logger="app.services.pipeline.schema_discovery")
    loop = asyncio.new_event_loop()
    cancel_ev = threading.Event()
    called = False

    def _cb() -> None:
        nonlocal called
        called = True

    try:
        assert run_callback_on_loop(loop, cancel_ev, _cb) is None
        assert not called
        assert "event loop stopped" in caplog.text
    finally:
        loop.close()

    caplog.clear()
    assert run_callback_on_loop(loop, cancel_ev, _cb) is None
    assert not called
    assert "event loop closed" in caplog.text

