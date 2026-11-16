"""Backend resilience tests for per-batch discovery failure handling in run_schema_discovery."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from google.genai import errors as genai_errors

from app.services.pipeline.schema_discovery import run_schema_discovery
from schematiq.core import schematiq as ScheMatiQ
from schematiq.core.llm_backends import LLMInterface, LLMRunAborted


def _api_error(code: int, status: str, message: str) -> genai_errors.APIError:
    with pytest.raises(genai_errors.APIError) as excinfo:
        genai_errors.APIError.raise_error(
            code,
            {"error": {"code": code, "status": status, "message": message}},
            None,
        )
    return excinfo.value


class FailingBatchFakeLLM(LLMInterface):
    """Fake LLM simulating transient errors, 503 outages, and 401 auth errors."""

    _provider = "openai"

    def __init__(self):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = 16384
        self.calls: List[str] = []

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        system = prompt[0].get("content", "") if isinstance(prompt, list) else ""
        user = prompt[1].get("content", "") if isinstance(prompt, list) else str(prompt)

        if "ObservationUnitLLM" in system:
            self.calls.append("observation_unit")
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
            self.calls.append("schema_discovery")
            match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
            text = match.group(1) if match else user

            if "DOC_TRANSIENT" in text:
                raise RuntimeError("transient LLM timeout")
            if "DOC_FAIL_ONE" in text:
                raise RuntimeError("fail one")
            if "DOC_FAIL_TWO" in text:
                raise RuntimeError("fail two")
            if "DOC_OUTAGE" in text:
                raise _api_error(503, "UNAVAILABLE", "The model is overloaded")
            if "DOC_BAD_KEY" in text:
                raise _api_error(401, "UNAUTHENTICATED", "API key not valid")

            cols: List[Dict[str, Any]] = []
            if "DOC_ALPHA" in text:
                cols.append(
                    {
                        "name": "case_identifier",
                        "definition": "Case ID",
                        "rationale": "Identifies case",
                        "value_kind": "identifier",
                    }
                )
            if "DOC_BETA" in text:
                cols.append(
                    {
                        "name": "court_level",
                        "definition": "Court tier",
                        "rationale": "Hierarchy",
                        "value_kind": "category",
                        "allowed_values": ["district", "appellate"],
                    }
                )
            return json.dumps(
                {
                    "document_helpful": True,
                    "columns": cols,
                    "suggested_value_additions": [],
                }
            )

        raise AssertionError(f"Unexpected prompt: {system[:100]}")


class RecordingWSManager:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []

    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": message.get("type")})

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        self.logs.append({"session_id": session_id, **log_data})


def _cfg(**extra: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "query": "What drives appeal outcomes?",
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
    cfg.update(extra)
    return cfg


async def _noop_progress(_stage: str, _fraction: float, _info: Any = None) -> None:
    return None


@pytest.mark.asyncio
async def test_run_schema_discovery_skips_failed_batch_and_logs_warning(tmp_path: Path) -> None:
    session_id = "sess_skip_batch"
    (tmp_path / session_id).mkdir()
    ws = RecordingWSManager()
    llm = FailingBatchFakeLLM()

    schema, evolution = await run_schema_discovery(
        session_id=session_id,
        documents=[
            "DOC_ALPHA: First ruling.",
            "DOC_TRANSIENT: Broken batch.",
            "DOC_BETA: Third ruling.",
        ],
        filenames=["alpha.txt", "broken.txt", "beta.txt"],
        schematiq_config=_cfg(),
        llm=llm,
        retriever=None,
        progress_callback=_noop_progress,
        is_stop_requested=lambda _sid: False,
        ws_manager=ws,
        work_dir=tmp_path,
    )

    assert [c.name for c in schema.columns] == ["case_identifier", "court_level"]
    assert len(evolution.snapshots) == 3
    assert evolution.snapshots[1].new_columns == []

    skip_file = tmp_path / session_id / "schema_discovery_skips.json"
    assert skip_file.exists()
    skips = json.loads(skip_file.read_text(encoding="utf-8"))
    assert len(skips) == 1
    assert skips[0]["filenames"] == ["broken.txt"]
    assert skips[0]["reason"] == "llm_call_failed"
    assert "transient LLM timeout" in skips[0]["error"]

    assert len(ws.logs) == 1
    assert ws.logs[0]["level"] == "warning"
    assert "broken.txt" in ws.logs[0]["message"]


@pytest.mark.asyncio
async def test_run_schema_discovery_skips_retrieval_failure_and_logs_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "sess_skip_retrieval"
    (tmp_path / session_id).mkdir()
    ws = RecordingWSManager()
    llm = FailingBatchFakeLLM()

    orig_select = ScheMatiQ.select_relevant_content

    def flaky_select(docs, query, retriever):
        if any("DOC_RETRIEVAL_FAIL" in d for d in docs):
            raise RuntimeError("retriever connection reset")
        return orig_select(docs, query, retriever)

    monkeypatch.setattr(ScheMatiQ, "select_relevant_content", flaky_select)

    schema, _evo = await run_schema_discovery(
        session_id=session_id,
        documents=[
            "DOC_ALPHA: First ruling.",
            "DOC_RETRIEVAL_FAIL: Fails retrieval.",
            "DOC_BETA: Third ruling.",
        ],
        filenames=["alpha.txt", "ret_fail.txt", "beta.txt"],
        schematiq_config=_cfg(),
        llm=llm,
        retriever=None,
        progress_callback=_noop_progress,
        is_stop_requested=lambda _sid: False,
        ws_manager=ws,
        work_dir=tmp_path,
    )

    assert [c.name for c in schema.columns] == ["case_identifier", "court_level"]
    skips = json.loads((tmp_path / session_id / "schema_discovery_skips.json").read_text())
    assert skips[0]["filenames"] == ["ret_fail.txt"]
    assert skips[0]["reason"] == "retrieval_failed"


@pytest.mark.asyncio
async def test_run_schema_discovery_aborts_on_three_consecutive_outages(tmp_path: Path) -> None:
    session_id = "sess_outages"
    (tmp_path / session_id).mkdir()
    ws = RecordingWSManager()
    llm = FailingBatchFakeLLM()

    with pytest.raises(LLMRunAborted, match="3 batches failed in a row"):
        await run_schema_discovery(
            session_id=session_id,
            documents=[
                "DOC_ALPHA: First ruling.",
                "DOC_OUTAGE: 1",
                "DOC_OUTAGE: 2",
                "DOC_OUTAGE: 3",
                "DOC_BETA: Unreached.",
            ],
            filenames=["alpha.txt", "o1.txt", "o2.txt", "o3.txt", "beta.txt"],
            schematiq_config=_cfg(),
            llm=llm,
            retriever=None,
            progress_callback=_noop_progress,
            is_stop_requested=lambda _sid: False,
            ws_manager=ws,
            work_dir=tmp_path,
        )

    assert llm.calls.count("schema_discovery") == 4


@pytest.mark.asyncio
async def test_run_schema_discovery_reraises_when_all_batches_fail(tmp_path: Path) -> None:
    session_id = "sess_all_fail"
    (tmp_path / session_id).mkdir()
    llm = FailingBatchFakeLLM()

    with pytest.raises(RuntimeError, match="fail two"):
        await run_schema_discovery(
            session_id=session_id,
            documents=["DOC_FAIL_ONE: 1", "DOC_FAIL_TWO: 2"],
            filenames=["f1.txt", "f2.txt"],
            schematiq_config=_cfg(
                initial_observation_unit={
                    "name": "Court Ruling",
                    "definition": "A single judicial decision",
                }
            ),
            llm=llm,
            retriever=None,
            progress_callback=_noop_progress,
            is_stop_requested=lambda _sid: False,
            ws_manager=None,
            work_dir=tmp_path,
        )

    assert llm.calls == ["schema_discovery", "schema_discovery"]


@pytest.mark.asyncio
async def test_run_schema_discovery_aborts_immediately_on_fatal_auth_error(tmp_path: Path) -> None:
    session_id = "sess_fatal"
    (tmp_path / session_id).mkdir()
    llm = FailingBatchFakeLLM()

    with pytest.raises(genai_errors.APIError):
        await run_schema_discovery(
            session_id=session_id,
            documents=["DOC_BAD_KEY: Invalid key", "DOC_ALPHA: Valid"],
            filenames=["bad.txt", "alpha.txt"],
            schematiq_config=_cfg(
                initial_observation_unit={
                    "name": "Court Ruling",
                    "definition": "A single judicial decision",
                }
            ),
            llm=llm,
            retriever=None,
            progress_callback=_noop_progress,
            is_stop_requested=lambda _sid: False,
            ws_manager=None,
            work_dir=tmp_path,
        )

    assert llm.calls == ["schema_discovery"]
    assert not (tmp_path / session_id / "schema_discovery_skips.json").exists()
