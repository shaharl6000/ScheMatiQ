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
    assert evolution.skipped_batches_by_reason == {"llm_call_failed": 1}
    assert evolution.skipped_filenames == ["broken.txt"]

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
    assert _evo.skipped_batches_by_reason == {"retrieval_failed": 1}
    assert _evo.skipped_filenames == ["ret_fail.txt"]
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


@pytest.mark.asyncio
async def test_run_schema_discovery_skips_obs_unit_failure_and_retries_next_batch(
    tmp_path: Path,
) -> None:
    class ObsUnitFailFirstFakeLLM(FailingBatchFakeLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            system = prompt[0].get("content", "") if isinstance(prompt, list) else ""
            user = prompt[1].get("content", "") if isinstance(prompt, list) else str(prompt)
            if "ObservationUnitLLM" in system and "DOC_BAD_OBS_UNIT" in user:
                self.calls.append("observation_unit")
                return "{invalid json"
            return super().generate(prompt, **kwargs)

    session_id = "sess_skip_obs_unit"
    (tmp_path / session_id).mkdir()
    ws = RecordingWSManager()
    llm = ObsUnitFailFirstFakeLLM()

    schema, evolution = await run_schema_discovery(
        session_id=session_id,
        documents=[
            "DOC_BAD_OBS_UNIT: Unparseable cover page.",
            "DOC_ALPHA: First valid ruling.",
            "DOC_BETA: Second valid ruling.",
        ],
        filenames=["bad_unit.txt", "alpha.txt", "beta.txt"],
        schematiq_config=_cfg(),
        llm=llm,
        retriever=None,
        progress_callback=_noop_progress,
        is_stop_requested=lambda _sid: False,
        ws_manager=ws,
        work_dir=tmp_path,
    )

    assert schema.observation_unit is not None
    assert schema.observation_unit.name == "Court Ruling"
    assert schema.observation_unit.source_document == "alpha.txt"
    assert [c.name for c in schema.columns] == ["case_identifier", "court_level"]
    assert evolution.skipped_batches_by_reason == {"observation_unit_discovery_failed": 1}
    assert evolution.skipped_filenames == ["bad_unit.txt"]

    skips = json.loads((tmp_path / session_id / "schema_discovery_skips.json").read_text())
    assert len(skips) == 1
    assert skips[0]["filenames"] == ["bad_unit.txt"]
    assert skips[0]["reason"] == "observation_unit_discovery_failed"
    assert len(ws.logs) == 1
    assert ws.logs[0]["level"] == "warning"
    assert "bad_unit.txt" in ws.logs[0]["message"]


@pytest.mark.asyncio
async def test_schematiq_runner_completion_message_includes_skipped_discovery_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from app.models.schematiq import LLMConfig, ScheMatiQConfig
    from app.models.session import (
        SessionMetadata,
        SessionStatus,
        SessionType,
        VisualizationSession,
    )
    from app.services import schematiq_runner as runner_mod
    from app.services.pipeline.schema_discovery import format_discovery_completion_message
    from app.services.schematiq_runner import ScheMatiQRunner
    from app.services.session_manager import SessionManager
    from app.storage.local_backend import LocalStorageBackend

    assert (
        format_discovery_completion_message("ScheMatiQ execution completed successfully", 0)
        == "ScheMatiQ execution completed successfully"
    )
    assert (
        format_discovery_completion_message("ScheMatiQ execution completed successfully", 2)
        == "ScheMatiQ execution completed successfully (2 discovery batches skipped)"
    )

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    sessions_dir = tmp_path / "sessions"
    docs_dir = tmp_path / "input_docs"
    for d in (data_dir, work_dir, sessions_dir, docs_dir):
        d.mkdir(parents=True, exist_ok=True)

    (docs_dir / "01_alpha.txt").write_text("DOC_ALPHA: First ruling.", encoding="utf-8")
    (docs_dir / "02_broken.txt").write_text("DOC_TRANSIENT: Broken batch.", encoding="utf-8")
    (docs_dir / "03_beta.txt").write_text("DOC_BETA: Third ruling.", encoding="utf-8")

    session_id = "sess_runner_skip_summary"
    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    session_manager = SessionManager(storage=storage)
    session_manager.create_session(
        VisualizationSession(
            id=session_id,
            type=SessionType.SCHEMATIQ,
            status=SessionStatus.PROCESSING,
            metadata=SessionMetadata(source="test"),
        )
    )

    ws = MagicMock()
    ws.broadcast_progress = AsyncMock()
    ws.broadcast_to_session = AsyncMock()
    ws.broadcast_log = AsyncMock()
    ws.broadcast_completion = AsyncMock()
    ws.broadcast_error = AsyncMock()

    runner = ScheMatiQRunner(
        work_dir=str(work_dir),
        websocket_manager=ws,
        session_manager=session_manager,
    )

    fake_llm = FailingBatchFakeLLM()
    monkeypatch.setattr(runner_mod, "DEFAULT_DATA_DIR", str(data_dir))
    monkeypatch.setattr(
        runner_mod, "resolve_docs_paths", AsyncMock(return_value=[str(docs_dir)])
    )
    monkeypatch.setattr(runner_mod, "enforce_release_llm_config", lambda cfg, **_kw: cfg)
    monkeypatch.setattr(runner_mod, "build_llm_interface", lambda **_kw: fake_llm)
    monkeypatch.setattr(runner, "check_global_quota", lambda *_args, **_kwargs: None)

    config = ScheMatiQConfig(
        query="What drives appeal outcomes?",
        docs_path=str(docs_dir),
        max_keys_schema=20,
        documents_batch_size=1,
        batch_strategy="fixed",
        convergence_threshold=5,
        skip_value_extraction=True,
        count_toward_quota=False,
        document_randomization_seed=42,
        output_path=str(work_dir / session_id / "output.json"),
        schema_creation_backend=LLMConfig(
            provider="openai",
            model="fake-deterministic",
            context_window_size=16384,
        ),
        value_extraction_backend=LLMConfig(
            provider="openai",
            model="fake-deterministic",
            context_window_size=16384,
        ),
    )

    await runner._execute_schematiq(session_id, config)

    ws.broadcast_completion.assert_awaited_once()
    _sid, completion_payload = ws.broadcast_completion.call_args.args
    assert _sid == session_id
    assert (
        completion_payload["message"]
        == "Schema discovery completed (value extraction skipped) (1 discovery batch skipped)"
    )
    assert completion_payload["skipped_discovery_batches"] == 1

    saved_session = session_manager.get_session(session_id)
    assert saved_session is not None
    assert saved_session.statistics is not None
    assert saved_session.statistics.schema_evolution is not None
    assert saved_session.statistics.schema_evolution.skipped_batches_by_reason == {
        "llm_call_failed": 1
    }
    assert saved_session.statistics.schema_evolution.skipped_filenames == ["02_broken.txt"]


@pytest.mark.asyncio
async def test_run_schema_discovery_split_batch_partial_failure_keeps_sibling_columns(
    tmp_path: Path,
) -> None:
    from app.models.session import SchemaEvolution as BackendSchemaEvolution
    from app.services.pipeline.data_query import compute_statistics

    class SplitPartialBackendFakeLLM(FailingBatchFakeLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            system = prompt[0].get("content", "") if isinstance(prompt, list) else ""
            user = prompt[1].get("content", "") if isinstance(prompt, list) else str(prompt)
            if "SchemaLLM" in system:
                match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
                text = match.group(1) if match else user
                if "DOC_ALPHA" in text and "DOC_TRANSIENT" in text:
                    self.calls.append("schema_discovery")
                    raise RuntimeError("This model's maximum context length exceeded")
            return super().generate(prompt, **kwargs)

    session_id = "sess_split_partial"
    (tmp_path / session_id).mkdir()
    ws = RecordingWSManager()
    llm = SplitPartialBackendFakeLLM()

    schema, evolution = await run_schema_discovery(
        session_id=session_id,
        documents=[
            "DOC_ALPHA: Valid ruling.",
            "DOC_TRANSIENT: Broken sibling.",
        ],
        filenames=["alpha.txt", "broken.txt"],
        schematiq_config=_cfg(
            batch_strategy="fixed",
            documents_batch_size=2,
            initial_observation_unit={
                "name": "Court Ruling",
                "definition": "A single judicial decision",
            },
        ),
        llm=llm,
        retriever=None,
        progress_callback=_noop_progress,
        is_stop_requested=lambda _sid: False,
        ws_manager=ws,
        work_dir=tmp_path,
    )

    assert [c.name for c in schema.columns] == ["case_identifier"]
    assert schema.columns[0].source_document == "alpha.txt"
    assert evolution.column_sources == {"case_identifier": "alpha.txt"}
    assert evolution.skipped_batches_by_reason == {"llm_call_failed": 1}
    assert evolution.skipped_filenames == ["broken.txt"]

    skips = json.loads((tmp_path / session_id / "schema_discovery_skips.json").read_text())
    assert len(skips) == 1
    assert skips[0]["filenames"] == ["broken.txt"]
    assert skips[0]["reason"] == "llm_call_failed"

    # compute_statistics retains col.source_document both with and without schema_evolution
    extracted_file = tmp_path / session_id / "extracted_data.jsonl"
    extracted_file.write_text(
        json.dumps({"row_name": "Case 1", "papers": ["alpha.txt"], "case_identifier": {"answer": "101"}})
        + "\n",
        encoding="utf-8",
    )
    stats_no_evo = compute_statistics(
        session_id=session_id,
        schema=schema,
        schema_evolution=None,
        work_dir=tmp_path,
    )
    assert stats_no_evo is not None
    assert stats_no_evo.column_stats[0].source_document == "alpha.txt"

    stats_with_evo = compute_statistics(
        session_id=session_id,
        schema=schema,
        schema_evolution=BackendSchemaEvolution(
            snapshots=[],
            column_sources=dict(evolution.column_sources),
        ),
        work_dir=tmp_path,
    )
    assert stats_with_evo is not None
    assert stats_with_evo.column_stats[0].source_document == "alpha.txt"

    # Unsplit multi-doc batch -> Column.source_document, evolution.column_sources, and ColumnInfo match
    multi_session = "sess_multi_doc_provenance"
    (tmp_path / multi_session).mkdir()
    multi_schema, multi_evo = await run_schema_discovery(
        session_id=multi_session,
        documents=["DOC_PLAIN: Intro.", "DOC_ALPHA: Ruling."],
        filenames=["intro.txt", "alpha.txt"],
        schematiq_config=_cfg(
            batch_strategy="fixed",
            documents_batch_size=2,
            initial_observation_unit={
                "name": "Court Ruling",
                "definition": "A single judicial decision",
            },
        ),
        llm=FailingBatchFakeLLM(),
        retriever=None,
        progress_callback=_noop_progress,
        is_stop_requested=lambda _sid: False,
        ws_manager=None,
        work_dir=tmp_path,
    )
    assert [c.name for c in multi_schema.columns] == ["case_identifier"]
    assert multi_schema.columns[0].source_document == "intro.txt, alpha.txt"
    assert multi_evo.column_sources == {"case_identifier": "intro.txt, alpha.txt"}

    (tmp_path / multi_session / "extracted_data.jsonl").write_text(
        json.dumps({"row_name": "Case 1", "papers": ["alpha.txt"], "case_identifier": {"answer": "101"}})
        + "\n",
        encoding="utf-8",
    )
    multi_stats = compute_statistics(
        session_id=multi_session,
        schema=multi_schema,
        schema_evolution=BackendSchemaEvolution(
            snapshots=[],
            column_sources=dict(multi_evo.column_sources),
        ),
        work_dir=tmp_path,
    )
    assert multi_stats is not None
    assert multi_stats.column_stats[0].source_document == "intro.txt, alpha.txt"
