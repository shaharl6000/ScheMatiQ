"""Equivalence tests for ContinueDiscoveryService._run_continue_discovery.

Captures deterministic outputs on ``main`` (multi-batch column discovery,
convergence stop, and stop request) and verifies exact equivalence after
routing ``ContinueDiscoveryService`` through ``schematiq.discover_schema``.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.models.session import (
    ColumnInfo,
    DataStatistics,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import continue_discovery_service as cd_mod
from app.services import data_utils
from app.services.continue_discovery_service import (
    ContinueDiscoveryOperation,
    ContinueDiscoveryService,
)
from app.services.session_manager import SessionManager
from app.storage.local_backend import LocalStorageBackend
from schematiq.core import schematiq as ScheMatiQ
from schematiq.core.llm_backends import LLMInterface

GOLDEN_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "continue_discovery_equivalence_golden.json"
)


class FakeContinueDiscoveryLLM(LLMInterface):
    """Deterministic fake LLM for continue-discovery equivalence testing."""

    _provider = "openai"

    def __init__(self, context_window_size: int = 16384):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = context_window_size
        self.calls: List[Dict[str, str]] = []

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        if isinstance(prompt, list):
            system = prompt[0].get("content", "") if len(prompt) > 0 else ""
            user = prompt[1].get("content", "") if len(prompt) > 1 else ""
        else:
            system = ""
            user = str(prompt)

        if "SchemaLLM" in system:
            self.calls.append({"stage": "schema_discovery", "user": user})
            passages_match = re.search(
                r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL
            )
            passages_text = passages_match.group(1) if passages_match else user
            columns: List[Dict[str, Any]] = []
            if "DOC_ALPHA" in passages_text:
                columns.extend(
                    [
                        {
                            "name": "case_identifier",
                            "definition": "Unique docket identifier of the case",
                            "rationale": "Identifies each ruling uniquely",
                            "value_kind": "identifier",
                            "allowed_values": None,
                        },
                        {
                            "name": "ruling_outcome",
                            "definition": "Outcome of the appeal ruling",
                            "rationale": "Primary dependent outcome variable",
                            "value_kind": "category",
                            "allowed_values": ["granted", "denied"],
                        },
                    ]
                )
            if "DOC_BETA" in passages_text:
                columns.append(
                    {
                        "name": "court_level",
                        "definition": "Level of court issuing the decision",
                        "rationale": "Captures institutional hierarchy",
                        "value_kind": "category",
                        "allowed_values": ["district", "appellate"],
                    }
                )
            return json.dumps(
                {
                    "document_helpful": True,
                    "columns": columns,
                    "suggested_value_additions": [],
                }
            )

        raise AssertionError(
            f"Unexpected prompt sent to FakeContinueDiscoveryLLM: {system[:120]}"
        )


class RecordingWSManager:
    """Fake WebSocket manager recording session events and log broadcasts."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []

    async def broadcast_to_session(
        self, session_id: str, message: Dict[str, Any]
    ) -> None:
        self.events.append(
            {
                "type": message.get("type"),
                "data": message.get("data"),
            }
        )

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        self.logs.append(dict(log_data))


async def _run_continue_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    scenario_name: str,
    docs: List[tuple[str, str]],
    convergence_threshold: int = 5,
    documents_batch_size: int = 1,
    stop_after_first_batch: bool = False,
) -> Dict[str, Any]:
    root = tmp_path / scenario_name
    data_dir = root / "data"
    work_dir = root / "schematiq_work"
    sessions_dir = root / "sessions"
    data_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    sessions_dir.mkdir(parents=True, exist_ok=True)

    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    session_manager = SessionManager(storage=storage)
    ws_manager = RecordingWSManager()
    svc = ContinueDiscoveryService(ws_manager, session_manager)

    session_id = f"sess_{scenario_name}"
    op_id = f"op_{scenario_name}"
    session_data_dir = data_dir / session_id
    docs_dir = session_data_dir / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    for fname, content in docs:
        (docs_dir / fname).write_text(content, encoding="utf-8")

    initial_rows = [
        {
            "row_name": "Case 1",
            "papers": ["alpha.txt"],
            "existing_col": {"answer": "A", "excerpts": []},
        },
        {
            "row_name": "Case 2",
            "papers": ["beta.txt"],
            "existing_col": {"answer": "B", "excerpts": []},
        },
    ]
    (session_data_dir / "data.jsonl").write_text(
        "\n".join(json.dumps(r) for r in initial_rows) + "\n",
        encoding="utf-8",
    )

    session = VisualizationSession(
        id=session_id,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", schema_discovery_completed=True),
        schema_query="What factors drive asylum appeal outcomes?",
        columns=[
            ColumnInfo(
                name="existing_col",
                definition="Pre-existing column",
                rationale="Already present",
            )
        ],
        statistics=DataStatistics(
            total_rows=2,
            total_columns=1,
            total_documents=2,
            completeness=100.0,
            column_stats=[
                ColumnInfo(
                    name="existing_col",
                    definition="Pre-existing column",
                    rationale="Already present",
                    non_null_count=2,
                    unique_count=2,
                )
            ],
        ),
    )
    session_manager.create_session(session)

    cfg_payload = {
        "document_source": "original",
        "cloud_dataset": None,
        "uploaded_files": None,
        "retriever_config": None,
        "max_keys_schema": 20,
        "documents_batch_size": documents_batch_size,
        "convergence_threshold": convergence_threshold,
        "document_randomization_seed": 42,
        "skip_value_extraction": True,
        "query": session.schema_query,
        "bypass_limit": True,
    }
    (session_data_dir / f"continue_discovery_config_{op_id}.json").write_text(
        json.dumps(cfg_payload), encoding="utf-8"
    )
    llm_payload = {
        "provider": "openai",
        "model": "fake-deterministic",
        "temperature": 0,
        "context_window_size": 16384,
    }
    (session_data_dir / f"continue_discovery_llm_{op_id}.json").write_text(
        json.dumps(llm_payload), encoding="utf-8"
    )

    op = ContinueDiscoveryOperation(
        operation_id=op_id,
        session_id=session_id,
        status="starting",
    )
    op.initial_columns = ["existing_col"]
    op.document_source = "original"
    op.llm_config = llm_payload
    svc.active_operations[op_id] = op

    fake_llm = FakeContinueDiscoveryLLM()
    monkeypatch.setattr(svc, "_get_data_dir", lambda: data_dir)
    monkeypatch.setattr(svc, "_get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(data_utils, "get_data_dir", lambda: data_dir)
    monkeypatch.setattr(data_utils, "get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(
        cd_mod.schematiq_utils, "build_llm", lambda _cfg: fake_llm
    )
    monkeypatch.setattr(
        cd_mod, "_enforce_release_llm_config", lambda cfg, **_kw: cfg
    )

    from app.services import orchestration as orch_mod

    monkeypatch.setattr(
        orch_mod.schematiq_runner,
        "record_external_usage",
        lambda *_args, **_kwargs: None,
    )

    if stop_after_first_batch:
        orig_eval = ScheMatiQ.evaluate_schema_convergence
        eval_calls = 0

        def _eval_and_stop(prev_schema, merged_schema, *args, **kwargs):
            nonlocal eval_calls
            eval_calls += 1
            res = orig_eval(prev_schema, merged_schema, *args, **kwargs)
            if eval_calls == 1:
                svc.stop_flags[op_id] = True
            return res

        monkeypatch.setattr(
            ScheMatiQ, "evaluate_schema_convergence", _eval_and_stop
        )

    await svc._run_continue_discovery(op_id)

    final_session = session_manager.get_session(session_id)
    assert final_session is not None
    data_rows = [
        json.loads(line)
        for line in (session_data_dir / "data.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]

    stats_dump = None
    if final_session.statistics is not None:
        evo_dump = (
            final_session.statistics.schema_evolution.model_dump()
            if final_session.statistics.schema_evolution is not None
            else None
        )
        stats_dump = {
            "total_rows": final_session.statistics.total_rows,
            "total_columns": final_session.statistics.total_columns,
            "total_documents": final_session.statistics.total_documents,
            "completeness": final_session.statistics.completeness,
            "column_stats": [
                {
                    "name": c.name,
                    "non_null_count": c.non_null_count,
                    "unique_count": c.unique_count,
                }
                for c in final_session.statistics.column_stats
            ],
            "schema_evolution": evo_dump,
        }

    baseline_dump = None
    if final_session.schema_baseline is not None:
        baseline_dump = {
            col_name: {
                "name": entry.name,
                "definition": entry.definition,
                "rationale": entry.rationale,
                "allowed_values": entry.allowed_values,
                "checksum": entry.checksum,
            }
            for col_name, entry in sorted(
                final_session.schema_baseline.columns.items()
            )
        }

    return {
        "operation_cleaned_up": op_id not in svc.active_operations,
        "operation_final": {
            "status": op.status,
            "phase": op.phase,
            "progress": op.progress,
            "current_batch": op.current_batch,
            "total_batches": op.total_batches,
            "initial_columns": op.initial_columns,
            "new_columns": op.new_columns,
            "total_documents": op.total_documents,
        },
        "session_status": (
            final_session.status.value
            if hasattr(final_session.status, "value")
            else final_session.status
        ),
        "session_columns": [
            {
                "name": c.name,
                "definition": c.definition,
                "rationale": c.rationale,
                "allowed_values": c.allowed_values,
                "value_kind": c.value_kind,
                "source_document": c.source_document,
                "discovery_iteration": c.discovery_iteration,
            }
            for c in final_session.columns
        ],
        "statistics": stats_dump,
        "baseline_columns": baseline_dump,
        "data_jsonl_rows": data_rows,
        "ws_events": ws_manager.events,
        "ws_logs": ws_manager.logs,
        "llm_call_stages": [c["stage"] for c in fake_llm.calls],
        "llm_call_count": len(fake_llm.calls),
    }


@pytest.mark.asyncio
async def test_continue_discovery_equivalence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    actual: Dict[str, Dict[str, Any]] = {}

    actual["multi_batch_new_columns"] = await _run_continue_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="multi_batch",
        docs=[
            ("alpha.txt", "DOC_ALPHA: Appeal 101 was granted."),
            ("beta.txt", "DOC_BETA: Heard at the appellate court level."),
            ("gamma.txt", "DOC_GAMMA: Additional procedural notes."),
        ],
        convergence_threshold=5,
    )

    actual["convergence_stop"] = await _run_continue_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="convergence",
        docs=[
            ("alpha.txt", "DOC_ALPHA: Appeal 101 was granted."),
            ("gamma.txt", "DOC_GAMMA: No new columns."),
            ("delta.txt", "DOC_DELTA: Still no new columns."),
            ("epsilon.txt", "DOC_EPSILON: Should not be reached."),
            ("zeta.txt", "DOC_ZETA: Should not be reached."),
        ],
        convergence_threshold=2,
    )

    actual["stop_request"] = await _run_continue_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="stop_request",
        docs=[
            ("alpha.txt", "DOC_ALPHA: Appeal 101 was granted."),
            ("beta.txt", "DOC_BETA: Heard at the appellate court level."),
            ("gamma.txt", "DOC_GAMMA: Extra doc."),
        ],
        convergence_threshold=5,
        stop_after_first_batch=True,
    )

    if os.environ.get("SCHEMATIQ_CAPTURE_GOLDEN") == "1":
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(actual, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    assert GOLDEN_PATH.exists(), (
        f"Golden fixture missing at {GOLDEN_PATH}; run with SCHEMATIQ_CAPTURE_GOLDEN=1 first."
    )
    expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert actual == expected


@pytest.mark.asyncio
async def test_continue_discovery_deterministic_under_reversed_glob_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsorted Linux tmpfs/ext4 glob order must not change column_sources or snapshots."""
    orig_glob = Path.glob

    def _reversed_glob(self: Path, pattern: str):
        entries = list(orig_glob(self, pattern))
        if self.name == "documents" and pattern == "*":
            return iter(sorted(entries, key=lambda p: p.name, reverse=True))
        return iter(entries)

    monkeypatch.setattr(Path, "glob", _reversed_glob)

    res = await _run_continue_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="convergence_rev_glob",
        docs=[
            ("alpha.txt", "DOC_ALPHA: Appeal 101 was granted."),
            ("gamma.txt", "DOC_GAMMA: No new columns."),
            ("delta.txt", "DOC_DELTA: Still no new columns."),
            ("epsilon.txt", "DOC_EPSILON: Should not be reached."),
            ("zeta.txt", "DOC_ZETA: Should not be reached."),
        ],
        convergence_threshold=2,
    )

    expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    evo = res["statistics"]["schema_evolution"]
    assert evo == expected["convergence_stop"]["statistics"]["schema_evolution"]
    assert evo["column_sources"] == {
        "case_identifier": "alpha.txt",
        "ruling_outcome": "alpha.txt",
    }
    assert evo["snapshots"][0]["documents_processed"] == [
        "alpha.txt",
        "delta.txt",
        "epsilon.txt",
        "gamma.txt",
        "zeta.txt",
    ]
