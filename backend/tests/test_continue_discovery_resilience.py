"""Verify ContinueDiscoveryService inherits token-limit split/skip resilience via discover_schema."""

from __future__ import annotations

import json
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
from schematiq.core.llm_backends import LLMInterface


class OverflowingContinueLLM(LLMInterface):
    """Fake LLM raising token-limit errors for DOC_OVERSIZED."""

    _provider = "openai"

    def __init__(self):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = 16384

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        user = prompt[1].get("content", "") if isinstance(prompt, list) else str(prompt)
        match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
        passages_text = match.group(1) if match else user

        if "DOC_OVERSIZED" in passages_text:
            raise RuntimeError(
                "400 INVALID_ARGUMENT: input token count exceeds maximum context length"
            )

        columns: List[Dict[str, Any]] = []
        if "DOC_ALPHA" in passages_text:
            columns.append(
                {
                    "name": "case_identifier",
                    "definition": "Unique docket identifier",
                    "rationale": "Identifies ruling",
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
        return json.dumps(
            {
                "document_helpful": True,
                "columns": columns,
                "suggested_value_additions": [],
            }
        )


class RecordingWSManager:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []

    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]) -> None:
        self.events.append({"type": message.get("type"), "data": message.get("data")})

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        self.logs.append(dict(log_data))


@pytest.mark.asyncio
async def test_continue_discovery_splits_and_skips_token_limit_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    sessions_dir = tmp_path / "sessions"
    data_dir.mkdir()
    work_dir.mkdir()
    sessions_dir.mkdir()

    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    session_manager = SessionManager(storage=storage)
    ws_manager = RecordingWSManager()
    svc = ContinueDiscoveryService(ws_manager, session_manager)

    session_id = "sess_overflow"
    op_id = "op_overflow"
    session_data_dir = data_dir / session_id
    docs_dir = session_data_dir / "documents"
    docs_dir.mkdir(parents=True)

    (docs_dir / "01_oversized.txt").write_text("DOC_OVERSIZED: Huge text", encoding="utf-8")
    (docs_dir / "02_alpha.txt").write_text("DOC_ALPHA: Appeal 101", encoding="utf-8")
    (docs_dir / "03_beta.txt").write_text("DOC_BETA: Appellate tier", encoding="utf-8")
    (session_data_dir / "data.jsonl").write_text(
        json.dumps({"row_name": "Case 1", "papers": ["02_alpha.txt"], "existing_col": {"answer": "A", "excerpts": []}}) + "\n",
        encoding="utf-8",
    )

    session = VisualizationSession(
        id=session_id,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", schema_discovery_completed=True),
        schema_query="Analyze rulings",
        columns=[ColumnInfo(name="existing_col", definition="d", rationale="r")],
        statistics=DataStatistics(
            total_rows=1,
            total_columns=1,
            total_documents=1,
            completeness=100.0,
            column_stats=[ColumnInfo(name="existing_col", definition="d", rationale="r", non_null_count=1, unique_count=1)],
        ),
    )
    session_manager.create_session(session)

    (session_data_dir / f"continue_discovery_config_{op_id}.json").write_text(
        json.dumps(
            {
                "document_source": "original",
                "max_keys_schema": 20,
                "documents_batch_size": 2,
                "convergence_threshold": 5,
                "query": "Analyze rulings",
                "bypass_limit": True,
            }
        ),
        encoding="utf-8",
    )
    llm_cfg = {"provider": "openai", "model": "fake-deterministic", "temperature": 0, "context_window_size": 16384}
    (session_data_dir / f"continue_discovery_llm_{op_id}.json").write_text(
        json.dumps(llm_cfg), encoding="utf-8"
    )

    op = ContinueDiscoveryOperation(operation_id=op_id, session_id=session_id, status="starting")
    op.initial_columns = ["existing_col"]
    op.document_source = "original"
    op.llm_config = llm_cfg
    svc.active_operations[op_id] = op

    monkeypatch.setattr(svc, "_get_data_dir", lambda: data_dir)
    monkeypatch.setattr(svc, "_get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(data_utils, "get_data_dir", lambda: data_dir)
    monkeypatch.setattr(data_utils, "get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(cd_mod.schematiq_utils, "build_llm", lambda _cfg: OverflowingContinueLLM())
    monkeypatch.setattr(cd_mod, "_enforce_release_llm_config", lambda cfg, **_kw: cfg)

    from app.services import orchestration as orch_mod
    monkeypatch.setattr(orch_mod.schematiq_runner, "record_external_usage", lambda *_a, **_kw: None)

    await svc._run_continue_discovery(op_id)

    assert op.status == "completed"
    discovered_names = {c["name"] for c in op.new_columns}
    assert discovered_names == {"case_identifier", "court_level"}

    skip_file = work_dir / session_id / "schema_discovery_skips.json"
    assert skip_file.exists()
    skips = json.loads(skip_file.read_text(encoding="utf-8"))
    assert len(skips) == 1
    assert skips[0]["filenames"] == ["01_oversized.txt"]
    assert skips[0]["reason"] == "single_document_exceeds_token_limit"
    assert any("Skipped '01_oversized.txt'" in entry["message"] for entry in ws_manager.logs)

