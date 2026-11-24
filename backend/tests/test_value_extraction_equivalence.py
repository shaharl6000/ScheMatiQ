"""Pre-fix equivalence tests for value extraction across library and backend services.

Captures deterministic outputs on ``main`` (multi-doc unit extraction, reordered
pass, skipped documents, ReextractionService, SchemaManager, UploadDocumentProcessor,
and legacy single-directory TableBuilder in ``all`` and ``one_by_one`` modes) using
a stateless, prompt-driven FakeExtractionLLM and compares them against a golden
fixture after fixing issues #663 and #661.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import numpy as np
import pytest

from app.core.config import DEFAULT_DATA_DIR, DEFAULT_SCHEMATIQ_WORK_DIR
from app.models.session import (
    ColumnInfo,
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import data_utils
from app.services.reextraction_service import ReextractionOperation, ReextractionService
from app.services.schema_manager import SchemaManager
from app.services.session_manager import SessionManager
from app.services.upload_document_processor import UploadDocumentProcessor
from app.storage.factory import reset_storage
from app.storage.local_backend import LocalStorageBackend
from schematiq.core import embedding_model, schema as schema_mod
from schematiq.core.llm_backends import LLMInterface
from schematiq.value_extraction.core.table_builder import TableBuilder
from schematiq.value_extraction.main import build_table_jsonl

GOLDEN_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "value_extraction_equivalence_golden.json"
)


class _FakeEmbeddingModel:
    """Fast deterministic CPU embedding stub so unit dedup needs no network."""

    device = "cpu"

    @staticmethod
    def _vec(text: str) -> np.ndarray:
        seed = int.from_bytes(hashlib.sha256(str(text).encode("utf-8")).digest()[:8], "big")
        vec = np.random.default_rng(seed).standard_normal(64).astype(np.float32)
        norm = np.linalg.norm(vec)
        return vec / norm if norm > 0 else vec

    def encode(self, texts, **_kwargs):
        if isinstance(texts, str):
            return self._vec(texts)
        return np.stack([self._vec(t) for t in texts])


class FakeExtractionLLM(LLMInterface):
    """Stateless, prompt-driven deterministic fake LLM for value extraction."""

    _provider = "openai"

    def __init__(self, context_window_size: int = 32768):
        super().__init__()
        self.model = "fake-extraction-deterministic"
        self.temperature = 0
        self.max_output_tokens = 4096
        self.context_window_size = context_window_size
        self.calls: List[Dict[str, str]] = []

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 4096

    @staticmethod
    def _tag_from_context(text: str, unit_hint: str) -> str:
        if "Case_A" in unit_hint:
            return "Case_A"
        if "Case_B" in unit_hint:
            return "Case_B"
        m = re.search(r"DOC_(\d+)", text)
        if m:
            return f"D{m.group(1)}"
        return "D0"

    def generate(self, prompt: Any, **kwargs) -> str:
        if isinstance(prompt, list):
            system = prompt[0].get("content", "") if len(prompt) > 0 else ""
            user = prompt[1].get("content", "") if len(prompt) > 1 else ""
        else:
            system = ""
            user = str(prompt)

        if "UnitIdentifierLLM" in system:
            self.calls.append({"stage": "unit_identification"})
            doc_match = re.search(r"<DOCUMENT>(.*?)</DOCUMENT>", user, flags=re.DOTALL)
            doc_text = doc_match.group(1) if doc_match else user
            if "DOC_EMPTY_UNITS" in doc_text:
                return json.dumps(
                    {
                        "observation_units": [],
                        "total_units_found": 0,
                        "notes": "No matching units in procedural notice",
                    }
                )
            if "DOC_MULTI" in doc_text:
                return json.dumps(
                    {
                        "observation_units": [
                            {"unit_name": "Case_A", "confidence": "high"},
                            {"unit_name": "Case_B", "confidence": "high"},
                        ],
                        "total_units_found": 2,
                        "notes": "Two cases in multi-unit document",
                    }
                )
            m = re.search(r"DOC_(\d+)", doc_text)
            idx = m.group(1) if m else "1"
            return json.dumps(
                {
                    "observation_units": [
                        {"unit_name": f"Unit_{idx}", "confidence": "high"}
                    ],
                    "total_units_found": 1,
                    "notes": "Single unit",
                }
            )

        if "ValueLLM" in system:
            is_reextract = (
                "re-examining a document" in system
                or "MISSED" in system
                or "Re-examination Pass" in system
            )
            is_strict = (
                "directly supported by the text" in system
                or "Strict Fallback Pass" in system
            )
            pass_kind = (
                "reextract" if is_reextract else ("strict" if is_strict else "initial")
            )
            self.calls.append({"stage": f"value_{pass_kind}"})

            paper_match = re.search(
                r"<PAPER_TEXT>(.*?)</PAPER_TEXT>", user, flags=re.DOTALL
            )
            paper_text = paper_match.group(1) if paper_match else user

            title_match = re.search(
                r"<PAPER_TITLE>(.*?)</PAPER_TITLE>", user, flags=re.DOTALL
            )
            title_text = title_match.group(1).strip() if title_match else ""

            unit_match = re.search(r"\*\*Unit Name\*\*:\s*(.+)", system)
            unit_hint = unit_match.group(1).strip() if unit_match else title_text
            tag = self._tag_from_context(paper_text, unit_hint)

            cols = re.findall(r"^\s*- \*\*([^*]+)\*\*:", user, flags=re.MULTILINE)
            if not cols:
                cols = re.findall(r"^\s*name:\s*(\S+)", user, flags=re.MULTILINE)

            result: Dict[str, Any] = {}
            for col in cols:
                if col == "case_id":
                    result[col] = {
                        "answer": f"ID-{tag}",
                        "excerpts": [f"case ID-{tag} verified"],
                    }
                elif col == "outcome":
                    if tag in {"Case_A", "D1", "D3", "D5"}:
                        val = "granted"
                    else:
                        val = "denied"
                    result[col] = {
                        "answer": val,
                        "excerpts": [f"outcome {val} for {tag}"],
                    }
                elif col == "explicit_null_col":
                    result[col] = {"answer": None, "excerpts": []}
                elif col == "reordered_col_1":
                    if is_reextract:
                        result[col] = {
                            "answer": f"reordered1-{tag}",
                            "excerpts": [f"reordered1-{tag} detail"],
                        }
                    else:
                        result[col] = {"answer": "", "excerpts": []}
                elif col == "reordered_col_2":
                    if is_reextract:
                        result[col] = {
                            "answer": f"reordered2-{tag}",
                            "excerpts": [f"reordered2-{tag} detail"],
                        }
                    else:
                        result[col] = {"answer": "", "excerpts": []}
                elif col == "strict_only_col":
                    if is_strict:
                        result[col] = {
                            "answer": f"strict-{tag}",
                            "excerpts": [f"strict-{tag} evidence"],
                        }
                    else:
                        result[col] = {"answer": "", "excerpts": []}
                elif col == "unfillable_col":
                    result[col] = {"answer": "", "excerpts": []}
                else:
                    raise AssertionError(f"Unexpected column requested: {col}")
            return json.dumps(result)

        raise AssertionError(f"Unexpected prompt sent to FakeExtractionLLM: {system[:120]}")


class RecordingWSManager:
    """Minimal async WebSocket manager stub."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []

    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": message.get("type")})

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        return None

    async def broadcast_progress(self, session_id: str, data: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": "progress"})

    async def broadcast_completion(self, session_id: str, data: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": "completion"})

    async def broadcast_error(
        self, session_id: str, error_message: str, error_details: Optional[Dict[str, Any]] = None
    ) -> None:
        self.events.append({"session_id": session_id, "type": "error", "error": error_message})

    async def broadcast_row_completed(self, session_id: str, row_data: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": "row_completed"})

    async def buffer_or_broadcast_cell(self, session_id: str, message: Dict[str, Any]) -> None:
        self.events.append({"session_id": session_id, "type": "cell_extracted"})


def _doc_text(tag: str, doc_marker: str, outcome_val: str) -> str:
    return (
        f"{doc_marker}: Summary record for {tag}.\n"
        f"The filing confirms case ID-{tag} verified on record.\n"
        f"Final ruling shows outcome {outcome_val} for {tag}.\n"
        f"Supporting appendix notes reordered1-{tag} detail and reordered2-{tag} detail.\n"
        f"Footnote gives strict-{tag} evidence."
    )


def _write_five_docs(docs_dir: Path) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)
    for i in range(1, 6):
        tag = f"D{i}"
        outcome_val = "granted" if i in {1, 3, 5} else "denied"
        (docs_dir / f"doc_{i}.txt").write_text(
            _doc_text(tag, f"DOC_{i}", outcome_val),
            encoding="utf-8",
        )


def _normalize_value(val: Any, tmp_root: Path) -> Any:
    root_str = str(tmp_root)
    if isinstance(val, str):
        return val.replace(root_str, "<TMP>")
    if isinstance(val, list):
        return [_normalize_value(v, tmp_root) for v in val]
    if isinstance(val, dict):
        return {k: _normalize_value(v, tmp_root) for k, v in val.items()}
    return val


def _read_jsonl(path: Path, tmp_root: Path, *, strip_retriever_meta: bool = False) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if strip_retriever_meta and isinstance(obj.get("_metadata"), dict):
            obj["_metadata"] = {
                k: v for k, v in obj["_metadata"].items() if k != "retriever"
            }
        rows.append(_normalize_value(obj, tmp_root))
    return rows


def _make_schema_dict(col_names: List[str], *, include_unit: bool = True) -> Dict[str, Any]:
    col_defs = {
        "case_id": ("Unique identifier", "Identifies record"),
        "outcome": ("Appeal outcome", "Primary outcome"),
        "explicit_null_col": ("Optional note", "May be absent"),
        "reordered_col_1": ("First secondary note", "Found on re-exam"),
        "reordered_col_2": ("Second secondary note", "Found on re-exam"),
        "strict_only_col": ("Strict footnote value", "Found in strict fallback"),
        "unfillable_col": ("Unreported attribute", "Never present"),
    }
    schema_list = [
        {
            "column": name,
            "definition": col_defs[name][0],
            "explanation": col_defs[name][1],
        }
        for name in col_names
    ]
    payload: Dict[str, Any] = {
        "query": "What are the case outcomes and procedural details?",
        "schema": schema_list,
    }
    if include_unit:
        payload["observation_unit"] = {
            "name": "Appeal Case",
            "definition": "A single judicial appeal case",
        }
    return payload


def _make_session_columns(col_names: List[str]) -> List[ColumnInfo]:
    return [
        ColumnInfo(name=name, definition=f"Definition for {name}", rationale=f"Rationale for {name}")
        for name in col_names
    ]


@pytest.fixture(autouse=True)
def _isolate_test_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Isolate storage, cwd, embedding model, and shared retriever cache."""
    import app.services as services_mod

    monkeypatch.chdir(tmp_path)
    reset_storage()
    monkeypatch.setattr(schema_mod, "_EMB_MODEL", _FakeEmbeddingModel())
    monkeypatch.setattr(
        embedding_model,
        "load_sentence_transformer",
        lambda *_a, **_kw: _FakeEmbeddingModel(),
    )
    if hasattr(services_mod, "_shared_retriever"):
        monkeypatch.setattr(services_mod, "_shared_retriever", None)
    if hasattr(services_mod, "_shared_retrievers"):
        monkeypatch.setattr(services_mod, "_shared_retrievers", {})
    yield
    reset_storage()


async def _collect_all_scenarios(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    # 1. 5-doc unit extraction with a missing/unfillable + strict-recoverable column
    s1_dir = tmp_path / "scen_five_missing"
    s1_docs = s1_dir / "docs"
    _write_five_docs(s1_docs)
    s1_schema = s1_dir / "schema.json"
    s1_schema.write_text(
        json.dumps(
            _make_schema_dict(
                ["case_id", "outcome", "explicit_null_col", "strict_only_col", "unfillable_col"]
            )
        ),
        encoding="utf-8",
    )
    s1_out = s1_dir / "out.jsonl"
    llm1 = FakeExtractionLLM()
    res1 = build_table_jsonl(
        schema_path=s1_schema,
        docs_directories=[s1_docs],
        output_path=s1_out,
        llm=llm1,
        retriever=None,
        max_workers=1,
        mode="all",
    )
    results["five_doc_unit_extraction_with_missing"] = {
        "rows": _read_jsonl(s1_out, tmp_path),
        "result": res1,
    }

    # 2. 5-doc unit extraction where all columns are filled or explicit null
    s2_dir = tmp_path / "scen_five_filled"
    s2_docs = s2_dir / "docs"
    _write_five_docs(s2_docs)
    s2_schema = s2_dir / "schema.json"
    s2_schema.write_text(
        json.dumps(_make_schema_dict(["case_id", "outcome", "explicit_null_col"])),
        encoding="utf-8",
    )
    s2_out = s2_dir / "out.jsonl"
    llm2 = FakeExtractionLLM()
    res2 = build_table_jsonl(
        schema_path=s2_schema,
        docs_directories=[s2_docs],
        output_path=s2_out,
        llm=llm2,
        retriever=None,
        max_workers=1,
        mode="all",
    )
    results["five_doc_unit_extraction_all_filled"] = {
        "rows": _read_jsonl(s2_out, tmp_path),
        "result": res2,
    }

    # 3. Multi-unit + reordered Pass-2 recovery + skipped empty-unit document
    s3_dir = tmp_path / "scen_multi_reordered_skip"
    s3_docs = s3_dir / "docs"
    s3_docs.mkdir(parents=True, exist_ok=True)
    (s3_docs / "doc_1.txt").write_text(
        _doc_text("D1", "DOC_1", "granted"), encoding="utf-8"
    )
    (s3_docs / "doc_multi.txt").write_text(
        "DOC_MULTI:\n"
        + _doc_text("Case_A", "CASE_A_BLOCK", "granted")
        + "\n"
        + _doc_text("Case_B", "CASE_B_BLOCK", "denied"),
        encoding="utf-8",
    )
    (s3_docs / "doc_empty.txt").write_text(
        "DOC_EMPTY_UNITS: Procedural docket notice only.", encoding="utf-8"
    )
    s3_schema = s3_dir / "schema.json"
    s3_schema.write_text(
        json.dumps(
            _make_schema_dict(["case_id", "outcome", "reordered_col_1", "reordered_col_2"])
        ),
        encoding="utf-8",
    )
    s3_out = s3_dir / "out.jsonl"
    llm3 = FakeExtractionLLM()
    res3 = build_table_jsonl(
        schema_path=s3_schema,
        docs_directories=[s3_docs],
        output_path=s3_out,
        llm=llm3,
        retriever=None,
        max_workers=1,
        mode="all",
    )
    results["multi_unit_and_reordered_and_skipped"] = {
        "rows": _read_jsonl(s3_out, tmp_path),
        "result": res3,
    }

    # Prepare shared local storage under cwd (tmp_path) for backend service scenarios
    data_root = tmp_path / "data"
    work_root = tmp_path / "schematiq_work"
    sessions_root = tmp_path / "sessions"
    data_root.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)
    sessions_root.mkdir(parents=True, exist_ok=True)
    storage = LocalStorageBackend(
        sessions_dir=str(sessions_root),
        data_dir=str(data_root),
        schematiq_work_dir=str(work_root),
    )
    session_manager = SessionManager(storage=storage)
    ws_manager = RecordingWSManager()

    # 4. ReextractionService on 5 documents with known_units
    sid4 = "sess_reextract_5"
    _write_five_docs(data_root / sid4 / "documents")
    existing_extracted = [
        {
            "_row_name": f"Unit_{i}",
            "_unit_name": f"Unit_{i}",
            "_source_document": f"doc_{i}",
            "_parent_document": f"doc_{i}",
            "_papers": [f"doc_{i}.txt"],
            "case_id": {"answer": f"ID-D{i}", "excerpts": [f"case ID-D{i} verified"]},
            "outcome": {"answer": "stale", "excerpts": []},
            "unfillable_col": None,
        }
        for i in range(1, 6)
    ]
    (work_root / sid4).mkdir(parents=True, exist_ok=True)
    (work_root / sid4 / "extracted_data.jsonl").write_text(
        "\n".join(json.dumps(r) for r in existing_extracted) + "\n",
        encoding="utf-8",
    )
    sess4 = VisualizationSession(
        id=sid4,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="test",
            uploaded_documents=[f"doc_{i}.txt" for i in range(1, 6)],
        ),
        columns=_make_session_columns(["case_id", "outcome", "unfillable_col"]),
        schema_query="What are the case outcomes?",
        observation_unit=ObservationUnitInfo(
            name="Appeal Case", definition="A single judicial appeal case"
        ),
    )
    session_manager.create_session(sess4)

    reextract_svc = ReextractionService(ws_manager, session_manager)

    async def _noop_summary(*_a, **_kw):
        return "Summary ok"

    monkeypatch.setattr(reextract_svc, "_summarize_reextraction", _noop_summary)
    llm4 = FakeExtractionLLM()
    op4 = ReextractionOperation(
        operation_id="op_reext_5",
        session_id=sid4,
        columns=["outcome", "unfillable_col"],
        status="starting",
    )
    op4.total_documents = 5
    op4.llm = llm4
    reextract_svc.active_operations[op4.operation_id] = op4
    await reextract_svc._run_reextraction(op4.operation_id)
    results["reextraction_service_five_docs"] = {
        "status": op4.status,
        "rows": _read_jsonl(work_root / sid4 / "extracted_data.jsonl", tmp_path),
    }

    # 5. SchemaManager: reprocess_column (mode="all") + reprocess_column_with_context (mode="one_by_one")
    sid5 = "sess_schema_mgr"
    _write_five_docs(data_root / sid5 / "documents")
    initial_data_rows = [
        {
            "row_name": f"Unit_{i}",
            "papers": [f"doc_{i}.txt"],
            "_source_document": f"doc_{i}",
            "data": {
                "case_id": {"answer": f"ID-D{i}", "excerpts": []},
                "outcome": {"answer": "old", "excerpts": []},
                "strict_only_col": {"answer": "", "excerpts": []},
            },
        }
        for i in range(1, 6)
    ]
    (data_root / sid5 / "data.jsonl").write_text(
        "\n".join(json.dumps(r) for r in initial_data_rows) + "\n",
        encoding="utf-8",
    )
    sess5 = VisualizationSession(
        id=sid5,
        type=SessionType.UPLOAD,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="upload",
            uploaded_documents=[f"doc_{i}.txt" for i in range(1, 6)],
        ),
        columns=_make_session_columns(["case_id", "outcome", "strict_only_col"]),
        schema_query="What are the case outcomes?",
        observation_unit=ObservationUnitInfo(
            name="Appeal Case", definition="A single judicial appeal case"
        ),
    )
    session_manager.create_session(sess5)
    schema_mgr = SchemaManager(ws_manager, session_manager)
    llm5 = FakeExtractionLLM()
    monkeypatch.setattr(
        schema_mgr, "_get_value_extraction_llm_from_session", lambda _sid: llm5
    )
    await schema_mgr.reprocess_column(sid5, "outcome")
    await schema_mgr.reprocess_column_with_context(
        sid5, "strict_only_col", {"query": "What are the case outcomes?"}
    )
    results["schema_manager_reprocess_column"] = {
        "rows": _read_jsonl(data_root / sid5 / "data.jsonl", tmp_path),
    }

    # 6. UploadDocumentProcessor.process_documents on pending_documents
    sid6 = "sess_upload_proc"
    pending6 = data_root / sid6 / "pending_documents"
    _write_five_docs(pending6)
    (data_root / sid6 / "data.jsonl").write_text("", encoding="utf-8")
    sess6 = VisualizationSession(
        id=sid6,
        type=SessionType.UPLOAD,
        status=SessionStatus.PROCESSING,
        metadata=SessionMetadata(
            source="upload",
            uploaded_documents=[f"doc_{i}.txt" for i in range(1, 6)],
        ),
        columns=_make_session_columns(["case_id", "outcome", "explicit_null_col"]),
        schema_query="What are the case outcomes?",
        observation_unit=ObservationUnitInfo(
            name="Appeal Case", definition="A single judicial appeal case"
        ),
    )
    session_manager.create_session(sess6)
    upload_proc = UploadDocumentProcessor(ws_manager, session_manager)
    llm6 = FakeExtractionLLM()
    monkeypatch.setattr(upload_proc, "_get_llm_for_session", lambda _sid, _s=None: llm6)
    monkeypatch.setattr(
        "app.services.upload_document_processor.PROGRESS_CHECK_INTERVAL", 0.01
    )
    await upload_proc.process_documents(sid6)
    results["upload_document_processor"] = {
        "rows": _read_jsonl(data_root / sid6 / "data.jsonl", tmp_path),
    }

    # 7. Legacy single-dir TableBuilder in mode="one_by_one" (short docs <= 1000 chars) and mode="all"
    s7_dir = tmp_path / "scen_legacy_single_dir"
    s7_docs = s7_dir / "docs"
    _write_five_docs(s7_docs)
    # Sanity check: ensure short docs stay <= MIN_DOCUMENT_SIZE_FOR_SNIPPETS (1000 chars)
    assert all(len(p.read_text(encoding="utf-8")) <= 1000 for p in s7_docs.iterdir())

    dummy_retriever = SimpleNamespace(model_name="all-MiniLM-L6-v2")

    s7_schema_obo = s7_dir / "schema_obo.json"
    s7_schema_obo.write_text(
        json.dumps(
            _make_schema_dict(
                ["case_id", "outcome", "unfillable_col"], include_unit=False
            )
        ),
        encoding="utf-8",
    )
    s7_out_obo = s7_dir / "out_obo.jsonl"
    llm7_obo = FakeExtractionLLM()
    TableBuilder(llm=llm7_obo, retriever=dummy_retriever).build_table_jsonl(
        schema_path=s7_schema_obo,
        docs_directory=s7_docs,
        output_path=s7_out_obo,
        mode="one_by_one",
        max_workers=0,
    )

    s7_schema_all = s7_dir / "schema_all.json"
    s7_schema_all.write_text(
        json.dumps(
            _make_schema_dict(
                ["case_id", "outcome", "strict_only_col", "unfillable_col"],
                include_unit=False,
            )
        ),
        encoding="utf-8",
    )
    s7_out_all = s7_dir / "out_all.jsonl"
    llm7_all = FakeExtractionLLM()
    TableBuilder(llm=llm7_all, retriever=dummy_retriever).build_table_jsonl(
        schema_path=s7_schema_all,
        docs_directory=s7_docs,
        output_path=s7_out_all,
        mode="all",
        max_workers=0,
    )

    # Note: _metadata["retriever"] is stripped here because #663 intentionally changes it
    # from {"type": ..., "model": ...} to {"type": None, "model": None} when DISABLE_RETRIEVER=True.
    results["legacy_single_dir_one_by_one_and_all"] = {
        "one_by_one_rows": _read_jsonl(s7_out_obo, tmp_path, strip_retriever_meta=True),
        "all_rows": _read_jsonl(s7_out_all, tmp_path, strip_retriever_meta=True),
    }

    return results


@pytest.mark.asyncio
async def test_value_extraction_equivalence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    actual = await _collect_all_scenarios(tmp_path, monkeypatch)

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
