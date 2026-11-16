"""Unit tests for per-config get_shared_retriever() (#661) and DISABLE_RETRIEVER in backend (#663)."""

from __future__ import annotations

from collections import OrderedDict
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

import app.services as services_mod
from app.models.schematiq import RetrieverConfig
from app.models.session import (
    ColumnInfo,
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services import continue_discovery_service as cd_mod
from app.services import reextraction_service as reext_mod
from app.services import schema_manager as sm_mod
from app.services import schematiq_runner as runner_mod
from app.services import upload_document_processor as udp_mod
from app.services.pipeline import value_extraction as ve_mod
from app.services.session_manager import SessionManager
from app.storage.factory import reset_storage
from app.storage.local_backend import LocalStorageBackend
from schematiq.core import embedding_model, retrievers as retrievers_mod
from schematiq.core.schema import Column, ObservationUnit, Schema


class _FakeEmbeddingRetriever:
    """Minimal stand-in for EmbeddingRetriever capturing init kwargs."""

    def __init__(self, **kwargs):
        self.init_kwargs = dict(kwargs)
        self.model_name = kwargs.get("model_name", "all-MiniLM-L6-v2")
        self.model = kwargs.get("model", SimpleNamespace(model_name=self.model_name))
        self.k = kwargs.get("k")
        self.max_words = kwargs.get("max_words")
        self.overlap_words = kwargs.get("overlap_words")
        self.enable_dynamic_k = kwargs.get("enable_dynamic_k")
        self.dynamic_k_threshold = kwargs.get("dynamic_k_threshold")
        self.dynamic_k_minimum = kwargs.get("dynamic_k_minimum")


@pytest.fixture(autouse=True)
def _reset_retriever_cache(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(services_mod, "_shared_retrievers", OrderedDict())
    monkeypatch.setattr(services_mod, "_shared_embedding_models", OrderedDict())
    yield
    monkeypatch.setattr(services_mod, "_shared_retrievers", OrderedDict())
    monkeypatch.setattr(services_mod, "_shared_embedding_models", OrderedDict())


def test_get_shared_retriever_caches_per_config_and_forwards_all_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: List[_FakeEmbeddingRetriever] = []

    def _factory(**kwargs):
        inst = _FakeEmbeddingRetriever(**kwargs)
        created.append(inst)
        return inst

    monkeypatch.setattr(retrievers_mod, "EmbeddingRetriever", _factory)

    # Identical default configs (None, empty dict, RetrieverConfig(), blank/None overrides) share 1 instance
    r_none = services_mod.get_shared_retriever()
    r_empty = services_mod.get_shared_retriever({})
    r_model = services_mod.get_shared_retriever(RetrieverConfig())
    r_partial = services_mod.get_shared_retriever(
        {"model_name": "   ", "passage_words": None, "k": 15, "unknown_key": "ignored"}
    )
    assert r_none is r_empty is r_model is r_partial
    assert len(created) == 1
    assert r_none.init_kwargs == {
        "model_name": "all-MiniLM-L6-v2",
        "k": 15,
        "max_words": 512,
        "overlap_words": 64,
        "enable_dynamic_k": True,
        "dynamic_k_threshold": 0.65,
        "dynamic_k_minimum": 3,
    }

    # Changing model_name via dict, RetrieverConfig, or kwarg produces distinct instances
    r_multi = services_mod.get_shared_retriever(
        {"model_name": "paraphrase-multilingual-MiniLM-L12-v2"}
    )
    r_multi_2 = services_mod.get_shared_retriever(
        model_name="paraphrase-multilingual-MiniLM-L12-v2"
    )
    assert r_multi is r_multi_2
    assert r_multi is not r_none
    assert r_multi.model_name == "paraphrase-multilingual-MiniLM-L12-v2"
    assert len(created) == 2

    # Each differing parameter creates its own cached instance while repeating reuses it
    variants = [
        {"passage_words": 1024},
        {"overlap": 128},
        {"k": 20},
        {"enable_dynamic_k": False},
        {"dynamic_k_threshold": 0.8},
        {"dynamic_k_minimum": 5},
    ]
    for idx, v in enumerate(variants, start=3):
        inst_a = services_mod.get_shared_retriever(v)
        inst_b = services_mod.get_shared_retriever(RetrieverConfig(**v))
        assert inst_a is inst_b
        assert inst_a is not r_none
        assert len(created) == idx

    # Legacy passage_chars / max_words / overlap_words aliases map cleanly to passage_words / overlap
    r_words_target = services_mod.get_shared_retriever({"passage_words": 1024})
    assert services_mod.get_shared_retriever({"max_words": 1024}) is r_words_target
    assert services_mod.get_shared_retriever({"passage_chars": 1024}) is r_words_target
    assert RetrieverConfig(passage_chars=768).passage_words == 768


def test_get_shared_retriever_rejects_non_embedding_type() -> None:
    with pytest.raises(ValueError, match="Unsupported shared retriever type"):
        services_mod.get_shared_retriever({"type": "prompting"})


def test_two_configs_with_same_model_name_load_sentence_transformer_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two distinct configs sharing model_name must load SentenceTransformer only once."""
    load_calls: List[tuple] = []

    def _fake_load(model_name: str = "all-MiniLM-L6-v2", **kwargs: Any):
        load_calls.append((model_name, kwargs.get("device")))
        return SimpleNamespace(tag=f"st-{model_name}-{len(load_calls)}")

    monkeypatch.setattr(embedding_model, "load_sentence_transformer", _fake_load)

    r1 = services_mod.get_shared_retriever(
        {"model_name": "all-MiniLM-L6-v2", "k": 10, "overlap": 32}
    )
    r2 = services_mod.get_shared_retriever(
        {"model_name": "all-MiniLM-L6-v2", "k": 25, "overlap": 128}
    )

    assert len(load_calls) == 1
    assert load_calls[0][0] == "all-MiniLM-L6-v2"
    assert r1 is not r2
    assert r1.k == 10 and r2.k == 25
    assert r1.overlap_words == 32 and r2.overlap_words == 128
    assert r1.model is r2.model

    # A different model_name loads its own model once
    r3 = services_mod.get_shared_retriever({"model_name": "BAAI/bge-m3", "k": 10})
    assert len(load_calls) == 2
    assert load_calls[1][0] == "BAAI/bge-m3"
    assert r3.model is not r1.model


def test_shared_retriever_lru_bound_evicts_wrappers_without_reloading_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_calls: List[str] = []

    def _fake_load(model_name: str = "all-MiniLM-L6-v2", **_kw: Any):
        load_calls.append(model_name)
        return SimpleNamespace(tag=f"st-{model_name}-{len(load_calls)}")

    monkeypatch.setattr(embedding_model, "load_sentence_transformer", _fake_load)
    monkeypatch.setattr(services_mod, "_MAX_SHARED_RETRIEVERS", 3)

    r1 = services_mod.get_shared_retriever({"k": 1})
    services_mod.get_shared_retriever({"k": 2})
    services_mod.get_shared_retriever({"k": 3})
    r4 = services_mod.get_shared_retriever({"k": 4})

    assert len(services_mod._shared_retrievers) == 3
    assert len(load_calls) == 1

    # k=1 was evicted as LRU, so re-requesting it creates a fresh wrapper
    # while still reusing the cached model weights (no extra load_sentence_transformer).
    r1_rebuilt = services_mod.get_shared_retriever({"k": 1})
    assert r1_rebuilt is not r1
    assert r1_rebuilt.model is r4.model
    assert len(services_mod._shared_retrievers) == 3
    assert len(load_calls) == 1


@pytest.mark.asyncio
async def test_schematiq_runner_passes_retriever_config_to_get_shared_retriever(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.models.schematiq import LLMConfig, ScheMatiQConfig

    monkeypatch.chdir(tmp_path)
    reset_storage()

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    sessions_dir = tmp_path / "sessions"
    docs_dir = tmp_path / "docs"
    for d in (data_dir, work_dir, sessions_dir, docs_dir):
        d.mkdir(parents=True, exist_ok=True)
    (docs_dir / "doc1.txt").write_text("Sample doc", encoding="utf-8")

    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    sm = SessionManager(storage=storage)
    ws = MagicMock()
    ws.broadcast_to_session = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_progress = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_completion = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_error = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_log = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())

    sid = "sess_runner_cfg"
    sm.create_session(
        VisualizationSession(
            id=sid,
            type=SessionType.SCHEMATIQ,
            status=SessionStatus.PROCESSING,
            metadata=SessionMetadata(source="test"),
            columns=[],
            schema_query="q",
        )
    )

    runner = runner_mod.ScheMatiQRunner(
        work_dir=str(work_dir), websocket_manager=ws, session_manager=sm
    )
    monkeypatch.setattr(runner_mod, "build_llm_interface", lambda **_kw: MagicMock())
    monkeypatch.setattr(
        runner_mod,
        "resolve_docs_paths",
        lambda *_a, **_kw: _async_val([str(docs_dir)]),
    )
    monkeypatch.setattr(runner_mod, "start_heartbeat", _async_fake_heartbeat)
    monkeypatch.setattr(runner, "_persist_extracted_data", _async_noop)
    monkeypatch.setattr(runner, "check_global_quota", lambda *_a, **_kw: None)

    captured_retriever_args: List[Any] = []
    fake_retriever = _FakeEmbeddingRetriever(model_name="BAAI/bge-m3")

    def _fake_get_shared(cfg=None, **_kw):
        captured_retriever_args.append(cfg)
        return fake_retriever

    monkeypatch.setattr(services_mod, "get_shared_retriever", _fake_get_shared)

    discovery_retrievers: List[Any] = []

    async def _fake_discovery(*args, **kwargs):
        # run_schema_discovery(session_id, documents, filenames, schematiq_config, llm, retriever, ...)
        discovery_retrievers.append(args[5] if len(args) > 5 else kwargs.get("retriever"))
        return (
            Schema(
                query="q",
                columns=[Column(name="c1", definition="d", rationale="r")],
                observation_unit=ObservationUnit(name="Doc", definition="One doc"),
            ),
            SimpleNamespace(snapshots=[]),
        )

    monkeypatch.setattr(runner_mod, "run_schema_discovery", _fake_discovery)
    monkeypatch.setattr(runner_mod, "compute_statistics", lambda *_a, **_kw: None)

    cfg = ScheMatiQConfig(
        query="q",
        docs_path=str(docs_dir),
        skip_value_extraction=True,
        schema_creation_backend=LLMConfig(provider="openai", model="fake"),
        value_extraction_backend=LLMConfig(provider="openai", model="fake"),
        retriever=RetrieverConfig(
            model_name="BAAI/bge-m3",
            passage_words=640,
            overlap=96,
            k=12,
            enable_dynamic_k=False,
            dynamic_k_threshold=0.5,
            dynamic_k_minimum=4,
        ),
        count_toward_quota=False,
        output_path=str(work_dir / sid / "discovered_schema.json"),
    )

    await runner._execute_schematiq(sid, cfg)

    assert len(captured_retriever_args) == 1
    assert captured_retriever_args[0]["model_name"] == "BAAI/bge-m3"
    assert captured_retriever_args[0]["passage_words"] == 640
    assert captured_retriever_args[0]["overlap"] == 96
    assert captured_retriever_args[0]["k"] == 12
    assert captured_retriever_args[0]["enable_dynamic_k"] is False
    assert captured_retriever_args[0]["dynamic_k_threshold"] == 0.5
    assert captured_retriever_args[0]["dynamic_k_minimum"] == 4
    assert discovery_retrievers == [fake_retriever]


@pytest.mark.asyncio
async def test_continue_discovery_forwards_retriever_config_and_skips_in_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    reset_storage()

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    sessions_dir = tmp_path / "sessions"
    for d in (data_dir, work_dir, sessions_dir):
        d.mkdir(parents=True, exist_ok=True)
    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    sm = SessionManager(storage=storage)
    ws = MagicMock()
    ws.broadcast_to_session = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_log = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_cell_extracted = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())

    sid = "sess_cd_cfg"
    op_id = "op_cd_1"
    sdata = data_dir / sid
    (sdata / "documents").mkdir(parents=True)
    (sdata / "documents" / "doc1.txt").write_text("DOC_ALPHA text", encoding="utf-8")
    (sdata / "data.jsonl").write_text(
        json.dumps({"row_name": "Unit_1", "papers": ["doc1.txt"], "c1": "v1"}) + "\n",
        encoding="utf-8",
    )

    sm.create_session(
        VisualizationSession(
            id=sid,
            type=SessionType.SCHEMATIQ,
            status=SessionStatus.COMPLETED,
            metadata=SessionMetadata(source="test"),
            schema_query="q",
            columns=[
                ColumnInfo(name="c1", definition="d1", rationale="r1"),
                ColumnInfo(name="c2", definition="d2", rationale="r2"),
            ],
        )
    )

    custom_ret_cfg = {"model_name": "intfloat/multilingual-e5-small", "k": 9, "overlap": 32}
    (sdata / f"continue_discovery_config_{op_id}.json").write_text(
        json.dumps(
            {
                "document_source": "original",
                "retriever_config": custom_ret_cfg,
                "documents_batch_size": 1,
                "skip_value_extraction": True,
                "query": "q",
                "bypass_limit": True,
            }
        ),
        encoding="utf-8",
    )
    (sdata / f"continue_discovery_llm_{op_id}.json").write_text(
        json.dumps({"provider": "openai", "model": "fake", "temperature": 0}),
        encoding="utf-8",
    )
    (sdata / f"extraction_config_{op_id}.json").write_text(
        json.dumps({"columns": ["c2"], "row_selection": "all"}),
        encoding="utf-8",
    )

    svc = cd_mod.ContinueDiscoveryService(ws, sm)
    monkeypatch.setattr(svc, "_get_data_dir", lambda: data_dir)
    monkeypatch.setattr(svc, "_get_schematiq_work_dir", lambda: work_dir)
    op = cd_mod.ContinueDiscoveryOperation(
        operation_id=op_id, session_id=sid, status="starting"
    )
    op.initial_columns = ["c1"]
    op.document_source = "original"
    op.llm_config = {"provider": "openai", "model": "fake", "temperature": 0}
    svc.active_operations[op_id] = op

    captured_calls: List[Any] = []
    fake_retriever = _FakeEmbeddingRetriever(model_name="intfloat/multilingual-e5-small")

    def _fake_get_shared(cfg=None, **_kw):
        captured_calls.append(cfg)
        return fake_retriever

    monkeypatch.setattr(services_mod, "get_shared_retriever", _fake_get_shared)
    monkeypatch.setattr(cd_mod.schematiq_utils, "build_llm", lambda *_a, **_kw: MagicMock())

    discover_retrievers: List[Any] = []

    def _fake_discover_schema(**kwargs):
        discover_retrievers.append(kwargs.get("retriever"))
        return (
            Schema(query="q", columns=[Column(name="c1", definition="d1", rationale="r1")]),
            SimpleNamespace(snapshots=[]),
        )

    monkeypatch.setattr(cd_mod, "discover_schema", _fake_discover_schema)

    await svc._run_continue_discovery(op_id)
    assert captured_calls == [custom_ret_cfg]
    assert discover_retrievers == [fake_retriever]

    # Now run incremental extraction with DISABLE_RETRIEVER=True: get_shared_retriever must NOT be called
    captured_calls.clear()
    build_table_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        cd_mod,
        "build_table_jsonl",
        lambda **kwargs: (
            build_table_calls.append(kwargs),
            Path(kwargs["output_path"]).write_text("", encoding="utf-8"),
        )[1],
    )
    monkeypatch.setattr(cd_mod, "DISABLE_RETRIEVER", True)
    svc.active_operations[op_id] = op
    await svc._run_incremental_extraction(op_id)

    assert captured_calls == []
    assert len(build_table_calls) == 1
    assert build_table_calls[0]["retriever"] is None


@pytest.mark.asyncio
async def test_all_backend_extraction_services_skip_get_shared_retriever_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    reset_storage()

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    sessions_dir = tmp_path / "sessions"
    for d in (data_dir, work_dir, sessions_dir):
        d.mkdir(parents=True, exist_ok=True)
    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    sm = SessionManager(storage=storage)
    ws = MagicMock()
    ws.broadcast_to_session = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_log = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_progress = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_completion = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_error = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())
    ws.broadcast_row_completed = MagicMock(side_effect=lambda *_a, **_kw: _async_noop())

    get_shared_spy = MagicMock(side_effect=AssertionError("get_shared_retriever must not be called"))
    monkeypatch.setattr(services_mod, "get_shared_retriever", get_shared_spy)

    sid = "sess_no_retriever"
    (data_dir / sid / "documents").mkdir(parents=True)
    (data_dir / sid / "documents" / "d1.txt").write_text("doc text", encoding="utf-8")
    (data_dir / sid / "pending_documents").mkdir(parents=True)
    (data_dir / sid / "pending_documents" / "d2.txt").write_text("pending text", encoding="utf-8")
    (data_dir / sid / "data.jsonl").write_text(
        json.dumps({"row_name": "U1", "papers": ["d1.txt"], "data": {"c1": "v"}}) + "\n",
        encoding="utf-8",
    )
    sm.create_session(
        VisualizationSession(
            id=sid,
            type=SessionType.SCHEMATIQ,
            status=SessionStatus.COMPLETED,
            metadata=SessionMetadata(source="test", uploaded_documents=["d1.txt"]),
            columns=[ColumnInfo(name="c1", definition="d", rationale="r")],
            schema_query="q",
            observation_unit=ObservationUnitInfo(name="Doc", definition="One doc"),
        )
    )

    # 1. ReextractionService
    reext_calls: List[Any] = []
    monkeypatch.setattr(reext_mod, "DISABLE_RETRIEVER", True)
    monkeypatch.setattr(
        reext_mod,
        "build_table_jsonl",
        lambda **kw: (reext_calls.append(kw.get("retriever")), Path(kw["output_path"]).write_text("", encoding="utf-8"))[1],
    )
    re_svc = reext_mod.ReextractionService(ws, sm)
    assert re_svc.get_cached_retriever() is None
    op = reext_mod.ReextractionOperation(operation_id="op1", session_id=sid, columns=["c1"], status="starting")
    op.total_documents = 1
    op.llm = MagicMock()
    re_svc.active_operations["op1"] = op
    monkeypatch.setattr(re_svc, "_summarize_reextraction", lambda *_a, **_kw: _async_noop())
    await re_svc._run_reextraction("op1")
    assert reext_calls == [None]

    # 2. SchemaManager (reprocess_column + reprocess_column_with_context)
    sm_calls: List[Any] = []
    monkeypatch.setattr(sm_mod, "DISABLE_RETRIEVER", True)
    monkeypatch.setattr(
        sm_mod,
        "build_table_jsonl",
        lambda **kw: (sm_calls.append(kw.get("retriever")), Path(kw["output_path"]).write_text("", encoding="utf-8"))[1],
    )
    schema_mgr = sm_mod.SchemaManager(ws, sm)
    monkeypatch.setattr(schema_mgr, "_get_value_extraction_llm_from_session", lambda _sid: MagicMock())
    await schema_mgr.reprocess_column(sid, "c1")
    await schema_mgr.reprocess_column_with_context(sid, "c1", {"query": "q"})
    assert sm_calls == [None, None]

    # 3. UploadDocumentProcessor
    udp_calls: List[Any] = []
    monkeypatch.setattr(udp_mod, "DISABLE_RETRIEVER", True)
    monkeypatch.setattr(udp_mod, "PROGRESS_CHECK_INTERVAL", 0.01)
    monkeypatch.setattr(
        udp_mod,
        "build_table_jsonl",
        lambda **kw: (udp_calls.append(kw.get("retriever")), Path(kw["output_path"]).write_text("", encoding="utf-8"), {})[2],
    )
    up = udp_mod.UploadDocumentProcessor(ws, sm)
    monkeypatch.setattr(up, "_get_llm_for_session", lambda _sid, _s=None: MagicMock())
    await up.process_documents(sid)
    assert udp_calls == [None]

    # 4. pipeline.value_extraction.run_value_extraction passes retriever=None even if given one
    (work_dir / sid).mkdir(parents=True, exist_ok=True)
    ve_calls: List[Any] = []
    monkeypatch.setattr(ve_mod, "DISABLE_RETRIEVER", True)
    monkeypatch.setattr(
        ve_mod,
        "build_table_jsonl",
        lambda **kw: (ve_calls.append(kw.get("retriever")), {})[1],
    )
    await ve_mod.run_value_extraction(
        session_id=sid,
        schematiq_config={"docs_path": str(data_dir / sid / "documents")},
        schema=Schema(
            query="q",
            columns=[Column(name="c1", definition="d", rationale="r")],
            observation_unit=ObservationUnit(name="Doc", definition="One doc"),
        ),
        llm=MagicMock(),
        retriever=_FakeEmbeddingRetriever(),
        progress_callback=lambda *_a, **_kw: _async_noop(),
        is_stop_requested=lambda _sid: False,
        ws_mixin=MagicMock(broadcast_row_completed=lambda *_a, **_kw: _async_noop()),
        ws_manager=ws,
        session_manager=sm,
        work_dir=work_dir,
    )
    assert ve_calls == [None]
    get_shared_spy.assert_not_called()


async def _async_noop(*_a, **_kw):
    return None


async def _async_val(val):
    return val


async def _async_fake_heartbeat(*_a, **_kw):
    import asyncio

    async def _loop():
        await asyncio.sleep(60)

    return asyncio.create_task(_loop())
