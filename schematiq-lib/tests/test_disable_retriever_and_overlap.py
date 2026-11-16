"""Unit tests for DISABLE_RETRIEVER behavior (#663) and EmbeddingRetriever overlap (#661)."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import numpy as np
import pytest

from schematiq.core import embedding_model, utils as core_utils
from schematiq.core.llm_backends import LLMInterface
from schematiq.core.retrievers import EmbeddingRetriever
from schematiq.core.schema import Column, ObservationUnit, Schema
from schematiq.value_extraction.core import paper_processor as pp_mod
from schematiq.value_extraction.core import table_builder as tb_mod
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.core.table_builder import TableBuilder

ve_main = importlib.import_module("schematiq.value_extraction.main")


class _EmptyOrPartialLLM(LLMInterface):
    """Minimal deterministic LLM returning fixed columns or empty JSON."""

    _provider = "openai"

    def __init__(self, initial_response: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.model = "fake-unit-test"
        self.temperature = 0
        self.max_output_tokens = 2048
        self.context_window_size = 32768
        self.initial_response = initial_response or {}
        self.calls: List[Any] = []

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        self.calls.append({"prompt": prompt, "kwargs": kwargs})
        system = prompt[0].get("content", "") if isinstance(prompt, list) and prompt else ""
        is_reextract = "re-examining" in system or "MISSED" in system
        is_strict = "directly supported by the text" in system
        if not is_reextract and not is_strict:
            return json.dumps(self.initial_response)
        return json.dumps({})


class _RecordingRetriever:
    """Lightweight retriever stub recording query() calls."""

    model_name = "fake-retriever-model"

    def __init__(self):
        self.query_calls: List[Dict[str, Any]] = []

    def query(self, docs, question, k=None):
        self.query_calls.append({"docs": list(docs), "question": question, "k": k})
        return list(docs)


def _schema(col_names: List[str]) -> Schema:
    return Schema(
        query="Extract test fields",
        columns=[
            Column(name=name, definition=f"Def {name}", rationale=f"Rat {name}")
            for name in col_names
        ],
        observation_unit=ObservationUnit(name="Paper", definition="A paper"),
    )


def test_pass3_skips_fallback_retriever_and_extra_call_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pp_mod, "DISABLE_RETRIEVER", True)
    llm_single = _EmptyOrPartialLLM(
        {"filled": {"answer": "ok", "excerpts": ["ok text"]}}
    )
    proc_single = PaperProcessor(llm=llm_single, retriever=None)
    fallback_spy = MagicMock(return_value=_RecordingRetriever())
    monkeypatch.setattr(proc_single, "_create_fallback_retriever", fallback_spy)

    proc_single.extract_values_for_paper(
        paper_title="p1",
        paper_text="ok text here",
        schema=_schema(["filled", "missing_1"]),
        max_new_tokens=1024,
        mode="all",
    )
    fallback_spy.assert_not_called()
    assert len(llm_single.calls) == 2  # Pass 1 + Pass 4 (Pass 2 skipped for 1 missing; Pass 3 skipped)

    llm_multi = _EmptyOrPartialLLM(
        {"filled": {"answer": "ok", "excerpts": ["ok text"]}}
    )
    proc_multi = PaperProcessor(llm=llm_multi, retriever=None)
    fallback_spy_multi = MagicMock(return_value=_RecordingRetriever())
    monkeypatch.setattr(proc_multi, "_create_fallback_retriever", fallback_spy_multi)

    proc_multi.extract_values_for_paper(
        paper_title="p2",
        paper_text="ok text here",
        schema=_schema(["filled", "missing_1", "missing_2"]),
        max_new_tokens=1024,
        mode="all",
    )
    fallback_spy_multi.assert_not_called()
    assert len(llm_multi.calls) == 3  # Pass 1 + Pass 2 + Pass 4 (Pass 3 skipped)


def test_pass3_uses_fallback_retriever_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pp_mod, "DISABLE_RETRIEVER", False)
    llm = _EmptyOrPartialLLM({"filled": {"answer": "ok", "excerpts": ["ok text"]}})
    proc = PaperProcessor(llm=llm, retriever=None)
    fake_ret = _RecordingRetriever()
    fallback_spy = MagicMock(return_value=fake_ret)
    monkeypatch.setattr(proc, "_create_fallback_retriever", fallback_spy)

    proc.extract_values_for_paper(
        paper_title="p1",
        paper_text="ok text here",
        schema=_schema(["filled", "missing_1"]),
        max_new_tokens=1024,
        mode="all",
    )
    fallback_spy.assert_called_once()
    assert len(fake_ret.query_calls) == 1
    assert len(llm.calls) == 3  # Pass 1 + Pass 3 + Pass 4


def test_one_by_one_skips_attempt2_when_retriever_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ret = _RecordingRetriever()
    llm_disabled = _EmptyOrPartialLLM({})
    monkeypatch.setattr(pp_mod, "DISABLE_RETRIEVER", True)
    proc_disabled = PaperProcessor(llm=llm_disabled, retriever=fake_ret)
    proc_disabled.extract_values_for_paper(
        paper_title="short_doc",
        paper_text="short text under 1000 chars",
        schema=_schema(["missing_1"]),
        max_new_tokens=1024,
        mode="one_by_one",
    )
    assert len(llm_disabled.calls) == 1
    assert fake_ret.query_calls == []

    llm_enabled = _EmptyOrPartialLLM({})
    monkeypatch.setattr(pp_mod, "DISABLE_RETRIEVER", False)
    proc_enabled = PaperProcessor(llm=llm_enabled, retriever=fake_ret)
    proc_enabled.extract_values_for_paper(
        paper_title="short_doc",
        paper_text="short text under 1000 chars",
        schema=_schema(["missing_1"]),
        max_new_tokens=1024,
        mode="one_by_one",
    )
    assert len(llm_enabled.calls) == 2
    assert len(fake_ret.query_calls) == 2


def test_table_builder_metadata_hides_retriever_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_ret = _RecordingRetriever()
    llm = _EmptyOrPartialLLM({})
    schema = _schema(["col1"])

    monkeypatch.setattr(tb_mod, "DISABLE_RETRIEVER", True)
    tb_disabled = TableBuilder(llm=llm, retriever=fake_ret)
    meta_disabled = tb_disabled._create_metadata(schema, tmp_path)
    assert meta_disabled["retriever"] == {"type": None, "model": None}

    monkeypatch.setattr(tb_mod, "DISABLE_RETRIEVER", False)
    tb_enabled = TableBuilder(llm=llm, retriever=fake_ret)
    meta_enabled = tb_enabled._create_metadata(schema, tmp_path)
    assert meta_enabled["retriever"] == {
        "type": "_RecordingRetriever",
        "model": "fake-retriever-model",
    }


def test_value_extraction_cli_main_skips_build_retriever_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "doc1.txt").write_text("sample text", encoding="utf-8")
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps({"query": "q", "schema": []}), encoding="utf-8")
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(
        json.dumps(
            {
                "schema_path": str(schema_path),
                "docs_directory": str(docs_dir),
                "output_path": str(tmp_path / "out.jsonl"),
                "backend_cfg": {"provider": "openai", "model": "gpt-4o-mini"},
                "retriever": {"type": "embedding", "model_name": "all-MiniLM-L6-v2"},
            }
        ),
        encoding="utf-8",
    )

    build_ret_spy = MagicMock(return_value=_RecordingRetriever())
    build_table_spy = MagicMock(return_value={})
    monkeypatch.setattr(ve_main.utils, "build_llm", lambda _cfg: _EmptyOrPartialLLM({}))
    monkeypatch.setattr(ve_main.utils, "build_retriever", build_ret_spy)
    monkeypatch.setattr(ve_main, "build_table_jsonl", build_table_spy)

    monkeypatch.setattr(ve_main, "DISABLE_RETRIEVER", True)
    ve_main.main(cfg_path)
    build_ret_spy.assert_not_called()
    assert build_table_spy.call_args.args[4] is None

    build_ret_spy.reset_mock()
    build_table_spy.reset_mock()
    monkeypatch.setattr(ve_main, "DISABLE_RETRIEVER", False)
    ve_main.main(cfg_path)
    build_ret_spy.assert_called_once()
    assert build_table_spy.call_args.args[4] is build_ret_spy.return_value


def test_embedding_retriever_respects_overlap_words_and_build_retriever_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeST:
        device = "cpu"

        def encode(self, texts, **_kwargs):
            return np.ones((len(texts), 4), dtype=np.float32)

    monkeypatch.setattr(embedding_model, "load_sentence_transformer", lambda *_a, **_kw: _FakeST())

    # Long single-sentence text (> 128 words, since max_words_chunk = max(int(max_words/k), 128) >= 128)
    long_doc = " ".join(f"word{i}" for i in range(400))

    r_no_overlap = core_utils.build_retriever(
        {
            "type": "embedding",
            "model_name": "custom-model",
            "passage_chars": 256,
            "overlap": 0,
            "k": 2,
            "enable_dynamic_k": True,
            "dynamic_k_threshold": 0.7,
            "dynamic_k_minimum": 1,
        }
    )
    assert isinstance(r_no_overlap, EmbeddingRetriever)
    assert r_no_overlap.model_name == "custom-model"
    assert r_no_overlap.max_words == 256
    assert r_no_overlap.overlap_words == 0
    assert r_no_overlap.k == 2
    assert r_no_overlap.enable_dynamic_k is True
    assert r_no_overlap.dynamic_k_threshold == 0.7
    assert r_no_overlap.dynamic_k_minimum == 1

    r_with_overlap = core_utils.build_retriever(
        {
            "type": "embedding",
            "model_name": "custom-model",
            "passage_chars": 256,
            "overlap": 96,
            "k": 2,
        }
    )
    assert r_with_overlap.overlap_words == 96

    r_no_overlap.preprocessor = None
    r_with_overlap.preprocessor = None

    captured: Dict[str, List[int]] = {"no_overlap": [], "with_overlap": []}
    orig_chunk = EmbeddingRetriever._improved_chunk

    def _spy_chunk_no(self, doc, **kw):
        res = orig_chunk(self, doc, **kw)
        captured["no_overlap"].append(len(res))
        return res

    def _spy_chunk_yes(self, doc, **kw):
        res = orig_chunk(self, doc, **kw)
        captured["with_overlap"].append(len(res))
        return res

    monkeypatch.setattr(r_no_overlap, "_improved_chunk", _spy_chunk_no.__get__(r_no_overlap))
    monkeypatch.setattr(r_with_overlap, "_improved_chunk", _spy_chunk_yes.__get__(r_with_overlap))

    r_no_overlap.query([long_doc], "query")
    r_with_overlap.query([long_doc], "query")

    # 400 words in 128-word windows: step=128 -> 4 chunks; step=128-96=32 -> 13 chunks
    assert captured["no_overlap"] == [4]
    assert captured["with_overlap"] == [13]


def test_embedding_retriever_reuses_injected_model_without_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_spy = MagicMock(side_effect=AssertionError("load_sentence_transformer must not run"))
    monkeypatch.setattr(embedding_model, "load_sentence_transformer", load_spy)

    shared_model = SimpleNamespace(device="cpu")
    retriever = EmbeddingRetriever(
        model_name="all-MiniLM-L6-v2",
        k=7,
        max_words=640,
        overlap_words=32,
        enable_preprocessing=False,
        model=shared_model,
    )
    assert retriever.model is shared_model
    assert retriever.is_cross_encoder is False
    assert retriever.k == 7
    load_spy.assert_not_called()
