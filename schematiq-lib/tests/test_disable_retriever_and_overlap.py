"""Unit tests for DISABLE_RETRIEVER behavior (#663) and EmbeddingRetriever overlap (#661)."""

from __future__ import annotations

import hashlib
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

    # Explicit passage_words sets max_words_chunk directly (no division by k, no 128 floor)
    long_doc = " ".join(f"word{i}" for i in range(400))

    r_no_overlap = core_utils.build_retriever(
        {
            "type": "embedding",
            "model_name": "custom-model",
            "passage_words": 128,
            "overlap": 0,
            "k": 2,
            "enable_dynamic_k": True,
            "dynamic_k_threshold": 0.7,
            "dynamic_k_minimum": 1,
        }
    )
    assert isinstance(r_no_overlap, EmbeddingRetriever)
    assert r_no_overlap.model_name == "custom-model"
    assert r_no_overlap.passage_words == 128
    assert r_no_overlap.max_words_chunk == 128
    assert r_no_overlap.overlap_words == 0
    assert r_no_overlap.k == 2
    assert r_no_overlap.enable_dynamic_k is True
    assert r_no_overlap.dynamic_k_threshold == 0.7
    assert r_no_overlap.dynamic_k_minimum == 1

    # Values below 128 (e.g. 64) and large values (e.g. 256 with k=10) are honored directly
    r_small = core_utils.build_retriever(
        {"type": "embedding", "model_name": "custom-model", "passage_words": 64, "overlap": 0, "k": 10}
    )
    assert r_small.passage_words == 64
    assert r_small.max_words_chunk == 64

    r_direct = core_utils.build_retriever(
        {"type": "embedding", "model_name": "custom-model", "passage_words": 256, "k": 10}
    )
    assert r_direct.passage_words == 256
    assert r_direct.max_words_chunk == 256

    # Legacy max_words / passage_chars convert using max(int(value / k), 128)
    r_legacy_budget = core_utils.build_retriever(
        {"type": "embedding", "model_name": "custom-model", "max_words": 600, "k": 3}
    )
    assert r_legacy_budget.passage_words == 200
    assert r_legacy_budget.max_words_chunk == 200

    r_legacy_chars = core_utils.build_retriever(
        {"type": "embedding", "model_name": "custom-model", "passage_chars": 512, "k": 15}
    )
    assert r_legacy_chars.passage_words == 128
    assert r_legacy_chars.max_words_chunk == 128

    r_with_overlap = core_utils.build_retriever(
        {
            "type": "embedding",
            "model_name": "custom-model",
            "passage_words": 128,
            "overlap": 96,
            "k": 2,
        }
    )
    assert r_with_overlap.overlap_words == 96

    r_no_overlap.preprocessor = None
    r_with_overlap.preprocessor = None
    r_small.preprocessor = None

    captured: Dict[str, List[int]] = {"no_overlap": [], "with_overlap": [], "small": []}
    orig_chunk = EmbeddingRetriever._improved_chunk

    def _spy(bucket: str):
        def _fn(self, doc, **kw):
            res = orig_chunk(self, doc, **kw)
            captured[bucket].append(len(res))
            return res
        return _fn

    monkeypatch.setattr(r_no_overlap, "_improved_chunk", _spy("no_overlap").__get__(r_no_overlap))
    monkeypatch.setattr(r_with_overlap, "_improved_chunk", _spy("with_overlap").__get__(r_with_overlap))
    monkeypatch.setattr(r_small, "_improved_chunk", _spy("small").__get__(r_small))

    r_no_overlap.query([long_doc], "query")
    r_with_overlap.query([long_doc], "query")
    r_small.query([long_doc], "query")

    # 400 words in 128-word windows: step=128 -> 4 chunks; step=128-96=32 -> 13 chunks
    # 400 words in 64-word windows: step=64 -> 7 chunks
    assert captured["no_overlap"] == [4]
    assert captured["with_overlap"] == [13]
    assert captured["small"] == [7]


def test_embedding_retriever_reuses_injected_model_without_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_spy = MagicMock(side_effect=AssertionError("load_sentence_transformer must not run"))
    monkeypatch.setattr(embedding_model, "load_sentence_transformer", load_spy)

    shared_model = SimpleNamespace(device="cpu")
    retriever = EmbeddingRetriever(
        model_name="all-MiniLM-L6-v2",
        k=7,
        passage_words=200,
        overlap_words=32,
        enable_preprocessing=False,
        model=shared_model,
    )
    assert retriever.model is shared_model
    assert retriever.is_cross_encoder is False
    assert retriever.k == 7
    assert retriever.passage_words == 200
    assert retriever.max_words_chunk == 200
    load_spy.assert_not_called()


def test_default_and_legacy_retriever_equivalence_with_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default passage_words=128 and legacy max(budget/k, 128) match origin/main passages."""

    class _DeterministicFakeST:
        device = "cpu"

        def encode(self, texts, **_kwargs):
            vecs = []
            for t in texts:
                digest = hashlib.sha256(t.encode("utf-8")).digest()
                arr = np.frombuffer(digest, dtype=np.uint8).astype(np.float32)[:16] - 128.0
                arr = arr / (np.linalg.norm(arr) + 1e-8)
                vecs.append(arr)
            return np.stack(vecs, axis=0)

    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: _DeterministicFakeST()
    )

    fixed_doc = "\n\n".join(
        [
            "# Introduction\n"
            + " ".join(
                f"intro_word_{i} clinical study cohort baseline." if i % 12 == 0 else f"intro_{i}"
                for i in range(180)
            ),
            "# Methods\n"
            + " ".join(
                f"method_word_{i} randomized trial dosage protocol." if i % 10 == 0 else f"method_{i}"
                for i in range(190)
            ),
            "# Results\n"
            + " ".join(
                f"result_word_{i} primary endpoint p_value outcome." if i % 11 == 0 else f"result_{i}"
                for i in range(185)
            ),
        ]
    )
    question = "What was the primary endpoint outcome in the randomized trial?"

    # 1. Default build_retriever({}) -> k=15, passage_words=128 matches main (max_words=512, k=15 -> 128)
    r_default = core_utils.build_retriever({})
    assert r_default.passage_words == 128
    assert r_default.max_words_chunk == 128
    default_passages = r_default.query([fixed_doc], question)
    assert len(default_passages) == 13
    assert (
        hashlib.sha256(json.dumps(default_passages).encode("utf-8")).hexdigest()
        == "8ae0a716dd64f7d88dc10c0a70935165a0866a84a837746e2c03e01cb096b8ca"
    )

    # 2. Legacy max_words=600 / passage_chars=600 with k=3 converts to 200 words/passage, matching main
    r_legacy_mw = core_utils.build_retriever(
        {"max_words": 600, "k": 3, "enable_dynamic_k": False}
    )
    r_legacy_pc = core_utils.build_retriever(
        {"passage_chars": 600, "k": 3, "enable_dynamic_k": False}
    )
    r_direct_200 = core_utils.build_retriever(
        {"passage_words": 200, "k": 3, "enable_dynamic_k": False}
    )
    legacy_mw_passages = r_legacy_mw.query([fixed_doc], question)
    legacy_pc_passages = r_legacy_pc.query([fixed_doc], question)
    direct_passages = r_direct_200.query([fixed_doc], question)
    assert legacy_mw_passages == legacy_pc_passages == direct_passages
    assert [len(p.split()) for p in legacy_mw_passages] == [51, 199, 53]
    assert (
        hashlib.sha256(json.dumps(legacy_mw_passages).encode("utf-8")).hexdigest()
        == "db58df69f754cc21f7836ce6a00ae6dc6b7d16d5e589f403cf94fc7e123f204a"
    )


def test_embedding_retriever_default_constructor_matches_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-arg EmbeddingRetriever() (k=3) and custom k with no size args match main's 512/k."""

    class _DeterministicFakeST:
        device = "cpu"

        def encode(self, texts, **_kwargs):
            vecs = []
            for t in texts:
                digest = hashlib.sha256(t.encode("utf-8")).digest()
                arr = np.frombuffer(digest, dtype=np.uint8).astype(np.float32)[:16] - 128.0
                arr = arr / (np.linalg.norm(arr) + 1e-8)
                vecs.append(arr)
            return np.stack(vecs, axis=0)

    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: _DeterministicFakeST()
    )

    fixed_doc = "\n\n".join(
        [
            "# Introduction\n"
            + " ".join(
                f"intro_word_{i} clinical study cohort baseline." if i % 12 == 0 else f"intro_{i}"
                for i in range(180)
            ),
            "# Methods\n"
            + " ".join(
                f"method_word_{i} randomized trial dosage protocol." if i % 10 == 0 else f"method_{i}"
                for i in range(190)
            ),
            "# Results\n"
            + " ".join(
                f"result_word_{i} primary endpoint p_value outcome." if i % 11 == 0 else f"result_{i}"
                for i in range(185)
            ),
        ]
    )
    question = "What was the primary endpoint outcome in the randomized trial?"

    # 1. No-argument constructor: k=3, default budget=512 -> max(int(512/3), 128) = 170 words
    r_noarg = EmbeddingRetriever()
    assert r_noarg.k == 3
    assert r_noarg.max_words == 512
    assert r_noarg.passage_words == 170
    assert r_noarg.max_words_chunk == 170
    noarg_passages = r_noarg.query([fixed_doc], question)
    assert [len(p.split()) for p in noarg_passages] == [75, 55, 1]
    assert (
        hashlib.sha256(json.dumps(noarg_passages).encode("utf-8")).hexdigest()
        == "b0bc05c449604273b3860e17df33787c4286402e0a28437b55396643f49f5ef8"
    )

    # 2. Another k (k=2) with neither passage_words nor max_words -> max(int(512/2), 128) = 256 words
    r_k2 = EmbeddingRetriever(k=2)
    assert r_k2.passage_words == 256
    assert r_k2.max_words_chunk == 256
    k2_passages = r_k2.query([fixed_doc], question)
    assert [len(p.split()) for p in k2_passages] == [255, 242]
    assert (
        hashlib.sha256(json.dumps(k2_passages).encode("utf-8")).hexdigest()
        == "ae02f6137e97e8fa465602ecaa957c1c6e922778f2525959be2b4d113816ac3f"
    )

    # 3. Large k (k=10) with neither size arg floors at 128 words
    r_k10 = EmbeddingRetriever(k=10)
    assert r_k10.passage_words == 128
    assert r_k10.max_words_chunk == 128
