"""Tests for parallel-stop work preservation, EmbeddingRetriever LRU cache bounds, and unit-cap warning."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np

from schematiq.core.results import ExtractionResult
from schematiq.core.retrievers import EmbeddingRetriever
from schematiq.core.schema import Column, ObservationUnit, Schema
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.core.table_builder import TableBuilder


def test_parallel_stop_commits_finished_papers_skips_partial_and_resumes_remaining(tmp_path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    for name in ("doc0", "doc1", "doc2"):
        (docs_dir / f"{name}.txt").write_text(f"body {name}", encoding="utf-8")

    schema_path = tmp_path / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "query": "q",
                "schema": [{"name": "col", "definition": "d"}],
                "observation_unit": {"name": "Unit", "definition": "u"},
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "out.jsonl"
    doc1_done = threading.Event()
    doc2_done = threading.Event()
    stop_state = {"stopped": False}
    seen_calls_run1: list[str] = []
    seen_calls_run2: list[str] = []
    lock = threading.Lock()

    class FakeStopProcessor:
        def __init__(self, calls_log, allow_stop_block=True):
            self.calls_log = calls_log
            self.allow_stop_block = allow_stop_block

        def extract_values_for_paper_with_units(self, paper_title, paper_text, **kwargs):
            with lock:
                self.calls_log.append(paper_title)
            if self.allow_stop_block and paper_title == "doc0":
                # Wait until doc1 and doc2 finish, trigger stop, then return partial stopped=True.
                assert doc1_done.wait(timeout=5.0)
                assert doc2_done.wait(timeout=5.0)
                # Give doc1/doc2 a brief moment to return their futures, then mark stopped.
                threading.Event().wait(0.05)
                stop_state["stopped"] = True
                return ExtractionResult(
                    rows=[{"_unit_name": "doc0-partial", "col": "partial"}],
                    stopped=True,
                )
            if self.allow_stop_block and paper_title == "doc1":
                doc1_done.set()
            elif self.allow_stop_block and paper_title == "doc2":
                doc2_done.set()
            return ExtractionResult(
                rows=[{"_unit_name": f"{paper_title}-u1", "col": f"val-{paper_title}"}],
                stopped=False,
            )

        def get_all_suggested_values(self):
            return {}

    tb1 = TableBuilder(llm=None, should_stop=lambda: stop_state["stopped"])
    tb1.paper_processor = FakeStopProcessor(seen_calls_run1, allow_stop_block=True)
    tb1.build_table_jsonl_multi_dirs(
        schema_path, [docs_dir], output_path, max_workers=3
    )

    assert tb1._stopped is True
    rows_after_stop = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    # doc1 and doc2 finished cleanly and are preserved in order; partial doc0 was skipped.
    assert [r["_row_name"] for r in rows_after_stop] == ["doc1-u1", "doc2-u1"]

    # Subsequent resume=True run skips doc1 and doc2 (via paper.name in processed_papers) and runs only doc0.
    stop_state["stopped"] = False
    tb2 = TableBuilder(llm=None)
    tb2.paper_processor = FakeStopProcessor(seen_calls_run2, allow_stop_block=False)
    tb2.build_table_jsonl_multi_dirs(
        schema_path, [docs_dir], output_path, resume=True, max_workers=3
    )

    assert seen_calls_run2 == ["doc0"]
    all_rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert sorted(r["_row_name"] for r in all_rows) == ["doc0-u1", "doc1-u1", "doc2-u1"]


def test_embedding_retriever_lru_count_and_byte_bounds():
    fake_model = SimpleNamespace(device="cpu")

    r = EmbeddingRetriever(
        model=fake_model,
        enable_preprocessing=False,
        max_cache_size=2,
        max_cache_bytes=10_000,
    )
    emb_small = np.zeros((1, 4), dtype=np.float32)  # 16 bytes
    r._cache_embeddings("h1", ["a"], emb_small)
    r._cache_embeddings("h2", ["b"], emb_small)
    # Touch h1 so h2 becomes LRU
    assert r._get_cached_embeddings("h1") is not None
    r._cache_embeddings("h3", ["c"], emb_small)

    assert "h1" in r._embedding_cache
    assert "h2" not in r._embedding_cache
    assert "h3" in r._embedding_cache

    # Byte-bounded eviction
    r_bytes = EmbeddingRetriever(
        model=fake_model,
        enable_preprocessing=False,
        max_cache_size=100,
        max_cache_bytes=200,
    )
    emb_120 = np.zeros((30, 1), dtype=np.float32)  # 120 bytes
    r_bytes._cache_embeddings("k1", ["x"], emb_120)
    r_bytes._cache_embeddings("k2", ["y"], emb_120)
    assert "k1" not in r_bytes._embedding_cache
    assert "k2" in r_bytes._embedding_cache
    assert r_bytes._cached_bytes <= 200

    r_bytes.clear_cache()
    assert len(r_bytes._embedding_cache) == 0
    assert r_bytes._cached_bytes == 0


def test_unit_cap_emits_warning_and_early_return_clears_figure_state(tmp_path):
    schema = Schema(
        query="q",
        columns=[Column(name="val", definition="d")],
        observation_unit=ObservationUnit(name="Unit", definition="u"),
    )

    warnings_emitted: list[tuple[str, str, str]] = []
    s = MagicMock()
    s._check_stop_requested.return_value = False
    s.on_warning = lambda doc, wtype, msg: warnings_emitted.append((doc, wtype, msg))

    def emit(paper_title, wtype, message):
        s.on_warning(paper_title, wtype, message)

    s._emit_warning.side_effect = emit
    s._deduplicate_units.side_effect = lambda u: u

    units_18 = [
        {
            "unit_name": f"Unit {i:02d}",
            "confidence": "high" if i < 15 else "medium",
            "relevant_passages": ["text"],
        }
        for i in range(18)
    ]
    s._attempt_unit_identification.return_value = SimpleNamespace(
        success=True,
        units=units_18,
        notes="",
        warnings=[],
        error=None,
    )

    ident_res = PaperProcessor.identify_observation_units(
        s, "docA", "text", schema.observation_unit, schema
    )
    assert len(ident_res.units) == 15
    assert len(warnings_emitted) == 1
    assert warnings_emitted[0][0] == "docA"
    assert warnings_emitted[0][1] == "unit_cap_reached"
    for dropped in ("Unit 15", "Unit 16", "Unit 17"):
        assert dropped in warnings_emitted[0][2]

    # Early return on known_units=[] still runs finally and clears loaded figures.
    fig_dir = tmp_path / "figs"
    fig_dir.mkdir()
    s_fig = MagicMock()
    s_fig._active_context_cache = None
    s_fig._uncached_doc_layout = None
    s_fig._active_figure_images = []
    s_fig._active_figures_by_id = {}
    s_fig._active_norm_captions_cache = None

    def fake_load(fdir):
        s_fig._active_figure_images = [("fig1", b"png", "image/png")]
        s_fig._active_figures_by_id = {"fig1": {"figure_id": "fig1"}}
        s_fig._active_norm_captions_cache = ("x", 1, [])

    s_fig._load_document_figures.side_effect = fake_load

    res = PaperProcessor.extract_values_for_paper_with_units(
        s_fig,
        "docA",
        "text",
        schema,
        100,
        known_units=[],
        figures_dir=fig_dir,
    )
    assert res.rows == []
    s_fig._delete_document_cache.assert_called_once()
    assert s_fig._active_figure_images == []
    assert s_fig._active_figures_by_id == {}
    assert s_fig._active_norm_captions_cache is None
