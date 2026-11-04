"""Parallel document extraction in the observation-unit path.

Documents are extracted on worker threads, but rows are written and callbacks
fired on the calling thread, in the original document order, so the output
matches the sequential path.
"""

import json
import random
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from schematiq.core.results import ExtractionResult
from schematiq.value_extraction.core.table_builder import TableBuilder


class FakeProcessor:
    def __init__(self, delays=None):
        self.delays = delays or {}
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def extract_values_for_paper_with_units(self, paper_title, paper_text, **kwargs):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(self.delays.get(paper_title, 0.01))
        with self.lock:
            self.active -= 1
        return ExtractionResult(rows=[
            {"_unit_name": f"{paper_title}-u1", "col": paper_text.strip()},
            {"_unit_name": f"{paper_title}-u2", "col": paper_text.strip()},
        ])


def _builder(processor, should_stop=None):
    tb = object.__new__(TableBuilder)
    tb.paper_processor = processor
    tb.should_stop = should_stop
    tb._skipped_documents = []
    tb._stopped = False
    tb.events = []
    main = threading.current_thread()

    def record(kind):
        def cb(*args):
            tb.events.append((kind, args[0] if kind != "row" else args[0]["_row_name"],
                              threading.current_thread() is main))
        return cb

    tb.on_document_started = record("start")
    tb.on_unit_row_written = record("row")
    tb.on_value_extracted = None
    return tb


def _setup(tmp_path, n):
    docs = []
    for i in range(n):
        p = tmp_path / f"doc{i:02d}.txt"
        p.write_text(f"text {i}")
        docs.append(p)
    schema = SimpleNamespace(query="q", observation_unit=SimpleNamespace(name="Unit"))
    rows = [(p.stem, [p]) for p in docs]
    return docs, schema, rows, {p: tmp_path for p in docs}


def _read(path):
    return [json.loads(line)["_row_name"] for line in path.read_text().splitlines()]


def _run_parallel(tb, rows, schema, doc_to_source, out, workers=3):
    processed, written = set(), set()
    tb._process_rows_with_units_parallel(
        rows, doc_to_source, schema, 8, 100, workers, processed, written, set(), out,
    )
    return processed, written


def test_output_order_matches_sequential(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 8)
    rng = random.Random(0)
    delays = {p.stem: rng.uniform(0.0, 0.05) for p in docs}

    seq_out = tmp_path / "seq.jsonl"
    seq = _builder(FakeProcessor(delays))
    seq_processed, seq_written = set(), set()
    for row_name, papers in rows:
        seq._process_papers_with_observation_units(
            row_name, papers, d2s, schema, 8, 100, {}, seq_processed, seq_written, seq_out,
        )

    par_out = tmp_path / "par.jsonl"
    par = _builder(FakeProcessor(delays))
    par_processed, par_written = _run_parallel(par, rows, schema, d2s, par_out)

    assert _read(par_out) == _read(seq_out)
    assert par_written == seq_written
    assert par_processed == seq_processed == set(docs)


def test_documents_really_run_in_parallel(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 6)
    proc = FakeProcessor({p.stem: 0.05 for p in docs})
    tb = _builder(proc)
    _run_parallel(tb, rows, schema, d2s, tmp_path / "out.jsonl", workers=3)
    assert proc.max_active == 3


def test_callbacks_and_writes_stay_on_calling_thread(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 6)
    tb = _builder(FakeProcessor({p.stem: 0.02 for p in docs}))
    _run_parallel(tb, rows, schema, d2s, tmp_path / "out.jsonl")
    assert tb.events and all(on_main for _, _, on_main in tb.events)
    row_events = [name for kind, name, _ in tb.events if kind == "row"]
    assert row_events == [f"doc{i:02d}-u{k}" for i in range(6) for k in (1, 2)]


def test_slow_first_document_does_not_reorder_output(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 4)
    tb = _builder(FakeProcessor({"doc00": 0.15}))
    out = tmp_path / "out.jsonl"
    _run_parallel(tb, rows, schema, d2s, out)
    assert _read(out)[:2] == ["doc00-u1", "doc00-u2"]


def test_stop_request_stops_writing(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 10)
    calls = {"n": 0}

    def should_stop():
        calls["n"] += 1
        return calls["n"] > 3

    tb = _builder(FakeProcessor({p.stem: 0.02 for p in docs}), should_stop=should_stop)
    out = tmp_path / "out.jsonl"
    processed, _ = _run_parallel(tb, rows, schema, d2s, out)
    assert tb._stopped
    assert len(processed) < len(docs)
    written = _read(out) if out.exists() else []
    assert written == [f"doc{i:02d}-u{k}" for i in range(len(written) // 2) for k in (1, 2)]


def test_error_in_one_document_does_not_stop_others(tmp_path):
    docs, schema, rows, d2s = _setup(tmp_path, 4)

    class Failing(FakeProcessor):
        def extract_values_for_paper_with_units(self, paper_title, paper_text, **kwargs):
            if paper_title == "doc01":
                raise RuntimeError("boom")
            return super().extract_values_for_paper_with_units(paper_title, paper_text, **kwargs)

    tb = _builder(Failing())
    out = tmp_path / "out.jsonl"
    processed, _ = _run_parallel(tb, rows, schema, d2s, out)
    assert [r for r in _read(out) if r.startswith("doc01")] == []
    assert Path(tmp_path / "doc01.txt") not in processed
    assert len(_read(out)) == 6
