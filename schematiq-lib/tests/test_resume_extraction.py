"""Resuming an interrupted value extraction, and riding out network outages."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from schematiq.core import network_resilience
from schematiq.core.schema import Column, ObservationUnit, Schema
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.core.table_builder import TableBuilder


@pytest.fixture
def setup(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    for name in ("a", "b", "c"):
        (docs / f"{name}.txt").write_text(f"text of {name}", encoding="utf-8")
    schema = Schema(
        query="q",
        columns=[Column(name="value", definition="d")],
        observation_unit=ObservationUnit(name="Thing", definition="a thing"),
    )
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema.to_full_dict()), encoding="utf-8")
    return docs, schema_path, tmp_path / "out.jsonl"


def _builder(extract):
    builder = TableBuilder(MagicMock())
    builder.paper_processor.extract_values_for_paper_with_units = extract
    return builder


def _rows_for(title):
    return SimpleNamespace(rows=[{"_unit_name": f"{title}-unit", "value": title}], skip_reason=None)


def _run(builder, docs, schema_path, out, resume):
    builder.build_table_jsonl_multi_dirs(schema_path, [docs], out, resume=resume, max_workers=1)


def _out_rows(out):
    return [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture
def instant_recovery(monkeypatch):
    monkeypatch.setattr(network_resilience, "TRANSIENT_RETRY_PAUSE_SECONDS", 0)
    monkeypatch.setattr(network_resilience, "network_available", lambda timeout=5.0: True)


def test_resume_redoes_only_failed_documents(setup):
    docs, schema_path, out = setup
    calls = []

    def flaky(paper_title, **kwargs):
        calls.append(paper_title)
        if paper_title == "b":
            raise ValueError("model returned garbage")
        return _rows_for(paper_title)

    _run(_builder(flaky), docs, schema_path, out, resume=False)
    assert sorted(r["value"] for r in _out_rows(out)) == ["a", "c"]

    calls.clear()
    _run(_builder(lambda paper_title, **kw: calls.append(paper_title) or _rows_for(paper_title)),
         docs, schema_path, out, resume=True)
    assert calls == ["b"]
    assert sorted(r["value"] for r in _out_rows(out)) == ["a", "b", "c"]


def test_resume_drops_rows_of_unfinished_document(setup):
    docs, schema_path, out = setup
    _run(_builder(lambda paper_title, **kw: _rows_for(paper_title)), docs, schema_path, out, resume=False)
    # Simulate a crash after b's rows were written but before b was marked done.
    progress = out.with_name("out_progress.jsonl")
    lines = [l for l in progress.read_text(encoding="utf-8").splitlines() if '"b.txt"' not in l]
    progress.write_text("\n".join(lines) + "\n", encoding="utf-8")

    calls = []
    _run(_builder(lambda paper_title, **kw: calls.append(paper_title) or _rows_for(paper_title)),
         docs, schema_path, out, resume=True)
    assert calls == ["b"]
    values = [r["value"] for r in _out_rows(out)]
    assert sorted(values) == ["a", "b", "c"]  # no duplicate b row


def test_fresh_run_ignores_old_progress(setup):
    docs, schema_path, out = setup
    _run(_builder(lambda paper_title, **kw: _rows_for(paper_title)), docs, schema_path, out, resume=False)
    out.unlink()
    calls = []
    _run(_builder(lambda paper_title, **kw: calls.append(paper_title) or _rows_for(paper_title)),
         docs, schema_path, out, resume=False)
    assert calls == ["a", "b", "c"]


def test_skip_records_survive_resume(setup):
    docs, schema_path, out = setup

    def none_in_a(paper_title, **kw):
        if paper_title == "a":
            return SimpleNamespace(rows=[], skip_reason="no units here")
        if paper_title == "c":
            raise ValueError("boom")
        return _rows_for(paper_title)

    _run(_builder(none_in_a), docs, schema_path, out, resume=False)
    builder = _builder(lambda paper_title, **kw: _rows_for(paper_title))
    _run(builder, docs, schema_path, out, resume=True)
    skipped = {d["document"]: d["reason"] for d in builder.get_skipped_documents_with_reasons()}
    assert skipped == {"a": "no units here"}


def test_network_outage_retries_document_instead_of_skipping(setup, instant_recovery):
    docs, schema_path, out = setup
    attempts = {"b": 0}

    def outage_once(paper_title, **kw):
        if paper_title == "b":
            attempts["b"] += 1
            if attempts["b"] == 1:
                raise httpx.ConnectError("getaddrinfo failed")
        return _rows_for(paper_title)

    builder = _builder(outage_once)
    _run(builder, docs, schema_path, out, resume=False)
    assert attempts["b"] == 2
    assert sorted(r["value"] for r in _out_rows(out)) == ["a", "b", "c"]
    assert builder.get_skipped_documents_with_reasons() == []


def test_permanent_error_is_not_retried(setup, instant_recovery):
    docs, schema_path, out = setup
    attempts = {"b": 0}

    def broken(paper_title, **kw):
        if paper_title == "b":
            attempts["b"] += 1
            raise ValueError("bad output")
        return _rows_for(paper_title)

    _run(_builder(broken), docs, schema_path, out, resume=False)
    assert attempts["b"] == 1


@pytest.mark.parametrize(
    "error, expected",
    [
        (httpx.ConnectError("no route"), True),
        (httpx.ReadTimeout("slow"), True),
        (RuntimeError("Value extraction failed: [Errno 11001] getaddrinfo failed"), True),
        (ValueError("could not parse JSON"), False),
        (Exception("API key not valid. Please pass a valid API key."), False),
    ],
)
def test_is_transient_error(error, expected):
    assert network_resilience.is_transient_error(error) is expected


def test_transient_cause_chain_is_detected():
    try:
        try:
            raise httpx.ConnectError("down")
        except httpx.ConnectError as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert network_resilience.is_transient_error(outer)


@pytest.mark.parametrize(
    "error",
    [Exception("API key not valid"), httpx.ConnectError("getaddrinfo failed")],
    ids=["bad_key", "outage"],
)
def test_identification_passes_up_errors_that_say_nothing_about_the_document(error):
    # A skip_reason would mark the document done, so resume would never retry it.
    s = MagicMock()
    s._check_stop_requested.return_value = False
    s._attempt_unit_identification.side_effect = error

    with pytest.raises(type(error)):
        PaperProcessor.identify_observation_units(s, "paper", "text", MagicMock(), MagicMock())
    assert s._attempt_unit_identification.call_count == 1


def test_identification_still_skips_after_a_document_specific_error():
    s = MagicMock()
    s._check_stop_requested.return_value = False
    s._attempt_unit_identification.side_effect = ValueError("could not parse")

    result = PaperProcessor.identify_observation_units(s, "paper", "text", MagicMock(), MagicMock())

    assert result.units == []
    assert "could not parse" in result.skip_reason
