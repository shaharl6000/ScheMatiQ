"""A run must stop, not skip document after document, when every LLM call
would fail the same way: an invalid API key, an exhausted quota, or a
provider that keeps failing (consecutive-failure breaker)."""

from unittest.mock import MagicMock

import pytest

from schematiq.core import llm_backends as lb
from schematiq.core import schematiq as sq
from schematiq.core.schema import ObservationUnit, Schema
from schematiq.value_extraction.core.paper_processor import _counted_llm_call
from test_parallel_unit_extraction import (
    FakeProcessor, _builder, _read, _run_parallel, _setup,
)

errors = pytest.importorskip("google.genai.errors")


def api_error(code, status, message):
    with pytest.raises(errors.APIError) as excinfo:
        errors.APIError.raise_error(code, {"error": {"code": code, "status": status, "message": message}}, None)
    return excinfo.value


def bad_key():
    return api_error(401, "UNAUTHENTICATED", "API key not valid")


def daily_quota():
    return api_error(
        429, "RESOURCE_EXHAUSTED",
        "You exceeded your current quota. quotaId: GenerateRequestsPerDayPerProjectPerModel-FreeTier",
    )


def outage():
    return api_error(503, "UNAVAILABLE", "The model is overloaded")


def minute_quota():
    return api_error(
        429, "RESOURCE_EXHAUSTED",
        "You exceeded your current quota. quotaId: GenerateRequestsPerMinutePerProjectPerModel",
    )


# ── classification ──────────────────────────────────────────────────────

def test_daily_quota_ends_the_run_but_a_per_minute_limit_does_not():
    assert lb._is_quota_exhausted_error(daily_quota())
    assert lb.should_abort_run(daily_quota())
    assert not lb._is_quota_exhausted_error(minute_quota())
    assert not lb.should_abort_run(minute_quota())


def test_wrapped_errors_are_followed_through_the_cause_chain():
    for inner in (bad_key(), daily_quota(), lb.LLMRunAborted("stop")):
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("step failed") from e
        except RuntimeError as wrapped:
            assert lb.should_abort_run(wrapped)


def test_ordinary_failures_do_not_end_the_run():
    assert not lb.should_abort_run(RuntimeError("timed out"))
    assert not lb.should_abort_run(api_error(503, "UNAVAILABLE", "overloaded"))
    assert not lb.should_abort_run(api_error(403, "PERMISSION_DENIED", "CachedContent not found"))


# ── breaker ─────────────────────────────────────────────────────────────

def _failing(error):
    def call(*args, **kwargs):
        raise error
    return call


def test_breaker_trips_after_the_limit_and_stays_tripped():
    breaker = lb.ConsecutiveFailureBreaker(limit=3)
    for _ in range(2):
        with pytest.raises(errors.APIError) as excinfo:
            _counted_llm_call(breaker, _failing(outage()))
        assert not isinstance(excinfo.value, lb.LLMRunAborted)
    with pytest.raises(lb.LLMRunAborted):
        _counted_llm_call(breaker, _failing(outage()))

    # Tripped: the next call is not even made.
    call = MagicMock(return_value="ok")
    with pytest.raises(lb.LLMRunAborted):
        _counted_llm_call(breaker, call)
    call.assert_not_called()


def test_a_success_resets_the_count():
    breaker = lb.ConsecutiveFailureBreaker(limit=2)
    with pytest.raises(errors.APIError):
        _counted_llm_call(breaker, _failing(outage()))
    assert _counted_llm_call(breaker, lambda: "ok") == "ok"
    with pytest.raises(errors.APIError) as excinfo:
        _counted_llm_call(breaker, _failing(outage()))
    assert not isinstance(excinfo.value, lb.LLMRunAborted)


def test_a_non_outage_error_does_not_count():
    # A 400 for one bad document means the provider is up.
    breaker = lb.ConsecutiveFailureBreaker(limit=2)
    bad_request = api_error(400, "INVALID_ARGUMENT", "Request payload size exceeds the limit")
    for _ in range(5):
        with pytest.raises(errors.APIError):
            _counted_llm_call(breaker, _failing(bad_request))
    assert _counted_llm_call(breaker, lambda: "ok") == "ok"


def test_expired_cache_does_not_count():
    breaker = lb.ConsecutiveFailureBreaker(limit=1)
    missing = api_error(403, "PERMISSION_DENIED", "CachedContent not found")
    with pytest.raises(errors.APIError):
        _counted_llm_call(breaker, _failing(missing))
    assert _counted_llm_call(breaker, lambda: "ok") == "ok"


# ── value extraction ────────────────────────────────────────────────────

class _AlwaysFails(FakeProcessor):
    def __init__(self, error):
        super().__init__()
        self.error = error
        self.calls = []

    def extract_values_for_paper_with_units(self, paper_title, paper_text, **kwargs):
        self.calls.append(paper_title)
        raise self.error


@pytest.mark.parametrize("make_error", [bad_key, daily_quota, lambda: lb.LLMRunAborted("stop")])
def test_fatal_error_stops_the_sequential_path(tmp_path, make_error):
    _, schema, rows, d2s = _setup(tmp_path, 4)
    processor = _AlwaysFails(make_error())
    tb = _builder(processor)
    out = tmp_path / "out.jsonl"
    with pytest.raises(Exception) as excinfo:
        for row_name, papers in rows:
            tb._process_papers_with_observation_units(
                row_name, papers, d2s, schema, 8, 100, {}, set(), set(), out,
            )
    assert lb.should_abort_run(excinfo.value)
    assert processor.calls == ["doc00"]


@pytest.mark.parametrize("make_error", [bad_key, daily_quota])
def test_fatal_error_stops_the_parallel_path(tmp_path, make_error):
    _, schema, rows, d2s = _setup(tmp_path, 6)
    processor = _AlwaysFails(make_error())
    tb = _builder(processor)
    out = tmp_path / "out.jsonl"
    with pytest.raises(Exception) as excinfo:
        _run_parallel(tb, rows, schema, d2s, out, workers=2)
    assert lb.should_abort_run(excinfo.value)
    # Documents not yet started are dropped, not each run to failure.
    assert len(processor.calls) < 6


def test_ordinary_document_failure_is_still_skipped(tmp_path):
    _, schema, rows, d2s = _setup(tmp_path, 3)
    processor = _AlwaysFails(RuntimeError("boom"))
    tb = _builder(processor)
    out = tmp_path / "out.jsonl"
    for row_name, papers in rows:
        tb._process_papers_with_observation_units(
            row_name, papers, d2s, schema, 8, 100, {}, set(), set(), out,
        )
    assert not out.exists() or _read(out) == []
    assert len(tb._skipped_documents) == 3
    assert len(processor.calls) == 3


# ── schema discovery ────────────────────────────────────────────────────

UNIT = ObservationUnit(name="NES", definition="A nuclear export signal.")


def _discover(n_docs):
    docs = [f"document {i} text" for i in range(n_docs)]
    names = [f"d{i}.txt" for i in range(n_docs)]
    return sq.discover_schema(
        query="nuclear export signals", documents=docs, filenames=names,
        max_keys_schema=20, llm=MagicMock(), retriever=None,
        documents_batch_size=1, context_window_size=100_000, max_iters=n_docs,
        initial_observation_unit=UNIT,
    )


def test_discovery_stops_after_consecutive_failed_batches(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        if len(calls) == 1:
            return Schema(query=query, max_keys=20), False, None, None
        raise outage()

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    with pytest.raises(lb.LLMRunAborted):
        _discover(10)
    assert len(calls) == 1 + sq._MAX_CONSECUTIVE_FAILED_BATCHES


def test_discovery_keeps_going_when_failures_are_not_consecutive(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        if len(calls) % 2 == 0:
            raise outage()
        return Schema(query=query, max_keys=20), False, None, None

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    _discover(8)
    assert len(calls) == 8


def test_discovery_does_not_stop_on_non_outage_failures(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        if len(calls) == 1:
            return Schema(query=query, max_keys=20), False, None, None
        raise RuntimeError("could not parse the schema")

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    _discover(6)
    assert len(calls) == 6


def test_discovery_stops_on_daily_quota(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        raise daily_quota()

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    with pytest.raises(errors.APIError):
        _discover(5)
    assert len(calls) == 1
