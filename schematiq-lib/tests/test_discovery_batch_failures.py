"""Schema discovery on a long run skips a batch whose LLM call failed, but
still stops on an invalid API key, even when a step wraps the provider error,
and fails when every batch failed."""

from unittest.mock import MagicMock

import pytest

from schematiq.core import schematiq as sq
from schematiq.core.schema import ObservationUnit, Schema

errors = pytest.importorskip("google.genai.errors")

UNIT = ObservationUnit(name="NES", definition="A nuclear export signal.")
DOCS = ["first document text", "second document text", "third document text"]
NAMES = ["a.txt", "b.txt", "c.txt"]


def api_error(code, status, message):
    with pytest.raises(errors.APIError) as excinfo:
        errors.APIError.raise_error(code, {"error": {"code": code, "status": status, "message": message}}, None)
    return excinfo.value


def _discover(**kwargs):
    return sq.discover_schema(
        query="nuclear export signals", documents=DOCS, filenames=NAMES,
        max_keys_schema=20, llm=MagicMock(), retriever=None,
        documents_batch_size=1, context_window_size=100_000, max_iters=3, **kwargs,
    )


def test_a_failed_batch_is_skipped(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        if len(calls) == 1:
            raise RuntimeError("timed out")
        return Schema(query=query, max_keys=20), False, None, None

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    schema, _, non_contributing, _ = _discover(initial_observation_unit=UNIT)

    assert len(calls) >= 2
    assert "a.txt" in non_contributing
    assert schema.observation_unit == UNIT


def test_every_batch_failing_raises_the_last_error(monkeypatch):
    def generate_schema(*args, **kwargs):
        raise RuntimeError("provider down")

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    with pytest.raises(RuntimeError, match="provider down"):
        _discover(initial_observation_unit=UNIT)


def test_invalid_api_key_stops_discovery(monkeypatch):
    generate_schema = MagicMock(side_effect=api_error(400, "INVALID_ARGUMENT", "API key not valid"))
    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    with pytest.raises(errors.APIError):
        _discover(initial_observation_unit=UNIT)
    assert generate_schema.call_count == 1


def test_wrapped_invalid_api_key_stops_discovery(monkeypatch):
    # _discover_observation_unit raises ObservationUnitDiscoveryError from the
    # provider error; the key error underneath must still stop the run.
    llm = MagicMock()
    llm.generate.side_effect = api_error(401, "UNAUTHENTICATED", "Request is missing required authentication")
    generate_schema = MagicMock()
    monkeypatch.setattr(sq, "generate_schema", generate_schema)

    with pytest.raises(sq.ObservationUnitDiscoveryError):
        sq.discover_schema(
            query="nuclear export signals", documents=DOCS, filenames=NAMES,
            max_keys_schema=20, llm=llm, retriever=None,
            documents_batch_size=1, context_window_size=100_000, max_iters=3,
        )
    assert llm.generate.call_count == 1
    generate_schema.assert_not_called()
