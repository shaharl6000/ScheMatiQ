"""A bad API key does not stop schema discovery: it costs the batch, like any
other failure, and the run only fails at the end if no batch succeeded."""

from unittest.mock import MagicMock

import pytest

from schematiq.core import schematiq as sq
from schematiq.core.schema import ObservationUnit, Schema

UNIT = ObservationUnit(name="NES", definition="A nuclear export signal.")
DOCS = ["first document text", "second document text", "third document text"]
NAMES = ["a.txt", "b.txt", "c.txt"]
BAD_KEY = "API key not valid. Please pass a valid API key."


def _discover():
    return sq.discover_schema(
        query="nuclear export signals", documents=DOCS, filenames=NAMES,
        max_keys_schema=20, llm=MagicMock(), retriever=None,
        documents_batch_size=1, context_window_size=100_000, max_iters=3,
        initial_observation_unit=UNIT,
    )


def test_bad_key_on_one_batch_costs_only_that_batch(monkeypatch):
    calls = []

    def generate_schema(passages, query, *args, **kwargs):
        calls.append(passages)
        if len(calls) == 1:
            raise Exception(BAD_KEY)
        return Schema(query=query, max_keys=20), False, None, None

    monkeypatch.setattr(sq, "generate_schema", generate_schema)
    schema, _, non_contributing, _ = _discover()

    assert len(calls) >= 2
    assert "a.txt" in non_contributing
    assert schema.observation_unit == UNIT


def test_bad_key_on_every_batch_fails_only_after_trying_them_all(monkeypatch):
    generate_schema = MagicMock(side_effect=Exception(BAD_KEY))
    monkeypatch.setattr(sq, "generate_schema", generate_schema)

    with pytest.raises(Exception, match="API key not valid"):
        _discover()
    assert generate_schema.call_count == len(DOCS)
