"""Constructing a Schema must not compute embeddings unless pruning needs them."""

import pytest

import schematiq.core.schema as schema_mod
from schematiq.core.schema import Column, Schema


def _cols(n: int):
    return [
        Column(name=f"column {chr(97 + i)}", definition="d", rationale="r" * (i + 1))
        for i in range(n)
    ]


def test_constructing_schema_does_not_embed(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail(_text: str):
        raise AssertionError("_embed must not be called when constructing a Schema")

    monkeypatch.setattr(schema_mod, "_embed", _fail)

    schema = Schema(query="What do courts decide?", columns=_cols(3))
    assert [c.name for c in schema.columns] == ["column a", "column b", "column c"]

    # Within the key budget: still no embedding.
    Schema(query="What do courts decide?", columns=_cols(3), max_keys=5)


def test_prune_still_embeds_query_when_cutting(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def _fake_embed(text: str):
        calls.append(text)
        return [1.0, 0.0]

    monkeypatch.setattr(schema_mod, "_embed", _fake_embed)

    schema = Schema(query="What do courts decide?", columns=_cols(4), max_keys=2)
    assert len(schema.columns) == 2
    assert calls[0] == "What do courts decide?"
    assert len(calls) == 1 + 4  # query once, then one embedding per column scored
