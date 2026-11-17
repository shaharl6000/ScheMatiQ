"""Schema dedup/pruning model and similarity threshold are configurable (#662)."""

import json
from typing import Any, List, Optional

import pytest

import schematiq.core.schema as schema_mod
from schematiq import DiscoveryConfig, discover_schema
from schematiq.core.llm_backends import LLMInterface
from schematiq.core.schema import Column, Schema
from schematiq.core.schematiq import _parse_schema_from_llm

MULTILINGUAL = "paraphrase-multilingual-MiniLM-L12-v2"


class _Sim(float):
    """A cosine score that also supports ``.item()``, like a torch scalar."""

    def item(self) -> float:
        return float(self)


@pytest.fixture
def embed_calls(monkeypatch: pytest.MonkeyPatch) -> List[tuple]:
    """Record (text, model_name) per _embed call. Every pair scores 0.85."""
    calls: List[tuple] = []

    def _fake_embed(text: str, model_name: Optional[str] = None) -> str:
        calls.append((text, model_name))
        return text

    monkeypatch.setattr(schema_mod, "_embed", _fake_embed)
    monkeypatch.setattr(schema_mod.util, "cos_sim", lambda a, b: _Sim(0.85))
    return calls


def _schema(*names: str, **kwargs: Any) -> Schema:
    return Schema(
        query="q",
        columns=[Column(name=n, rationale="r", definition="d") for n in names],
        **kwargs,
    )


def test_defaults_are_unchanged(embed_calls: List[tuple]) -> None:
    merged = _schema("verdict").merge(_schema("ruling"))

    # 0.85 is below the default 0.9, so the names stay separate.
    assert [c.name for c in merged.columns] == ["verdict", "ruling"]
    assert {model for _, model in embed_calls} == {None}
    assert merged.embedding_model is None
    assert merged.similarity_threshold is None


def test_custom_threshold_and_model_drive_merge(embed_calls: List[tuple]) -> None:
    base = _schema("verdict", embedding_model=MULTILINGUAL, similarity_threshold=0.8)
    merged = base.merge(_schema("ruling"))

    assert [c.name for c in merged.columns] == ["verdict"]
    assert {model for _, model in embed_calls} == {MULTILINGUAL}
    assert merged.embedding_model == MULTILINGUAL
    assert merged.similarity_threshold == 0.8


def test_settings_are_taken_from_other_when_self_has_none(embed_calls: List[tuple]) -> None:
    other = _schema("ruling", embedding_model=MULTILINGUAL, similarity_threshold=0.8)
    merged = _schema("verdict").merge(other)

    assert [c.name for c in merged.columns] == ["verdict"]
    assert merged.embedding_model == MULTILINGUAL
    assert merged.similarity_threshold == 0.8


def test_prune_uses_configured_model(embed_calls: List[tuple]) -> None:
    schema = _schema("a", "b", "c", max_keys=2, embedding_model=MULTILINGUAL)

    assert len(schema.columns) == 2
    assert embed_calls and {model for _, model in embed_calls} == {MULTILINGUAL}


def test_apply_embedding_settings_keeps_unset_values() -> None:
    schema = _schema("a", embedding_model=MULTILINGUAL, similarity_threshold=0.8)
    schema.apply_embedding_settings(None, None)
    assert (schema.embedding_model, schema.similarity_threshold) == (MULTILINGUAL, 0.8)

    schema.apply_embedding_settings(similarity_threshold=0.95)
    assert (schema.embedding_model, schema.similarity_threshold) == (MULTILINGUAL, 0.95)


@pytest.mark.parametrize("bad", [0, 0.0, -0.1, 1.5, True])
def test_invalid_threshold_is_rejected(bad: Any) -> None:
    with pytest.raises(ValueError):
        _schema("a", similarity_threshold=bad)
    with pytest.raises(ValueError):
        _schema("a").apply_embedding_settings(similarity_threshold=bad)
    with pytest.raises(ValueError):
        DiscoveryConfig(schema_similarity_threshold=bad)


def test_settings_are_not_serialized() -> None:
    schema = _schema("a", embedding_model=MULTILINGUAL, similarity_threshold=0.8)
    data = schema.to_full_dict()
    assert "embedding_model" not in json.dumps(data)
    assert Schema.from_dict(data).embedding_model is None


def test_non_default_models_are_cached_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: List[str] = []

    def _fake_load(name: str) -> object:
        loaded.append(name)
        return object()

    monkeypatch.setattr(schema_mod, "load_sentence_transformer", _fake_load)
    monkeypatch.setattr(schema_mod, "_EMB_MODELS", {})
    sentinel = object()
    monkeypatch.setattr(schema_mod, "_EMB_MODEL", sentinel)

    first = schema_mod._get_emb_model(MULTILINGUAL)
    assert schema_mod._get_emb_model(MULTILINGUAL) is first
    assert loaded == [MULTILINGUAL]
    # The default path still returns the shared default model.
    assert schema_mod._get_emb_model() is sentinel
    assert schema_mod._get_emb_model(schema_mod.EMB_MODEL_NAME) is sentinel


def test_parsed_llm_schema_gets_settings_before_prune(embed_calls: List[tuple]) -> None:
    raw = json.dumps(
        {
            "document_helpful": True,
            "columns": [
                {"name": n, "definition": "d", "rationale": "r"} for n in ("a", "b", "c")
            ],
        }
    )
    schema, _, _ = _parse_schema_from_llm(
        raw,
        query="q",
        max_keys_schema=2,
        embedding_model=MULTILINGUAL,
        similarity_threshold=0.8,
    )
    assert len(schema.columns) == 2
    assert {model for _, model in embed_calls} == {MULTILINGUAL}
    assert schema.similarity_threshold == 0.8


def test_config_from_dict_reads_settings() -> None:
    cfg = DiscoveryConfig.from_dict(
        {"schema_embedding_model": MULTILINGUAL, "schema_similarity_threshold": 0.8}
    )
    assert cfg.schema_embedding_model == MULTILINGUAL
    assert cfg.schema_similarity_threshold == 0.8

    defaults = DiscoveryConfig.from_dict({})
    assert defaults.schema_embedding_model is None
    assert defaults.schema_similarity_threshold is None


class _TwoBatchLLM(LLMInterface):
    """Returns one paraphrase column per discovery call."""

    _provider = "openai"

    def __init__(self) -> None:
        super().__init__()
        self.model = "fake"
        self.context_window_size = 16384
        self._names = iter(["verdict", "ruling"])

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs: Any) -> str:
        name = next(self._names, "extra")
        return json.dumps(
            {
                "document_helpful": True,
                "columns": [{"name": name, "definition": "d", "rationale": "r"}],
            }
        )


def test_discover_schema_applies_config(embed_calls: List[tuple]) -> None:
    schema, _ = discover_schema(
        documents=["DOC_ONE text.", "DOC_TWO text."],
        filenames=["one.txt", "two.txt"],
        query="",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=1,
            identify_observation_unit=False,
            schema_embedding_model=MULTILINGUAL,
            schema_similarity_threshold=0.8,
        ),
        llm=_TwoBatchLLM(),
    )

    # "ruling" scores 0.85 against "verdict": a paraphrase at threshold 0.8.
    assert [c.name for c in schema.columns] == ["verdict"]
    assert embed_calls and {model for _, model in embed_calls} == {MULTILINGUAL}
    assert schema.embedding_model == MULTILINGUAL
    assert schema.similarity_threshold == 0.8
