"""Unit dedup model and threshold are configurable (kwargs or env).

The default model is English-only, so a non-English corpus needs its own model
and threshold. These tests stub the embedding so no model is loaded.
"""

from types import SimpleNamespace

import pytest

from schematiq.value_extraction.core import paper_processor as pp_mod
from schematiq.value_extraction.core.paper_processor import (
    DEFAULT_UNIT_DEDUP_THRESHOLD,
    PaperProcessor,
    _unit_dedup_model_from_env,
    _unit_dedup_threshold_from_env,
)


class _FakeTensor:
    def __init__(self, value):
        self.value = value

    def item(self):
        return self.value


@pytest.fixture
def stub_embedding(monkeypatch):
    """Every pair of names scores 0.9; records the model each embed used."""
    seen_models = []

    def fake_embed(text, model_name=None):
        seen_models.append(model_name)
        return text

    monkeypatch.setattr(pp_mod, "_embed", fake_embed)
    monkeypatch.setattr(pp_mod.st_util, "cos_sim", lambda a, b: _FakeTensor(0.9))
    return seen_models


def _units(*names):
    return [{"unit_name": n, "confidence": "high"} for n in names]


def test_default_threshold_merges_at_point_nine(stub_embedding):
    result = PaperProcessor._deduplicate_units(SimpleNamespace(), _units("Alpha", "Bravo"))
    assert len(result) == 1
    assert stub_embedding == [None, None]


def test_configured_model_and_threshold_are_used(stub_embedding):
    self_ = SimpleNamespace(unit_dedup_model="multi-model", unit_dedup_threshold=0.95)
    result = PaperProcessor._deduplicate_units(self_, _units("Alpha", "Bravo"))
    assert len(result) == 2
    assert stub_embedding == ["multi-model", "multi-model"]


def test_explicit_sim_threshold_overrides_configured(stub_embedding):
    self_ = SimpleNamespace(unit_dedup_model=None, unit_dedup_threshold=0.95)
    result = PaperProcessor._deduplicate_units(self_, _units("Alpha", "Bravo"), sim_threshold=0.5)
    assert len(result) == 1


def test_env_settings(monkeypatch):
    monkeypatch.setenv("SCHEMATIQ_UNIT_DEDUP_MODEL", " multi-model ")
    monkeypatch.setenv("SCHEMATIQ_UNIT_DEDUP_THRESHOLD", "0.93")
    assert _unit_dedup_model_from_env() == "multi-model"
    assert _unit_dedup_threshold_from_env() == 0.93


@pytest.mark.parametrize("raw", ["abc", "0", "1.5", "-0.2"])
def test_invalid_env_threshold_falls_back(monkeypatch, raw):
    monkeypatch.setenv("SCHEMATIQ_UNIT_DEDUP_THRESHOLD", raw)
    assert _unit_dedup_threshold_from_env() == DEFAULT_UNIT_DEDUP_THRESHOLD


def test_unset_env_uses_defaults(monkeypatch):
    monkeypatch.delenv("SCHEMATIQ_UNIT_DEDUP_MODEL", raising=False)
    monkeypatch.delenv("SCHEMATIQ_UNIT_DEDUP_THRESHOLD", raising=False)
    assert _unit_dedup_model_from_env() is None
    assert _unit_dedup_threshold_from_env() == DEFAULT_UNIT_DEDUP_THRESHOLD
