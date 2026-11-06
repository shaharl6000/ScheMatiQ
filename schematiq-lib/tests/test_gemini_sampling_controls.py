"""Sampling controls for Gemini 3.x (issue #556).

Gemini 3.x ignores temperature, so value extraction ran with sampling on.
These tests cover the two remaining controls: thinking_budget=0 maps to low
(minimal is opt-in, where the model accepts it), and a fixed seed is sent (and
dropped if the API ever rejects it). Methods are tested unbound against a
minimal stand-in, so no API key or network is needed.
"""

from types import SimpleNamespace

import pytest

from schematiq.core import llm_backends
from schematiq.core.llm_backends import GeminiLLM
from schematiq.core.model_specs import ModelNames, get_model_spec


def _fake(model, seed=None):
    spec = get_model_spec("gemini", model)
    return SimpleNamespace(
        model=model,
        seed=seed,
        allowed_thinking_levels=spec.allowed_thinking_levels,
        fallback_thinking_level=spec.fallback_thinking_level,
        _THINKING_LEVEL_THRESHOLDS=GeminiLLM._THINKING_LEVEL_THRESHOLDS,
    )


def _level(model, budget):
    return GeminiLLM._thinking_budget_to_level(_fake(model), budget)


@pytest.mark.parametrize(
    "model", [ModelNames.GEMINI_35_FLASH_LITE, ModelNames.GEMINI_31_FLASH_LITE]
)
def test_zero_budget_maps_to_low_on_flash_lite(model, monkeypatch):
    monkeypatch.delenv("SCHEMATIQ_GEMINI_ZERO_BUDGET_LEVEL", raising=False)
    assert _level(model, 0) == "low"


@pytest.mark.parametrize(
    "model", [ModelNames.GEMINI_35_FLASH_LITE, ModelNames.GEMINI_31_FLASH_LITE]
)
def test_minimal_is_opt_in_on_flash_lite(model, monkeypatch):
    monkeypatch.setenv("SCHEMATIQ_GEMINI_ZERO_BUDGET_LEVEL", "minimal")
    assert _level(model, 0) == "minimal"


@pytest.mark.parametrize(
    "model",
    [
        ModelNames.GEMINI_35_FLASH,
        ModelNames.GEMINI_36_FLASH,
        ModelNames.GEMINI_37_FLASH,
        ModelNames.GEMINI_38_FLASH,
        ModelNames.GEMINI_31_PRO_PREVIEW,
    ],
)
def test_zero_budget_never_sends_minimal_elsewhere(model, monkeypatch):
    # Even when opted in, minimal falls back to low on models that reject it.
    monkeypatch.setenv("SCHEMATIQ_GEMINI_ZERO_BUDGET_LEVEL", "minimal")
    assert _level(model, 0) == "low"


def test_nonzero_budgets_unchanged():
    m = ModelNames.GEMINI_35_FLASH_LITE
    assert _level(m, 1024) == "low"
    assert _level(m, 4096) == "medium"
    assert _level(m, 20000) == "high"
    assert _level(ModelNames.GEMINI_31_PRO_PREVIEW, 4096) == "high"


def test_seed_from_env(monkeypatch):
    monkeypatch.delenv("SCHEMATIQ_GEMINI_SEED", raising=False)
    assert llm_backends._gemini_seed_from_env() == llm_backends._DEFAULT_GEMINI_SEED
    monkeypatch.setenv("SCHEMATIQ_GEMINI_SEED", "7")
    assert llm_backends._gemini_seed_from_env() == 7
    monkeypatch.setenv("SCHEMATIQ_GEMINI_SEED", "off")
    assert llm_backends._gemini_seed_from_env() is None
    monkeypatch.setenv("SCHEMATIQ_GEMINI_SEED", "abc")
    assert llm_backends._gemini_seed_from_env() is None


def test_apply_seed_prefers_per_call_seed():
    fake = _fake(ModelNames.GEMINI_35_FLASH_LITE, seed=1234)
    cfg = {}
    GeminiLLM._apply_seed(fake, cfg, None)
    assert cfg["seed"] == 1234
    cfg = {}
    GeminiLLM._apply_seed(fake, cfg, 99)
    assert cfg["seed"] == 99
    cfg = {}
    GeminiLLM._apply_seed(_fake(ModelNames.GEMINI_35_FLASH_LITE), cfg, None)
    assert "seed" not in cfg


class _ApiError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def test_rejected_seed_is_dropped_once():
    fake = _fake(ModelNames.GEMINI_35_FLASH_LITE, seed=1234)
    cfg = {"seed": 1234}
    err = _ApiError(400, "400 INVALID_ARGUMENT: seed is not supported for this model")
    assert GeminiLLM._drop_rejected_seed(fake, cfg, err) is True
    assert "seed" not in cfg
    assert fake.seed is None
    # Second time there is no seed to drop, so the error is handled normally.
    assert GeminiLLM._drop_rejected_seed(fake, cfg, err) is False


def test_unrelated_400_does_not_drop_seed():
    fake = _fake(ModelNames.GEMINI_35_FLASH_LITE, seed=1234)
    cfg = {"seed": 1234}
    err = _ApiError(400, "400 INVALID_ARGUMENT: response_schema is invalid")
    assert GeminiLLM._drop_rejected_seed(fake, cfg, err) is False
    assert cfg["seed"] == 1234
