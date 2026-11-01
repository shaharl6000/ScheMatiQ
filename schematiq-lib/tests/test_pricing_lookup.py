"""Model pricing lookup: exact, longest-prefix, schedules, unknown fallback.

Regression for estimates that were badly off: the default extraction model
(gemini-3.5-flash-lite) was priced as gemini-3.5-flash through a bidirectional
substring match, and the default schema model (gemini-3.8-flash) was missing
and fell back to the unknown-model rates.
"""

import json
from datetime import date
from pathlib import Path

import pytest

from schematiq.core import cost_estimator
from schematiq.core.cost_estimator import calculate_cost, get_model_pricing
from schematiq.core.model_specs import ModelNames

BEFORE_2027 = date(2026, 11, 1)
FROM_2027 = date(2027, 1, 1)


def test_default_models_are_priced_explicitly():
    config = json.loads(
        (Path(cost_estimator.__file__).parent / "pricing_config.json").read_text()
    )
    gemini = config["providers"]["gemini"]
    for model in (
        ModelNames.DEFAULT_SCHEMA_CREATION,
        ModelNames.DEFAULT_VALUE_EXTRACTION,
        ModelNames.DEFAULT_RELEASE_EXTRACTION,
    ):
        assert model in gemini, f"{model} has no entry in pricing_config.json"


def test_flash_lite_is_not_priced_as_flash():
    lite = get_model_pricing("gemini", "gemini-3.5-flash-lite", as_of=BEFORE_2027)
    assert (lite["input"], lite["output"]) == (0.30, 2.50)
    flash = get_model_pricing("gemini", "gemini-3.5-flash", as_of=BEFORE_2027)
    assert flash["input"] != lite["input"]


def test_scheduled_price_change_applies_by_date():
    now = get_model_pricing("gemini", "gemini-3.8-flash", as_of=BEFORE_2027)
    later = get_model_pricing("gemini", "gemini-3.8-flash", as_of=FROM_2027)
    assert (now["input"], now["output"]) == (0.75, 3.75)
    assert (later["input"], later["output"]) == (1.50, 7.50)
    assert "schedule" not in now


def test_suffixed_id_uses_longest_prefix():
    p = get_model_pricing("gemini", "models/gemini-3.5-flash-lite-001", as_of=BEFORE_2027)
    assert (p["input"], p["output"]) == (0.30, 2.50)


def test_shorter_unknown_name_does_not_match_a_longer_key():
    # Reverse substring ("gemini-3" inside "gemini-3-flash-preview") must not match.
    p = get_model_pricing("gemini", "gemini-3", as_of=BEFORE_2027)
    assert (p["input"], p["output"]) == (1.00, 3.00)


def test_unknown_model_falls_back_without_raising():
    p = get_model_pricing("gemini", "gemini-99-ultra", as_of=BEFORE_2027)
    assert p["input"] == 1.00 and p["output"] == 3.00


def test_cost_math():
    p = get_model_pricing("gemini", "gemini-3.8-flash", as_of=BEFORE_2027)
    assert calculate_cost(1_000_000, 1_000_000, p) == pytest.approx(4.50)
