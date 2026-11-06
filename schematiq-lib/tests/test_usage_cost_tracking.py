"""Cost from provider-reported token usage.

The tracker used to price every call as ``len(text) // 4`` and never saw
thinking tokens (billed as output on Gemini 3.x), image/PDF input, cached
input, cache storage, or billed calls that returned empty text.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from schematiq.core.cost_estimator import get_model_pricing
from schematiq.core.llm_backends import _gemini_usage, _openai_usage
from schematiq.core.llm_call_tracker import LLMCallTracker

MODEL = "gemini-3.8-flash"


@pytest.fixture
def tracker():
    t = LLMCallTracker.get_instance()
    t.reset()
    yield t
    t.reset()


def _rates():
    return get_model_pricing("gemini", MODEL)


def test_thinking_tokens_are_billed_as_output(tracker):
    tracker.increment(
        model=MODEL, prompt_length=40, completion_length=4,
        input_tokens=1_000_000, output_tokens=0, thinking_tokens=1_000_000,
    )
    r = _rates()
    result = tracker.calculate_current_cost()
    assert result["total_cost_usd"] == pytest.approx(r["input"] + r["output"])
    assert result["total_thinking_tokens"] == 1_000_000
    assert result["total_output_tokens"] == 1_000_000
    assert result["estimated_calls"] == 0


def test_cached_input_uses_cached_rate(tracker):
    tracker.increment(model=MODEL, input_tokens=1_000_000, output_tokens=0, cached_tokens=800_000)
    r = _rates()
    expected = 0.2 * r["input"] + 0.8 * r["cached_input"]
    assert tracker.calculate_current_cost()["total_cost_usd"] == pytest.approx(expected)


def test_missing_usage_falls_back_to_chars_and_is_flagged(tracker):
    tracker.increment(model=MODEL, prompt_length=4_000_000, completion_length=0)
    result = tracker.calculate_current_cost()
    assert result["total_input_tokens"] == 1_000_000
    assert result["estimated_calls"] == 1


def test_uncounted_call_costs_but_does_not_count(tracker):
    tracker.increment(model=MODEL, input_tokens=1000, output_tokens=0, thinking_tokens=500, count_call=False)
    assert tracker.get_total() == 0
    assert tracker.calculate_current_cost()["total_cost_usd"] > 0


def test_cache_storage_priced_per_token_hour(tracker):
    tracker.record_cache_storage(model=MODEL, tokens=1_000_000, seconds=1800)
    r = _rates()
    result = tracker.calculate_current_cost()
    assert result["cache_storage_cost_usd"] == pytest.approx(r["cache_storage_per_hour"] / 2)
    assert tracker.get_total() == 0


def test_gemini_usage_extraction():
    response = SimpleNamespace(usage_metadata=SimpleNamespace(
        prompt_token_count=1200, candidates_token_count=80,
        thoughts_token_count=900, cached_content_token_count=1000,
    ))
    assert _gemini_usage(response) == {
        "input_tokens": 1200, "output_tokens": 80,
        "thinking_tokens": 900, "cached_tokens": 1000,
    }


def test_gemini_usage_absent_or_mocked_is_empty():
    assert _gemini_usage(SimpleNamespace()) == {}
    assert _gemini_usage(MagicMock()) == {}  # MagicMock attrs are not ints


def test_openai_usage_extraction():
    resp = SimpleNamespace(usage=SimpleNamespace(
        prompt_tokens=50, completion_tokens=10,
        prompt_tokens_details=SimpleNamespace(cached_tokens=20),
    ))
    assert _openai_usage(resp) == {"input_tokens": 50, "output_tokens": 10, "cached_tokens": 20}
