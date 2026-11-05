"""Retry settings for long runs: OpenAI timeouts are retried, and a bad
environment value falls back to the default instead of breaking the import."""

import httpx
import pytest

from schematiq.core import llm_backends
from schematiq.core.llm_backends import _env_number, _is_retryable_server_error

openai = pytest.importorskip("openai")


def _request():
    return httpx.Request("POST", "https://api.openai.com/v1/chat/completions")


def test_openai_timeout_is_retried():
    # LLM_REQUEST_TIMEOUT_SECONDS makes a stalled call raise this; it must be
    # retried like any other transport failure, not fail the call.
    assert _is_retryable_server_error(openai.APITimeoutError(request=_request()))


def test_openai_connection_error_is_retried():
    assert _is_retryable_server_error(openai.APIConnectionError(request=_request()))


def test_unrelated_error_is_not_retried():
    assert not _is_retryable_server_error(ValueError("Request timed out."))


@pytest.mark.parametrize("raw", ["abc", "-1", ""])
def test_bad_retry_count_falls_back_to_default(monkeypatch, raw):
    monkeypatch.setenv("SCHEMATIQ_LLM_MAX_RETRIES", raw)
    assert _env_number("SCHEMATIQ_LLM_MAX_RETRIES", 6, int, 0) == 6


@pytest.mark.parametrize("raw", ["soon", "0", "-5"])
def test_bad_timeout_falls_back_to_default(monkeypatch, raw):
    monkeypatch.setenv("SCHEMATIQ_LLM_TIMEOUT_SECONDS", raw)
    assert _env_number("SCHEMATIQ_LLM_TIMEOUT_SECONDS", 300.0, float, 1.0) == 300.0


def test_valid_values_are_used(monkeypatch):
    monkeypatch.setenv("SCHEMATIQ_LLM_MAX_RETRIES", "0")
    monkeypatch.setenv("SCHEMATIQ_LLM_TIMEOUT_SECONDS", "45.5")
    assert _env_number("SCHEMATIQ_LLM_MAX_RETRIES", 6, int, 0) == 0
    assert _env_number("SCHEMATIQ_LLM_TIMEOUT_SECONDS", 300.0, float, 1.0) == 45.5


def test_defaults():
    assert llm_backends.LLM_MAX_RETRIES >= 0
    assert llm_backends.LLM_REQUEST_TIMEOUT_SECONDS >= 1.0
