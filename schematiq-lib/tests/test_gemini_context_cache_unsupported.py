"""A 404 on cache create is remembered, so later documents skip the call.

Builds a real GeminiLLM with a dummy key and stubs only the SDK client.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.genai")

from schematiq.core.llm_backends import GeminiLLM


class _ApiError(Exception):
    def __init__(self, code):
        super().__init__(f"{code} error")
        self.code = code


def _llm(error):
    llm = GeminiLLM(model="gemini-3.8-flash", api_key="test-key")
    llm._client = MagicMock()
    llm._client.caches.create.side_effect = error
    return llm


def test_404_is_remembered_and_skips_later_creates():
    llm = _llm(_ApiError(404))
    assert llm.create_context_cache("RULES", "doc one") is None
    assert llm.create_context_cache("RULES", "doc two") is None
    assert llm._client.caches.create.call_count == 1


def test_other_errors_retry_on_next_document():
    llm = _llm(_ApiError(500))
    assert llm.create_context_cache("RULES", "doc one") is None
    assert llm.create_context_cache("RULES", "doc two") is None
    assert llm._client.caches.create.call_count == 2


def test_success_after_transient_error():
    llm = _llm(None)
    llm._client.caches.create.side_effect = [
        _ApiError(500),
        SimpleNamespace(name="cachedContents/x", usage_metadata=None),
    ]
    assert llm.create_context_cache("RULES", "doc one") is None
    assert llm.create_context_cache("RULES", "doc two") is not None
