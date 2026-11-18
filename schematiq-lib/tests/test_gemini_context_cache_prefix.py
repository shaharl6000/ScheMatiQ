"""system_prefix reaches the context cache the same way it reaches generate().

Cached extraction calls take their system prompt from the cache, so a prefix
missing there would silently differ from uncached calls. Tests build a real
GeminiLLM with a dummy key (no network at construction) and stub only the SDK
client.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("google.genai")

from schematiq.core.llm_backends import GeminiLLM


def _llm(system_prefix):
    llm = GeminiLLM(
        model="gemini-3.8-flash", api_key="test-key", system_prefix=system_prefix,
    )
    llm._client = MagicMock()
    llm._client.caches.create.return_value = SimpleNamespace(
        name="cachedContents/test", usage_metadata=None,
    )
    llm._client.models.generate_content.return_value = SimpleNamespace(
        text="ok", candidates=[], usage_metadata=None,
    )
    return llm


def _cached_system(llm):
    return llm._client.caches.create.call_args.kwargs["config"].system_instruction


@pytest.mark.parametrize(
    "prefix, instruction, expected",
    [
        ("PREFIX", "RULES", "PREFIX\n\nRULES"),
        ("PREFIX", "", "PREFIX"),
        (None, "RULES", "RULES"),
        ("", "RULES", "RULES"),
    ],
)
def test_cache_system_instruction(prefix, instruction, expected):
    llm = _llm(prefix)
    assert llm.create_context_cache(instruction, "doc body") is not None
    assert _cached_system(llm) == expected


@pytest.mark.parametrize("prefix", ["PREFIX", None])
def test_cache_matches_generate(prefix):
    llm = _llm(prefix)
    llm.create_context_cache("RULES", "doc body")
    llm.generate([
        {"role": "system", "content": "RULES"},
        {"role": "user", "content": "doc body"},
    ])
    generated = llm._client.models.generate_content.call_args.kwargs["config"]
    assert _cached_system(llm) == generated.system_instruction
