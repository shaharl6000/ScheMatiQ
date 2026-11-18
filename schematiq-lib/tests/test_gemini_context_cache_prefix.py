"""Tests for system_prefix propagation in GeminiLLM.create_context_cache."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

genai_types = pytest.importorskip("google.genai.types")

from schematiq.core.llm_backends import GeminiLLM


def _stub_gemini_llm(system_prefix=None):
    """Build a stand-in GeminiLLM usable with unbound GeminiLLM methods."""
    llm = MagicMock()
    llm.model = "gemini-3.8-flash"
    llm.types = genai_types
    llm.max_output_tokens = 1024
    llm.temperature = 0.0
    llm.safety_settings = None
    llm._disable_automatic_function_calling = None
    llm.system_prefix = system_prefix
    llm._client.caches.create.return_value = SimpleNamespace(
        name="cachedContents/test-1",
        usage_metadata=None,
    )
    llm._client.models.generate_content.return_value = SimpleNamespace(
        text="ok",
        candidates=[],
        usage_metadata=None,
    )
    llm._build_contents.side_effect = lambda prompt_text, images=None: prompt_text
    return llm


def test_create_context_cache_prepends_system_prefix_and_matches_generate():
    llm = _stub_gemini_llm(system_prefix="GLOBAL_PREFIX")

    GeminiLLM.create_context_cache(llm, "EXTRACTION_RULES", "doc body", ttl_seconds=600)
    GeminiLLM.generate(
        llm,
        [
            {"role": "system", "content": "EXTRACTION_RULES"},
            {"role": "user", "content": "doc body"},
        ],
    )

    _, cache_kwargs = llm._client.caches.create.call_args
    cached_system = cache_kwargs["config"].system_instruction

    _, gen_kwargs = llm._client.models.generate_content.call_args
    gen_system = gen_kwargs["config"].system_instruction

    assert cached_system == "GLOBAL_PREFIX\n\nEXTRACTION_RULES"
    assert cached_system == gen_system


def test_create_context_cache_uses_system_prefix_when_instruction_is_empty():
    llm = _stub_gemini_llm(system_prefix="GLOBAL_PREFIX")

    GeminiLLM.create_context_cache(llm, "", "doc body")

    _, cache_kwargs = llm._client.caches.create.call_args
    assert cache_kwargs["config"].system_instruction == "GLOBAL_PREFIX"


@pytest.mark.parametrize("prefix", [None, ""])
def test_create_context_cache_keeps_instruction_when_prefix_absent(prefix):
    llm = _stub_gemini_llm(system_prefix=prefix)

    GeminiLLM.create_context_cache(llm, "EXTRACTION_RULES", "doc body")

    _, cache_kwargs = llm._client.caches.create.call_args
    assert cache_kwargs["config"].system_instruction == "EXTRACTION_RULES"


def test_create_context_cache_ignores_non_string_mock_system_prefix():
    llm = MagicMock()
    llm.model = "gemini-3.8-flash"
    llm.types = genai_types
    llm._client.caches.create.return_value = SimpleNamespace(
        name="cachedContents/test-2",
        usage_metadata=None,
    )

    GeminiLLM.create_context_cache(llm, "EXTRACTION_RULES", "doc body")

    _, cache_kwargs = llm._client.caches.create.call_args
    assert cache_kwargs["config"].system_instruction == "EXTRACTION_RULES"

