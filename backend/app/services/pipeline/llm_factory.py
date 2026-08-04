"""LLM interface construction and release-mode enforcement."""

import logging
from typing import Optional

from app.core.config import ALLOW_LLM_CONFIG, DEVELOPER_MODE, RELEASE_CONFIG

logger = logging.getLogger(__name__)

from schematiq.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM


def build_llm_interface(
    provider: str,
    model: str,
    max_output_tokens: Optional[int],
    temperature: float,
    api_key: str = None,
    context_window_size: Optional[int] = None
) -> LLMInterface:
    """Build LLM interface based on provider.

    Args:
        provider: LLM provider name (together, openai, gemini)
        model: Model name/identifier (empty string uses provider default)
        max_output_tokens: Maximum tokens the model can generate in its response.
            If None, auto-detected from model specs.
        temperature: Sampling temperature
        api_key: Optional user-provided API key (falls back to env var if None)
        context_window_size: Maximum context window size. If None, auto-detected from model specs.
    """
    if provider.lower() == "together":
        if not model:
            raise ValueError("Model must be specified for Together AI provider")
        return TogetherLLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            context_window_size=context_window_size,
            api_key=api_key
        )
    elif provider.lower() == "openai":
        if not model:
            raise ValueError("Model must be specified for OpenAI provider")
        return OpenAILLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            context_window_size=context_window_size,
            api_key=api_key
        )
    if provider.lower() == "gemini":
        kwargs = {
            "max_output_tokens": max_output_tokens,
            "temperature": temperature,
            "context_window_size": context_window_size,
            "api_key": api_key
        }
        if model:
            kwargs["model"] = model
        llm = GeminiLLM(**kwargs)
        logger.info(f"Gemini LLM created: model={llm.model}, max_output_tokens={llm.max_output_tokens}, context_window={llm.context_window_size}")
        return llm
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


def enforce_release_llm_config(backend_config: dict, is_schema_creation: bool = False) -> dict:
    """Override LLM config with release-mode defaults if not in developer mode.

    Args:
        backend_config: The original LLM backend configuration dict
        is_schema_creation: True for schema creation LLM, False for value extraction

    Returns:
        The config dict, potentially with provider/model/temperature overridden
    """
    if DEVELOPER_MODE or ALLOW_LLM_CONFIG:
        return backend_config

    return {
        **backend_config,
        "provider": RELEASE_CONFIG["llm_provider"],
        "model": RELEASE_CONFIG["schema_creation_model"] if is_schema_creation else RELEASE_CONFIG["value_extraction_model"],
        "temperature": RELEASE_CONFIG["llm_temperature"],
    }
