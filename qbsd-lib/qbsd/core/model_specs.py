"""
Model specifications for auto-detecting token limits based on model name.

This module provides a centralized registry of model specifications to prevent
truncation issues caused by incorrect token limits.
"""

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ModelSpec:
    """Immutable specification for an LLM model."""
    context_window: int
    max_output_tokens: int


# Centralized model specifications
MODEL_SPECS: Dict[str, Dict[str, ModelSpec]] = {
    "gemini": {
        "gemini-2.5-flash": ModelSpec(1_048_576, 65_536),
        "gemini-2.5-flash-lite": ModelSpec(1_048_576, 65_536),
        "gemini-2.5-pro": ModelSpec(1_048_576, 65_536),
        "gemini-3-flash-preview": ModelSpec(1_000_000, 64_000),
        "gemini-3-pro-preview": ModelSpec(1_000_000, 64_000),
        "_default": ModelSpec(1_000_000, 32_000),
    },
    "openai": {
        "gpt-4.1": ModelSpec(1_000_000, 32_768),
        "gpt-4.1-mini": ModelSpec(1_000_000, 32_768),
        "gpt-4.1-nano": ModelSpec(1_000_000, 32_768),
        "gpt-4o": ModelSpec(128_000, 32_768),
        "gpt-4o-mini": ModelSpec(128_000, 16_000),
        "_default": ModelSpec(128_000, 16_000),
    },
    "together": {
        "meta-llama/Llama-3.3-70B-Instruct-Turbo": ModelSpec(128_000, 8_192),
        "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free": ModelSpec(128_000, 8_192),
        "_default": ModelSpec(128_000, 4_096),
    },
}

GLOBAL_FALLBACK = ModelSpec(32_000, 4_096)


def get_model_spec(provider: str, model: str) -> ModelSpec:
    """
    Get model spec with prefix-based fallback.

    Lookup order:
    1. Exact match for the model name
    2. Prefix match (handles versioned models like "-002", "-latest")
    3. Provider default
    4. Global fallback

    Args:
        provider: The LLM provider name (e.g., "gemini", "openai", "together")
        model: The model name (e.g., "gemini-2.5-flash", "gpt-4o")

    Returns:
        ModelSpec with context_window and max_output_tokens
    """
    provider_specs = MODEL_SPECS.get(provider.lower(), {})

    # 1. Exact match
    if model in provider_specs:
        return provider_specs[model]

    # 2. Prefix match (handles versioned models like "-002", "-latest")
    for spec_model, spec in provider_specs.items():
        if spec_model != "_default" and model.startswith(spec_model):
            return spec

    # 3. Provider default
    if "_default" in provider_specs:
        return provider_specs["_default"]

    # 4. Global fallback
    return GLOBAL_FALLBACK
