"""
Model specifications and per-task token budgets.

Two layers control ``max_output_tokens``:

1. **Model spec** – hard ceiling per model (``MODEL_SPECS``).
2. **Task budget** – how many tokens a specific task *needs*
   (``TASK_TOKEN_BUDGETS``).  ``None`` = use the full model limit.

Use :func:`get_max_output_tokens` to resolve the effective value::

    tokens = get_max_output_tokens("gemini", "gemini-2.5-flash-lite",
                                   task="unit_identification")
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class ModelSpec:
    """Immutable specification for an LLM model."""
    context_window: int
    max_output_tokens: int


# ── Model specifications ────────────────────────────────────────────
MODEL_SPECS: Dict[str, Dict[str, ModelSpec]] = {
    "gemini": {
        "gemini-2.5-flash": ModelSpec(1_048_576, 65_535),
        "gemini-2.5-flash-lite": ModelSpec(1_048_576, 65_535),
        "gemini-2.5-pro": ModelSpec(1_048_576, 65_535),
        "gemini-3-flash-preview": ModelSpec(1_000_000, 64_000),
        "gemini-3-pro-preview": ModelSpec(1_000_000, 64_000),
        "gemini-3.1-flash-lite-preview": ModelSpec(1_000_000, 64_000),
        "gemini-3.1-pro-preview": ModelSpec(1_000_000, 64_000),
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


# ── Per-task token budgets ──────────────────────────────────────────
# None = use the model's full max_output_tokens (no cap).
# An int value caps the output to that many tokens (but never exceeds
# the model's own limit).
#
# Default: all tasks use the model maximum.  To cap a specific task,
# replace None with an int (e.g. 4_096).
TASK_TOKEN_BUDGETS: Dict[str, Optional[int]] = {
    "schema_discovery":            None,    # suggested cap: 8_192
    "observation_unit_discovery":  None,    # suggested cap: 4_096
    "unit_identification":         None,    # suggested cap: 4_096
    "value_extraction":            None,    # suggested cap: None (large output)
    "retrieval":                   None,    # suggested cap: 2_048
}


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


def get_max_output_tokens(
    provider: str,
    model: str,
    task: Optional[str] = None,
) -> int:
    """
    Resolve the effective ``max_output_tokens`` for a (model, task) pair.

    Priority:
    1. Task budget from ``TASK_TOKEN_BUDGETS`` (if task is given and has an entry).
    2. Model limit from ``MODEL_SPECS``.

    The returned value never exceeds the model's hard limit.

    Args:
        provider: LLM provider (e.g. "gemini", "openai").
        model:    Model name (e.g. "gemini-2.5-flash-lite").
        task:     Optional task name (e.g. "value_extraction",
                  "unit_identification"). ``None`` = use model max.

    Returns:
        Effective max_output_tokens for this call.
    """
    spec = get_model_spec(provider, model)
    model_max = spec.max_output_tokens

    if task is None:
        return model_max

    task_budget = TASK_TOKEN_BUDGETS.get(task)
    if task_budget is None:
        return model_max

    return min(task_budget, model_max)
