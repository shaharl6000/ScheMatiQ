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
    supports_thinking: bool = False
    # Gemini 3.x+ models replace the integer ``thinking_budget`` with the
    # semantic ``thinking_level`` enum. They also deprecated the sampling
    # params (temperature/top_p/top_k): the API silently ignores them today
    # and returns 400 in future model generations, so they must not be sent.
    # See https://ai.google.dev/gemini-api/docs/latest-model
    uses_thinking_level: bool = False
    # Whether the provider still honours temperature/top_p/top_k. When False the
    # values are dropped before the request rather than sent and ignored, so a
    # caller asking for temperature=0 is never quietly told it worked.
    supports_sampling_params: bool = True
    # Thinking levels this pipeline may send to this model. Sending a level the
    # model does not accept returns 400 (gemini-3.1-pro-preview rejects the
    # medium level). This is the *sendable* set, not everything the model
    # supports: the minimal level is deliberately excluded everywhere because it
    # requires thought-signature handling that this pipeline does not implement.
    # Empty means no constraint is known and the translated level is used as is.
    allowed_thinking_levels: tuple = ()
    # Level substituted when the translated level is not in the sendable set.
    # Only a clamp target. It is never sent on its own when the caller passes no
    # thinking_budget, so omitting a budget still means "let the server decide".
    fallback_thinking_level: Optional[str] = None


# ── Canonical model names ───────────────────────────────────────────
class ModelNames:
    # Gemini
    GEMINI_25_FLASH = "gemini-2.5-flash"
    GEMINI_25_FLASH_LITE = "gemini-2.5-flash-lite"
    GEMINI_31_FLASH_LITE = "gemini-3.1-flash-lite"
    GEMINI_35_FLASH = "gemini-3.5-flash"
    GEMINI_35_FLASH_LITE = "gemini-3.5-flash-lite"
    GEMINI_36_FLASH = "gemini-3.6-flash"
    GEMINI_37_FLASH = "gemini-3.7-flash"
    GEMINI_31_PRO_PREVIEW = "gemini-3.1-pro-preview"
    GEMINI_15_FLASH = "gemini-1.5-flash"

    # OpenAI
    GPT_41 = "gpt-4.1"
    GPT_41_MINI = "gpt-4.1-mini"
    GPT_41_NANO = "gpt-4.1-nano"

    # Together
    LLAMA_33_70B_TURBO = "meta-llama/Llama-3.3-70B-Instruct-Turbo"

    # Research / evaluation only
    LLAMA_33_70B = "meta-llama/Llama-3.3-70B-Instruct"
    LLAMA_32_3B = "meta-llama/Llama-3.2-3B-Instruct"
    LLAMA_3_70B_CHAT = "meta-llama/Llama-3-70b-chat-hf"
    MISTRAL_7B = "mistralai/Mistral-7B-Instruct-v0.2"
    MIXTRAL_8X7B = "mistralai/Mixtral-8x7B-Instruct-v0.1"

    # Tiktoken encoding reference (not used as LLM)
    TIKTOKEN_ENCODING_MODEL = "gpt-4o"

    # Role-based defaults
    DEFAULT_SCHEMA_CREATION = GEMINI_37_FLASH
    DEFAULT_VALUE_EXTRACTION = GEMINI_35_FLASH_LITE
    DEFAULT_RELEASE_EXTRACTION = GEMINI_35_FLASH_LITE
    DEFAULT_EVALUATION = GEMINI_15_FLASH
    DEFAULT_TOGETHER = LLAMA_33_70B_TURBO
    DEFAULT_OPENAI = GPT_41
    DEFAULT_HF = LLAMA_33_70B


# ── Model specifications ────────────────────────────────────────────
MODEL_SPECS: Dict[str, Dict[str, ModelSpec]] = {
    "gemini": {
        # Gemini 2.5: legacy integer thinking_budget, sampling params still honoured.
        ModelNames.GEMINI_25_FLASH: ModelSpec(1_048_576, 65_535, supports_thinking=True),
        ModelNames.GEMINI_25_FLASH_LITE: ModelSpec(1_048_576, 65_535, supports_thinking=True),
        # Gemini 3.x: thinking_level enum, sampling params deprecated and ignored.
        # Defaults per https://ai.google.dev/gemini-api/docs/latest-model
        ModelNames.GEMINI_31_FLASH_LITE: ModelSpec(
            1_048_576, 65_536, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "medium", "high"),
            fallback_thinking_level="low",
        ),
        ModelNames.GEMINI_35_FLASH: ModelSpec(
            1_048_576, 65_536, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "medium", "high"),
            fallback_thinking_level="medium",
        ),
        ModelNames.GEMINI_36_FLASH: ModelSpec(
            1_048_576, 65_536, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "medium", "high"),
            fallback_thinking_level="medium",
        ),
        ModelNames.GEMINI_37_FLASH: ModelSpec(
            1_048_576, 65_536, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "medium", "high"),
            fallback_thinking_level="medium",
        ),
        ModelNames.GEMINI_35_FLASH_LITE: ModelSpec(
            1_048_576, 65_536, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "medium", "high"),
            fallback_thinking_level="low",
        ),
        # Pro rejects "medium" (400 INVALID_ARGUMENT); low/high only.
        ModelNames.GEMINI_31_PRO_PREVIEW: ModelSpec(
            1_000_000, 64_000, supports_thinking=True, uses_thinking_level=True,
            supports_sampling_params=False,
            allowed_thinking_levels=("low", "high"),
            fallback_thinking_level="high",
        ),
        "_default": ModelSpec(1_000_000, 32_000, supports_thinking=False),
    },
    "openai": {
        ModelNames.GPT_41: ModelSpec(1_000_000, 32_768),
        ModelNames.GPT_41_MINI: ModelSpec(1_000_000, 32_768),
        ModelNames.GPT_41_NANO: ModelSpec(1_000_000, 32_768),
        "_default": ModelSpec(128_000, 16_000),
    },
    "together": {
        ModelNames.LLAMA_33_70B_TURBO: ModelSpec(128_000, 8_192),
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
