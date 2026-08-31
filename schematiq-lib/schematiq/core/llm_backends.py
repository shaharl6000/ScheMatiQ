# llm_backends.py
"""
Concrete implementations of `LLMInterface` for Together AI and OpenAI.
The API keys are pulled from standard environment variables by default:
    OPENAI_API_KEY       – OpenAI
    TOGETHER_API_KEY     – Together AI
"""

import logging
import os
import time
import random
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union, Optional, Tuple
import re

import httpx

from schematiq.core.model_specs import get_model_spec, get_max_output_tokens, ModelNames
from schematiq.core.llm_call_tracker import LLMCallTracker

logger = logging.getLogger(__name__)


##############################################################################
# Rate limit retry utilities                                                 #
##############################################################################

_RATE_LIMIT_STATUS_CODE = 429
# Transient failures worth retrying with a fixed backoff. 429 is excluded
# because it is classified separately and uses the provider's own retry hint.
_TRANSIENT_STATUS_CODES = frozenset({408, 500, 502, 503, 504})
_AUTH_STATUS_CODES = frozenset({401, 403})
# Transport failures carry no HTTP status, so they need their own check. These
# are the two the google-genai client retries internally, and generation is a
# read-only call, so replaying it has no side effect beyond the token cost.
_TRANSPORT_RETRY_ERRORS = (httpx.TimeoutException, httpx.ConnectError)


def _status_code(error: BaseException) -> Optional[int]:
    """Return the HTTP status code a provider SDK error carries, if any.

    ``google.genai.errors.APIError`` exposes it as ``code``, the OpenAI SDK as
    ``status_code``. Unrelated exception types may also define a ``code``
    attribute, so only plausible HTTP error values are accepted; anything else
    returns None and the caller falls back to matching the message text.
    """
    for attr in ("code", "status_code"):
        value = getattr(error, attr, None)
        if isinstance(value, int) and 400 <= value < 600:
            return value
    return None


def _is_rate_limit_error(error: BaseException) -> bool:
    """Check if error is a rate limit / quota error (provider-side).

    The status code is authoritative when the SDK exposes one; the text checks
    remain for exceptions that do not (transport errors, wrapped exceptions).
    """
    if _status_code(error) == _RATE_LIMIT_STATUS_CODE:
        return True
    error_str = str(error)
    el = error_str.lower()
    if "429" in error_str and (
        "rate" in el or "quota" in el or "limit" in el or "exhausted" in el
    ):
        return True
    # Gemini / gRPC-style quota messages often omit the literal "429" substring
    if "resource_exhausted" in el or "resource exhausted" in el:
        return True
    if "too many requests" in el:
        return True
    return False

def _is_retryable_server_error(error: BaseException) -> bool:
    """Check if error is a transient server-side failure worth retrying.

    Any 408/5xx from the provider qualifies, regardless of message wording:
    500 INTERNAL and 504 DEADLINE_EXCEEDED are as transient as 503 UNAVAILABLE.
    Timeouts and connection failures qualify as well, since they carry no status
    code and would otherwise be treated as permanent. The 503 text match stays
    as a fallback for exceptions with neither a status code nor an httpx type.
    """
    if _status_code(error) in _TRANSIENT_STATUS_CODES:
        return True
    if isinstance(error, _TRANSPORT_RETRY_ERRORS):
        return True
    error_str = str(error)
    if "503" not in error_str:
        return False
    error_lower = error_str.lower()
    return any(indicator in error_lower for indicator in [
        "overloaded", "not ready", "high demand", "unavailable",
        "try again later", "service unavailable",
    ])


def _is_invalid_api_key_error(error: BaseException) -> bool:
    """Check if error indicates a malformed, invalid, or rejected API key.

    These errors occur when the API key has invalid characters, is corrupted,
    or is otherwise malformed. The key should be permanently marked as invalid.
    A 401/403 is treated the same way: the request will never succeed on retry.
    """
    if _status_code(error) in _AUTH_STATUS_CODES:
        return True
    error_lower = str(error).lower()

    # gRPC plugin credential errors (malformed key with illegal characters)
    if "illegal header value" in error_lower or "invalid metadata" in error_lower:
        return True

    # Generic invalid API key errors
    invalid_key_indicators = [
        "invalid api key", "api key not valid", "api_key_invalid",
        "invalid credential", "authentication failed", "unauthorized",
        "permission denied", "invalid_api_key", "api key is invalid"
    ]

    return any(indicator in error_lower for indicator in invalid_key_indicators)

_MIN_RETRY_WAIT_SECONDS = 10
_MAX_RETRY_WAIT_SECONDS = 120
# google.rpc.RetryInfo in JSON form; snake_case appears in gRPC-style payloads.
_RETRY_DELAY_KEYS = ("retryDelay", "retry_delay")
# A protobuf Duration serialized as JSON, e.g. "54s" or "1.5s".
_DURATION_RE = re.compile(r"\s*(\d+(?:\.\d+)?)s?\s*")


def _clamp_wait(seconds: float) -> int:
    """Keep a retry wait within sane bounds.

    A malformed or extreme hint should not produce a near-instant retry that
    burns an attempt, nor stall a request for hours. The bounds are wide enough
    that they never bind on the fallback paths below.
    """
    return int(min(max(seconds, _MIN_RETRY_WAIT_SECONDS), _MAX_RETRY_WAIT_SECONDS))


def _parse_duration(value: Any) -> Optional[float]:
    """Parse a protobuf Duration in its JSON form. Returns None if unparseable."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        match = _DURATION_RE.fullmatch(value)
        if match:
            return float(match.group(1))
    return None


def _retry_delay_from_details(error: BaseException) -> Optional[float]:
    """Read the provider's own retry delay from a structured error payload.

    A 429 carries ``google.rpc.RetryInfo`` inside ``APIError.details``, which is
    the API telling us exactly when the quota window reopens. The nesting depth
    varies by transport, so the payload is walked rather than indexed by a fixed
    path. Returns None when no delay is present.
    """
    stack = [getattr(error, "details", None)]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key in _RETRY_DELAY_KEYS:
                seconds = _parse_duration(node.get(key))
                if seconds is not None:
                    return seconds
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return None


def _extract_wait_time(error: BaseException) -> int:
    """Seconds to wait before retrying a rate-limited request.

    The provider's RetryInfo wins when present. The text patterns below remain
    for SDKs that only put the delay in the message, and the fixed defaults are
    the last resort.
    """
    provider_delay = _retry_delay_from_details(error)
    if provider_delay is not None:
        # Small buffer so the retry lands after the window reopens, not on it.
        return _clamp_wait(provider_delay + random.randint(5, 10))

    error_str = str(error)

    retry_match = re.search(r"retry in (\d+(?:\.\d+)?)s", error_str.lower())
    if retry_match:
        return _clamp_wait(float(retry_match.group(1)) + random.randint(5, 10))

    delay_match = re.search(r"retry_delay.*?seconds:\s*(\d+)", error_str)
    if delay_match:
        return _clamp_wait(int(delay_match.group(1)) + random.randint(5, 10))

    # For per-minute limits, default to longer wait
    if "per minute" in error_str.lower():
        return _clamp_wait(90 + random.randint(5, 15))

    # Default fallback
    return _clamp_wait(45 + random.randint(5, 15))

##############################################################################
# Base class (copied from scaffold for convenience – delete if already there)
##############################################################################

class LLMInterface(ABC):
    """Minimal wrapper so core code is backend-agnostic."""

    # Subclasses set this to their provider name (e.g. "gemini", "openai").
    _provider: str = "unknown"

    @abstractmethod
    def __init__(self, **backend_kwargs):
        self.backend_kwargs = backend_kwargs

    def generate(self, prompt: str, **kwargs) -> str:           # noqa: D401
        """Return a raw text completion for *prompt*."""
        raise NotImplementedError

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        """Resolve ``max_output_tokens`` for a specific *task*.

        Uses ``TASK_TOKEN_BUDGETS`` from ``model_specs``, capped by the
        model's hard limit.  ``task=None`` returns the model's full max.
        """
        model = getattr(self, "model", "") or getattr(self, "model_name", "")
        return get_max_output_tokens(self._provider, model, task=task)


##############################################################################
# 1. Together AI implementation                                              #
##############################################################################

class TogetherLLM(LLMInterface):
    """
     llm = TogetherLLM(model="meta-llama/Llama-3-8b-chat-hf")
     answer = llm.generate("What is the capital of France?")
    """
    _provider = "together"

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        max_output_tokens: Optional[int] = None,
        temperature: float = 0.3,
        context_window_size: Optional[int] = None,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.api_key = api_key or os.getenv("TOGETHER_API_KEY")
        if not self.api_key:
            raise ValueError("Together AI key missing. Set TOGETHER_API_KEY.")

        # Auto-detect token limits from model specs
        spec = get_model_spec("together", model)
        self.max_output_tokens = max_output_tokens if max_output_tokens is not None else spec.max_output_tokens
        self.context_window_size = context_window_size if context_window_size is not None else spec.context_window

        try:
            from together import Together   # import locally to keep deps optional
        except ImportError as e:
            raise ImportError(
                "pip install together-python "
                "(https://pypi.org/project/together/)") from e

        self._client = Together(api_key=self.api_key)
        self._default_args: Dict[str, Any] = dict(
            model=self.model,
            max_tokens=self.max_output_tokens,  # Together API uses max_tokens
            temperature=temperature,
        )
        # Removed API key printing for security


    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt → wrapped as [{'role':'user', 'content': prompt}]
            • list – already‑formatted chat messages (role/content pairs)
        """
        # Calculate prompt length for tracking
        prompt_len = sum(len(m.get("content", "")) for m in (prompt if isinstance(prompt, list) else [{"content": prompt}]))

        params = {**self._default_args, **kwargs}

        # Detect format
        if isinstance(prompt, list):
            params["messages"] = prompt        # already chat‑style
        else:
            params["messages"] = [{"role": "user", "content": prompt}]

        # Retry logic for rate limits
        max_retries = 3
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                resp = self._client.chat.completions.create(**params)
                content = resp.choices[0].message.content.strip()
                
                # Track LLM call after success
                LLMCallTracker.get_instance().increment(
                    model=self.model, 
                    prompt_length=prompt_len,
                    completion_length=len(content)
                )
                return content
            except Exception as e:
                error_str = str(e)
                last_exception = e
                
                # Check if this is a retryable error
                if _is_rate_limit_error(e):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(e)
                        print(f"🚦 Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Rate limit error after {max_retries} retries: {error_str}")
                elif _is_retryable_server_error(e):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"🔄 Transient provider error (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Transient provider error after {max_retries} retries: {error_str}")
                else:
                    # Not a retryable error, don't retry
                    break
        
        # Re-raise the last exception
        raise last_exception


##############################################################################
# 2. OpenAI implementation                                                   #
##############################################################################

class OpenAILLM(LLMInterface):
    """
     llm = OpenAILLM(model="gpt-4o-mini")
     answer = llm.generate("List three Israeli cities.")
    """
    _provider = "openai"

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        max_output_tokens: Optional[int] = None,
        temperature: float = 0.3,
        context_window_size: Optional[int] = None,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI key missing. Set OPENAI_API_KEY.")

        # Auto-detect token limits from model specs
        spec = get_model_spec("openai", model)
        self.max_output_tokens = max_output_tokens if max_output_tokens is not None else spec.max_output_tokens
        self.context_window_size = context_window_size if context_window_size is not None else spec.context_window

        try:
            import openai  # noqa: F401
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "pip install openai>=1.0.0  (https://pypi.org/project/openai/)") from e

        self._client = OpenAI(api_key=self.api_key)
        self._default_args: Dict[str, Any] = dict(
            model=self.model,
            max_tokens=self.max_output_tokens,  # OpenAI API uses max_tokens
            temperature=temperature,
        )

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt → wrapped as [{'role':'user', 'content': prompt}]
            • list – already‑formatted chat messages (role/content pairs)
        """
        # Calculate prompt length for tracking
        prompt_len = sum(len(m.get("content", "")) for m in (prompt if isinstance(prompt, list) else [{"content": prompt}]))

        params = {**self._default_args, **kwargs}

        # Detect format
        if isinstance(prompt, list):
            params["messages"] = prompt        # already chat‑style
        else:
            params["messages"] = [{"role": "user", "content": prompt}]

        # Retry logic for rate limits and server overload
        max_retries = 3
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                resp = self._client.chat.completions.create(**params)
                content = resp.choices[0].message.content.strip()
                
                # Track LLM call after success
                LLMCallTracker.get_instance().increment(
                    model=self.model, 
                    prompt_length=prompt_len,
                    completion_length=len(content)
                )
                return content
            except Exception as e:
                error_str = str(e)
                last_exception = e
                
                # Check if this is a retryable error
                if _is_rate_limit_error(e):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(e)
                        print(f"🚦 Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Rate limit error after {max_retries} retries: {error_str}")
                elif _is_retryable_server_error(e):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"🔄 Transient provider error (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Transient provider error after {max_retries} retries: {error_str}")
                else:
                    # Not a retryable error, don't retry
                    break
        
        # Re-raise the last exception
        raise last_exception

##############################################################################
# 3. HuggingFace Transformers implementation (with quantization)           #
##############################################################################

class HuggingFaceLLM(LLMInterface):
    """
     llm = HuggingFaceLLM(model="meta-llama/Llama-3.3-70B-Instruct")
     answer = llm.generate("What's the meaning of life?")
    """
    _provider = "hf"

    def __init__(
        self,
        model: str,
        max_output_tokens: int = 1024,
        temperature: float = 0.3,
        device: Optional[str] = None,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig, pipeline
        except ImportError as e:
            raise ImportError(
                "pip install transformers accelerate"
                "(https://pypi.org/project/transformers/)") from e

        self.model_name = model
        self.max_output_tokens = max_output_tokens
        self.temperature = temperature
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")

        # Check number of parameters from config
        config = AutoConfig.from_pretrained(model)
        n_params = config.hidden_size * config.num_hidden_layers * config.vocab_size
        use_quant = n_params > 20e9  # >20B

        quant_args = {}
        if use_quant:
            try:
                from transformers import BitsAndBytesConfig
            except ImportError:
                raise ImportError("pip install bitsandbytes")

            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_quant_type="nf4"
            )

            quant_args = {
                "quantization_config": bnb_config,
                "device_map": "auto",
            }
        else:
            quant_args = {
                "torch_dtype": "auto",
                "device_map": "auto" if self.device == "cuda" else None,
            }

        token = os.getenv("HF_TOKEN")

        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, token=token)
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            token=token,
            **quant_args
        )
        self.generator = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
        )

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages (merged to prompt text)
        """
        # Calculate prompt length for tracking
        prompt_len = sum(len(m.get("content", "")) for m in (prompt if isinstance(prompt, list) else [{"content": prompt}]))

        if isinstance(prompt, list):
            # Convert messages to plain prompt text
            prompt = "\n".join([f"{m['role']}: {m['content']}" for m in prompt])

        gen_args = {
            "max_new_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "do_sample": True,
            "return_full_text": False,
        }

        output = self.generator(prompt, **gen_args)[0]["generated_text"].strip()
        
        # Track LLM call after success
        LLMCallTracker.get_instance().increment(
            model=self.model_name, 
            prompt_length=prompt_len,
            completion_length=len(output)
        )
        
        return output


##############################################################################
# 4. Google Gemini implementation (using new google.genai SDK)              #
##############################################################################

class GeminiLLM(LLMInterface):
    """
    Gemini LLM using the new google.genai SDK.

    The old google.generativeai SDK was deprecated (support ended Nov 30, 2025).
    This implementation uses the new google-genai package with client-based architecture.

    Usage:
        llm = GeminiLLM()  # Uses gemini-3.1-flash-lite by default
        answer = llm.generate("What is the capital of France?")

    API Key Loading:
        1. Explicit api_key parameter
        2. GEMINI_API_KEY environment variable

    Token Limits:
        max_output_tokens and context_window_size are auto-detected from model specs
        when not explicitly provided. Override with explicit values if needed.
    """
    _provider = "gemini"

    def __init__(
        self,
        model: str = ModelNames.DEFAULT_VALUE_EXTRACTION,
        api_key: Optional[str] = None,
        max_output_tokens: Optional[int] = None,
        temperature: float = 0.3,
        context_window_size: Optional[int] = None,
        system_prefix: Optional[str] = None,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.temperature = temperature
        self.system_prefix = system_prefix

        # Auto-detect token limits and capabilities from model specs
        spec = get_model_spec("gemini", model)
        self.max_output_tokens = max_output_tokens if max_output_tokens is not None else spec.max_output_tokens
        self.context_window_size = context_window_size if context_window_size is not None else spec.context_window
        # Whether this model accepts thinking_config. Lite models reject it when
        # combined with response_schema, causing 400 INVALID_ARGUMENT.
        self.supports_thinking = spec.supports_thinking
        # Gemini 3.x+ models use the thinking_level enum instead of the legacy
        # integer thinking_budget. See _apply_thinking_config for the translation.
        self.uses_thinking_level = spec.uses_thinking_level
        # Per-model sampling and thinking-level facts. The Gemini 3.x family is
        # not uniform, so these come from the spec rather than one shared rule.
        self.supports_sampling_params = spec.supports_sampling_params
        self.allowed_thinking_levels = spec.allowed_thinking_levels
        self.fallback_thinking_level = spec.fallback_thinking_level

        # Load single API key
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")

        # Backward compatibility: if comma-separated, use first key
        if self.api_key and ',' in self.api_key:
            first_key = self.api_key.split(',')[0].strip()
            print("Warning: Multiple keys detected. Using first key only (multi-key support removed).")
            self.api_key = first_key

        if not self.api_key:
            raise ValueError("Gemini API key missing. Set GEMINI_API_KEY.")

        # Validate key format
        if not self._validate_api_key(self.api_key, "GEMINI_API_KEY"):
            raise ValueError("Invalid Gemini API key format.")

        # Import new SDK
        try:
            from google import genai
            from google.genai import types
            self.genai = genai
            self.types = types
        except ImportError as e:
            raise ImportError(
                "pip install google-genai "
                "(https://pypi.org/project/google-genai/)") from e

        # Create client with API key (new SDK uses client-based architecture)
        self._client = genai.Client(api_key=self.api_key)

        # Configure safety settings to be less restrictive for scientific content
        self.safety_settings = [
            types.SafetySetting(
                category="HARM_CATEGORY_HARASSMENT",
                threshold="BLOCK_ONLY_HIGH"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_HATE_SPEECH",
                threshold="BLOCK_ONLY_HIGH"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                threshold="BLOCK_ONLY_HIGH"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_DANGEROUS_CONTENT",
                threshold="BLOCK_ONLY_HIGH"
            ),
        ]

        # ScheMatiQ uses plain text generation only — disable SDK default AFC.
        self._disable_automatic_function_calling = (
            types.AutomaticFunctionCallingConfig(disable=True)
        )

    def _validate_api_key(self, key: str, key_source: str = "unknown") -> bool:
        """Validate that an API key doesn't have invalid characters.

        gRPC will crash with 'Illegal header value' if the key contains
        newlines, non-ASCII characters, or other invalid header chars.
        """
        if not key:
            return False

        # Check for newlines (common issue with env vars)
        if '\n' in key or '\r' in key:
            print(f"API key from {key_source} contains newline characters")
            return False

        # Check for non-printable characters
        if not key.isprintable():
            print(f"API key from {key_source} contains non-printable characters")
            return False

        # Check for spaces (API keys shouldn't have spaces)
        if ' ' in key:
            print(f"API key from {key_source} contains spaces")
            return False

        # Check for common invalid chars in HTTP headers
        invalid_chars = set('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f')
        if any(c in invalid_chars for c in key):
            print(f"API key from {key_source} contains control characters")
            return False

        return True

    # Map a legacy integer thinking_budget onto a Gemini 3.x thinking_level.
    # (max_budget_inclusive, level). Anything larger maps to "high". Used only
    # to translate a caller's budget hint; the result is then clamped to the
    # levels the specific model accepts.
    _THINKING_LEVEL_THRESHOLDS = (
        (2048, "low"),
        (8192, "medium"),
    )

    def _thinking_budget_to_level(self, budget: int) -> str:
        """Translate a legacy thinking_budget hint to a Gemini 3.x thinking_level.

        The requested level is constrained to ``allowed_thinking_levels`` for this
        model, because the set differs per model and sending an unsupported value
        returns 400 (gemini-3-pro rejects "medium", for example). When the model
        declares no allowed set, the translated level is used unchanged.
        """
        level = "high"
        for max_budget, candidate in self._THINKING_LEVEL_THRESHOLDS:
            if budget <= max_budget:
                level = candidate
                break

        allowed = self.allowed_thinking_levels
        if not allowed or level in allowed:
            return level

        # Substitute this model's fallback, then the nearest level it does
        # accept, so a budget hint can never produce a 400.
        if self.fallback_thinking_level and self.fallback_thinking_level in allowed:
            return self.fallback_thinking_level
        return allowed[-1]

    def _apply_thinking_config(self, config_kwargs: dict, thinking_budget) -> None:
        """Attach thinking config and prune sampling params for this model.

        Both behaviours come from the model's ``ModelSpec`` rather than from a
        single Gemini-3-wide assumption, because the family is not uniform: the
        accepted ``thinking_level`` values and the default level differ per model.

        ``supports_sampling_params=False`` drops temperature/top_p/top_k. Gemini
        3.x deprecated them, the API ignores them today, and future generations
        return 400. Dropping them here means a caller asking for temperature=0
        is not silently told it took effect. Models that still honour the
        sampling params keep them. Mutates ``config_kwargs`` in place.
        """
        if not self.supports_sampling_params:
            for stale in ("temperature", "top_p", "top_k"):
                config_kwargs.pop(stale, None)

        if self.uses_thinking_level:
            # No budget means no thinking_config, same as before: let the model
            # apply its own default rather than pinning a level here.
            if thinking_budget is not None:
                config_kwargs["thinking_config"] = self.types.ThinkingConfig(
                    thinking_level=self._thinking_budget_to_level(thinking_budget)
                )
        elif thinking_budget is not None and self.supports_thinking:
            config_kwargs["thinking_config"] = self.types.ThinkingConfig(
                thinking_budget=thinking_budget
            )

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Generate a response from Gemini with retry logic.

        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages with role/content pairs.
              System messages are extracted as system_instruction,
              user/assistant messages become the prompt content.
        **kwargs:
            thinking_budget : int | None – Gemini thinking budget (0 = no thinking)
            response_schema : dict | None – Gemini controlled generation schema
            max_output_tokens : int | None – override max output tokens
            temperature : float | None – override temperature
        """
        # Calculate prompt length for tracking
        prompt_len = sum(len(m.get("content", "")) for m in (prompt if isinstance(prompt, list) else [{"content": prompt}]))

        # Separate system instructions from user content when messages are provided
        system_instruction = None
        if isinstance(prompt, list):
            system_parts = [m["content"] for m in prompt if m["role"] == "system"]
            user_parts = [m["content"] for m in prompt if m["role"] != "system"]
            if system_parts:
                system_instruction = "\n\n".join(system_parts)
            prompt_text = "\n\n".join(user_parts)
        else:
            prompt_text = prompt

        # Prepend system_prefix if configured
        if self.system_prefix:
            if system_instruction:
                system_instruction = f"{self.system_prefix}\n\n{system_instruction}"
            else:
                system_instruction = self.system_prefix

        # Log prompt size for performance correlation
        print(f"🚀 Starting Gemini API call (model: {self.model}, prompt: ~{len(prompt_text):,} chars)")
        start_time = time.time()

        # Build generation config using new SDK types
        config_kwargs = {
            "max_output_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "safety_settings": self.safety_settings,
            "automatic_function_calling": self._disable_automatic_function_calling,
        }
        # Add system instruction
        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction
        # Add controlled generation if response_schema provided
        if kwargs.get("response_schema") is not None:
            config_kwargs["response_mime_type"] = "application/json"
            config_kwargs["response_schema"] = kwargs["response_schema"]
        # Attach thinking config and prune sampling params per model family.
        # Gemini 3.x uses thinking_level and rejects temperature/top_p/top_k;
        # earlier Gemini keeps the legacy integer thinking_budget. Lite models
        # reject thinking_config with 400 INVALID_ARGUMENT when combined with
        # response_schema, which is handled inside the helper via supports_thinking.
        self._apply_thinking_config(config_kwargs, kwargs.get("thinking_budget"))
        config = self.types.GenerateContentConfig(**config_kwargs)

        # Retry logic (3 retries like OpenAI/Together)
        max_retries = 3
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                # New SDK API: client.models.generate_content()
                response = self._client.models.generate_content(
                    model=self.model,
                    contents=prompt_text,
                    config=config,
                )

                elapsed = time.time() - start_time
                print(f"⏱️  Gemini API call completed in {elapsed:.1f}s")

                # Handle safety filtering or empty responses
                if not response.candidates:
                    feedback = getattr(response, 'prompt_feedback', None)
                    print(f"Gemini returned no candidates. Feedback: {feedback}")
                    return "No response generated due to safety filters or other restrictions."

                candidate = response.candidates[0]
                finish_reason = getattr(candidate, 'finish_reason', None)
                finish_reason_name = finish_reason.name if hasattr(finish_reason, 'name') else str(finish_reason)

                # Log non-STOP finish reasons (including MAX_TOKENS) but continue
                if finish_reason and finish_reason_name != "STOP":
                    if finish_reason_name == "MAX_TOKENS":
                        logger.warning(
                            "⚠️  Gemini output TRUNCATED (finish_reason=MAX_TOKENS, model=%s, "
                            "max_output_tokens=%s). Response is likely incomplete — consider "
                            "increasing max_output_tokens.",
                            self.model,
                            kwargs.get("max_output_tokens", self.max_output_tokens),
                        )
                    else:
                        logger.warning(
                            "Gemini finish_reason=%s (model=%s)",
                            finish_reason_name, self.model,
                        )

                # Check for empty content
                if not candidate.content or not candidate.content.parts:
                    print("Gemini returned empty content")
                    return "Empty response from Gemini."

                content = response.text.strip()
                
                # Track LLM call after success
                LLMCallTracker.get_instance().increment(
                    model=self.model, 
                    prompt_length=prompt_len,
                    completion_length=len(content)
                )
                
                return content

            except Exception as e:
                error_str = str(e)
                last_exception = e
                elapsed = time.time() - start_time
                logger.exception(
                    "Gemini API call failed after %.1fs (model=%s, prompt_chars=%d)",
                    elapsed,
                    self.model,
                    len(prompt_text),
                )
                print(repr(e))

                # Handle safety filter errors specifically - don't retry
                if "Invalid operation" in error_str and "finish_reason" in error_str:
                    print(f"Gemini safety filter triggered: {error_str}")
                    return "Response blocked by Gemini safety filters. Please try rephrasing your request."

                # Check for invalid/malformed API key errors - don't retry
                if _is_invalid_api_key_error(e):
                    print(f"Invalid/malformed API key: {error_str[:200]}")
                    raise

                # Check if this is a retryable error
                if _is_rate_limit_error(e):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(e)
                        snippet = error_str.replace("\n", " ")[:280]
                        print(
                            f"[Gemini rate limit] attempt {attempt + 1}/{max_retries + 1}, "
                            f"retry in {wait_time}s — provider API quota/RPM (not ScheMatiQ LLM_CALL_GLOBAL_LIMIT). "
                            f"error: {snippet}",
                            flush=True,
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        snippet = error_str.replace("\n", " ")[:500]
                        print(
                            f"[Gemini rate limit] gave up after {max_retries} retries — "
                            f"provider quota/RPM or burst limit (not app LLM_CALL_GLOBAL_LIMIT). "
                            f"Last error: {snippet}",
                            flush=True,
                        )
                elif _is_retryable_server_error(e):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"Transient provider error (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Transient provider error after {max_retries} retries: {error_str}")
                else:
                    # Not a retryable error, don't retry
                    break

        # Re-raise the last exception (GeminiLLM.generate)
        raise last_exception

    @staticmethod
    def _extract_grounding_sources(candidate: Any) -> List[Dict[str, str]]:
        """Return de-duplicated web sources from Gemini grounding metadata."""
        metadata = getattr(candidate, "grounding_metadata", None)
        chunks = getattr(metadata, "grounding_chunks", None) or []
        sources: List[Dict[str, str]] = []
        seen = set()
        for chunk in chunks:
            web = getattr(chunk, "web", None)
            uri = getattr(web, "uri", None) if web is not None else None
            if not uri or uri in seen:
                continue
            seen.add(uri)
            sources.append({
                "url": str(uri),
                "title": str(getattr(web, "title", None) or uri),
            })
        return sources

    def generate_grounded(
        self,
        prompt: Union[str, List[Dict[str, str]]],
        **kwargs,
    ) -> Tuple[str, List[Dict[str, str]]]:
        """Generate with Google Search enabled and return text plus web sources.

        This is deliberately separate from :meth:`generate`: ordinary document
        extraction never receives a search tool, so its document-only guarantee
        is enforced structurally rather than by prompt wording.
        """
        prompt_len = sum(
            len(message.get("content", ""))
            for message in (
                prompt if isinstance(prompt, list) else [{"content": prompt}]
            )
        )
        system_instruction = None
        if isinstance(prompt, list):
            system_parts = [m["content"] for m in prompt if m["role"] == "system"]
            user_parts = [m["content"] for m in prompt if m["role"] != "system"]
            if system_parts:
                system_instruction = "\n\n".join(system_parts)
            prompt_text = "\n\n".join(user_parts)
        else:
            prompt_text = prompt

        if self.system_prefix:
            system_instruction = (
                f"{self.system_prefix}\n\n{system_instruction}"
                if system_instruction
                else self.system_prefix
            )

        config_kwargs = {
            "max_output_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "safety_settings": self.safety_settings,
            "tools": [self.types.Tool(google_search=self.types.GoogleSearch())],
        }
        if system_instruction:
            config_kwargs["system_instruction"] = system_instruction
        if kwargs.get("response_schema") is not None:
            config_kwargs["response_mime_type"] = "application/json"
            config_kwargs["response_schema"] = kwargs["response_schema"]
        self._apply_thinking_config(config_kwargs, kwargs.get("thinking_budget"))
        config = self.types.GenerateContentConfig(**config_kwargs)

        max_retries = 3
        last_exception = None
        start_time = time.time()
        for attempt in range(max_retries + 1):
            try:
                response = self._client.models.generate_content(
                    model=self.model,
                    contents=prompt_text,
                    config=config,
                )
                if not response.candidates:
                    return "", []
                candidate = response.candidates[0]
                if not candidate.content or not candidate.content.parts:
                    return "", []

                content = response.text.strip()
                sources = self._extract_grounding_sources(candidate)
                LLMCallTracker.get_instance().increment(
                    model=self.model,
                    prompt_length=prompt_len,
                    completion_length=len(content),
                )
                return content, sources
            except Exception as exc:
                last_exception = exc
                error_str = str(exc)
                logger.exception(
                    "Grounded Gemini call failed after %.1fs (model=%s, prompt_chars=%d)",
                    time.time() - start_time,
                    self.model,
                    len(prompt_text),
                )
                if _is_invalid_api_key_error(exc):
                    raise
                if _is_rate_limit_error(exc) and attempt < max_retries:
                    time.sleep(_extract_wait_time(exc))
                    continue
                if _is_retryable_server_error(exc) and attempt < max_retries:
                    time.sleep(10 + random.randint(5, 15))
                    continue
                if "Invalid operation" in error_str and "finish_reason" in error_str:
                    return "", []
                break

        raise last_exception

    # ── Context Caching ──────────────────────────────────────────────

    def create_context_cache(self, system_instruction: str, document_text: str, ttl_seconds: int = 1800):
        """Create a context cache for system prompt + document.

        Returns cache object or None on failure.
        Requires minimum ~1,024 tokens of cached content for Flash models.
        """
        try:
            cache = self._client.caches.create(
                model=self.model,
                config=self.types.CreateCachedContentConfig(
                    system_instruction=system_instruction,
                    contents=[document_text],
                    ttl=f"{ttl_seconds}s",
                )
            )
            logger.info(
                "Gemini context cache CREATED (model=%s, doc_chars=%d, ttl=%ds, cache=%s)",
                self.model, len(document_text), ttl_seconds, cache.name,
            )
            return cache
        except Exception as e:
            logger.warning("Gemini context cache creation FAILED (model=%s, doc_chars=%d): %s", self.model, len(document_text), e)
            return None

    def delete_context_cache(self, cache):
        """Delete a context cache. Best-effort cleanup."""
        try:
            if cache:
                self._client.caches.delete(name=cache.name)
                logger.info("Gemini context cache DELETED (cache=%s)", cache.name)
        except Exception as e:
            logger.debug("Gemini context cache delete failed (cache=%s): %s", cache.name, e)

    def generate_with_cache(self, prompt: str, cache, **kwargs) -> str:
        """Generate using a cached context. Falls back to regular generate if cache is None."""
        if cache is None:
            return self.generate(prompt, **kwargs)

        prompt_len = len(prompt) if isinstance(prompt, str) else sum(
            len(m.get("content", "")) for m in prompt
        )

        # When using cache, prompt should be plain text (user query part only)
        if isinstance(prompt, list):
            prompt_text = "\n\n".join(
                m["content"] for m in prompt if m["role"] != "system"
            )
        else:
            prompt_text = prompt

        logger.info(
            "Gemini cached API call START (model=%s, cache=%s, prompt_chars=%d)",
            self.model, cache.name, len(prompt_text),
        )
        print(f"🚀 Starting Gemini cached API call (model: {self.model}, prompt: ~{len(prompt_text):,} chars)")
        start_time = time.time()

        config_kwargs = {
            "max_output_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "safety_settings": self.safety_settings,
            "automatic_function_calling": self._disable_automatic_function_calling,
            "cached_content": cache.name,
        }
        if kwargs.get("response_schema") is not None:
            config_kwargs["response_mime_type"] = "application/json"
            config_kwargs["response_schema"] = kwargs["response_schema"]
        self._apply_thinking_config(config_kwargs, kwargs.get("thinking_budget"))
        config = self.types.GenerateContentConfig(**config_kwargs)

        max_retries = 3
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                response = self._client.models.generate_content(
                    model=self.model,
                    contents=prompt_text,
                    config=config,
                )

                elapsed = time.time() - start_time
                print(f"⏱️  Gemini cached API call completed in {elapsed:.1f}s")

                if not response.candidates:
                    return "No response generated due to safety filters or other restrictions."

                candidate = response.candidates[0]
                finish_reason = getattr(candidate, 'finish_reason', None)
                finish_reason_name = finish_reason.name if hasattr(finish_reason, 'name') else str(finish_reason)
                if finish_reason and finish_reason_name != "STOP":
                    if finish_reason_name == "MAX_TOKENS":
                        logger.warning(
                            "⚠️  Gemini cached output TRUNCATED (finish_reason=MAX_TOKENS, "
                            "model=%s, max_output_tokens=%s). Response is likely incomplete — "
                            "consider increasing max_output_tokens.",
                            self.model,
                            kwargs.get("max_output_tokens", self.max_output_tokens),
                        )
                    else:
                        logger.warning(
                            "Gemini cached call finish_reason=%s (model=%s)",
                            finish_reason_name, self.model,
                        )

                if not candidate.content or not candidate.content.parts:
                    return "Empty response from Gemini."

                content = response.text.strip()

                LLMCallTracker.get_instance().increment(
                    model=self.model,
                    prompt_length=prompt_len,
                    completion_length=len(content)
                )
                return content

            except Exception as e:
                error_str = str(e)
                last_exception = e
                elapsed = time.time() - start_time
                logger.exception(
                    "Gemini cached API call failed after %.1fs (model=%s, prompt_chars=%d)",
                    elapsed,
                    self.model,
                    len(prompt_text),
                )
                print(repr(e))

                if "Invalid operation" in error_str and "finish_reason" in error_str:
                    return "Response blocked by Gemini safety filters."
                if _is_invalid_api_key_error(e):
                    raise
                if _is_rate_limit_error(e):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(e)
                        snippet = error_str.replace("\n", " ")[:280]
                        print(
                            f"[Gemini rate limit/cached] attempt {attempt + 1}/{max_retries + 1}, "
                            f"retry in {wait_time}s — provider API quota/RPM (not ScheMatiQ LLM_CALL_GLOBAL_LIMIT). "
                            f"error: {snippet}",
                            flush=True,
                        )
                        time.sleep(wait_time)
                        continue
                    snippet = error_str.replace("\n", " ")[:500]
                    print(
                        f"[Gemini rate limit/cached] gave up after {max_retries} retries — "
                        f"provider quota/RPM (not app LLM_CALL_GLOBAL_LIMIT). Last error: {snippet}",
                        flush=True,
                    )
                elif _is_retryable_server_error(e):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"Transient provider error (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                else:
                    break

        # Re-raise the last exception (GeminiLLM.generate_with_cache)
        raise last_exception
