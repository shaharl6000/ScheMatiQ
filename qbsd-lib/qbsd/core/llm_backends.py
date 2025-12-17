# llm_backends.py
"""
Concrete implementations of `LLMInterface` for Together AI and OpenAI.
The API keys are pulled from standard environment variables by default:
    OPENAI_API_KEY       – OpenAI
    TOGETHER_API_KEY     – Together AI
"""

from __future__ import annotations
import os
import time
import random
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union
import re


##############################################################################
# Custom Exceptions                                                          #
##############################################################################

class AllKeysFailedError(Exception):
    """Raised when all API keys have failed and no more retries are possible.

    This exception signals that the caller should save any data and exit gracefully.
    """
    def __init__(self, message: str = "All API keys exhausted or invalid",
                 keys_tried: int = 0, last_error: str = None):
        self.message = message
        self.keys_tried = keys_tried
        self.last_error = last_error
        super().__init__(self.message)


##############################################################################
# Rate limit retry utilities                                                 #
##############################################################################

def _is_rate_limit_error(error_str: str) -> bool:
    """Check if error is a rate limit error (429)."""
    return "429" in error_str and ("rate limit" in error_str.lower() or "rate_limit" in error_str.lower())

def _is_quota_exhausted_error(error_str: str) -> bool:
    """Check if error is a quota exhausted (RPD/daily) error - requires key switch.

    ULTRA-CONSERVATIVE: Only return True if error EXPLICITLY mentions "daily".
    Any ambiguous "exhausted" error is treated as temporary rate limit, NOT RPD.

    This prevents false positives that would incorrectly mark keys as dead for 24h.
    """
    error_lower = error_str.lower()

    # Must be a 429 error
    if "429" not in error_str:
        return False

    # ONLY mark as RPD if error EXPLICITLY mentions "daily" or "per day"
    # This is the ONLY way to be certain it's a daily quota, not per-minute
    explicit_daily_indicators = [
        "daily",        # "daily quota", "daily limit", etc.
        "per day",      # "requests per day", "per day limit", etc.
        "rpd"           # Explicit RPD mention
    ]

    for indicator in explicit_daily_indicators:
        if indicator in error_lower:
            print(f"🔍 RPD detection: Found '{indicator}' in error - confirming as daily quota")
            return True

    # Everything else is NOT confirmed as daily quota
    # Generic "exhausted", "quota", "billing" etc. could be per-minute limits
    return False

def _is_rpm_error(error_str: str) -> bool:
    """Check if error is a rate limit that should be handled with waiting/rotation.

    CATCH-ALL: Any 429 error that isn't confirmed as daily quota (RPD)
    is treated as a temporary rate limit (likely RPM) that will reset shortly.

    This ensures we never incorrectly mark keys as dead when it's just a
    per-minute limit that will reset in 60 seconds.
    """
    error_lower = error_str.lower()

    # Must be a 429 error
    if "429" not in error_str:
        return False

    # If it's NOT a confirmed daily quota error, treat as RPM (temporary)
    # This is the safe default - wait and retry instead of marking key dead
    if not _is_quota_exhausted_error(error_str):
        return True

    return False

def _is_server_overloaded_error(error_str: str) -> bool:
    """Check if error is a server overloaded error (503)."""
    return "503" in error_str and ("overloaded" in error_str.lower() or "not ready" in error_str.lower())


def _is_invalid_api_key_error(error_str: str) -> bool:
    """Check if error indicates a malformed or invalid API key.

    These errors occur when the API key has invalid characters, is corrupted,
    or is otherwise malformed. The key should be permanently marked as invalid.
    """
    error_lower = error_str.lower()

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

def _extract_wait_time(error_str: str) -> int:
    """Extract wait time from rate limit error or return default."""
    # Try to extract actual retry delay from Gemini error
    retry_match = re.search(r"retry in ([\d.]+)s", error_str.lower())
    if retry_match:
        try:
            retry_seconds = float(retry_match.group(1))
            # Add small buffer to avoid immediate retry
            return int(retry_seconds) + random.randint(5, 10)
        except (ValueError, IndexError):
            pass
    
    # Try to extract from retry_delay field 
    delay_match = re.search(r"retry_delay.*?seconds:\s*(\d+)", error_str)
    if delay_match:
        try:
            delay_seconds = int(delay_match.group(1))
            return delay_seconds + random.randint(5, 10)
        except (ValueError, IndexError):
            pass
    
    # For per-minute limits, default to longer wait
    if "per minute" in error_str.lower():
        return 90 + random.randint(5, 15)
    
    # Default fallback
    return 45 + random.randint(5, 15)

##############################################################################
# Base class (copied from scaffold for convenience – delete if already there)
##############################################################################

class LLMInterface(ABC):
    """Minimal wrapper so core code is backend-agnostic."""

    @abstractmethod
    def __init__(self, **backend_kwargs):
        self.backend_kwargs = backend_kwargs

    def generate(self, prompt: str, **kwargs) -> str:           # noqa: D401
        """Return a raw text completion for *prompt*."""
        raise NotImplementedError


##############################################################################
# 1. Together AI implementation                                              #
##############################################################################

class TogetherLLM(LLMInterface):
    """
     llm = TogetherLLM(model="meta-llama/Llama-3-8b-chat-hf")
     answer = llm.generate("What is the capital of France?")
    """

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        max_output_tokens: int = 1024,
        temperature: float = 0.3,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.api_key = api_key or os.getenv("TOGETHER_API_KEY")
        if not self.api_key:
            raise ValueError("Together AI key missing. Set TOGETHER_API_KEY.")
        try:
            from together import Together   # import locally to keep deps optional
        except ImportError as e:
            raise ImportError(
                "pip install together-python "
                "(https://pypi.org/project/together/)") from e

        self._client = Together(api_key=self.api_key)
        self._default_args: Dict[str, Any] = dict(
            model=self.model,
            max_tokens=max_output_tokens,  # Together API uses max_tokens
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
                return resp.choices[0].message.content.strip()
            except Exception as e:
                error_str = str(e)
                last_exception = e
                
                # Check if this is a retryable error
                if _is_rate_limit_error(error_str):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(error_str)
                        print(f"🚦 Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Rate limit error after {max_retries} retries: {error_str}")
                elif _is_server_overloaded_error(error_str):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"🔄 Server overloaded (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Server overloaded error after {max_retries} retries: {error_str}")
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

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        max_output_tokens: int = 1024,
        temperature: float = 0.3,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI key missing. Set OPENAI_API_KEY.")
        try:
            import openai  # noqa: F401
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "pip install openai>=1.0.0  (https://pypi.org/project/openai/)") from e

        self._client = OpenAI(api_key=self.api_key)
        self._default_args: Dict[str, Any] = dict(
            model=self.model,
            max_tokens=max_output_tokens,  # OpenAI API uses max_tokens
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
                return resp.choices[0].message.content.strip()
            except Exception as e:
                error_str = str(e)
                last_exception = e
                
                # Check if this is a retryable error
                if _is_rate_limit_error(error_str):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(error_str)
                        print(f"🚦 Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Rate limit error after {max_retries} retries: {error_str}")
                elif _is_server_overloaded_error(error_str):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"🔄 Server overloaded (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Server overloaded error after {max_retries} retries: {error_str}")
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

    def __init__(
        self,
        model: str,
        max_output_tokens: int = 1024,
        temperature: float = 0.3,
        device: str | None = None,
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
        if isinstance(prompt, list):
            # Convert messages to plain prompt text
            prompt = "\n".join([f"{m['role']}: {m['content']}" for m in prompt])

        gen_args = {
            "max_new_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "do_sample": True,
            "return_full_text": False,
        }

        output = self.generator(prompt, **gen_args)[0]["generated_text"]
        return output.strip()


##############################################################################
# 4. Google Gemini implementation                                           #
##############################################################################

class GeminiLLM(LLMInterface):
    """
    Enhanced Gemini LLM with multi-key support for rate limit failover.

    Usage:
        llm = GeminiLLM()  # Uses gemini-2.5-flash by default
        answer = llm.generate("What is the capital of France?")

    API Key Loading (in priority order):
        1. Explicit api_key parameter
        2. GEMINI_API_KEYS=key1,key2,key3 (comma-separated)
        3. GEMINI_API_KEY_1, GEMINI_API_KEY_2, ... (individual keys)
        4. GEMINI_API_KEY (single key, legacy)

    Key Rotation Strategy:
        - Round-robin rotation every N requests (default: 5)
        - On rate limit errors: rotate to next key immediately
        - On quota exhausted (RPD): mark key exhausted, rotate
        - Exhausted keys reset after 24 hours (RPD resets daily)
        - Fails after 2 full rotations through all keys with only errors
    """

    def __init__(
        self,
        model: str = "gemini-2.5-flash",
        api_key: str | None = None,
        max_output_tokens: int = 8192,
        temperature: float = 0.3,
        context_window_size: int = 1000000,  # Gemini has 1M context by default
        rotation_requests: int = 5,  # Rotate keys every N requests
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.context_window_size = context_window_size
        self.rotation_requests = rotation_requests
        self.max_output_tokens = max_output_tokens

        # Load multiple API keys with fallback strategy
        self.api_keys = self._load_api_keys(api_key)
        self.current_key_index = 0
        self.exhausted_keys: Dict[int, float] = {}  # key_index -> timestamp when exhausted (RPD)
        self.rate_limited_keys: Dict[int, float] = {}  # key_index -> timestamp when rate limited (RPM)
        self.invalid_keys: set = set()  # Keys that are permanently invalid (malformed, wrong format)
        self.request_count = 0       # Track requests for round-robin rotation
        self.RPM_RESET_SECONDS = 60  # Per-minute limits reset after 60 seconds
        
        try:
            import google.generativeai as genai
            self.genai = genai
        except ImportError as e:
            raise ImportError(
                "pip install google-generativeai "
                "(https://pypi.org/project/google-generativeai/)") from e

        # Configure safety settings to be less restrictive for scientific content
        self.safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_ONLY_HIGH"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_ONLY_HIGH"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_ONLY_HIGH"
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_ONLY_HIGH"
            }
        ]
        
        self._default_args: Dict[str, Any] = dict(
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=max_output_tokens,
                temperature=temperature,
            )
        )
        
        # Initialize with first available key
        self._switch_to_key(0)
    
    def _validate_api_key(self, key: str, key_source: str = "unknown") -> bool:
        """Validate that an API key doesn't have invalid characters.

        gRPC will crash with 'Illegal header value' if the key contains
        newlines, non-ASCII characters, or other invalid header chars.
        """
        if not key:
            return False

        # Check for newlines (common issue with env vars)
        if '\n' in key or '\r' in key:
            print(f"⚠️  API key from {key_source} contains newline characters - skipping")
            return False

        # Check for non-printable characters
        if not key.isprintable():
            print(f"⚠️  API key from {key_source} contains non-printable characters - skipping")
            return False

        # Check for spaces (API keys shouldn't have spaces)
        if ' ' in key:
            print(f"⚠️  API key from {key_source} contains spaces - skipping")
            return False

        # Check for common invalid chars in HTTP headers
        invalid_chars = set('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f')
        if any(c in invalid_chars for c in key):
            print(f"⚠️  API key from {key_source} contains control characters - skipping")
            return False

        return True

    def _load_api_keys(self, explicit_key: str | None = None) -> List[str]:
        """Load API keys from various sources in priority order."""
        keys = []
        skipped_keys = 0

        # 1. Explicit parameter (supports comma-separated keys for multi-key mode)
        if explicit_key:
            if ',' in explicit_key:
                # Multi-key mode: split comma-separated keys
                for k in explicit_key.split(","):
                    k = k.strip()
                    if k and self._validate_api_key(k, "explicit parameter"):
                        keys.append(k)
                    elif k:
                        skipped_keys += 1
            else:
                # Single key mode
                if self._validate_api_key(explicit_key.strip(), "explicit parameter"):
                    keys.append(explicit_key.strip())
                else:
                    skipped_keys += 1
            if keys:
                if skipped_keys:
                    print(f"⚠️  Skipped {skipped_keys} invalid key(s)")
                print(f"🔑 Loaded {len(keys)} valid Gemini API key(s)")
                return keys

        # 2. Comma-separated environment variable
        comma_separated = os.getenv("GEMINI_API_KEYS")
        if comma_separated:
            for k in comma_separated.split(","):
                k = k.strip()
                if k and self._validate_api_key(k, "GEMINI_API_KEYS"):
                    if k not in keys:
                        keys.append(k)
                elif k:
                    skipped_keys += 1

        # 3. Individual numbered environment variables
        i = 1
        while True:
            key = os.getenv(f"GEMINI_API_KEY_{i}")
            if not key:
                break
            key = key.strip()
            if self._validate_api_key(key, f"GEMINI_API_KEY_{i}"):
                if key not in keys:
                    keys.append(key)
            else:
                skipped_keys += 1
            i += 1

        # 4. Legacy single key
        single_key = os.getenv("GEMINI_API_KEY")
        if single_key:
            single_key = single_key.strip()
            if self._validate_api_key(single_key, "GEMINI_API_KEY"):
                if single_key not in keys:
                    keys.append(single_key)
            else:
                skipped_keys += 1

        if not keys:
            raise ValueError(
                "No valid Gemini API keys found. Set one of:\n"
                "  • GEMINI_API_KEYS=key1,key2,key3\n"
                "  • GEMINI_API_KEY_1, GEMINI_API_KEY_2, ...\n"
                "  • GEMINI_API_KEY (single key)\n"
                f"Note: {skipped_keys} key(s) were skipped due to invalid characters."
            )

        if skipped_keys:
            print(f"⚠️  Skipped {skipped_keys} invalid key(s)")
        print(f"🔑 Loaded {len(keys)} valid Gemini API key(s)")
        return keys
    
    def _is_key_exhausted(self, key_index: int) -> bool:
        """Check if a key is exhausted (RPD). Resets after 24 hours (RPD resets daily)."""
        if key_index not in self.exhausted_keys:
            return False

        exhausted_time = self.exhausted_keys[key_index]
        hours_since_exhausted = (time.time() - exhausted_time) / 3600

        # Reset after 24 hours (RPD resets daily)
        if hours_since_exhausted >= 24:
            del self.exhausted_keys[key_index]
            print(f"🔓 Key #{key_index + 1} reset after 24 hours")
            return False

        return True

    def _is_key_rate_limited(self, key_index: int) -> bool:
        """Check if a key is currently rate limited (RPM). Resets after 60 seconds."""
        if key_index not in self.rate_limited_keys:
            return False

        limited_time = self.rate_limited_keys[key_index]
        seconds_since_limited = time.time() - limited_time

        # RPM resets after 60 seconds
        if seconds_since_limited >= self.RPM_RESET_SECONDS:
            del self.rate_limited_keys[key_index]
            return False

        return True

    def _mark_key_rate_limited(self, key_index: int) -> None:
        """Mark a key as temporarily rate limited (RPM)."""
        self.rate_limited_keys[key_index] = time.time()
        print(f"⏱️  Marked key #{key_index + 1} as rate limited (RPM) - will reset in 60s")

    def _get_seconds_until_key_available(self, key_index: int) -> float:
        """Get seconds until a rate-limited key becomes available again."""
        if key_index not in self.rate_limited_keys:
            return 0
        limited_time = self.rate_limited_keys[key_index]
        seconds_since_limited = time.time() - limited_time
        remaining = self.RPM_RESET_SECONDS - seconds_since_limited
        return max(0, remaining)

    def _get_fresh_keys(self) -> List[int]:
        """Get list of keys that are not currently rate limited, exhausted, or invalid."""
        return [i for i in range(len(self.api_keys))
                if not self._is_key_exhausted(i)
                and not self._is_key_rate_limited(i)
                and i not in self.invalid_keys]

    def should_sleep_before_request(self) -> bool:
        """Check if caller should sleep before making a request.

        Returns True only if ALL keys are currently rate-limited.
        When fresh keys are available, there's no need to sleep -
        just rotate to a fresh key and continue immediately.

        This is used by table_builder to implement dynamic sleep:
        only sleep when necessary, not between every paper.
        """
        fresh_keys = self._get_fresh_keys()
        return len(fresh_keys) == 0

    def _switch_to_key(self, key_index: int) -> bool:
        """Switch to a different API key. Returns True if successful."""
        if key_index >= len(self.api_keys) or self._is_key_exhausted(key_index) or key_index in self.invalid_keys:
            return False

        self.current_key_index = key_index
        current_key = self.api_keys[key_index]

        # Configure Gemini with new key
        self.genai.configure(api_key=current_key)
        self._client = self.genai.GenerativeModel(
            self.model,
            safety_settings=self.safety_settings
        )

        print(f"🔄 Switched to API key #{key_index + 1}")
        return True

    def _get_available_keys(self) -> List[int]:
        """Get list of non-exhausted and non-invalid key indices."""
        return [i for i in range(len(self.api_keys))
                if not self._is_key_exhausted(i) and i not in self.invalid_keys]

    def _mark_key_invalid(self, key_index: int) -> None:
        """Mark a key as permanently invalid (malformed, wrong format, etc.)."""
        self.invalid_keys.add(key_index)
        print(f"🚫 Marked key #{key_index + 1} as INVALID (malformed or unauthorized)")

    def _rotate_to_next_key(self, prefer_fresh: bool = True) -> bool:
        """Rotate to the next available key in round-robin fashion.

        Args:
            prefer_fresh: If True, prefer keys that are not rate-limited (RPM).
                         This allows immediate retry without waiting.
        """
        if len(self.api_keys) <= 1:
            return False

        # First, try to find a fresh key (not rate limited)
        if prefer_fresh:
            fresh_keys = self._get_fresh_keys()
            if fresh_keys:
                # Pick the next fresh key after current index
                for key_idx in fresh_keys:
                    if key_idx != self.current_key_index:
                        return self._switch_to_key(key_idx)
                # If only one fresh key and it's current, no rotation needed
                return False

        # Fallback to any available key (may be rate limited but not exhausted)
        available_keys = self._get_available_keys()

        if len(available_keys) <= 1:
            return False

        # Find current index in available keys list
        try:
            current_pos = available_keys.index(self.current_key_index)
            next_pos = (current_pos + 1) % len(available_keys)
            next_key_index = available_keys[next_pos]
        except ValueError:
            # Current key not in available keys, pick first available
            next_key_index = available_keys[0]

        if next_key_index != self.current_key_index:
            return self._switch_to_key(next_key_index)
        return False

    def _get_min_wait_for_rate_limited_keys(self) -> float:
        """Get the minimum time to wait for any rate-limited key to become available."""
        if not self.rate_limited_keys:
            return 0

        min_wait = float('inf')
        for key_idx in self.rate_limited_keys:
            if key_idx not in self.invalid_keys and not self._is_key_exhausted(key_idx):
                wait = self._get_seconds_until_key_available(key_idx)
                if wait < min_wait:
                    min_wait = wait

        return min_wait if min_wait != float('inf') else 0

    def _should_rotate_key(self) -> bool:
        """Check if we should rotate to next key based on request count."""
        return (len(self.api_keys) > 1 and
                self.request_count > 0 and
                self.request_count % self.rotation_requests == 0)

    def _mark_key_exhausted(self, key_index: int) -> None:
        """Mark a key as exhausted (hit daily quota)."""
        self.exhausted_keys[key_index] = time.time()
        print(f"🚫 Marked key #{key_index + 1} as exhausted (RPD limit)")

    def _calculate_backoff(self, attempt: int, base_wait: int) -> int:
        """Calculate exponential backoff with jitter."""
        # Exponential: base * 2^attempt, capped at 5 minutes
        backoff = min(base_wait * (2 ** attempt), 300)
        # Add jitter (10-30% of backoff)
        jitter = random.randint(int(backoff * 0.1), int(backoff * 0.3))
        return backoff + jitter

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Enhanced generate method with round-robin rotation and smart error handling.

        Strategy:
        - Rotate keys on each rate limit error (spreads load)
        - Track full rotations through all keys
        - Fail after 2 full rotations with only errors (all keys tried twice)
        - Use exponential backoff for wait times
        - Log MAX_TOKENS but continue with truncated response

        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages (converted to plain prompt)
        """
        # Increment request counter and check for proactive round-robin rotation
        self.request_count += 1
        if self._should_rotate_key():
            if self._rotate_to_next_key():
                print(f"🔁 Round-robin rotation to key #{self.current_key_index + 1} (request #{self.request_count})")

        # Convert chat messages to plain text if needed
        if isinstance(prompt, list):
            prompt_text = "\n".join([f"{m['role']}: {m['content']}" for m in prompt])
        else:
            prompt_text = prompt

        # Add scientific context to help with safety filtering
        scientific_context = "Context: This is a scientific research task about cellular biology and protein sequences. Terms like 'nuclear' refer to cell nuclei (the cellular organelle), not weapons or harmful content."
        prompt_text = f"{scientific_context}\n\n{prompt_text}"

        # Merge generation config
        gen_config = self._default_args["generation_config"]
        if kwargs.get("max_output_tokens"):
            gen_config.max_output_tokens = kwargs["max_output_tokens"]
        if kwargs.get("temperature") is not None:
            gen_config.temperature = kwargs["temperature"]

        # Track rotation state for 2x full rotation failure detection
        available_keys = self._get_available_keys()
        num_available_keys = len(available_keys)
        max_total_attempts = num_available_keys * 2  # 2 full rotations through all keys
        total_attempts = 0
        last_exception = None
        starting_key = self.current_key_index

        while total_attempts < max_total_attempts:
            total_attempts += 1
            current_key_display = f"#{self.current_key_index + 1}"
            rotation_num = (total_attempts - 1) // num_available_keys + 1
            attempt_in_rotation = (total_attempts - 1) % num_available_keys + 1

            print(f"🔄 Attempt {total_attempts}/{max_total_attempts} (rotation {rotation_num}, key {attempt_in_rotation}/{num_available_keys}) with key {current_key_display}")

            try:
                response = self._client.generate_content(
                    prompt_text,
                    generation_config=gen_config
                )

                # Handle safety filtering or empty responses
                if not response.candidates:
                    print(f"⚠️  Gemini returned no candidates. Feedback: {response.prompt_feedback}")
                    return "No response generated due to safety filters or other restrictions."

                candidate = response.candidates[0]
                finish_reason = candidate.finish_reason.name if candidate.finish_reason else None

                # Log non-STOP finish reasons (including MAX_TOKENS) but continue
                if finish_reason and finish_reason != "STOP":
                    if finish_reason == "MAX_TOKENS":
                        print(f"⚠️  Response truncated (MAX_TOKENS). Output may be incomplete.")
                    else:
                        print(f"ℹ️  Response finish reason: {finish_reason}")

                if not candidate.content or not candidate.content.parts:
                    print("⚠️  Gemini returned empty content")
                    return "Empty response from Gemini."

                return response.text.strip()

            except Exception as e:
                error_str = str(e)
                last_exception = e

                # Handle safety filter errors specifically - don't retry
                if "Invalid operation" in error_str and "finish_reason" in error_str:
                    print(f"⚠️  Gemini safety filter triggered: {error_str}")
                    return "Response blocked by Gemini safety filters. Please try rephrasing your request."

                # Check for invalid/malformed API key errors - mark key as permanently invalid
                if _is_invalid_api_key_error(error_str):
                    print(f"🔑 Invalid/malformed API key detected for key {current_key_display}")
                    self._mark_key_invalid(self.current_key_index)

                    # Update available keys after marking invalid
                    available_keys = self._get_available_keys()
                    if not available_keys:
                        print(f"❌ All API keys are exhausted or invalid. Cannot continue.")
                        print(f"💾 Raising AllKeysFailedError - caller should save data and exit gracefully.")
                        raise AllKeysFailedError(
                            message="All API keys are exhausted or invalid",
                            keys_tried=len(self.api_keys),
                            last_error=error_str[:500]
                        )

                    # Update max attempts based on remaining keys
                    num_available_keys = len(available_keys)
                    max_total_attempts = num_available_keys * 2

                    # Rotate to next available key (prefer fresh if available)
                    self._rotate_to_next_key(prefer_fresh=True)
                    continue

                # Check for quota exhausted (RPD) errors - mark key and rotate
                elif _is_quota_exhausted_error(error_str):
                    print(f"💳 RPD quota exhausted for key {current_key_display}")
                    self._mark_key_exhausted(self.current_key_index)

                    # Update available keys after exhaustion
                    available_keys = self._get_available_keys()
                    if not available_keys:
                        print(f"❌ All API keys exhausted (daily quota). Cannot continue.")
                        print(f"💾 Raising AllKeysFailedError - caller should save data and exit gracefully.")
                        raise AllKeysFailedError(
                            message="All API keys exhausted (daily quota)",
                            keys_tried=len(self.api_keys),
                            last_error=error_str[:500]
                        )

                    # Update max attempts based on remaining keys
                    num_available_keys = len(available_keys)
                    max_total_attempts = num_available_keys * 2

                    # Rotate to next available key (prefer fresh if available)
                    self._rotate_to_next_key(prefer_fresh=True)
                    continue

                # For RPM/rate limit errors - mark key and try fresh key immediately
                elif _is_rpm_error(error_str) or _is_rate_limit_error(error_str):
                    # Mark current key as rate limited (will reset in 60s)
                    self._mark_key_rate_limited(self.current_key_index)

                    # Try to find a fresh key (not rate limited)
                    fresh_keys = self._get_fresh_keys()

                    if fresh_keys:
                        # Fresh key available - rotate and retry IMMEDIATELY (no wait!)
                        print(f"🚦 Rate limit hit with key {current_key_display}. Rotating to fresh key...")
                        self._rotate_to_next_key(prefer_fresh=True)
                        # No sleep - fresh key from different project has fresh quota
                        continue
                    else:
                        # All keys are rate limited - wait for the first one to reset
                        min_wait = self._get_min_wait_for_rate_limited_keys()
                        if min_wait > 0:
                            # Add small buffer
                            wait_time = min_wait + random.randint(2, 5)
                            print(f"🚦 All keys rate limited. Waiting {wait_time:.0f}s for key to reset...")
                            time.sleep(wait_time)
                            # Clear expired rate limits and try again
                            self._rotate_to_next_key(prefer_fresh=True)
                        else:
                            # Shouldn't happen, but fallback to short wait
                            time.sleep(5)
                        continue

                # Server overload errors - rotate to different key, short wait
                elif _is_server_overloaded_error(error_str):
                    # Server overload is global, not per-key, but rotation may help
                    # as different keys might hit different server instances
                    wait_time = 5 + random.randint(2, 8)  # Short wait, server overload is usually brief

                    print(f"🔄 Server overloaded. Rotating and waiting {wait_time}s...")

                    if num_available_keys > 1:
                        self._rotate_to_next_key(prefer_fresh=True)

                    time.sleep(wait_time)
                    continue

                else:
                    # Non-retryable error - rotate and try next key
                    print(f"⚠️  Error with key {current_key_display}: {error_str[:200]}...")

                    if num_available_keys > 1:
                        self._rotate_to_next_key(prefer_fresh=True)
                    continue

        # Exhausted 2 full rotations with only errors
        print(f"❌ Failed after {total_attempts} attempts across 2 full rotations of {num_available_keys} key(s)")
        print(f"💾 Raising AllKeysFailedError - caller should save data and exit gracefully.")
        raise AllKeysFailedError(
            message=f"Failed after {total_attempts} attempts across 2 full rotations",
            keys_tried=num_available_keys,
            last_error=str(last_exception)[:500] if last_exception else None
        )