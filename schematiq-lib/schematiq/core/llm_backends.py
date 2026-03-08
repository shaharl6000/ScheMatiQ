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
from typing import List, Dict, Any, Union, Optional
import re

from schematiq.core.model_specs import get_model_spec, get_max_output_tokens
from schematiq.core.llm_call_tracker import LLMCallTracker


##############################################################################
# Rate limit retry utilities                                                 #
##############################################################################

def _is_rate_limit_error(error_str: str) -> bool:
    """Check if error is a rate limit error (429)."""
    return "429" in error_str and ("rate limit" in error_str.lower() or "rate_limit" in error_str.lower())

def _is_server_overloaded_error(error_str: str) -> bool:
    """Check if error is a server overloaded/unavailable error (503)."""
    if "503" not in error_str:
        return False
    error_lower = error_str.lower()
    return any(indicator in error_lower for indicator in [
        "overloaded", "not ready", "high demand", "unavailable",
        "try again later", "service unavailable",
    ])


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
        api_key: str | None = None,
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
    _provider = "openai"

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
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
    _provider = "hf"

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
        llm = GeminiLLM()  # Uses gemini-2.5-flash-lite by default
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
        model: str = "gemini-2.5-flash-lite",
        api_key: str | None = None,
        max_output_tokens: Optional[int] = None,
        temperature: float = 0.3,
        context_window_size: Optional[int] = None,
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.temperature = temperature

        # Auto-detect token limits from model specs
        spec = get_model_spec("gemini", model)
        self.max_output_tokens = max_output_tokens if max_output_tokens is not None else spec.max_output_tokens
        self.context_window_size = context_window_size if context_window_size is not None else spec.context_window

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

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Generate a response from Gemini with retry logic.

        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages (converted to plain prompt)
        """
        # Calculate prompt length for tracking
        prompt_len = sum(len(m.get("content", "")) for m in (prompt if isinstance(prompt, list) else [{"content": prompt}]))

        # Convert chat messages to plain text if needed
        if isinstance(prompt, list):
            prompt_text = "\n".join([f"{m['role']}: {m['content']}" for m in prompt])
        else:
            prompt_text = prompt

        # Log prompt size for performance correlation
        print(f"🚀 Starting Gemini API call (model: {self.model}, prompt: ~{len(prompt_text):,} chars)")
        start_time = time.time()

        # Add scientific context to help with safety filtering
        scientific_context = "Context: This is a scientific research task about cellular biology and protein sequences. Terms like 'nuclear' refer to cell nuclei (the cellular organelle), not weapons or harmful content."
        prompt_text = f"{scientific_context}\n\n{prompt_text}"

        # Build generation config using new SDK types
        config_kwargs = {
            "max_output_tokens": kwargs.get("max_output_tokens", self.max_output_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "safety_settings": self.safety_settings,
        }
        # Add controlled generation if response_schema provided
        if kwargs.get("response_schema") is not None:
            config_kwargs["response_mime_type"] = "application/json"
            config_kwargs["response_schema"] = kwargs["response_schema"]
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
                        print(f"Response truncated (MAX_TOKENS). Output may be incomplete.")
                    else:
                        print(f"Response finish reason: {finish_reason_name}")

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
                print(f"⚠️  Gemini API call failed after {elapsed:.1f}s: {str(e)[:100]}")

                # Handle safety filter errors specifically - don't retry
                if "Invalid operation" in error_str and "finish_reason" in error_str:
                    print(f"Gemini safety filter triggered: {error_str}")
                    return "Response blocked by Gemini safety filters. Please try rephrasing your request."

                # Check for invalid/malformed API key errors - don't retry
                if _is_invalid_api_key_error(error_str):
                    print(f"Invalid/malformed API key: {error_str[:200]}")
                    raise

                # Check if this is a retryable error
                if _is_rate_limit_error(error_str):
                    if attempt < max_retries:
                        wait_time = _extract_wait_time(error_str)
                        print(f"Rate limit hit (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Rate limit error after {max_retries} retries: {error_str}")
                elif _is_server_overloaded_error(error_str):
                    if attempt < max_retries:
                        wait_time = 10 + random.randint(5, 15)
                        print(f"Server overloaded (attempt {attempt + 1}/{max_retries + 1}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Server overloaded error after {max_retries} retries: {error_str}")
                else:
                    # Not a retryable error, don't retry
                    break

        # Re-raise the last exception
        raise last_exception
