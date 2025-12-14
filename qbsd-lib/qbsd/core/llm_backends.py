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
# Rate limit retry utilities                                                 #
##############################################################################

def _is_rate_limit_error(error_str: str) -> bool:
    """Check if error is a rate limit error (429)."""
    return "429" in error_str and ("rate limit" in error_str.lower() or "rate_limit" in error_str.lower())

def _is_quota_exhausted_error(error_str: str) -> bool:
    """Check if error is a quota exhausted (RPD/daily) error - requires key switch."""
    error_lower = error_str.lower()
    
    # Exclude per-minute errors from being classified as quota exhausted
    if "per minute" in error_lower or "perminute" in error_lower:
        return False
    
    # Look for daily/billing quota indicators
    rpd_indicators = [
        "daily quota", "requests per day", "rpd", 
        "billing", "insufficient quota", "plan and billing",
        "quota exhausted", "quota limit exceeded"
    ]
    
    # Must be a 429 error and contain RPD indicators
    return "429" in error_str and any(indicator in error_lower for indicator in rpd_indicators)

def _is_rpm_error(error_str: str) -> bool:
    """Check if error is requests per minute (RPM) error - requires waiting."""
    error_lower = error_str.lower()
    
    # Specific Gemini RPM indicators
    gemini_rpm_indicators = [
        "generatereq​uestsperminuteperprojectpermodel",
        "perminute", "per minute"
    ]
    
    # General RPM indicators
    general_rpm_indicators = [
        "requests per minute", "rpm", "per minute limit",
        "minute quota", "too many requests"
    ]
    
    # Must be a 429 error with per-minute indicators
    if "429" in error_str:
        return (any(indicator in error_lower for indicator in gemini_rpm_indicators) or 
                any(indicator in error_lower for indicator in general_rpm_indicators))
    
    return False

def _is_server_overloaded_error(error_str: str) -> bool:
    """Check if error is a server overloaded error (503)."""
    return "503" in error_str and ("overloaded" in error_str.lower() or "not ready" in error_str.lower())

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
        max_tokens: int = 1024,
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
            max_tokens=max_tokens,
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
        max_tokens: int = 1024,
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
            max_tokens=max_tokens,
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
        max_tokens: int = 1024,
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
        self.max_tokens = max_tokens
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
            "max_new_tokens": kwargs.get("max_tokens", self.max_tokens),
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
        llm = GeminiLLM(model="gemini-1.5-flash")
        answer = llm.generate("What is the capital of France?")
    
    API Key Loading (in priority order):
        1. Explicit api_key parameter
        2. GEMINI_API_KEYS=key1,key2,key3 (comma-separated)
        3. GEMINI_API_KEY_1, GEMINI_API_KEY_2, ... (individual keys)
        4. GEMINI_API_KEY (single key, legacy)
    """

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.3,
        max_context_tokens: int = 1000000,  # Gemini has 1M context by default
        rotation_requests: int = 10,  # Rotate keys every N requests
        **backend_kwargs,
    ):
        super().__init__(**backend_kwargs)
        self.model = model
        self.max_context_tokens = max_context_tokens
        self.rotation_requests = rotation_requests
        
        # Load multiple API keys with fallback strategy
        self.api_keys = self._load_api_keys(api_key)
        self.current_key_index = 0
        self.exhausted_keys = set()  # Keys that hit daily quota
        self.failed_keys = set()     # Keys that are temporarily failing
        self.request_count = 0       # Track requests for round-robin rotation
        
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
                max_output_tokens=max_tokens,
                temperature=temperature,
            )
        )
        
        # Initialize with first available key
        self._switch_to_key(0)
    
    def _load_api_keys(self, explicit_key: str | None = None) -> List[str]:
        """Load API keys from various sources in priority order."""
        keys = []

        # 1. Explicit parameter (supports comma-separated keys for multi-key mode)
        if explicit_key:
            if ',' in explicit_key:
                # Multi-key mode: split comma-separated keys
                keys.extend([k.strip() for k in explicit_key.split(",") if k.strip()])
            else:
                # Single key mode
                keys.append(explicit_key)
            return keys

        # 2. Comma-separated environment variable
        comma_separated = os.getenv("GEMINI_API_KEYS")
        if comma_separated:
            keys.extend([k.strip() for k in comma_separated.split(",") if k.strip()])
        
        # 3. Individual numbered environment variables
        i = 1
        while True:
            key = os.getenv(f"GEMINI_API_KEY_{i}")
            if not key:
                break
            keys.append(key)
            i += 1
        
        # 4. Legacy single key
        single_key = os.getenv("GEMINI_API_KEY")
        if single_key and single_key not in keys:
            keys.append(single_key)
        
        if not keys:
            raise ValueError(
                "No Gemini API keys found. Set one of:\n"
                "  • GEMINI_API_KEYS=key1,key2,key3\n"
                "  • GEMINI_API_KEY_1, GEMINI_API_KEY_2, ...\n"
                "  • GEMINI_API_KEY (single key)"
            )
        
        print(f"🔑 Loaded {len(keys)} Gemini API key(s)")
        return keys
    
    def _switch_to_key(self, key_index: int) -> bool:
        """Switch to a different API key. Returns True if successful."""
        if key_index >= len(self.api_keys) or key_index in self.exhausted_keys:
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
    
    def _find_next_available_key(self) -> int | None:
        """Find the next non-exhausted key index."""
        for i in range(len(self.api_keys)):
            if i not in self.exhausted_keys and i not in self.failed_keys:
                return i
        return None
    
    def _rotate_to_next_key(self) -> bool:
        """Rotate to the next available key in round-robin fashion."""
        if len(self.api_keys) <= 1:
            return False
            
        available_keys = [i for i in range(len(self.api_keys)) 
                         if i not in self.exhausted_keys and i not in self.failed_keys]
        
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
    
    def _mark_key_failed(self, key_index: int, temporary: bool = True) -> None:
        """Mark a key as failed. If temporary, it can be retried later."""
        if temporary:
            self.failed_keys.add(key_index)
            # Remove from failed keys after some time (simple approach)
            # In a real implementation, you might want a more sophisticated approach
        else:
            self.exhausted_keys.add(key_index)
        print(f"🚫 Marked key #{key_index + 1} as {'temporarily failed' if temporary else 'exhausted'}")
    
    def _should_rotate_key(self) -> bool:
        """Check if we should rotate to next key based on request count."""
        return (len(self.api_keys) > 1 and 
                self.request_count > 0 and 
                self.request_count % self.rotation_requests == 0)

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Enhanced generate method with round-robin rotation and smart error handling.
        
        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages (converted to plain prompt)
        """
        # Increment request counter and check for round-robin rotation
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
        if kwargs.get("max_tokens"):
            gen_config.max_output_tokens = kwargs["max_tokens"]
        if kwargs.get("temperature") is not None:
            gen_config.temperature = kwargs["temperature"]

        # Enhanced retry logic with multi-key support and resilient error handling
        max_retries_per_key = 3
        max_key_switches = len(self.api_keys) - 1  # Try all keys except current one
        key_switches_attempted = 0
        last_exception = None
        
        # Clear temporary failures periodically (simple approach)
        if self.request_count % 50 == 0:  # Every 50 requests
            self.failed_keys.clear()
            print(f"🔄 Cleared temporary key failures (request #{self.request_count})")
        
        while key_switches_attempted <= max_key_switches:
            current_key_display = f"#{self.current_key_index + 1}"
            
            for attempt in range(max_retries_per_key + 1):
                try:
                    response = self._client.generate_content(
                        prompt_text,
                        generation_config=gen_config
                    )
                    
                    # Handle safety filtering or empty responses
                    if not response.candidates:
                        print(f"⚠️  Gemini returned no candidates. Finish reason: {response.prompt_feedback}")
                        return "No response generated due to safety filters or other restrictions."
                    
                    candidate = response.candidates[0]
                    finish_reason = candidate.finish_reason.name
                    if finish_reason and finish_reason != "STOP":  # 1 = STOP (normal completion)
                        print(f"✅ Response generated. Finish reason: {finish_reason}")
                    
                    if not candidate.content or not candidate.content.parts:
                        print("⚠️  Gemini returned empty content")
                        return "Empty response from Gemini."
                    
                    return response.text.strip()
                    
                except Exception as e:
                    error_str = str(e)
                    last_exception = e
                    
                    # Handle safety filter errors specifically
                    if "Invalid operation" in error_str and "finish_reason" in error_str:
                        print(f"⚠️  Gemini safety filter triggered: {error_str}")
                        return "Response blocked by Gemini safety filters. Please try rephrasing your request."
                    
                    # Check for quota exhausted (RPD) errors - requires key switch
                    if _is_quota_exhausted_error(error_str):
                        print(f"💳 RPD quota exhausted for key {current_key_display}: {error_str}")
                        self.exhausted_keys.add(self.current_key_index)
                        
                        # Try to switch to next available key
                        next_key_index = self._find_next_available_key()
                        if next_key_index is not None and key_switches_attempted < max_key_switches:
                            if self._switch_to_key(next_key_index):
                                key_switches_attempted += 1
                                print(f"🔄 Switched to key #{next_key_index + 1}, retrying request...")
                                break  # Break out of retry loop for current key, try new key
                        else:
                            print(f"❌ All API keys exhausted. Cannot continue.")
                            raise last_exception
                    
                    # Check for RPM errors - just wait and retry with same key
                    elif _is_rpm_error(error_str):
                        if attempt < max_retries_per_key:
                            wait_time = _extract_wait_time(error_str)
                            print(f"🚦 RPM rate limit hit with key {current_key_display} (attempt {attempt + 1}/{max_retries_per_key + 1}). Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print(f"❌ RPM rate limit error after {max_retries_per_key} retries with key {current_key_display}")
                            # For RPM errors, we don't switch keys - the key isn't exhausted
                            raise last_exception
                    
                    # Check for general rate limit errors
                    elif _is_rate_limit_error(error_str):
                        if attempt < max_retries_per_key:
                            wait_time = _extract_wait_time(error_str)
                            print(f"🚦 Rate limit hit with key {current_key_display} (attempt {attempt + 1}/{max_retries_per_key + 1}). Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print(f"❌ Rate limit error after {max_retries_per_key} retries with key {current_key_display}")
                            raise last_exception
                    
                    # Check for server overload errors  
                    elif _is_server_overloaded_error(error_str):
                        if attempt < max_retries_per_key:
                            wait_time = 10 + random.randint(5, 15)
                            print(f"🔄 Server overloaded with key {current_key_display} (attempt {attempt + 1}/{max_retries_per_key + 1}). Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print(f"❌ Server overloaded error after {max_retries_per_key} retries with key {current_key_display}")
                            raise last_exception
                    else:
                        # Not a retryable error, don't retry
                        print(f"❌ Non-retryable error with key {current_key_display}: {error_str}")
                        raise last_exception
            
            # If we reach here, we either successfully processed or exhausted retries for current key
            # If we broke out due to key switch, continue with new key
            if key_switches_attempted <= max_key_switches and self.current_key_index not in self.exhausted_keys:
                continue
            else:
                break
        
        # Re-raise the last exception if we've exhausted all options
        print(f"❌ Exhausted all retry options across {key_switches_attempted + 1} API key(s)")
        raise last_exception