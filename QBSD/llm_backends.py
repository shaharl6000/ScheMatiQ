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

def _is_server_overloaded_error(error_str: str) -> bool:
    """Check if error is a server overloaded error (503)."""
    return "503" in error_str and ("overloaded" in error_str.lower() or "not ready" in error_str.lower())

def _extract_wait_time(error_str: str) -> int:
    """Extract wait time from rate limit error or return default."""
    # For Together AI rate limits, wait based on the per-minute limit
    if "per minute" in error_str.lower():
        # Default to 1 minute + jitter for per-minute limits
        return 65 + random.randint(5, 15)
    # Default fallback
    return 30 + random.randint(5, 15)

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
     llm = GeminiLLM(model="gemini-1.5-flash")
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
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("Gemini API key missing. Set GEMINI_API_KEY.")
        
        try:
            import google.generativeai as genai
        except ImportError as e:
            raise ImportError(
                "pip install google-generativeai "
                "(https://pypi.org/project/google-generativeai/)") from e

        genai.configure(api_key=self.api_key)
        
        # Configure safety settings to be less restrictive for scientific content
        safety_settings = [
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
        
        self._client = genai.GenerativeModel(
            self.model,
            safety_settings=safety_settings
        )
        self._default_args: Dict[str, Any] = dict(
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            )
        )

    def generate(self,
                 prompt: Union[str, List[Dict[str, str]]],
                 **kwargs) -> str:
        """
        Args
        ----
        prompt : str | list[dict]
            • str  – plain prompt
            • list – chat-style messages (converted to plain prompt)
        """
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

        # Retry logic for rate limits
        max_retries = 3
        last_exception = None
        
        for attempt in range(max_retries + 1):
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
                
                # Check if this is a retryable error
                if _is_rate_limit_error(error_str) or "quota" in error_str.lower():
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