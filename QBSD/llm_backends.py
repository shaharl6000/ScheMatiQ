# llm_backends.py
"""
Concrete implementations of `LLMInterface` for Together AI and OpenAI.
The API keys are pulled from standard environment variables by default:
    OPENAI_API_KEY       – OpenAI
    TOGETHER_API_KEY     – Together AI
"""

from __future__ import annotations
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any

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


    def generate(self, prompt: str, **kwargs) -> str:
        params = {**self._default_args, **kwargs}
        # Together chat endpoint expects OpenAI-style 'messages'
        params["messages"] = [{"role": "user", "content": prompt}]
        resp = self._client.chat.completions.create(**params)
        return resp.choices[0].message.content.strip()


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

    def generate(self, prompt: str, **kwargs) -> str:
        params = {**self._default_args, **kwargs}
        params["messages"] = [{"role": "user", "content": prompt}]
        resp = self._client.chat.completions.create(**params)
        return resp.choices[0].message.content.strip()
