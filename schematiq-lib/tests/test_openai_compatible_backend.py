"""Tests for the OpenAI / Together chat-completions backend.

Regression: _OpenAICompatibleChatMixin forwarded every generate() kwarg to
chat.completions.create. Pipeline callers always pass max_output_tokens (and
value extraction passes response_schema=None), which the OpenAI SDK rejects
with TypeError, so every OpenAI or Together run failed on its first call.
Earlier tests used a MagicMock LLM, which accepts any kwarg.
"""

import inspect
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from schematiq.core.llm_backends import OpenAILLM, _chat_completion_params


def _response(content):
    message = SimpleNamespace(content=content)
    return SimpleNamespace(choices=[SimpleNamespace(message=message)], usage=None)


@pytest.fixture
def openai_llm(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    llm = OpenAILLM(model="gpt-4.1-mini", temperature=0)
    llm._client = MagicMock()
    llm._client.chat.completions.create.return_value = _response('{"ok": true}')
    return llm


def _sent(llm):
    return llm._client.chat.completions.create.call_args.kwargs


def test_params_map_max_output_tokens_and_drop_gemini_kwargs():
    params = _chat_completion_params(
        {"model": "m", "max_tokens": 100, "temperature": 0.3},
        {
            "max_output_tokens": 50,
            "temperature": 0,
            "thinking_budget": 1024,
            "response_schema": None,
            "images": [],
            "seed": None,
        },
    )
    assert params == {"model": "m", "max_tokens": 50, "temperature": 0}


def test_none_max_output_tokens_keeps_the_default():
    params = _chat_completion_params({"max_tokens": 100}, {"max_output_tokens": None})
    assert params == {"max_tokens": 100}


def test_generate_sends_only_arguments_the_openai_sdk_accepts(openai_llm):
    openai = pytest.importorskip("openai")
    from openai.resources.chat.completions import Completions

    out = openai_llm.generate(
        [{"role": "user", "content": "hi"}],
        max_output_tokens=50,
        temperature=0,
        response_schema=None,
        thinking_budget=1024,
    )
    assert out == '{"ok": true}'
    sent = _sent(openai_llm)
    assert sent["max_tokens"] == 50
    accepted = set(inspect.signature(Completions.create).parameters)
    assert set(sent) <= accepted, set(sent) - accepted


def test_a_schema_the_backend_cannot_use_is_logged(openai_llm, caplog):
    with caplog.at_level(logging.WARNING, logger="schematiq.core.llm_backends"):
        openai_llm.generate("hi", response_schema={"type": "OBJECT"})
    assert "response_schema" not in _sent(openai_llm)
    assert "only supported by the Gemini backend" in caplog.text


def test_empty_message_content_returns_empty_string(openai_llm):
    openai_llm._client.chat.completions.create.return_value = _response(None)
    assert openai_llm.generate("hi") == ""
