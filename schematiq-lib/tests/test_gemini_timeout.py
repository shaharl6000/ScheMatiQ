"""Gemini requests must time out instead of hanging (dropped connections).

google-genai sends requests with no timeout by default. A connection that
dies silently then blocks the call, and the extraction, forever.
"""

import socket
import threading
import time

import pytest

from schematiq.core.llm_backends import (
    _is_retryable_server_error,
    gemini_http_options,
    gemini_timeout_seconds,
)


def test_default_timeout(monkeypatch):
    monkeypatch.delenv("SCHEMATIQ_GEMINI_TIMEOUT_S", raising=False)
    assert gemini_timeout_seconds() == 180.0


@pytest.mark.parametrize("raw", ["off", "none", "0", ""])
def test_timeout_can_be_disabled(monkeypatch, raw):
    monkeypatch.setenv("SCHEMATIQ_GEMINI_TIMEOUT_S", raw)
    assert gemini_timeout_seconds() is None
    assert gemini_http_options() is None


def test_invalid_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("SCHEMATIQ_GEMINI_TIMEOUT_S", "soon")
    assert gemini_timeout_seconds() == 180.0


def test_http_options_use_milliseconds(monkeypatch):
    pytest.importorskip("google.genai")
    monkeypatch.setenv("SCHEMATIQ_GEMINI_TIMEOUT_S", "2.5")
    assert gemini_http_options().timeout == 2500


def test_hanging_server_raises_a_retryable_timeout(monkeypatch):
    """A server that accepts and never answers must not block the call."""
    genai = pytest.importorskip("google.genai")
    from google.genai import types

    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    conns = []
    threading.Thread(target=lambda: conns.append(server.accept()), daemon=True).start()

    monkeypatch.setenv("SCHEMATIQ_GEMINI_TIMEOUT_S", "1")
    opts = gemini_http_options()
    opts.base_url = f"http://127.0.0.1:{port}"
    client = genai.Client(api_key="test-key", http_options=opts)

    start = time.monotonic()
    with pytest.raises(Exception) as info:
        client.models.generate_content(model="gemini-test", contents="hi")
    elapsed = time.monotonic() - start
    server.close()

    import httpx

    # The connection stays open and silent, so this is a real timeout, not a
    # dropped connection.
    assert isinstance(info.value, httpx.TimeoutException)
    assert elapsed < 10
    assert _is_retryable_server_error(info.value)
