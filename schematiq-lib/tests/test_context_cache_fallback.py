"""A document's context cache can expire mid-document: a stalled call runs
until the request timeout before it is retried, and its retry then names a cache that is gone (Gemini
answers 403 "CachedContent not found"). _generate() must then carry on
without the cache instead of failing the call, which used to drop every unit
of the document.

Drives _generate() unbound with a MagicMock ``self``, as
test_figure_vision_context.py does.
"""
from unittest.mock import MagicMock

import pytest

from schematiq.value_extraction.core.paper_processor import PaperProcessor

errors = pytest.importorskip("google.genai.errors")


def api_error(code, status, message):
    with pytest.raises(errors.APIError) as excinfo:
        errors.APIError.raise_error(code, {"error": {"code": code, "status": status, "message": message}}, None)
    return excinfo.value


def _make_self(layout=None):
    s = MagicMock()
    s._active_context_cache = MagicMock()
    s._active_figure_images = []
    s._uncached_doc_layout = layout
    s._inline_system_into_first_user = PaperProcessor._inline_system_into_first_user
    return s


PROMPT = [
    {"role": "system", "content": "unit instructions"},
    {"role": "user", "content": "document text and columns"},
]


def test_expired_cache_falls_back_to_uncached_call():
    s = _make_self()
    s.llm.generate_with_cache.side_effect = api_error(
        403, "PERMISSION_DENIED", "CachedContent not found (or permission denied)"
    )
    s.llm.generate.return_value = "uncached response"

    assert PaperProcessor._generate(s, PROMPT, max_output_tokens=10) == "uncached response"
    # The original prompt, system message included, since no cache carries it now.
    s.llm.generate.assert_called_once_with(PROMPT, max_output_tokens=10)
    assert s._active_context_cache is None


def test_expired_cache_is_still_handed_to_cleanup():
    # Its bookkeeping (and storage cost) is released like any finished cache's.
    s = _make_self()
    cache = s._active_context_cache
    s.llm.generate_with_cache.side_effect = api_error(404, "NOT_FOUND", "CachedContent not found")

    PaperProcessor._generate(s, PROMPT)
    s.llm.delete_context_cache.assert_called_once_with(cache)


def test_expired_cache_falls_back_to_the_uncached_document_layout():
    # A Gemini document keeps its layout, so without the cache it is sent the
    # way a document too small for a cache is: rules, document, per-call prompt.
    s = _make_self(layout=("cache rules", "the document"))
    s.llm.generate_with_cache.side_effect = api_error(403, "PERMISSION_DENIED", "CachedContent not found")
    s.llm.generate.return_value = "ok"

    assert PaperProcessor._generate(s, PROMPT) == "ok"
    s.llm.generate.assert_called_once_with([
        {"role": "system", "content": "cache rules"},
        {"role": "user", "content": "the document"},
        {"role": "user", "content": "unit instructions\n\ndocument text and columns"},
    ])


def test_later_calls_skip_the_dropped_cache():
    s = _make_self()
    s.llm.generate_with_cache.side_effect = api_error(404, "NOT_FOUND", "CachedContent not found")
    s.llm.generate.return_value = "ok"

    PaperProcessor._generate(s, PROMPT)
    PaperProcessor._generate(s, PROMPT)

    assert s.llm.generate_with_cache.call_count == 1
    assert s.llm.generate.call_count == 2


def test_other_errors_still_raise():
    s = _make_self()
    s.llm.generate_with_cache.side_effect = api_error(403, "PERMISSION_DENIED", "API key not valid")

    with pytest.raises(errors.APIError):
        PaperProcessor._generate(s, PROMPT)
    s.llm.generate.assert_not_called()
