"""Multimodal contents assembly for GeminiLLM.generate()/generate_with_cache().

Images must ride alongside the prompt text in the same generate_content()
call — never summarized into a separate description step first — so the
model always sees a figure together with whatever the prompt is asking it
to extract. See llm_backends.py:_build_contents and paper_processor.py's
_load_document_figures + _generate.

Follows the repo pattern of driving the method unbound with a stand-in
``self`` (see test_extract_values_for_unit_narrowing.py), so no real API
key / google-genai client is needed.
"""
from unittest.mock import MagicMock

import pytest

genai_types = pytest.importorskip("google.genai.types")

from schematiq.core.llm_backends import GeminiLLM


def _stub_llm():
    """A stand-in GeminiLLM with just the .types attribute _build_contents needs."""
    llm = MagicMock()
    llm.types = genai_types
    return llm


def test_no_images_returns_plain_text_unchanged():
    """Every existing text-only caller must see identical behavior."""
    llm = _stub_llm()
    assert GeminiLLM._build_contents(llm, "hello", None) == "hello"


def test_empty_images_list_returns_plain_text_unchanged():
    llm = _stub_llm()
    assert GeminiLLM._build_contents(llm, "hello", []) == "hello"


def test_images_ride_alongside_text_in_one_call():
    llm = _stub_llm()
    images = [(b"fake-png-bytes", "image/png"), (b"fake-jpg-bytes", "image/jpeg")]

    contents = GeminiLLM._build_contents(llm, "extract Column X", images)

    assert isinstance(contents, list)
    assert contents[0] == "extract Column X"
    assert len(contents) == 1 + len(images)
    for part, (data, mime) in zip(contents[1:], images):
        assert isinstance(part, genai_types.Part)
        assert part.inline_data.mime_type == mime
        assert part.inline_data.data == data
