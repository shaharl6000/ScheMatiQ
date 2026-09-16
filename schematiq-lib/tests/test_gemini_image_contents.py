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
    images = [
        ("Figure 1: A sample chart.", b"fake-png-bytes", "image/png"),
        ("Table 1: A sample table.", b"fake-jpg-bytes", "image/jpeg"),
    ]

    contents = GeminiLLM._build_contents(llm, "extract Column X", images)

    assert isinstance(contents, list)
    assert contents[0] == "extract Column X"
    # Each image is preceded by its own label as a separate text part.
    assert len(contents) == 1 + 2 * len(images)
    idx = 1
    for label, data, mime in images:
        assert contents[idx] == label
        part = contents[idx + 1]
        assert isinstance(part, genai_types.Part)
        assert part.inline_data.mime_type == mime
        assert part.inline_data.data == data
        idx += 2


def test_image_with_no_label_gets_no_extra_text_part():
    """An empty label (e.g. a figure with no caption/name at all) must not
    inject a stray empty text part — just the image itself."""
    llm = _stub_llm()
    images = [("", b"fake-png-bytes", "image/png")]

    contents = GeminiLLM._build_contents(llm, "extract Column X", images)

    assert len(contents) == 2  # prompt_text + the bare image, no label part
    assert isinstance(contents[1], genai_types.Part)
