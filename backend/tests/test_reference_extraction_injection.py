"""Tests for injecting external reference context into value extraction.

Covers the prompt builder injection, the threading through PaperProcessor /
TableBuilder, and the backend build_reference_context helper.
"""

import pytest

from app.services.reference_context import build_reference_context
from schematiq.value_extraction.utils.prompt_builder import PromptBuilder


COLUMNS = [{"column": "President", "definition": "Appointing president"}]


def test_prompt_has_no_reference_block_without_context():
    messages = PromptBuilder().build_val_messages("q", "Title", "body", COLUMNS, mode="all")
    assert "EXTERNAL_REFERENCE" not in messages[1]["content"]


def test_prompt_injects_reference_block_after_paper_text():
    pb = PromptBuilder(reference_context="judge,president\nSmith,Trump")
    content = pb.build_val_messages("q", "Title", "body", COLUMNS, mode="all")[1]["content"]
    assert "<EXTERNAL_REFERENCE>" in content and "</EXTERNAL_REFERENCE>" in content
    assert "Smith,Trump" in content
    # The external reference must come after the source paper text, not replace it.
    assert content.index("</PAPER_TEXT>") < content.index("<EXTERNAL_REFERENCE>")


def test_prompt_injects_reference_in_one_by_one_mode():
    pb = PromptBuilder(reference_context="ref data")
    content = pb.build_val_messages("q", "Title", "body", COLUMNS, mode="one_by_one")[1][
        "content"
    ]
    assert "<EXTERNAL_REFERENCE>" in content


def test_reference_context_threads_through_table_builder():
    """PromptBuilder built by the processor must carry the run's reference context
    when set, and default to None otherwise."""
    from schematiq.value_extraction.core.table_builder import TableBuilder

    tb = TableBuilder(llm=None, reference_context="ctx-123")
    assert tb.paper_processor.prompt_builder.reference_context == "ctx-123"

    assert TableBuilder(llm=None).paper_processor.prompt_builder.reference_context is None


# --- backend helper ---------------------------------------------------------

class _Ref:
    def __init__(self, filename, content):
        self.filename = filename
        self.content = content  # inline content -> load_reference_text returns it


class _Session:
    def __init__(self, refs):
        self.reference_documents = refs


@pytest.mark.asyncio
async def test_build_reference_context_none_when_empty():
    assert await build_reference_context(_Session([])) is None
    assert await build_reference_context(object()) is None  # missing attribute -> None


@pytest.mark.asyncio
async def test_build_reference_context_concatenates_labelled():
    ctx = await build_reference_context(
        _Session([_Ref("a.csv", "judge,pres\nSmith,Trump"), _Ref("b.txt", "extra")])
    )
    assert "a.csv" in ctx and "Smith,Trump" in ctx
    assert "b.txt" in ctx and "extra" in ctx


@pytest.mark.asyncio
async def test_build_reference_context_skips_blank():
    ctx = await build_reference_context(_Session([_Ref("blank.txt", "   ")]))
    assert ctx is None
