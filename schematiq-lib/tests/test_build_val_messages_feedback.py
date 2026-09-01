"""``feedback`` block in PromptBuilder.build_val_messages.

feedback=None (the default, used by every existing caller) must produce a
byte-identical prompt to today. A non-empty feedback must be visible near the
top of the user message, wrapped in <PRIOR_ATTEMPT_FEEDBACK>.
"""

from schematiq.value_extraction.utils.prompt_builder import PromptBuilder


def _columns():
    return [{"column": "A", "definition": "def A"}]


def test_feedback_none_produces_no_block():
    builder = PromptBuilder()

    without_default = builder.build_val_messages(
        "query", "paper", "text", _columns(),
    )
    with_explicit_none = builder.build_val_messages(
        "query", "paper", "text", _columns(), feedback=None,
    )

    assert without_default == with_explicit_none
    assert "<PRIOR_ATTEMPT_FEEDBACK>" not in without_default[1]["content"]


def test_feedback_injects_block_when_provided():
    builder = PromptBuilder()

    messages = builder.build_val_messages(
        "query", "paper", "text", _columns(), feedback="You got this wrong before",
    )

    content = messages[1]["content"]
    assert "<PRIOR_ATTEMPT_FEEDBACK>" in content
    assert "You got this wrong before" in content
    assert content.index("<PRIOR_ATTEMPT_FEEDBACK>") < content.index("<QUESTION>")
    # Wraps the caller's fact with search-strategy guidance (mirrors
    # SYSTEM_PROMPT_VAL_REEXTRACT) and an explicit "don't fabricate" escape
    # hatch, rather than only relaying the caller's text verbatim.
    assert "tables, figures" in content
    assert "do not invent a new value just to be different" in content
