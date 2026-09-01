"""``feedback`` threading through extract_values_for_unit.

extract_values_for_unit(feedback=...) must forward the note verbatim into
every build_val_messages call it makes; omitting it (the default, used by
every existing caller) must not silently pass anything else.

Follows the repo pattern of driving the method unbound with a MagicMock
``self`` (see test_extract_values_for_unit_narrowing.py), so no real
LLM/retriever is needed.
"""

from unittest.mock import MagicMock, patch

from schematiq.core.schema import Schema, Column
from schematiq.value_extraction.core.paper_processor import PaperProcessor


def _make_self():
    """A MagicMock self wired just enough for pass-1 + retry to run."""
    s = MagicMock()
    s._check_stop_requested.return_value = False
    s._should_skip_truncation.return_value = True
    s.llm.context_window_size = 8192
    s.llm.max_tokens_for_task.return_value = 512
    s._gemini_kwargs.return_value = {}
    s.prompt_builder.build_val_messages.return_value = [{"content": ""}]
    s._build_response_schema.return_value = (None, {})
    s._generate.return_value = "{}"
    s.json_parser.parse_response.return_value = {}
    s._remap_response_keys.return_value = {}
    s.json_parser.postprocess.return_value = ({}, {})
    s._attach_source_to_excerpts.side_effect = lambda cleaned, _title: cleaned
    return s


def _schema():
    return Schema(
        query="q",
        columns=[Column(name="A"), Column(name="B")],
        observation_unit="row",
    )


@patch("schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
       side_effect=lambda msgs, **kw: msgs)
def test_feedback_forwarded_to_build_val_messages(_fit):
    s = _make_self()
    schema = _schema()

    PaperProcessor.extract_values_for_unit(
        s, unit_name="Unit 1", relevant_passages=["text"], schema=schema,
        max_new_tokens=256, paper_title="doc1", feedback="wrong, try again",
    )

    for call in s.prompt_builder.build_val_messages.call_args_list:
        assert call.kwargs["feedback"] == "wrong, try again"


@patch("schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
       side_effect=lambda msgs, **kw: msgs)
def test_no_feedback_is_unchanged(_fit):
    s = _make_self()
    schema = _schema()

    PaperProcessor.extract_values_for_unit(
        s, unit_name="Unit 1", relevant_passages=["text"], schema=schema,
        max_new_tokens=256, paper_title="doc1",
    )

    for call in s.prompt_builder.build_val_messages.call_args_list:
        assert call.kwargs.get("feedback") is None
