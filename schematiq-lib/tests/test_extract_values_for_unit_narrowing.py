"""Per-unit column narrowing for only_empty re-extraction.

extract_values_for_unit(target_columns=...) must extract exactly the listed
columns for a unit (never the already-filled ones), narrow passes 2-4 to the
same set, skip the LLM entirely when the target set is empty, and always return
a row aligned to the FULL schema so the row shape is unchanged.

Follows the repo pattern of driving the method unbound with a MagicMock ``self``
(see test_controlled_generation.py), so no real LLM/retriever is needed.
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
        columns=[Column(name="A"), Column(name="B"), Column(name="C"), Column(name="D")],
        observation_unit="row",
    )


def _requested_column_names(mock_self):
    """Column names passed to build_val_messages across all pass-1 batches."""
    names = []
    for call in mock_self.prompt_builder.build_val_messages.call_args_list:
        col_dicts = call.args[3]  # 4th positional arg: [c.to_dict() ...]
        names.extend(cd["name"] for cd in col_dicts)
    return names


@patch("schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
       side_effect=lambda msgs, **kw: msgs)
def test_target_columns_narrows_pass1_and_retry(_fit):
    s = _make_self()
    schema = _schema()

    result = PaperProcessor.extract_values_for_unit(
        s, unit_name="Unit 1", relevant_passages=["text"], schema=schema,
        max_new_tokens=256, paper_title="doc1", target_columns=["B"],
    )

    # Pass 1 requested ONLY the empty target column, never A/C/D.
    assert _requested_column_names(s) == ["B"]
    # Passes 2-4 saw a schema narrowed to the same single column.
    retry_schema = s._retry_missing_columns.call_args.kwargs["schema"]
    assert [c.name for c in retry_schema.columns] == ["B"]
    # Row is still aligned to the FULL schema (shape unchanged).
    assert set(result.keys()) == {"A", "B", "C", "D"}


@patch("schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
       side_effect=lambda msgs, **kw: msgs)
def test_empty_target_skips_llm_entirely(_fit):
    s = _make_self()
    schema = _schema()

    result = PaperProcessor.extract_values_for_unit(
        s, unit_name="Unit 1", relevant_passages=["text"], schema=schema,
        max_new_tokens=256, paper_title="doc1", target_columns=[],
    )

    # No LLM work at all — this is the cost saving.
    s._generate.assert_not_called()
    s.prompt_builder.build_val_messages.assert_not_called()
    s._retry_missing_columns.assert_not_called()
    # Still returns a full-schema-aligned (empty) row for the merge no-op.
    assert set(result.keys()) == {"A", "B", "C", "D"}


@patch("schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
       side_effect=lambda msgs, **kw: msgs)
def test_none_target_is_unchanged_behavior(_fit):
    s = _make_self()
    schema = _schema()

    PaperProcessor.extract_values_for_unit(
        s, unit_name="Unit 1", relevant_passages=["text"], schema=schema,
        max_new_tokens=256, paper_title="doc1", target_columns=None,
    )

    # Default path still requests every schema column, retry sees full schema.
    assert set(_requested_column_names(s)) == {"A", "B", "C", "D"}
    retry_schema = s._retry_missing_columns.call_args.kwargs["schema"]
    assert [c.name for c in retry_schema.columns] == ["A", "B", "C", "D"]
