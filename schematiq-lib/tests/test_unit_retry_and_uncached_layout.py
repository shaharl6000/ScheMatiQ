"""Unit tests for unit-scoped retry prompts and uncached large-document layout."""

from unittest.mock import MagicMock, patch

from schematiq.core.schema import Column, ObservationUnit, Schema
from schematiq.value_extraction.config.prompts import build_val_with_unit_retry_prompt
from schematiq.value_extraction.core.paper_processor import PaperProcessor


def _make_processor():
    s = MagicMock()
    s._check_stop_requested.return_value = False
    s._should_skip_truncation.return_value = True
    s.llm.context_window_size = 32768
    s.llm.max_tokens_for_task.return_value = 512
    s._gemini_kwargs.return_value = {}
    s._build_response_schema.return_value = (None, {})
    s._generate.return_value = "{}"
    s.json_parser.parse_response.return_value = {}
    s._remap_response_keys.return_value = {}
    s.json_parser.postprocess.return_value = ({}, {})
    s._attach_source_to_excerpts.side_effect = lambda cleaned, _title: cleaned
    s._ground_and_enforce.side_effect = lambda cleaned, _text, _label: cleaned
    s.prompt_builder.build_val_messages.side_effect = lambda *a, **kw: [
        {"role": "system", "content": "default-system"},
        {"role": "user", "content": "user-body"},
    ]
    return s


def _schema():
    return Schema(
        query="q",
        columns=[Column(name="A"), Column(name="B")],
        observation_unit=ObservationUnit(name="Opinion", definition="One justice opinion"),
    )


@patch(
    "schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
    side_effect=lambda msgs, **kw: msgs,
)
def test_extract_values_for_unit_passes_unit_scoped_retry_prompts(_fit):
    s = _make_processor()
    schema = _schema()

    PaperProcessor.extract_values_for_unit(
        s,
        unit_name="Justice B",
        relevant_passages=["doc text"],
        schema=schema,
        max_new_tokens=256,
        paper_title="case1",
    )

    retry_kwargs = s._retry_missing_columns.call_args.kwargs
    assert retry_kwargs["system_prompt_override"] == build_val_with_unit_retry_prompt(
        "Justice B", strict=False
    )
    assert retry_kwargs["strict_system_prompt_override"] == build_val_with_unit_retry_prompt(
        "Justice B", strict=True
    )
    assert retry_kwargs["reference_query"] == "Justice B"


@patch(
    "schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
    side_effect=lambda msgs, **kw: msgs,
)
def test_retry_missing_columns_forwards_unit_prompt_to_pass2_and_pass4(_fit):
    s = _make_processor()
    s.retriever = None
    s._batch_column_attempt.return_value = {}
    schema = _schema()

    reextract_prompt = build_val_with_unit_retry_prompt("Justice B", strict=False)
    strict_prompt = build_val_with_unit_retry_prompt("Justice B", strict=True)

    PaperProcessor._retry_missing_columns(
        s,
        cleaned={},
        schema=schema,
        paper_title="case1 - Justice B",
        paper_text="doc text",
        effective_text="doc text",
        max_new_tokens=256,
        system_prompt_override=reextract_prompt,
        strict_system_prompt_override=strict_prompt,
        reference_query="Justice B",
    )

    # Pass 2 (reordered) must use the unit-scoped reextract prompt
    pass2_msgs = s._generate.call_args.args[0]
    assert pass2_msgs[0]["content"] == reextract_prompt
    assert s.prompt_builder.build_val_messages.call_args.kwargs["reference_query"] == "Justice B"

    # Pass 4 (snippet fallback) must receive the unit-scoped strict prompt
    assert s._batch_column_attempt.called
    batch_kwargs = s._batch_column_attempt.call_args.kwargs
    assert batch_kwargs["system_prompt_override"] == strict_prompt
    assert batch_kwargs["reference_query"] == "Justice B"


@patch(
    "schematiq.value_extraction.core.paper_processor.utils.fit_prompt",
    side_effect=lambda msgs, **kw: msgs,
)
def test_batch_column_attempt_applies_system_prompt_override(_fit):
    s = _make_processor()
    schema = _schema()
    strict_prompt = build_val_with_unit_retry_prompt("Justice B", strict=True)

    PaperProcessor._batch_column_attempt(
        s,
        columns=schema.columns,
        retriever=None,
        paper_text="doc text",
        schema=schema,
        paper_title="case1 - Justice B",
        system_prompt_override=strict_prompt,
        reference_query="Justice B",
    )

    sent_msgs = s._generate.call_args.args[0]
    assert sent_msgs[0]["content"] == strict_prompt
    assert s.prompt_builder.build_val_messages.call_args.kwargs["reference_query"] == "Justice B"


def _run_paper_with_units(paper_text: str, cache_obj):
    s = _make_processor()
    s._is_gemini_backend.return_value = True
    s._create_document_cache.return_value = cache_obj
    s._active_figure_images = []
    captured_layout = []

    def _capture_extract(**kwargs):
        captured_layout.append(s._uncached_doc_layout)
        return {"A": {"answer": "v", "excerpts": []}}

    s.extract_values_for_unit.side_effect = _capture_extract

    PaperProcessor.extract_values_for_paper_with_units(
        s,
        paper_title="case1",
        paper_text=paper_text,
        schema=_schema(),
        max_new_tokens=256,
        known_units=["Justice A"],
    )
    return captured_layout[0]


def test_large_uncached_document_is_not_duplicated():
    large_doc = "x" * 5000
    assert _run_paper_with_units(large_doc, cache_obj=None) is None


def test_short_uncached_document_keeps_uncached_layout():
    short_doc = "x" * 1000
    layout = _run_paper_with_units(short_doc, cache_obj=None)
    assert layout is not None
    assert layout[1] == short_doc


def test_cached_large_document_keeps_expired_cache_fallback_layout():
    large_doc = "x" * 5000
    layout = _run_paper_with_units(large_doc, cache_obj=MagicMock())
    assert layout is not None
    assert layout[1] == large_doc

