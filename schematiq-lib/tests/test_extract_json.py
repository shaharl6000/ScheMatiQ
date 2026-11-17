"""Tests for extract_json_payload and the parsers that use it.

Regression: the old helper replaced every '}}' with '}', which broke valid
compact JSON with nested objects (the closing braces of the inner and outer
object are adjacent). The value-extraction and unit parsers had their own
copies of the extraction logic; all three now share one helper.
"""

import json
from unittest.mock import MagicMock

import pytest

from schematiq.core.json_payload import extract_json_payload
from schematiq.core.schematiq import (
    COMPLETE_COLUMNS_RESPONSE_SCHEMA,
    _parse_observation_unit_from_llm,
    complete_partial_columns,
)
from schematiq.core.schema import Column
from schematiq.value_extraction.core.json_parser import JSONResponseParser
from schematiq.value_extraction.core.unit_parser import UnitIdentificationParser


@pytest.mark.parametrize(
    "payload",
    [
        '{"observation_unit": {"name": "x", "definition": "y"}}',
        '{"a": {"b": {"c": 1}}}',
        '{"columns": [{"name": "n", "definition": "d"}]}',
        '{"text": "literal {{braces}} in a string"}',
    ],
)
def test_valid_json_is_returned_unchanged(payload: str) -> None:
    assert extract_json_payload(payload) == payload
    assert extract_json_payload(f"  {payload}\n") == payload


@pytest.mark.parametrize(
    "raw",
    [
        'Here you go:\n```json\n{"a": {"b": 1}}\n```\n',
        '```JSON\n{"a": {"b": 1}}\n```',
        '```\n{"a": {"b": 1}}\n```',
        'Here you go: {"a": {"b": 1}}',
        '{"a": {"b": 1}}\nLet me know if you need more.',
    ],
)
def test_payload_is_found_inside_prose_and_fences(raw: str) -> None:
    assert json.loads(extract_json_payload(raw)) == {"a": {"b": 1}}


def test_double_braces_copied_from_prompt_are_normalized() -> None:
    raw = '{{"name": "x", "definition": "y"}}'
    assert json.loads(extract_json_payload(raw)) == {"name": "x", "definition": "y"}


def test_invalid_json_is_not_rewritten_when_collapsing_does_not_help() -> None:
    # Two objects: still invalid after collapsing, so the text is returned
    # as found and the caller's own fallbacks see the original braces.
    raw = '{"a": {"b": 1}} {"c": 2}'
    assert extract_json_payload(raw) == raw


def test_parse_observation_unit_from_compact_nested_json() -> None:
    raw = '{"observation_unit": {"name": "Judge", "definition": "A judge on the panel"}}'
    unit = _parse_observation_unit_from_llm(raw)
    assert unit.name == "Judge"
    assert unit.definition == "A judge on the panel"


def test_unit_parser_reads_compact_nested_json_with_trailing_prose() -> None:
    raw = '{"observation_units": [{"unit_name": "Patient 1", "confidence": "high"}]} Done.'
    result = UnitIdentificationParser().parse_response(raw)
    assert result.success
    assert [u["unit_name"] for u in result.units] == ["Patient 1"]


@pytest.mark.parametrize("parse", [
    lambda t: JSONResponseParser().extract_json_str(t),
    lambda t: UnitIdentificationParser()._extract_json_str(t),
])
def test_parsers_raise_when_there_is_no_json_object(parse) -> None:
    with pytest.raises(ValueError, match="No JSON object found"):
        parse("I could not find anything.")


def test_value_parser_reads_fenced_json() -> None:
    raw = 'Result:\n```json\n{"model": {"answer": "GPT-4", "excerpts": []}}\n```'
    parsed = JSONResponseParser().parse_response(raw)
    assert parsed["model"]["answer"] == "GPT-4"


def _completion_llm(provider: str) -> MagicMock:
    llm = MagicMock()
    llm._provider = provider
    llm.max_tokens_for_task = MagicMock(return_value=1024)
    llm.generate.return_value = json.dumps(
        {"columns": [{"name": "age", "definition": "d", "rationale": "r"}]}
    )
    return llm


def test_complete_partial_columns_uses_controlled_generation_on_gemini() -> None:
    llm = _completion_llm("gemini")
    cols = complete_partial_columns(
        [Column(name="age", locked=True)], "q", None, [], llm, context_window_size=1000
    )
    assert llm.generate.call_args.kwargs["response_schema"] is COMPLETE_COLUMNS_RESPONSE_SCHEMA
    assert cols[0].definition == "d"


def test_complete_partial_columns_sends_no_schema_to_other_backends() -> None:
    llm = _completion_llm("openai")
    complete_partial_columns(
        [Column(name="age", locked=True)], "q", None, [], llm, context_window_size=1000
    )
    assert "response_schema" not in llm.generate.call_args.kwargs
