"""Tests for _extract_json and the parsers that use it.

Regression: _extract_json replaced every '}}' with '}', which broke valid
compact JSON with nested objects (the closing braces of the inner and outer
object are adjacent).
"""

import json

import pytest

from schematiq.core.schematiq import (
    _extract_json,
    _parse_observation_unit_from_llm,
)


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
    assert _extract_json(payload) == payload
    assert json.loads(_extract_json(payload)) == json.loads(payload)


def test_code_fence_is_stripped() -> None:
    raw = 'Here you go:\n```json\n{"a": {"b": 1}}\n```\n'
    assert json.loads(_extract_json(raw)) == {"a": {"b": 1}}


def test_double_braces_copied_from_prompt_are_normalized() -> None:
    raw = '{{"name": "x", "definition": "y"}}'
    assert json.loads(_extract_json(raw)) == {"name": "x", "definition": "y"}


def test_parse_observation_unit_from_compact_nested_json() -> None:
    raw = '{"observation_unit": {"name": "Judge", "definition": "A judge on the panel"}}'
    unit = _parse_observation_unit_from_llm(raw)
    assert unit.name == "Judge"
    assert unit.definition == "A judge on the panel"
