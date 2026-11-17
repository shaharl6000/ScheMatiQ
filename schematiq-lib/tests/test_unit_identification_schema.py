"""Tests for the Gemini response_schema used in unit identification."""

from types import SimpleNamespace

from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.utils.schema_builder import (
    build_unit_identification_response_schema,
)


def _item(schema):
    return schema["properties"]["observation_units"]["items"]


def test_schema_matches_prompt_format_without_passages():
    schema = build_unit_identification_response_schema(include_passages=False)
    assert schema["propertyOrdering"][0] == "observation_units"
    assert _item(schema)["propertyOrdering"] == ["unit_name", "confidence"]
    assert _item(schema)["properties"]["confidence"]["enum"] == ["high", "medium", "low"]


def test_schema_includes_passages_when_retriever_is_enabled():
    schema = build_unit_identification_response_schema(include_passages=True)
    assert _item(schema)["propertyOrdering"] == [
        "unit_name", "relevant_passages", "confidence",
    ]


def test_schema_only_for_gemini():
    gemini = SimpleNamespace(_is_gemini_backend=lambda: True)
    other = SimpleNamespace(_is_gemini_backend=lambda: False)
    assert PaperProcessor._unit_identification_response_schema(gemini) is not None
    assert PaperProcessor._unit_identification_response_schema(other) is None
