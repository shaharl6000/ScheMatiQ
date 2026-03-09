"""Tests for controlled generation (response_schema) and schema builder."""

import json
from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from schematiq.value_extraction.utils.schema_builder import (
    build_extraction_response_schema,
)
from schematiq.value_extraction.core.json_parser import JSONResponseParser


@dataclass
class FakeColumn:
    name: str
    rationale: str = ""
    definition: str = ""
    allowed_values: Optional[List[str]] = None


# ── Schema Builder Tests ──────────────────────────────────────────────────


class TestBuildExtractionResponseSchema:
    def test_basic(self):
        cols = [
            FakeColumn(name="model"),
            FakeColumn(name="accuracy"),
        ]
        schema = build_extraction_response_schema(cols)
        assert schema["type"] == "OBJECT"
        assert "model" in schema["properties"]
        assert "accuracy" in schema["properties"]
        assert "required" not in schema  # all columns optional

    def test_empty(self):
        schema = build_extraction_response_schema([])
        assert schema["type"] == "OBJECT"
        assert schema["properties"] == {}

    def test_column_structure(self):
        cols = [FakeColumn(name="x")]
        schema = build_extraction_response_schema(cols)
        col_schema = schema["properties"]["x"]
        assert col_schema["properties"]["answer"]["type"] == "STRING"
        assert col_schema["properties"]["excerpts"]["type"] == "ARRAY"
        assert "answer" in col_schema["required"]
        assert "excerpts" in col_schema["required"]

    def test_suggested_for_allowed_values_optional(self):
        cols = [FakeColumn(name="y")]
        schema = build_extraction_response_schema(cols)
        col_schema = schema["properties"]["y"]
        assert "suggested_for_allowed_values" in col_schema["properties"]
        assert "suggested_for_allowed_values" not in col_schema["required"]


# ── JSON Parser Tests (controlled generation paths) ──────────────────────


class TestJSONParserControlledGeneration:
    def setup_method(self):
        self.parser = JSONResponseParser()

    def test_direct_json(self):
        raw = '{"model": {"answer": "GPT-4", "excerpts": ["GPT-4 achieves..."]}}'
        result = self.parser.parse_response(raw)
        assert result["model"]["answer"] == "GPT-4"
        assert result["model"]["excerpts"] == ["GPT-4 achieves..."]

    def test_fenced_fallback(self):
        raw = 'Here is the extraction:\n```json\n{"model": {"answer": "GPT-4", "excerpts": []}}\n```'
        result = self.parser.parse_response(raw)
        assert result["model"]["answer"] == "GPT-4"

    def test_empty_controlled(self):
        result = self.parser.parse_response("{}")
        assert result == {}

    def test_controlled_vs_text_same_output(self):
        """Both controlled generation and fenced JSON produce identical results."""
        clean = '{"model": {"answer": "GPT-4", "excerpts": ["GPT-4 achieves..."]}}'
        fenced = '```json\n{"model": {"answer": "GPT-4", "excerpts": ["GPT-4 achieves..."]}}\n```'
        result_controlled = self.parser.parse_response(clean)
        result_text = self.parser.parse_response(fenced)
        assert result_controlled == result_text

    def test_suggested_for_allowed_values_preserved(self):
        raw = json.dumps({
            "model_type": {
                "answer": "mamba",
                "excerpts": ["We propose a novel Mamba-based architecture..."],
                "suggested_for_allowed_values": True,
            }
        })
        result = self.parser.parse_response(raw)
        assert result["model_type"]["suggested_for_allowed_values"] is True

    def test_multiple_columns(self):
        raw = json.dumps({
            "model": {"answer": "GPT-4", "excerpts": ["GPT-4..."]},
            "dataset": {"answer": "MMLU", "excerpts": ["MMLU benchmark"]},
            "accuracy": {"answer": "86.4", "excerpts": ["86.4%"]},
        })
        result = self.parser.parse_response(raw)
        assert len(result) == 3
        assert result["accuracy"]["answer"] == "86.4"


# ── Feature Flag Tests ───────────────────────────────────────────────────


class TestFeatureFlag:
    def test_controlled_generation_disabled(self):
        import schematiq.value_extraction.core.paper_processor as pp_module

        original = pp_module.ENABLE_CONTROLLED_GENERATION
        try:
            pp_module.ENABLE_CONTROLLED_GENERATION = False
            processor = MagicMock()
            processor._is_gemini_backend = lambda: True
            # Call the real method
            from schematiq.value_extraction.core.paper_processor import PaperProcessor
            result = PaperProcessor._build_response_schema(processor, [FakeColumn(name="x")])
            assert result is None
        finally:
            pp_module.ENABLE_CONTROLLED_GENERATION = original

    def test_non_gemini_backend_returns_none(self):
        processor = MagicMock()
        processor._is_gemini_backend = lambda: False
        from schematiq.value_extraction.core.paper_processor import PaperProcessor
        result = PaperProcessor._build_response_schema(processor, [FakeColumn(name="x")])
        assert result is None

    def test_gemini_backend_returns_schema(self):
        import schematiq.value_extraction.core.paper_processor as pp_module

        original = pp_module.ENABLE_CONTROLLED_GENERATION
        try:
            pp_module.ENABLE_CONTROLLED_GENERATION = True
            processor = MagicMock()
            processor._is_gemini_backend = lambda: True
            from schematiq.value_extraction.core.paper_processor import PaperProcessor
            result = PaperProcessor._build_response_schema(processor, [FakeColumn(name="x")])
            assert result is not None
            assert result["type"] == "OBJECT"
            assert "x" in result["properties"]
        finally:
            pp_module.ENABLE_CONTROLLED_GENERATION = original
