"""Regression tests for LLM JSON extraction/parsing in schematiq.core.schematiq.

Covers the bug where _extract_json blindly collapsed `}}` -> `}`, mangling any
compact nested JSON returned by Gemini under response_schema.
"""

import json

import pytest

from schematiq.core.schematiq import (
    _extract_json,
    _parse_observation_unit_from_llm,
    ObservationUnitDiscoveryError,
)


# ── _extract_json: must not mangle valid nested JSON ──────────────────────

class TestExtractJsonPreservesNestedBraces:
    def test_compact_nested_object_round_trips(self):
        """Compact nested JSON must remain parseable after extraction."""
        raw = '{"observation_unit": {"name": "Patient", "definition": "X"}}'
        cleaned = _extract_json(raw)
        # Must still be valid JSON — the trailing }} must NOT be collapsed.
        json.loads(cleaned)

    def test_pretty_printed_nested_object_round_trips(self):
        raw = (
            '{\n'
            '  "observation_unit": {\n'
            '    "name": "Patient",\n'
            '    "definition": "X"\n'
            '  }\n'
            '}'
        )
        cleaned = _extract_json(raw)
        json.loads(cleaned)

    def test_code_fence_is_stripped(self):
        raw = '```json\n{"observation_unit": {"name": "X", "definition": "Y"}}\n```'
        cleaned = _extract_json(raw)
        obj = json.loads(cleaned)
        assert obj["observation_unit"]["name"] == "X"


# ── _parse_observation_unit_from_llm: end-to-end ──────────────────────────

class TestParseObservationUnit:
    def test_parses_compact_gemini_response(self):
        """The exact failure mode from the bug: compact single-line response."""
        raw = (
            '{"observation_unit": {"name": "Patient with Type 2 Diabetes",'
            ' "definition": "A clinical research subject diagnosed with T2DM",'
            ' "example_names": ["P-001", "P-002"]}}'
        )
        unit = _parse_observation_unit_from_llm(raw)
        assert unit.name == "Patient with Type 2 Diabetes"
        assert unit.example_names == ["P-001", "P-002"]

    def test_parses_pretty_printed_response(self):
        raw = (
            '{\n'
            '  "observation_unit": {\n'
            '    "name": "Protein",\n'
            '    "definition": "A characterized protein entity",\n'
            '    "example_names": []\n'
            '  }\n'
            '}'
        )
        unit = _parse_observation_unit_from_llm(raw)
        assert unit.name == "Protein"

    def test_parses_response_with_code_fence(self):
        raw = '```json\n{"observation_unit": {"name": "Drug", "definition": "A drug"}}\n```'
        unit = _parse_observation_unit_from_llm(raw)
        assert unit.name == "Drug"

    def test_fallback_handles_double_brace_artifact(self):
        """If an LLM literally copies {{ }} from prompt examples, fallback should rescue."""
        raw = '{{"observation_unit": {{"name": "Gene", "definition": "A gene entity"}}}}'
        unit = _parse_observation_unit_from_llm(raw)
        assert unit.name == "Gene"

    def test_genuinely_invalid_json_still_raises(self):
        raw = '{"observation_unit": {"name": "X", "definition":'  # truncated
        with pytest.raises(ObservationUnitDiscoveryError):
            _parse_observation_unit_from_llm(raw)
