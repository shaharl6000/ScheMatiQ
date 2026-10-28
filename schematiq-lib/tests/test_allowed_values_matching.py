"""Categorical allowed_values matching, without a live model."""

import json

from schematiq.core.prompts import (
    SYSTEM_PROMPT_COMPLETE_COLUMNS,
    SYSTEM_PROMPT_DOCUMENT_ONLY,
    SYSTEM_PROMPT_QUERY_ONLY,
    SYSTEM_PROMPT_STANDARD,
)
from schematiq.core.schematiq import SCHEMA_DISCOVERY_RESPONSE_SCHEMA
from schematiq.value_extraction.config.prompts import (
    SYSTEM_PROMPT_VAL,
    SYSTEM_PROMPT_VAL_REEXTRACT,
    SYSTEM_PROMPT_VAL_STRICT,
    SYSTEM_PROMPT_VAL_WITH_UNIT,
)
from schematiq.value_extraction.core.json_parser import JSONResponseParser
from schematiq.value_extraction.utils.prompt_builder import PromptBuilder
from schematiq.value_extraction.utils.schema_builder import (
    build_extraction_response_schema,
)


def _parser() -> JSONResponseParser:
    return JSONResponseParser()


def test_case_whitespace_and_end_punctuation_map_to_the_listed_value():
    parser = _parser()
    allowed = {"status": ["yes", "no"]}
    for raw in ("Yes.", "  yes ", "YES", "yes!", "(yes)"):
        parsed = {"status": {"answer": raw, "excerpts": ["the study says yes"]}}
        out, unmatched = parser.postprocess(parsed, ["status"], allowed)
        assert out["status"]["answer"] == "yes", raw
        assert out["status"].get("normalized_to_allowed") is True
        assert "unmatched_value" not in out["status"]
        assert unmatched == {}


def test_different_value_stays_in_the_cell_and_is_not_a_suggestion():
    parser = _parser()
    parsed = {
        "court": {
            "answer": "district court",
            "excerpts": ["the district court held"],
            "suggested_for_allowed_values": True,
        }
    }
    out, unmatched = parser.postprocess(
        parsed,
        ["court"],
        {"court": ["supreme court", "appellate court"]},
    )
    assert out["court"]["answer"] == "district court"
    assert "unmatched_value" not in out["court"]
    assert "suggested_for_allowed_values" not in out["court"]
    assert unmatched == {}


def test_type_1_does_not_become_type_2():
    parser = _parser()
    parsed = {"kind": {"answer": "Type 1", "excerpts": ["Type 1 diabetes"]}}
    out, unmatched = parser.postprocess(
        parsed, ["kind"], {"kind": ["Type 2", "Type 3"]}
    )
    assert out["kind"]["answer"] == "Type 1"
    assert unmatched == {}

    both = parser.postprocess(
        {"kind": {"answer": "Type 1", "excerpts": ["Type 1"]}},
        ["kind"],
        {"kind": ["Type 1", "Type 2"]},
    )[0]
    assert both["kind"]["answer"] == "Type 1"

    spaced = parser.postprocess(
        {"kind": {"answer": "type  1", "excerpts": ["type  1"]}},
        ["kind"],
        {"kind": ["Type 1", "Type 2"]},
    )[0]
    assert spaced["kind"]["answer"] == "Type 1"


def test_date_number_and_range_normalization_is_unchanged():
    parser = _parser()

    out, matched, unmatched = parser._normalize_to_allowed_values("May 6, 2025", ["date"])
    assert matched and out == "2025-05-06" and unmatched is None

    out, matched, unmatched = parser._normalize_to_allowed_values("95%", ["number"])
    assert matched and out == "95" and unmatched is None

    out, matched, unmatched = parser._normalize_to_allowed_values("42", ["0-100"])
    assert matched and out == "42" and unmatched is None

    out, matched, unmatched = parser._normalize_to_allowed_values("150", ["0-100"])
    assert matched and out == "100.0" and unmatched is None

    out, matched, unmatched = parser._normalize_to_allowed_values("not a date", ["date"])
    assert not matched and out == "not a date" and unmatched == "not a date"


def test_controlled_generation_schema_has_no_suggestion_flag():
    from dataclasses import dataclass

    @dataclass
    class Col:
        name: str

    schema, _key_map = build_extraction_response_schema([Col(name="court")])
    assert schema is not None
    props = schema["properties"]["court"]["properties"]
    assert "suggested_for_allowed_values" not in props
    assert "answer" in props and "excerpts" in props


def test_parse_response_drops_a_stray_suggestion_flag():
    parser = _parser()
    raw = json.dumps({
        "court": {
            "answer": "district court",
            "excerpts": ["district court"],
            "suggested_for_allowed_values": True,
        }
    })
    result = parser.parse_response(raw)
    assert "suggested_for_allowed_values" not in result["court"]


def test_column_block_states_the_categorical_rules():
    builder = PromptBuilder()
    messages = builder.build_val_messages(
        "q",
        "title",
        "The district court reversed.",
        [{
            "column": "court",
            "definition": "Which court decided the case",
            "allowed_values": ["supreme court", "appellate court"],
        }],
    )
    user = messages[1]["content"]
    assert "supreme court" in user
    assert "Do not pick the closest listed value" in user
    assert "use null" in user

    number_messages = builder.build_val_messages(
        "q",
        "title",
        "The score was 95.",
        [{"column": "score", "definition": "The score", "allowed_values": ["0-100"]}],
    )
    number_user = number_messages[1]["content"]
    assert "0-100" in number_user
    assert "closest listed value" not in number_user


def test_prompts_that_are_sent_describe_categorical_rules():
    for prompt in (
        SYSTEM_PROMPT_VAL,
        SYSTEM_PROMPT_VAL_STRICT,
        SYSTEM_PROMPT_VAL_REEXTRACT,
        SYSTEM_PROMPT_VAL_WITH_UNIT.format(unit_name="Method B"),
    ):
        assert "suggested_for_allowed_values" not in prompt
        assert "closest" in prompt
        assert "Supreme Court" in prompt

    for prompt in (
        SYSTEM_PROMPT_STANDARD,
        SYSTEM_PROMPT_DOCUMENT_ONLY,
        SYSTEM_PROMPT_QUERY_ONLY,
        SYSTEM_PROMPT_COMPLETE_COLUMNS,
    ):
        assert "Evolving allowed_values" not in prompt
        assert "suggested_value_additions" not in prompt
        assert "short labels shared by many documents" in prompt

    assert "suggested_value_additions" not in SCHEMA_DISCOVERY_RESPONSE_SCHEMA["properties"]
