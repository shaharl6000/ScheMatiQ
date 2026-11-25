"""Rendering of SYSTEM_PROMPT_VAL_WITH_UNIT for a unit and for the shared cache."""

import inspect
import json
from pathlib import Path

from schematiq.value_extraction.config.prompts import (
    SYSTEM_PROMPT_VAL_WITH_UNIT,
    build_val_with_unit_prompt,
    build_val_with_unit_retry_prompt,
)
from schematiq.value_extraction.core import paper_processor


def test_per_call_prompt_names_the_unit_everywhere():
    p = build_val_with_unit_prompt("Amir H. Ali")
    assert "**Unit Name**: Amir H. Ali" in p
    assert 'Extract values ONLY for the unit named "Amir H. Ali"' in p
    assert 'belongs to "Amir H. Ali"' in p
    assert "{unit_" not in p


def test_unit_name_with_braces_is_inserted_literally():
    p = build_val_with_unit_prompt("Unit {x}")
    assert "**Unit Name**: Unit {x}" in p


def test_cache_prompt_keeps_rules_and_names_no_unit():
    p = build_val_with_unit_prompt(None)
    assert "**Unit Name**: given in each request below" in p
    assert "Extract values ONLY for the unit named in the current request" in p
    assert 'unit named "' not in p
    assert "{unit_" not in p
    assert "omit that column" in p
    assert len(p) > 0.9 * len(SYSTEM_PROMPT_VAL_WITH_UNIT)


def test_retry_prompts_preserve_unit_scoping():
    pass1 = build_val_with_unit_prompt("Unit X")
    assert "omit that column" in pass1
    assert '"answer": null' not in pass1

    reextract = build_val_with_unit_retry_prompt("Unit X", strict=False)
    strict = build_val_with_unit_retry_prompt("Unit X", strict=True)
    for prompt in (reextract, strict):
        assert "**Unit Name**: Unit X" in prompt
        assert 'unit named "Unit X"' in prompt
        assert '"answer": null' in prompt
        assert "omit that column" not in prompt
    assert "Re-examination Pass" in reextract
    assert "Strict Fallback Pass" in strict


def test_paper_processor_renders_only_through_the_helper():
    src = inspect.getsource(paper_processor)
    assert "cache_system_prompt = build_val_with_unit_prompt(None)" in src
    assert "system_prompt = build_val_with_unit_prompt(unit_name)" in src
    assert "SYSTEM_PROMPT_VAL_WITH_UNIT.format" not in src


def test_rendered_unit_prompts_match_baseline_fixture_byte_for_byte():
    fixture_path = Path(__file__).parent / "fixtures" / "val_with_unit_prompts.json"
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    actual = {}
    for u in ("X", None):
        actual[f"pass1_{u}"] = build_val_with_unit_prompt(u)
        for s in (False, True):
            actual[f"retry_{u}_strict_{s}"] = build_val_with_unit_retry_prompt(u, strict=s)
    assert set(actual.keys()) == set(expected.keys())
    for key in expected:
        assert actual[key].encode("utf-8") == expected[key].encode("utf-8"), (
            f"Rendered prompt {key!r} drifted from baseline fixture"
        )
