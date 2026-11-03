"""Rendering of SYSTEM_PROMPT_VAL_WITH_UNIT for a unit and for the shared cache."""

import inspect

from schematiq.value_extraction.config.prompts import (
    SYSTEM_PROMPT_VAL_WITH_UNIT,
    build_val_with_unit_prompt,
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
    assert "general knowledge" in p
    assert len(p) > 0.9 * len(SYSTEM_PROMPT_VAL_WITH_UNIT)


def test_paper_processor_renders_only_through_the_helper():
    src = inspect.getsource(paper_processor)
    assert "cache_system_prompt = build_val_with_unit_prompt(None)" in src
    assert "system_prompt = build_val_with_unit_prompt(unit_name)" in src
    assert "SYSTEM_PROMPT_VAL_WITH_UNIT.format" not in src
