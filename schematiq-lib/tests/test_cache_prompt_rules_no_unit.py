"""The shared context cache keeps the extraction rules but names no unit."""

import inspect

from schematiq.value_extraction.config.prompts import SYSTEM_PROMPT_VAL_WITH_UNIT
from schematiq.value_extraction.core import paper_processor
from schematiq.value_extraction.core.paper_processor import _unit_neutral_val_prompt


def test_neutral_prompt_names_no_unit_and_keeps_rules():
    p = _unit_neutral_val_prompt()
    assert "{" not in p.replace("{\n", "").split("### Example")[0]
    assert "A judge" not in p
    assert 'unit named "' not in p
    assert "the unit named in the current request" in p
    # Rules are kept: same length class as the per-call prompt.
    assert len(p) > 0.9 * len(SYSTEM_PROMPT_VAL_WITH_UNIT)
    assert "general knowledge" in p


def test_cache_uses_neutral_prompt():
    src = inspect.getsource(paper_processor.PaperProcessor)
    assert "cache_system_prompt = _unit_neutral_val_prompt()" in src
    assert "format(unit_name=observation_unit.name)" not in src
