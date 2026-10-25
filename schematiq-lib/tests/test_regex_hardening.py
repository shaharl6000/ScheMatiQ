"""Regex patterns fixed in the regex audit: prompt shortening, the ranked-JSON salvage, numeric answers."""

import importlib.util
import time
from pathlib import Path

import pytest

from schematiq.core import utils
from schematiq.core.retrievers import safe_parse_json

# Loaded from its file: the schematiq.evaluation package fails to import (run_eval.py's "from metrics import").
_GT = Path(__file__).resolve().parents[1] / "schematiq" / "evaluation" / "gt_comparator.py"
_spec = importlib.util.spec_from_file_location("gt_comparator_under_test", _GT)
_gt_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gt_module)
GTComparator = _gt_module.GTComparator


# ---------------------------------------------------------------- fit_prompt: sentences and paper blocks

def _papers_prompt(n_papers: int, n_sentences: int) -> str:
    abstract = " ".join(f"Sentence number {i} says something about nuclear export." for i in range(n_sentences))
    papers = [f"Title {p}\nPaper content: {abstract}" for p in range(n_papers)]
    return "Header line." + "\n\nPapers:\n\n" + "\n\n".join(papers)


def test_fit_prompt_shortens_paper_contents_by_sentence():
    messages = [{"role": "system", "content": ""}, {"role": "user", "content": _papers_prompt(3, 120)}]
    out = utils.fit_prompt(messages, max_new=100, safety_margins=0, context_window_size=1200)[1]["content"]
    # It trimmed each paper to a few sentences, kept every paper and the real "Papers:" separator,
    # rather than falling through to the header only.
    assert out.startswith("Header line.\n\nPapers:\n\n")
    assert out.count("Paper content:") == 3
    assert out.count("Sentence number") < 3 * 120
    assert "Sentence number 0 says" in out


def test_fit_prompt_leaves_a_fitting_prompt_alone():
    content = _papers_prompt(1, 2)
    messages = [{"role": "system", "content": ""}, {"role": "user", "content": content}]
    assert utils.fit_prompt(messages)[1]["content"] == content


# ---------------------------------------------------------------- safe_parse_json: ranked salvage

def test_ranked_salvage_keeps_i_and_score_of_the_same_object():
    # Not valid JSON (the "x" object has no score); the salvage must not pair i=1 with the score of i=2.
    broken = '{"ranked": [{"i": 1, "note": "x"}, {"i": 2, "score": 0.5}, {"i": 3, "score": 0.25,}] oops'
    assert safe_parse_json(broken, fallback_key="ranked") == {"ranked": [{"i": 2, "score": 0.5}, {"i": 3, "score": 0.25}]}


def test_ranked_salvage_reads_whole_numbers_only():
    broken = '{"ranked": [{"i": 4, "score": 1.0.0}] oops'
    assert safe_parse_json(broken, fallback_key="ranked") == {"ranked": [{"i": 4, "score": 1.0}]}


# ---------------------------------------------------------------- JSON at the end of an LLM reply

def _parsers():
    from schematiq.value_extraction.core.json_parser import JSONResponseParser
    from schematiq.value_extraction.core.unit_parser import UnitIdentificationParser
    return [JSONResponseParser().extract_json_str, UnitIdentificationParser()._extract_json_str]


@pytest.mark.parametrize("text,expected", [
    ('Here it is: {"a": {"b": 1}}', '{"a": {"b": 1}}'),
    ('{"a": 1}\n\n', '{"a": 1}\n\n'),                        # trailing whitespace kept, as before
    ('noise {"x": 1} more {"a": 2}', '{"x": 1} more {"a": 2}'),  # from the first "{", as before
])
def test_trailing_json_is_found(text, expected):
    for extract in _parsers():
        assert extract(text) == expected


def test_trailing_json_requires_a_closing_brace_at_the_end():
    for extract in _parsers():
        with pytest.raises(ValueError):
            extract('{"a": 1} and then some prose')


def test_trailing_json_search_is_linear():
    text = "{" * 20000 + "x"  # many "{" and no closing "}": quadratic before (a retry from every "{")
    for extract in _parsers():
        start = time.perf_counter()
        with pytest.raises(ValueError):
            extract(text)
        assert time.perf_counter() - start < 0.5


# ---------------------------------------------------------------- GTComparator: numeric answers

@pytest.mark.parametrize("predicted,gt,exact", [
    ("The value is -3.5 units", "-3.5", True),
    ("about 12 residues.", "12", True),
    ("v1.2.3 gives 7", "1.2", True),  # the first number, not "1.2.3"
])
def test_numeric_match_reads_a_signed_number(predicted, gt, exact):
    assert GTComparator()._numeric_match(predicted, gt).exact_match is exact
