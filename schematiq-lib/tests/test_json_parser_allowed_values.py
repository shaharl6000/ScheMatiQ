"""Tests for JSONResponseParser.postprocess's allowed_values normalization
and its interaction with excerpts.
"""

import pytest

from schematiq.value_extraction.core.json_parser import JSONResponseParser


@pytest.fixture
def parser() -> JSONResponseParser:
    return JSONResponseParser()


def test_excerpts_preserved_on_case_insensitive_exact_match(parser: JSONResponseParser) -> None:
    """A same-content match (only casing/whitespace differs) still
    describes what the excerpt supports, so the excerpt should survive."""
    parsed = {
        "graph_type": {
            "answer": "bar chart",
            "excerpts": [{"text": "shown as a bar chart in Figure 2", "source": "paper1"}],
        },
    }
    out, _ = parser.postprocess(parsed, ["graph_type"], {"graph_type": ["Bar Chart", "line plot"]})

    assert out["graph_type"]["answer"] == "Bar Chart"
    assert out["graph_type"].get("normalized_to_allowed") is True
    assert out["graph_type"]["excerpts"] == [
        {"text": "shown as a bar chart in Figure 2", "source": "paper1"}
    ]


def test_excerpts_preserved_on_fuzzy_replacement(parser: JSONResponseParser) -> None:
    """A fuzzy replacement changes the answer's actual content, so the
    excerpt (written to support the pre-normalization answer) may no
    longer perfectly describe it -- but postprocess must NOT clear it.

    Tried once: clearing left `excerpts: []` on an otherwise-real answer,
    and paper_processor.py's _ground_and_enforce nulls the WHOLE answer
    whenever excerpts end up empty (its rule for "claims an answer with no
    excerpts at all") outside the vision-derived path. That traded "answer
    with an imperfect citation" for "answer gone entirely" -- worse than
    the mismatch it was meant to avoid. Leaving the real excerpt in place
    lets _ground_and_enforce's own source-text verification decide its
    fate instead.
    """
    allowed = ["accuracy of 86.4 percent on the MMLU benchmark for large models"]
    parsed = {
        "key_finding": {
            "answer": "accuracy of 86.4% on MMLU benchmark for large models",
            "excerpts": [{"text": "we report 86.4% on MMLU", "source": "paper1"}],
        },
    }
    out, _ = parser.postprocess(parsed, ["key_finding"], {"key_finding": allowed})

    assert out["key_finding"]["answer"] == allowed[0]
    assert out["key_finding"].get("normalized_to_allowed") is True
    assert out["key_finding"]["excerpts"] == [{"text": "we report 86.4% on MMLU", "source": "paper1"}]


def test_excerpts_preserved_when_not_normalized(parser: JSONResponseParser) -> None:
    """No allowed_values in play at all -- excerpts always pass through untouched."""
    parsed = {
        "key_finding": {
            "answer": "some real, document-specific finding",
            "excerpts": [{"text": "supporting quote", "source": "paper1"}],
        },
    }
    out, _ = parser.postprocess(parsed, ["key_finding"], {})

    assert out["key_finding"]["answer"] == "some real, document-specific finding"
    assert out["key_finding"]["excerpts"] == [{"text": "supporting quote", "source": "paper1"}]
    assert "normalized_to_allowed" not in out["key_finding"]


def test_excerpts_preserved_when_answer_unmatched(parser: JSONResponseParser) -> None:
    """Answer doesn't match any allowed_value closely enough -- kept as-is
    (today's soft-enforcement behavior), excerpts untouched."""
    parsed = {
        "key_finding": {
            "answer": "a completely unrelated real finding from this document",
            "excerpts": [{"text": "supporting quote", "source": "paper1"}],
        },
    }
    out, unmatched = parser.postprocess(
        parsed, ["key_finding"], {"key_finding": ["totally different allowed value"]}
    )

    assert out["key_finding"]["answer"] == "a completely unrelated real finding from this document"
    assert out["key_finding"]["excerpts"] == [{"text": "supporting quote", "source": "paper1"}]
    assert "normalized_to_allowed" not in out["key_finding"]
    assert unmatched == {"key_finding": ["a completely unrelated real finding from this document"]}
