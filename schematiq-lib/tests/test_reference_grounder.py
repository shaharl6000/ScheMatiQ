"""Tests for reference excerpt re-attribution (ReferenceGrounder).

Loaded directly from its module path so the test does not import the heavy
schematiq package (which pulls sentence-transformers at import time).
"""

import importlib.util
import os
import sys

_MODULE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "schematiq",
    "value_extraction",
    "utils",
    "reference_grounder.py",
)
_spec = importlib.util.spec_from_file_location("reference_grounder", _MODULE_PATH)
reference_grounder = importlib.util.module_from_spec(_spec)
sys.modules["reference_grounder"] = reference_grounder
_spec.loader.exec_module(reference_grounder)

ReferenceGrounder = reference_grounder.ReferenceGrounder


TABULAR_BLOB = (
    "--- Reference document: judges.csv ---\n"
    "name,appointing_president\n"
    "William Acker,Nixon\n"
    "Jane Doe,Reagan\n"
    "\n"
    "--- Reference document: notes.txt ---\n"
    "Some prose about the case that is not tabular at all here."
)


def test_no_reference_is_noop():
    g = ReferenceGrounder(None)
    assert g.active is False
    data = {"col": {"excerpts": [{"text": "x", "source": "ruling.pdf"}]}}
    g.reattribute(data)
    assert data["col"]["excerpts"][0]["source"] == "ruling.pdf"


def test_tabular_excerpt_reattributed_and_narrowed_to_single_row():
    g = ReferenceGrounder(TABULAR_BLOB)
    assert g.active is True
    data = {
        "appointing_president": {
            "answer": "Nixon",
            "excerpts": [{"text": "William Acker,Nixon", "source": "ruling.pdf"}],
        }
    }
    g.reattribute(data)
    exc = data["appointing_president"]["excerpts"][0]
    assert exc["source"] == "judges.csv"
    # Header is kept and only the matching row is included (not Jane Doe).
    assert "name,appointing_president" in exc["text"]
    assert "William Acker,Nixon" in exc["text"]
    assert "Jane Doe" not in exc["text"]


def test_source_document_excerpt_is_left_untouched():
    g = ReferenceGrounder(TABULAR_BLOB)
    data = {
        "summary": {
            "answer": "x",
            "excerpts": [
                {"text": "This quote is only in the source document.", "source": "ruling.pdf"}
            ],
        }
    }
    g.reattribute(data)
    assert data["summary"]["excerpts"][0]["source"] == "ruling.pdf"


def test_prose_reference_excerpt_reattributed_without_narrowing():
    g = ReferenceGrounder(TABULAR_BLOB)
    data = {
        "note": {
            "answer": "y",
            "excerpts": [{"text": "prose about the case", "source": "ruling.pdf"}],
        }
    }
    g.reattribute(data)
    exc = data["note"]["excerpts"][0]
    assert exc["source"] == "notes.txt"
    # Prose is not narrowed; the model's excerpt text is preserved.
    assert exc["text"] == "prose about the case"


def test_matching_is_whitespace_and_quote_insensitive():
    blob = (
        "--- Reference document: r.txt ---\n"
        "The court held that \u201cthe statute applies\u201d in full."
    )
    g = ReferenceGrounder(blob)
    data = {
        "c": {
            "answer": "z",
            # Straight quotes and collapsed spacing vs. curly quotes in the source.
            "excerpts": [{"text": 'the  statute   applies', "source": "doc.pdf"}],
        }
    }
    g.reattribute(data)
    assert data["c"]["excerpts"][0]["source"] == "r.txt"


def test_ignores_underscore_columns_and_string_excerpts():
    g = ReferenceGrounder(TABULAR_BLOB)
    data = {
        "_cell_status": {"appointing_president": "external_source"},
        "col": {"answer": "a", "excerpts": ["William Acker,Nixon"]},  # not a dict excerpt
    }
    # Should not raise, and string excerpts are skipped (only dict excerpts fixed).
    g.reattribute(data)
    assert data["col"]["excerpts"] == ["William Acker,Nixon"]


def test_short_bare_value_is_not_reattributed():
    """A short cell value that also plausibly appears in the source document must
    not flip to the reference on a bare substring match."""
    g = ReferenceGrounder(TABULAR_BLOB)
    data = {"c": {"excerpts": [{"text": "Nixon", "source": "ruling.pdf"}]}}
    g.reattribute(data)
    assert data["c"]["excerpts"][0]["source"] == "ruling.pdf"


def test_multi_row_excerpt_is_attributed_but_not_narrowed():
    """An excerpt spanning several rows is attributed to the reference but kept
    whole — narrowing must never silently drop rows."""
    blob = (
        "--- Reference document: t.csv ---\n"
        "a,b\n"
        "row1x,1\n"
        "row2y,2"
    )
    g = ReferenceGrounder(blob)
    data = {"c": {"excerpts": [{"text": "row1x,1\nrow2y,2", "source": "x.pdf"}]}}
    g.reattribute(data)
    exc = data["c"]["excerpts"][0]
    assert exc["source"] == "t.csv"
    assert "row1x,1" in exc["text"] and "row2y,2" in exc["text"]


def test_headerless_numeric_table_does_not_prepend_a_row():
    """A CSV whose first line is data (not labels) must not have that line
    prepended as a bogus header."""
    blob = (
        "--- Reference document: nohdr.csv ---\n"
        "Acker,Nixon,1990\n"
        "Doe,Reagan,1985\n"
        "Roe,Bush,1991"
    )
    g = ReferenceGrounder(blob)
    data = {"c": {"excerpts": [{"text": "Doe,Reagan,1985", "source": "x.pdf"}]}}
    g.reattribute(data)
    exc = data["c"]["excerpts"][0]
    assert exc["source"] == "nohdr.csv"
    assert exc["text"] == "Doe,Reagan,1985"
    assert "Acker" not in exc["text"]


def test_all_text_table_with_repeating_column_is_headerless():
    """An all-text table whose first row's values recur in their column is treated
    as headerless (no false header prepend)."""
    blob = (
        "--- Reference document: pairs.csv ---\n"
        "Nixon,Republican\n"
        "Reagan,Republican\n"
        "Carter,Democrat"
    )
    g = ReferenceGrounder(blob)
    data = {"c": {"excerpts": [{"text": "Reagan,Republican", "source": "x.pdf"}]}}
    g.reattribute(data)
    exc = data["c"]["excerpts"][0]
    assert exc["source"] == "pairs.csv"
    assert exc["text"] == "Reagan,Republican"
