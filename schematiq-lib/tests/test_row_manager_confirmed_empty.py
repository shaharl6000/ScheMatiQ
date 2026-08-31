"""Regression tests for _confirmed_empty preservation in RowManager.merge_row_data.

A cell the LLM explicitly confirmed empty is stored as
``{"answer": "", "excerpts": [], "_confirmed_empty": True}``. That marker lets
only_empty re-extraction runs treat the cell as resolved (via ``_is_fillable_gap``)
instead of re-billing the model on the same gap every run. merge_row_data used to
reconstruct merged cells with only ``answer``/``excerpts`` and to drop empty new
cells entirely, silently losing the marker. These tests pin the preservation.
"""

from schematiq.value_extraction.core.row_manager import RowDataManager


def _confirmed_empty_cell():
    return {"answer": "", "excerpts": [], "_confirmed_empty": True}


def test_new_confirmed_empty_column_is_preserved():
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["paper1"]}
    new = {"_row_name": "Unit A", "col": _confirmed_empty_cell()}

    merged = rm.merge_row_data(existing, new, "paper2")

    assert "col" in merged, "confirmed-empty cell was dropped on first write"
    assert merged["col"].get("_confirmed_empty") is True


def test_plain_empty_new_column_is_still_omitted():
    # Only *confirmed* empties are preserved; a blank-without-marker cell keeps
    # the prior behaviour of being omitted so it stays a fillable gap.
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["paper1"]}
    new = {"_row_name": "Unit A", "col": {"answer": "", "excerpts": []}}

    merged = rm.merge_row_data(existing, new, "paper2")

    assert "col" not in merged


def test_confirmed_empty_survives_merge_when_both_empty():
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["paper1"], "col": _confirmed_empty_cell()}
    new = {"_row_name": "Unit A", "col": _confirmed_empty_cell()}

    merged = rm.merge_row_data(existing, new, "paper2")

    assert merged["col"].get("_confirmed_empty") is True
    assert merged["col"]["answer"] == ""


def test_real_answer_clears_confirmed_empty_marker():
    # When a later paper supplies a real value, the cell is now filled and the
    # marker must NOT linger (otherwise only_empty would wrongly skip a real cell).
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["paper1"], "col": _confirmed_empty_cell()}
    new = {"_row_name": "Unit A", "col": {"answer": "42", "excerpts": ["found it"]}}

    merged = rm.merge_row_data(existing, new, "paper2")

    assert merged["col"]["answer"] == "42"
    assert "_confirmed_empty" not in merged["col"]


def test_existing_real_answer_is_not_overwritten_by_confirmed_empty():
    # A confirmed-empty new cell must not wipe an existing real value.
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["paper1"], "col": {"answer": "keep me", "excerpts": []}}
    new = {"_row_name": "Unit A", "col": _confirmed_empty_cell()}

    merged = rm.merge_row_data(existing, new, "paper2")

    assert merged["col"]["answer"] == "keep me"
    assert "_confirmed_empty" not in merged["col"]
