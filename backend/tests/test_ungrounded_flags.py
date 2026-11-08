"""Cells kept with ``_ungrounded`` (no supporting excerpt): exports, gaps, edits."""

from app.services.grounding_flags import any_ungrounded, is_ungrounded_cell, ungrounded_columns
from app.services.reextraction_service import _is_fillable_gap

FLAGGED = {"answer": "Barack Obama", "excerpts": [], "_ungrounded": True}
GROUNDED = {"answer": "district", "excerpts": [{"text": "District Court"}]}


def test_helpers():
    assert is_ungrounded_cell(FLAGGED)
    assert not is_ungrounded_cell(GROUNDED)
    assert not is_ungrounded_cell("plain")
    cells = {"court_level": GROUNDED, "appointing_president": FLAGGED, "is_trump_appointee": dict(FLAGGED)}
    cols = ["court_level", "appointing_president", "is_trump_appointee"]
    assert ungrounded_columns(cells, cols) == "Appointing President; Is Trump Appointee"
    assert ungrounded_columns({"court_level": GROUNDED}, cols) == ""
    assert any_ungrounded([{"a": GROUNDED}, cells])
    assert not any_ungrounded([{"a": GROUNDED}])


def test_flagged_cell_is_not_a_gap_unless_retrying_confirmed():
    assert _is_fillable_gap(FLAGGED) is False
    assert _is_fillable_gap(FLAGGED, retry_confirmed_empty=True) is True
    # Unchanged behavior for other cells.
    assert _is_fillable_gap(GROUNDED, retry_confirmed_empty=True) is False
    assert _is_fillable_gap({"answer": "", "excerpts": [], "_confirmed_empty": True}) is False
    assert _is_fillable_gap(None) is True


def test_manual_edit_clears_the_flag():
    from types import SimpleNamespace

    from app.services.data_editor import DataEditor

    nested = {"data": {"appointing_president": dict(FLAGGED)}}
    DataEditor._apply_cell_update(SimpleNamespace(), nested, "appointing_president", "Joe Biden")
    cell = nested["data"]["appointing_president"]
    assert cell["answer"] == "Joe Biden" and cell["manually_edited"] is True
    assert "_ungrounded" not in cell

    flat = {"appointing_president": dict(FLAGGED)}
    DataEditor._apply_cell_update(SimpleNamespace(), flat, "appointing_president", "Joe Biden")
    assert "_ungrounded" not in flat["appointing_president"]
