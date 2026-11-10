"""Unit tests for explicit cell gap-fill lifecycle state transitions."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.data_editor import DataEditor
from app.services.reextraction_service import (
    CellGapState,
    ReextractionService,
    _can_overwrite_in_only_empty,
    _is_fillable_gap,
    _is_retriable_gap,
    classify_cell_gap_state,
)
from schematiq.value_extraction.core.row_manager import RowDataManager


RAW_EMPTY_NONE = None
RAW_EMPTY_DICT = {"answer": "", "excerpts": []}
CONFIRMED_EMPTY = {"answer": "", "excerpts": [], "_confirmed_empty": True}
UNGROUNDED = {"answer": "Barack Obama", "excerpts": [], "_ungrounded": True}
GROUNDED = {"answer": "District Court", "excerpts": [{"text": "District Court"}]}


def test_classify_cell_gap_state_and_retriable_predicate():
    assert classify_cell_gap_state(RAW_EMPTY_NONE) == CellGapState.RAW_EMPTY
    assert classify_cell_gap_state(RAW_EMPTY_DICT) == CellGapState.RAW_EMPTY
    assert classify_cell_gap_state(CONFIRMED_EMPTY) == CellGapState.CONFIRMED_EMPTY
    assert classify_cell_gap_state(UNGROUNDED) == CellGapState.UNGROUNDED
    assert classify_cell_gap_state(GROUNDED) == CellGapState.FILLED
    assert classify_cell_gap_state(0) == CellGapState.FILLED

    assert _is_retriable_gap(CONFIRMED_EMPTY) is True
    assert _is_retriable_gap(UNGROUNDED) is True
    assert _is_retriable_gap(RAW_EMPTY_NONE) is False
    assert _is_retriable_gap(GROUNDED) is False


def test_scan_only_empty_scope_marks_ungrounded_as_retriable_and_excludes_confirmed_empty_from_filled(
    tmp_path, monkeypatch
):
    session_id = "sess-gap"
    work_dir = tmp_path / "work" / session_id
    work_dir.mkdir(parents=True)
    data_file = work_dir / "extracted_data.jsonl"
    row = {
        "_row_name": "Case 1",
        "_papers": ["doc1.pdf"],
        "col_ungrounded": dict(UNGROUNDED),
        "col_confirmed_empty": dict(CONFIRMED_EMPTY),
        "col_grounded": dict(GROUNDED),
    }
    data_file.write_text(json.dumps(row) + "\n", encoding="utf-8")

    monkeypatch.setattr(
        "app.services.reextraction_service.DEFAULT_SCHEMATIQ_WORK_DIR",
        str(tmp_path / "work"),
    )
    monkeypatch.setattr(
        "app.services.reextraction_service.DEFAULT_DATA_DIR",
        str(tmp_path / "data"),
    )

    svc = ReextractionService(MagicMock(), MagicMock())

    # 1. Default scan (retry_confirmed_empty=False) on a scope with only an ungrounded cell:
    # must set confirmed_empty_hits=True so start_gated_reextraction triggers its one-time rescan.
    scan_ungrounded = svc._scan_only_empty_scope(
        session_id,
        columns=["col_ungrounded"],
        documents=None,
        retry_confirmed_empty=False,
    )
    assert scan_ungrounded.readable is True
    assert scan_ungrounded.empty_columns == set()
    assert scan_ungrounded.confirmed_empty_hits is True

    # Rescan with retry_confirmed_empty=True finds col_ungrounded fillable.
    rescan_ungrounded = svc._scan_only_empty_scope(
        session_id,
        columns=["col_ungrounded"],
        documents=None,
        retry_confirmed_empty=True,
    )
    assert rescan_ungrounded.empty_columns == {"col_ungrounded"}

    # 2. Confirmed-empty cells are not included in plan["filled"] context.
    scan_all = svc._scan_only_empty_scope(
        session_id,
        columns=["col_confirmed_empty", "col_grounded"],
        documents=None,
        retry_confirmed_empty=False,
    )
    unit_plan = scan_all.plan["doc1"]["Case 1"]
    assert "col_confirmed_empty" not in unit_plan["filled"]
    assert "col_grounded" in unit_plan["filled"]


def test_can_overwrite_in_only_empty_protects_ungrounded_answer_from_empty_retry():
    # Retrying an ungrounded cell with a new grounded value succeeds.
    assert (
        _can_overwrite_in_only_empty(
            UNGROUNDED,
            GROUNDED,
            retry_confirmed_empty=True,
        )
        is True
    )
    # Retrying an ungrounded cell that comes back empty/confirmed-empty must NOT wipe out the answer.
    assert (
        _can_overwrite_in_only_empty(
            UNGROUNDED,
            CONFIRMED_EMPTY,
            retry_confirmed_empty=True,
        )
        is False
    )
    # Without retry_confirmed_empty, ungrounded is not overwritten at all.
    assert (
        _can_overwrite_in_only_empty(
            UNGROUNDED,
            GROUNDED,
            retry_confirmed_empty=False,
        )
        is False
    )


def test_manual_edit_clears_confirmed_empty_and_ungrounded():
    nested = {"data": {"col": dict(CONFIRMED_EMPTY)}}
    DataEditor._apply_cell_update(SimpleNamespace(), nested, "col", "")
    cell = nested["data"]["col"]
    assert cell["manually_edited"] is True
    assert "_confirmed_empty" not in cell
    assert _is_fillable_gap(cell, retry_confirmed_empty=False) is True

    flat = {"col": dict(CONFIRMED_EMPTY)}
    DataEditor._apply_cell_update(SimpleNamespace(), flat, "col", "Manual Answer")
    assert "_confirmed_empty" not in flat["col"]


def test_row_manager_preserves_ungrounded_until_grounded():
    rm = RowDataManager()
    existing = {"_row_name": "Unit A", "_papers": ["p1"], "col": dict(UNGROUNDED)}
    empty_new = {"_row_name": "Unit A", "col": dict(CONFIRMED_EMPTY)}

    merged_still_ungrounded = rm.merge_row_data(existing, empty_new, "p2")
    assert merged_still_ungrounded["col"]["answer"] == "Barack Obama"
    assert merged_still_ungrounded["col"].get("_ungrounded") is True

    grounded_new = {
        "_row_name": "Unit A",
        "col": {"answer": "Barack Obama", "excerpts": [{"text": "President Barack Obama"}]},
    }
    merged_now_grounded = rm.merge_row_data(merged_still_ungrounded, grounded_new, "p3")
    assert "_ungrounded" not in merged_now_grounded["col"]
    assert len(merged_now_grounded["col"]["excerpts"]) == 1
