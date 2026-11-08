"""A unit whose extraction raises is dropped so the document's other units are
still written. The failure must stay visible: a document left with no rows gets
a skip_reason (so it reaches the skip report), and one left with some rows gets
a warning naming the failed units.

Drives extract_values_for_paper_with_units() unbound with a MagicMock ``self``,
as test_context_cache_fallback.py does with _generate().
"""
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from schematiq.core.results import ExtractionResult
from schematiq.core.schema import Column, ObservationUnit, Schema
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.core.table_builder import TableBuilder

SCHEMA = Schema(
    query="q",
    columns=[Column(name="value", definition="d")],
    observation_unit=ObservationUnit(name="Thing", definition="a thing"),
)


def _make_self(outcomes, identified=None, stop_after=None):
    """``outcomes`` maps a unit to a row dict, or to an exception it raises."""
    s = MagicMock()
    calls = []

    def extract(unit_name, **kwargs):
        calls.append(unit_name)
        outcome = outcomes[unit_name]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def stop_requested():
        return stop_after is not None and len(calls) >= stop_after

    s.extract_values_for_unit.side_effect = extract
    s._check_stop_requested.side_effect = stop_requested
    s.identify_observation_units.return_value = MagicMock(
        units=[{"unit_name": name} for name in (identified or [])], skip_reason=None
    )
    return s


def _run(s, known_units=None):
    return PaperProcessor.extract_values_for_paper_with_units(
        s, "paper", "text", SCHEMA, 100, known_units=known_units,
    )


def _row():
    return {"value": "x"}


def test_all_units_failing_with_known_units_sets_skip_reason():
    # Re-extraction: no identification step that could have reported anything.
    s = _make_self({"a": TimeoutError("timed out"), "b": TimeoutError("still down")})

    result = _run(s, known_units=["a", "b"])

    assert result.rows == []
    assert "2 of 2 units" in result.skip_reason
    assert "still down" in result.skip_reason
    s._emit_warning.assert_not_called()


def test_all_units_failing_after_identification_sets_skip_reason():
    s = _make_self({"a": TimeoutError("timed out")}, identified=["a"])

    result = _run(s)

    assert result.rows == []
    assert "1 of 1 units" in result.skip_reason


def test_failures_with_the_other_units_empty_set_skip_reason():
    # Not every unit failed, but the document still has no rows to show for it.
    s = _make_self({"a": TimeoutError("timed out"), "b": {}})

    result = _run(s, known_units=["a", "b"])

    assert result.rows == []
    assert "1 of 2 units" in result.skip_reason


def test_partial_failure_keeps_rows_and_warns_naming_the_failed_units():
    s = _make_self({"a": TimeoutError("t1"), "b": _row(), "c": ValueError("t2")})

    result = _run(s, known_units=["a", "b", "c"])

    assert [r["_unit_name"] for r in result.rows] == ["b"]
    assert result.skip_reason is None
    s._emit_warning.assert_called_once()
    title, warning_type, message = s._emit_warning.call_args.args
    assert (title, warning_type) == ("paper", "unit_extraction_failed")
    assert "2 of 3 units" in message
    assert "a, c" in message
    assert "t2" in message


def test_no_failures_reports_nothing():
    s = _make_self({"a": _row(), "b": {}})

    result = _run(s, known_units=["a", "b"])

    assert [r["_unit_name"] for r in result.rows] == ["a"]
    assert result.skip_reason is None
    s._emit_warning.assert_not_called()


def test_stop_after_a_partial_failure_counts_only_units_that_ran():
    s = _make_self({"a": TimeoutError("t"), "b": _row(), "c": _row()}, stop_after=2)

    result = _run(s, known_units=["a", "b", "c"])

    assert [r["_unit_name"] for r in result.rows] == ["b"]
    assert "a." in s._emit_warning.call_args.args[2]


def test_invalid_api_key_still_ends_the_document():
    genai_errors = pytest.importorskip("google.genai.errors")
    with pytest.raises(genai_errors.APIError) as excinfo:
        genai_errors.APIError.raise_error(
            400, {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "API key not valid"}}, None
        )
    s = _make_self({"a": excinfo.value})

    with pytest.raises(genai_errors.APIError):
        _run(s, known_units=["a"])


def test_all_failed_document_reaches_the_skip_report(tmp_path):
    s = _make_self({"a": TimeoutError("timed out")})
    result = _run(s, known_units=["a"])
    assert isinstance(result, ExtractionResult)

    tb = object.__new__(TableBuilder)
    tb._skipped_documents = []
    tb.on_unit_row_written = None
    tb.on_value_extracted = None
    paper = Path(tmp_path / "paper.txt")
    processed = set()

    written = tb._commit_paper_units(
        "row", paper, result, {paper: tmp_path}, SCHEMA, tmp_path / "out.jsonl", processed,
    )

    assert written == 0
    assert [d.document for d in tb._skipped_documents] == ["paper"]
    assert "1 of 1 units" in tb._skipped_documents[0].reason
