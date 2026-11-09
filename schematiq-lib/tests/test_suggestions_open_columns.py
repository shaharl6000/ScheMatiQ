"""Categorical values are never tracked as suggestions, on open or closed columns."""

from schematiq.value_extraction.core.json_parser import JSONResponseParser


def _post(parsed, allowed):
    return JSONResponseParser().postprocess(parsed, list(parsed), column_allowed_values=allowed)


def test_flag_on_open_column_is_not_tracked():
    parsed = {"judge_name": {"answer": "Amir H. Ali", "excerpts": ["AMIR H. ALI"], "suggested_for_allowed_values": True}}
    out, unmatched = _post(parsed, {})
    assert unmatched == {}
    assert "unmatched_value" not in out["judge_name"]
    assert out["judge_name"]["answer"] == "Amir H. Ali"


def test_value_not_on_closed_list_stays_in_cell_and_is_not_tracked():
    parsed = {"ruling": {"answer": "vacated", "excerpts": ["the order is vacated"]}}
    out, unmatched = _post(parsed, {"ruling": ["granted", "denied"]})
    assert unmatched == {}
    assert "unmatched_value" not in out["ruling"]
    assert out["ruling"]["answer"] == "vacated"


def test_flag_on_closed_column_is_ignored():
    parsed = {"ruling": {"answer": "granted", "excerpts": ["granted"], "suggested_for_allowed_values": True}}
    out, unmatched = _post(parsed, {"ruling": ["granted", "denied"]})
    assert unmatched == {}
    assert out["ruling"]["answer"] == "granted"
