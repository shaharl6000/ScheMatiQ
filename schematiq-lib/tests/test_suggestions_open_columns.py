"""Suggested values are tracked only for columns that already have allowed_values."""

from schematiq.value_extraction.core.json_parser import JSONResponseParser


def _post(parsed, allowed):
    return JSONResponseParser().postprocess(parsed, list(parsed), column_allowed_values=allowed)


def test_flag_on_open_column_is_not_tracked():
    parsed = {"judge_name": {"answer": "Amir H. Ali", "excerpts": ["AMIR H. ALI"], "suggested_for_allowed_values": True}}
    out, unmatched = _post(parsed, {})
    assert unmatched == {}
    assert "unmatched_value" not in out["judge_name"]
    assert out["judge_name"]["answer"] == "Amir H. Ali"


def test_unmatched_value_on_closed_column_is_tracked():
    parsed = {"ruling": {"answer": "vacated", "excerpts": ["the order is vacated"]}}
    out, unmatched = _post(parsed, {"ruling": ["granted", "denied"]})
    assert unmatched == {"ruling": ["vacated"]}
    assert out["ruling"]["unmatched_value"] == "vacated"


def test_flag_on_closed_column_is_tracked():
    parsed = {"ruling": {"answer": "granted", "excerpts": ["granted"], "suggested_for_allowed_values": True}}
    _, unmatched = _post(parsed, {"ruling": ["granted", "denied"]})
    assert unmatched == {"ruling": ["granted"]}
