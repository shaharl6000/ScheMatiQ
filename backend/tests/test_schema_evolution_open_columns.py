"""Schema evolution never changes a column's allowed_values list.

An open column never gets a list, and an existing list does not grow from
suggestions. Only the user edits the list.
"""

import asyncio
from types import SimpleNamespace

from app.models.session import ColumnInfo
from app.services.pipeline.value_extraction import process_suggested_values


class _WS:
    def __init__(self):
        self.calls = []

    async def broadcast_schema_updated(self, session_id, payload):
        self.calls.append(payload)


def _run(columns, suggested):
    session = SimpleNamespace(id="s1", columns=columns)
    ws = _WS()
    asyncio.run(process_suggested_values(session, None, suggested, ws))
    return session, ws


def _sugg(value, n):
    return {value: {"count": n, "documents": [f"doc{i}" for i in range(n)]}}


def test_open_column_never_gets_allowed_values():
    col = ColumnInfo(name="judge_name", definition="Name of the judge")
    session, ws = _run([col], {"judge_name": {**_sugg("Thacker", 3), **_sugg("Sorokin", 2)}})
    assert col.allowed_values is None
    assert not col.pending_values
    assert ws.calls == []


def test_empty_list_counts_as_open():
    col = ColumnInfo(name="judge_name", definition="d", allowed_values=[])
    _run([col], {"judge_name": _sugg("Thacker", 3)})
    assert col.allowed_values == []
    assert not col.pending_values


def test_closed_column_list_does_not_grow():
    col = ColumnInfo(name="injunction_ruling", definition="d", allowed_values=["granted", "denied"])
    _, ws = _run([col], {"injunction_ruling": {**_sugg("granted_in_part", 2), **_sugg("vacated", 1)}})
    assert col.allowed_values == ["granted", "denied"]
    assert not col.pending_values
    assert ws.calls == []
