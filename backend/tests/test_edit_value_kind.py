"""Editing a column's value_kind from the Schema sheet (PUT /api/schema/edit-column)."""

import pytest
from fastapi.testclient import TestClient

import app.services as services
from app.main import app
from app.models.session import (
    ColumnInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)


@pytest.fixture
def client(monkeypatch):
    sessions = {}

    def get_session(session_id):
        return sessions.get(session_id)

    monkeypatch.setattr(services.session_manager, "get_session", get_session)
    monkeypatch.setattr(services.session_manager, "update_session", lambda s: sessions.__setitem__(s.id, s))

    async def _noop(*args, **kwargs):
        return None

    from app.api.routes import schema as schema_routes
    monkeypatch.setattr(schema_routes.websocket_manager, "broadcast_schema_updated", _noop)

    session = VisualizationSession(
        id="s1",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[
            ColumnInfo(name="court_level", allowed_values=["district", "appeals"], value_kind="category"),
            ColumnInfo(name="damages", allowed_values=["number"], value_kind="number"),
            ColumnInfo(name="judge_name", value_kind="identifier"),
        ],
    )
    sessions["s1"] = session
    return TestClient(app), sessions


def _edit(client, name, kind):
    return client.put("/api/schema/edit-column/s1", json={"old_name": name, "value_kind": kind, "reprocess": False})


def _col(sessions, name):
    return next(c for c in sessions["s1"].columns if c.name == name)


def test_category_to_identifier_clears_the_list(client):
    http, sessions = client
    assert _edit(http, "court_level", "identifier").status_code == 200
    col = _col(sessions, "court_level")
    assert col.value_kind == "identifier"
    assert col.allowed_values is None


def test_number_and_date_get_their_constraint_so_extraction_enforces_them(client):
    http, sessions = client
    assert _edit(http, "judge_name", "number").status_code == 200
    assert _col(sessions, "judge_name").allowed_values == ["number"]
    assert _edit(http, "judge_name", "date").status_code == 200
    assert _col(sessions, "judge_name").allowed_values == ["date"]


def test_existing_range_is_kept_when_set_to_number(client):
    http, sessions = client
    _col(sessions, "damages").allowed_values = ["0-100"]
    assert _edit(http, "damages", "number").status_code == 200
    assert _col(sessions, "damages").allowed_values == ["0-100"]


def test_identifier_to_category_keeps_the_column_and_sets_the_kind(client):
    http, sessions = client
    assert _edit(http, "judge_name", "category").status_code == 200
    col = _col(sessions, "judge_name")
    assert col.value_kind == "category"
    assert col.allowed_values is None  # the user adds the list next


def test_number_keeps_its_constraint(client):
    http, sessions = client
    assert _edit(http, "damages", "number").status_code == 200
    assert _col(sessions, "damages").allowed_values == ["number"]


def test_category_list_is_replaced_by_the_number_constraint(client):
    http, sessions = client
    assert _edit(http, "court_level", "number").status_code == 200
    assert _col(sessions, "court_level").allowed_values == ["number"]


def test_empty_value_clears_the_kind_and_keeps_the_list(client):
    http, sessions = client
    assert _edit(http, "court_level", "").status_code == 200
    col = _col(sessions, "court_level")
    assert col.value_kind is None
    assert col.allowed_values == ["district", "appeals"]


def test_unknown_kind_is_rejected(client):
    http, sessions = client
    resp = _edit(http, "court_level", "banana")
    assert resp.status_code == 400
    assert _col(sessions, "court_level").value_kind == "category"
