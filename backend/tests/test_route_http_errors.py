"""Routes return their own 4xx status instead of wrapping it in a 500 (#709)."""

import pytest
from fastapi.testclient import TestClient

import app.services as services
from app.main import app


@pytest.fixture
def client(monkeypatch):
    # No session exists, so every session route hits its own 404 branch.
    monkeypatch.setattr(services.session_manager, "get_session", lambda session_id: None)
    return TestClient(app)


@pytest.mark.parametrize(
    "method, path",
    [
        ("get", "/api/load/processing-status/missing"),
        ("get", "/api/schema/change-status/missing"),
        ("get", "/api/schematiq/status/missing"),
        ("get", "/api/schematiq/schema/missing"),
    ],
)
def test_missing_session_is_404_not_500(client, method, path):
    response = getattr(client, method)(path)

    assert response.status_code == 404, response.text


def test_invalid_upload_is_400_not_500(client):
    response = client.post(
        "/api/load/file", files={"file": ("notes.exe", b"binary", "application/octet-stream")}
    )

    assert response.status_code == 400, response.text
