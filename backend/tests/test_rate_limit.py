"""Tests for per-client rate limits on LLM-starting endpoints."""

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from limits import parse

from app.core import rate_limit


@pytest.fixture
def limited_app(monkeypatch):
    monkeypatch.setattr(rate_limit, "RATE_LIMIT_ENABLED", True)
    monkeypatch.setattr(rate_limit, "_RUNS_LIMIT", parse("2/hour"))
    monkeypatch.setattr(rate_limit, "TRUSTED_PROXY_HOPS", 1)
    rate_limit.reset_for_tests()

    app = FastAPI()

    @app.post("/run", dependencies=[Depends(rate_limit.limit_llm_runs)])
    async def run():
        return {"ok": True}

    yield TestClient(app)
    rate_limit.reset_for_tests()


def test_blocks_after_limit_with_retry_after(limited_app):
    headers = {"X-Forwarded-For": "203.0.113.5"}
    assert limited_app.post("/run", headers=headers).status_code == 200
    assert limited_app.post("/run", headers=headers).status_code == 200
    resp = limited_app.post("/run", headers=headers)
    assert resp.status_code == 429
    assert int(resp.headers["Retry-After"]) > 0


def test_clients_have_separate_buckets(limited_app):
    for _ in range(2):
        limited_app.post("/run", headers={"X-Forwarded-For": "203.0.113.5"})
    assert limited_app.post("/run", headers={"X-Forwarded-For": "203.0.113.5"}).status_code == 429
    assert limited_app.post("/run", headers={"X-Forwarded-For": "198.51.100.7"}).status_code == 200


def test_spoofed_leftmost_forwarded_for_is_ignored(limited_app):
    # The proxy appends the real client on the right; a client that prepends
    # fake addresses must still land in its own bucket.
    for fake in ("1.1.1.1", "2.2.2.2", "3.3.3.3"):
        resp = limited_app.post("/run", headers={"X-Forwarded-For": f"{fake}, 203.0.113.5"})
    assert resp.status_code == 429


def test_disabled_never_blocks(limited_app, monkeypatch):
    monkeypatch.setattr(rate_limit, "RATE_LIMIT_ENABLED", False)
    for _ in range(5):
        assert limited_app.post("/run").status_code == 200


def test_llm_routes_carry_the_limit():
    from app.api.routes import chat, load, observation_unit, reference, schema, schematiq

    runs, chat_dep = rate_limit.limit_llm_runs, rate_limit.limit_chat
    expected = [
        (schematiq.router, "/run/{session_id}", runs),
        (schematiq.router, "/resume/{session_id}", runs),
        (schema.router, "/reextract/{session_id}", runs),
        (schema.router, "/reprocess/{session_id}", runs),
        (schema.router, "/continue-discovery/start/{session_id}", runs),
        (load.router, "/rediscover/{session_id}", runs),
        (load.router, "/process-documents/{session_id}", runs),
        (observation_unit.router, "/add/{session_id}", runs),
        (reference.router, "/{session_id}/fill-column", runs),
        (chat.router, "/{session_id}/message", chat_dep),
        (chat.router, "/{session_id}/confirm", chat_dep),
    ]
    for router, path, dep in expected:
        deps = [
            d.dependency
            for route in router.routes
            if getattr(route, "path", None) == path
            for d in getattr(route, "dependencies", [])
        ]
        assert dep in deps, path
