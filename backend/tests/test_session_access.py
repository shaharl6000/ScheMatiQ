"""Tests for session ownership enforcement.

Covers both rollout phases: AUTH_ENFORCED off (record ownership, deny nothing)
and AUTH_ENFORCED on (ownership required), plus the legacy-session policy that
decides what happens to sessions nobody has claimed.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.core import session_access as access_module
from app.core.auth import AuthenticatedUser
from app.core.session_access import require_session_access

OWNER = AuthenticatedUser(id="owner-uuid-aaaa", email="owner@example.org")
STRANGER = AuthenticatedUser(id="stranger-uuid-bbbb", email="stranger@example.org")


class _Session:
    def __init__(self, session_id: str, owner_id: str | None = None) -> None:
        self.id = session_id
        self.owner_id = owner_id


@pytest.fixture
def store(monkeypatch):
    """Stand-in session store that records writes, so claims are observable."""
    sessions: dict[str, _Session] = {}
    writes: list[str] = []

    def _get_session(session_id: str):
        return sessions.get(session_id)

    def _update_session(session):
        writes.append(session.id)

    monkeypatch.setattr(
        access_module.session_manager, "get_session", _get_session, raising=True
    )
    monkeypatch.setattr(
        access_module.session_manager, "update_session", _update_session, raising=True
    )
    return type("Store", (), {"sessions": sessions, "writes": writes})()


def _mode(monkeypatch, *, enforced: bool, legacy: str = "allow") -> None:
    monkeypatch.setattr(access_module, "AUTH_ENFORCED", enforced, raising=True)
    monkeypatch.setattr(
        access_module, "AUTH_LEGACY_SESSION_POLICY", legacy, raising=True
    )


# --- nothing to decide ----------------------------------------------------


@pytest.mark.asyncio
async def test_routes_without_a_session_are_untouched(store, monkeypatch):
    _mode(monkeypatch, enforced=True)
    assert await require_session_access(session_id=None, user=None) is None


@pytest.mark.asyncio
async def test_unknown_session_is_left_to_the_route(store, monkeypatch):
    """The route already 404s; this must not become a second not-found path."""
    _mode(monkeypatch, enforced=True)
    assert await require_session_access(session_id="ghost", user=OWNER) is None


# --- phase 1: AUTH_ENFORCED off ------------------------------------------


@pytest.mark.asyncio
async def test_authenticated_caller_claims_an_unowned_session(store, monkeypatch):
    """This is the migration: real users backfill ownership just by using the app."""
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=None)

    await require_session_access(session_id="s1", user=OWNER)

    assert store.sessions["s1"].owner_id == OWNER.id
    assert store.writes == ["s1"], "the claim must be persisted"


@pytest.mark.asyncio
async def test_claiming_happens_even_before_enforcement(store, monkeypatch):
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=None)
    await require_session_access(session_id="s1", user=OWNER)
    assert store.sessions["s1"].owner_id == OWNER.id


@pytest.mark.asyncio
async def test_a_stranger_is_not_denied_while_enforcement_is_off(store, monkeypatch):
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    assert await require_session_access(session_id="s1", user=STRANGER) is None


@pytest.mark.asyncio
async def test_anonymous_is_not_denied_while_enforcement_is_off(store, monkeypatch):
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    assert await require_session_access(session_id="s1", user=None) is None


@pytest.mark.asyncio
async def test_an_owned_session_is_not_reclaimed(store, monkeypatch):
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    await require_session_access(session_id="s1", user=STRANGER)
    assert store.sessions["s1"].owner_id == OWNER.id
    assert store.writes == []


@pytest.mark.asyncio
async def test_a_failed_claim_does_not_break_the_request(store, monkeypatch):
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=None)

    def _boom(session):
        raise RuntimeError("storage down")

    monkeypatch.setattr(
        access_module.session_manager, "update_session", _boom, raising=True
    )
    assert await require_session_access(session_id="s1", user=OWNER) is None


# --- phase 2: AUTH_ENFORCED on -------------------------------------------


@pytest.mark.asyncio
async def test_owner_is_allowed(store, monkeypatch):
    _mode(monkeypatch, enforced=True)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    assert await require_session_access(session_id="s1", user=OWNER) is None


@pytest.mark.asyncio
async def test_stranger_gets_404_not_403(store, monkeypatch):
    """403 would confirm the id exists, which is the enumeration signal we removed."""
    _mode(monkeypatch, enforced=True)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    with pytest.raises(HTTPException) as exc:
        await require_session_access(session_id="s1", user=STRANGER)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_anonymous_gets_401_on_an_owned_session(store, monkeypatch):
    _mode(monkeypatch, enforced=True)
    store.sessions["s1"] = _Session("s1", owner_id=OWNER.id)
    with pytest.raises(HTTPException) as exc:
        await require_session_access(session_id="s1", user=None)
    assert exc.value.status_code == 401
    assert exc.value.headers["WWW-Authenticate"] == "Bearer"


# --- legacy sessions under enforcement -----------------------------------


@pytest.mark.asyncio
async def test_legacy_deny_blocks_anonymous_access_to_unowned(store, monkeypatch):
    _mode(monkeypatch, enforced=True, legacy="deny")
    store.sessions["s1"] = _Session("s1", owner_id=None)
    with pytest.raises(HTTPException) as exc:
        await require_session_access(session_id="s1", user=None)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_legacy_allow_permits_anonymous_access_to_unowned(store, monkeypatch):
    _mode(monkeypatch, enforced=True, legacy="allow")
    store.sessions["s1"] = _Session("s1", owner_id=None)
    assert await require_session_access(session_id="s1", user=None) is None


@pytest.mark.asyncio
async def test_authenticated_caller_still_claims_under_enforcement(store, monkeypatch):
    _mode(monkeypatch, enforced=True, legacy="deny")
    store.sessions["s1"] = _Session("s1", owner_id=None)
    await require_session_access(session_id="s1", user=OWNER)
    assert store.sessions["s1"].owner_id == OWNER.id


@pytest.mark.asyncio
async def test_a_claimed_legacy_session_then_excludes_others(store, monkeypatch):
    """End to end: claim under phase 1, then enforce and lock others out."""
    _mode(monkeypatch, enforced=False)
    store.sessions["s1"] = _Session("s1", owner_id=None)
    await require_session_access(session_id="s1", user=OWNER)

    _mode(monkeypatch, enforced=True)
    assert await require_session_access(session_id="s1", user=OWNER) is None
    with pytest.raises(HTTPException) as exc:
        await require_session_access(session_id="s1", user=STRANGER)
    assert exc.value.status_code == 404


# --- the model default ----------------------------------------------------


def test_new_sessions_default_to_unowned():
    from app.models.session import SessionMetadata, SessionType, VisualizationSession

    session = VisualizationSession(
        id="s1", type=SessionType.UPLOAD, metadata=SessionMetadata(source="test")
    )
    assert session.owner_id is None


# --- the wiring in main.py -------------------------------------------------


@pytest.mark.asyncio
async def test_the_dependency_is_actually_wired_to_session_routes(monkeypatch):
    """Unit tests above prove the rule; this proves it is attached to the app.

    Goes through the real ASGI app so a missing `dependencies=[...]` on an
    include_router call cannot pass unnoticed.
    """
    import time

    import jwt
    from fastapi.testclient import TestClient

    import app.main as main_module
    from app.core import auth as auth_module

    secret = "test-jwt-secret-value-long-enough-for-sha256"
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_SECRET", secret, raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_AUDIENCE", "authenticated", raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWKS_URL", "", raising=True)

    sessions = {"proj-1": _Session("proj-1", owner_id=OWNER.id)}
    monkeypatch.setattr(
        access_module.session_manager, "get_session", lambda sid: sessions.get(sid),
        raising=True,
    )
    _mode(monkeypatch, enforced=True, legacy="deny")

    def token(subject: str) -> str:
        return jwt.encode(
            {"sub": subject, "aud": "authenticated", "exp": int(time.time()) + 600},
            secret,
            algorithm="HS256",
        )

    client = TestClient(main_module.app, raise_server_exceptions=False)
    url = "/api/load/sessions/proj-1"

    assert client.get(url).status_code == 401
    stranger = client.get(url, headers={"Authorization": f"Bearer {token(STRANGER.id)}"})
    assert stranger.status_code == 404, "must not reveal that the id exists"
    owner = client.get(url, headers={"Authorization": f"Bearer {token(OWNER.id)}"})
    assert owner.status_code not in (401, 404), "the owner must get past the guard"
