"""Tests for /api/auth/me and the two identity dependencies.

The route functions are called directly rather than through a TestClient so the
whole app (and its ML imports) stays out of the way.
"""

from __future__ import annotations

import time

import jwt
import pytest
from fastapi import HTTPException

from app.api.routes import auth as auth_route
from app.core import auth as auth_module
from app.core.auth import AuthenticatedUser, get_optional_user, require_user

SECRET = "test-jwt-secret-value-long-enough-for-sha256"


class _Request:
    """Minimal stand-in for starlette's Request: only headers are read."""

    def __init__(self, authorization: str | None = None) -> None:
        self.headers = {"authorization": authorization} if authorization else {}


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_SECRET", SECRET, raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_AUDIENCE", "authenticated", raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWKS_URL", "", raising=True)
    auth_module.reset_jwks_client_cache()


def _token(**overrides) -> str:
    claims = {
        "sub": "user-uuid-1234",
        "email": "eliya@example.org",
        "aud": "authenticated",
        "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    return jwt.encode(claims, SECRET, algorithm="HS256")


# --- get_optional_user ----------------------------------------------------


@pytest.mark.asyncio
async def test_no_header_is_anonymous():
    assert await get_optional_user(_Request()) is None


@pytest.mark.asyncio
async def test_valid_bearer_token_identifies_the_caller():
    user = await get_optional_user(_Request(f"Bearer {_token()}"))
    assert user is not None and user.id == "user-uuid-1234"


@pytest.mark.asyncio
async def test_bearer_scheme_is_case_insensitive():
    user = await get_optional_user(_Request(f"bearer {_token()}"))
    assert user is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header",
    [
        "Basic dXNlcjpwYXNz",  # wrong scheme
        "Bearer ",  # empty credential
        "Bearer not-a-token",  # unparseable
        "justatokenwithnoscheme",
    ],
)
async def test_bad_headers_are_treated_as_anonymous(header):
    """A stale or malformed token must not make the app unusable."""
    assert await get_optional_user(_Request(header)) is None


@pytest.mark.asyncio
async def test_expired_token_is_anonymous():
    assert await get_optional_user(_Request(f"Bearer {_token(exp=1)}")) is None


# --- require_user ---------------------------------------------------------


@pytest.mark.asyncio
async def test_require_user_rejects_anonymous():
    with pytest.raises(HTTPException) as exc:
        await require_user(user=None)
    assert exc.value.status_code == 401
    assert exc.value.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.asyncio
async def test_require_user_passes_through_a_known_caller():
    user = AuthenticatedUser(id="abc", email="a@b.c")
    assert await require_user(user=user) is user


# --- the route ------------------------------------------------------------


@pytest.mark.asyncio
async def test_me_reports_anonymous_without_a_token(monkeypatch):
    monkeypatch.setattr(auth_route, "AUTH_ENFORCED", False, raising=True)
    status = await auth_route.read_current_user(user=None)
    assert status.authenticated is False
    assert status.user_id is None
    assert status.enforced is False


@pytest.mark.asyncio
async def test_me_reports_the_caller_and_the_enforcement_flag(monkeypatch):
    monkeypatch.setattr(auth_route, "AUTH_ENFORCED", True, raising=True)
    status = await auth_route.read_current_user(
        user=AuthenticatedUser(id="user-uuid-1234", email="eliya@example.org")
    )
    assert status.authenticated is True
    assert status.user_id == "user-uuid-1234"
    assert status.email == "eliya@example.org"
    assert status.enforced is True


def test_short_id_does_not_leak_the_full_user_id():
    assert AuthenticatedUser(id="0123456789abcdef").short_id == "01234567"
