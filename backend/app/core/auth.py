"""Caller identity, derived from a Supabase Auth access token.

Verification is local: the token's signature is checked against either the
project's legacy shared secret (HS256) or a public key from the JWKS endpoint
(ES256/RS256). No request is made to Supabase on the hot path, and PyJWKClient
caches the public keys in memory.

Supporting both algorithm families is not optional. Supabase projects created
before JWT signing keys shipped are still on the shared secret, while newer
projects and Supabase CLI >= 2.71.1 default to asymmetric keys. A backend that
verifies against only one of them breaks on the other.

Security note: the two verification paths are kept strictly separate. An
asymmetric algorithm is only ever verified with a JWKS public key, and HS256 is
only ever verified with the shared secret. Selecting the key material by the
token's own `alg` header without that separation is the classic algorithm
confusion attack, where a public key is passed as an HMAC secret.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import jwt
from fastapi import Depends, HTTPException, Request
from jwt import PyJWKClient

from app.core.config import (
    SUPABASE_JWKS_URL,
    SUPABASE_JWT_AUDIENCE,
    SUPABASE_JWT_SECRET,
)

logger = logging.getLogger(__name__)

# Only these are ever accepted. Notably excludes "none".
_ASYMMETRIC_ALGORITHMS = ("ES256", "RS256")
_SYMMETRIC_ALGORITHMS = ("HS256",)

_jwks_client: Optional[PyJWKClient] = None


class AuthError(Exception):
    """The presented token could not be verified."""


@dataclass(frozen=True)
class AuthenticatedUser:
    """The verified subject of a Supabase access token."""

    id: str
    email: Optional[str] = None

    @property
    def short_id(self) -> str:
        """For log lines, so full user ids don't end up in logs."""
        return self.id[:8]


def _get_jwks_client() -> PyJWKClient:
    """Lazily build a cached JWKS client.

    Built on first use rather than at import time so the module stays importable
    with no Supabase configuration, which is how local development and the test
    suite run.
    """
    global _jwks_client
    if _jwks_client is None:
        if not SUPABASE_JWKS_URL:
            raise AuthError("no JWKS url configured")
        # PyJWKClient keeps fetched keys in memory, so key rotation resolves on
        # the next miss without a redeploy.
        _jwks_client = PyJWKClient(SUPABASE_JWKS_URL, cache_keys=True)
    return _jwks_client


def reset_jwks_client_cache() -> None:
    """Drop the cached JWKS client. Used by tests and after config changes."""
    global _jwks_client
    _jwks_client = None


def verify_access_token(token: str) -> AuthenticatedUser:
    """Verify a Supabase access token and return its subject.

    Raises AuthError for anything that is not a valid, unexpired token for this
    project.
    """
    if not token:
        raise AuthError("empty token")

    try:
        header = jwt.get_unverified_header(token)
    except jwt.PyJWTError as exc:
        raise AuthError(f"malformed token header: {exc}") from exc

    alg = header.get("alg")
    if alg in _ASYMMETRIC_ALGORITHMS:
        try:
            signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        except AuthError:
            raise
        except Exception as exc:
            raise AuthError(f"no usable JWKS key: {exc}") from exc
        key: Any = signing_key.key
        algorithms = list(_ASYMMETRIC_ALGORITHMS)
    elif alg in _SYMMETRIC_ALGORITHMS:
        if not SUPABASE_JWT_SECRET:
            raise AuthError("HS256 token presented but no JWT secret configured")
        key = SUPABASE_JWT_SECRET
        algorithms = list(_SYMMETRIC_ALGORITHMS)
    else:
        raise AuthError(f"unsupported token algorithm: {alg!r}")

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=algorithms,
            audience=SUPABASE_JWT_AUDIENCE,
            options={"require": ["exp", "sub"]},
        )
    except jwt.PyJWTError as exc:
        raise AuthError(f"token rejected: {exc}") from exc

    subject = claims.get("sub")
    if not subject:
        raise AuthError("token has no subject")
    return AuthenticatedUser(id=str(subject), email=claims.get("email"))


def _bearer_token(request: Request) -> Optional[str]:
    header = request.headers.get("authorization") or ""
    scheme, _, value = header.partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return None
    return value.strip()


async def get_optional_user(request: Request) -> Optional[AuthenticatedUser]:
    """Identify the caller if it presented a valid token, else return None.

    Never raises on a bad token: this is for endpoints that behave differently
    for signed-in callers but stay reachable without one. An invalid token is
    logged and treated as anonymous, so a stale token in a browser cannot make
    the app unusable.
    """
    token = _bearer_token(request)
    if not token:
        return None
    try:
        return verify_access_token(token)
    except AuthError as exc:
        logger.info("ignoring unverifiable access token: %s", exc)
        return None


async def require_user(
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
) -> AuthenticatedUser:
    """Identify the caller, or reject the request with 401."""
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user
