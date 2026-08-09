"""Tests for Supabase access token verification.

Tokens are minted locally with keys generated in-test, so nothing here talks to
Supabase. Both signing schemes are covered because real projects use both: the
legacy shared secret (HS256) and asymmetric signing keys (ES256/RS256), which
newer projects and Supabase CLI >= 2.71.1 default to.
"""

from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from app.core import auth as auth_module
from app.core.auth import AuthError, verify_access_token

SECRET = "test-jwt-secret-value-long-enough-for-sha256"
AUDIENCE = "authenticated"


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    """Point the module at the test secret and audience, no JWKS by default."""
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_SECRET", SECRET, raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_AUDIENCE", AUDIENCE, raising=True)
    monkeypatch.setattr(auth_module, "SUPABASE_JWKS_URL", "", raising=True)
    auth_module.reset_jwks_client_cache()
    yield
    auth_module.reset_jwks_client_cache()


def _claims(**overrides):
    base = {
        "sub": "user-uuid-1234",
        "email": "eliya@example.org",
        "aud": AUDIENCE,
        "exp": int(time.time()) + 3600,
    }
    base.update(overrides)
    return base


def _hs256(**overrides) -> str:
    return jwt.encode(_claims(**overrides), SECRET, algorithm="HS256")


@pytest.fixture
def ec_key():
    return ec.generate_private_key(ec.SECP256R1())


def _es256(private_key, **overrides) -> str:
    from cryptography.hazmat.primitives import serialization

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return jwt.encode(_claims(**overrides), pem, algorithm="ES256")


def _install_jwks(monkeypatch, private_key):
    """Serve the matching public key the way PyJWKClient would."""

    class _Key:
        key = private_key.public_key()

    class _Client:
        def get_signing_key_from_jwt(self, token):
            return _Key()

    monkeypatch.setattr(auth_module, "SUPABASE_JWKS_URL", "https://x/jwks", raising=True)
    monkeypatch.setattr(auth_module, "_jwks_client", _Client(), raising=False)


# --- HS256 (legacy shared secret) ----------------------------------------


def test_valid_hs256_token_is_accepted():
    user = verify_access_token(_hs256())
    assert user.id == "user-uuid-1234"
    assert user.email == "eliya@example.org"


def test_hs256_signed_with_the_wrong_secret_is_rejected():
    token = jwt.encode(_claims(), "some-other-secret", algorithm="HS256")
    with pytest.raises(AuthError):
        verify_access_token(token)


def test_hs256_is_rejected_when_no_secret_is_configured(monkeypatch):
    monkeypatch.setattr(auth_module, "SUPABASE_JWT_SECRET", "", raising=True)
    with pytest.raises(AuthError, match="no JWT secret"):
        verify_access_token(_hs256())


# --- ES256 (asymmetric signing keys) -------------------------------------


def test_valid_es256_token_is_accepted(monkeypatch, ec_key):
    _install_jwks(monkeypatch, ec_key)
    user = verify_access_token(_es256(ec_key))
    assert user.id == "user-uuid-1234"


def test_es256_signed_by_a_different_key_is_rejected(monkeypatch, ec_key):
    _install_jwks(monkeypatch, ec_key)
    impostor = ec.generate_private_key(ec.SECP256R1())
    with pytest.raises(AuthError):
        verify_access_token(_es256(impostor))


def test_es256_is_rejected_when_no_jwks_is_configured(ec_key):
    with pytest.raises(AuthError, match="JWKS"):
        verify_access_token(_es256(ec_key))


# --- claim validation -----------------------------------------------------


def test_expired_token_is_rejected():
    with pytest.raises(AuthError):
        verify_access_token(_hs256(exp=int(time.time()) - 60))


def test_token_for_another_audience_is_rejected():
    with pytest.raises(AuthError):
        verify_access_token(_hs256(aud="some-other-service"))


def test_token_without_a_subject_is_rejected():
    claims = _claims()
    del claims["sub"]
    token = jwt.encode(claims, SECRET, algorithm="HS256")
    with pytest.raises(AuthError):
        verify_access_token(token)


def test_token_without_an_expiry_is_rejected():
    claims = _claims()
    del claims["exp"]
    token = jwt.encode(claims, SECRET, algorithm="HS256")
    with pytest.raises(AuthError):
        verify_access_token(token)


# --- algorithm handling ---------------------------------------------------


def test_unsigned_token_is_rejected():
    """alg=none must never be honoured."""
    token = jwt.encode(_claims(), key="", algorithm="none")
    with pytest.raises(AuthError, match="unsupported token algorithm"):
        verify_access_token(token)


def test_algorithm_confusion_is_rejected(monkeypatch, ec_key):
    """A public key must never be usable as an HMAC secret.

    The attack: take the project's published ES256 public key, sign a token with
    it as an HS256 secret, and hope the backend picks key material by the token's
    own alg header. The two verification paths are separate, so the HS256 branch
    only ever reaches for SUPABASE_JWT_SECRET.

    The token is assembled by hand because PyJWT's own encode() refuses to use an
    asymmetric key as an HMAC secret, so a real attacker's token cannot be
    produced through the library.
    """
    import base64
    import hashlib
    import hmac
    import json

    from cryptography.hazmat.primitives import serialization

    public_pem = ec_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    _install_jwks(monkeypatch, ec_key)

    def b64(raw: bytes) -> bytes:
        return base64.urlsafe_b64encode(raw).rstrip(b"=")

    signing_input = (
        b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
        + b"."
        + b64(json.dumps(_claims()).encode())
    )
    signature = hmac.new(public_pem, signing_input, hashlib.sha256).digest()
    forged = (signing_input + b"." + b64(signature)).decode()

    with pytest.raises(AuthError):
        verify_access_token(forged)


def test_garbage_is_rejected():
    for value in ("", "not-a-token", "a.b.c"):
        with pytest.raises(AuthError):
            verify_access_token(value)
