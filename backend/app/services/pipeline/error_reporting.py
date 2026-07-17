"""Translate raw LLM-provider exceptions into user-facing messages.

Provider SDKs (google-genai, openai, together) raise errors whose ``str()`` is a
raw protocol dump, e.g. ``"400 INVALID_ARGUMENT. {'error': {'code': 400, ...}}"``.
Surfacing that verbatim in the UI is unhelpful and leaks internals, while the
detail we actually need for debugging (status, code, field violations) belongs in
the server logs. :func:`describe_llm_error` splits those two concerns.
"""

from typing import Tuple

# Exception classes from these modules are treated as provider API errors.
_PROVIDER_MODULE_HINTS = (
    "google.genai",
    "google.api_core",
    "google.generativeai",
    "openai",
    "together",
)


def _looks_like_provider_error(exc: BaseException) -> bool:
    """True when ``exc`` originates from a known LLM-provider SDK.

    Detected either by the defining module of the exception class or, as a
    fallback, by the SDK convention of exposing both a ``status`` and a ``code``.
    """
    module = (type(exc).__module__ or "").lower()
    if any(hint in module for hint in _PROVIDER_MODULE_HINTS):
        return True
    return getattr(exc, "status", None) is not None and getattr(exc, "code", None) is not None


def describe_llm_error(exc: BaseException) -> Tuple[str, str]:
    """Return ``(user_message, log_detail)`` for an exception raised during a run.

    ``user_message`` is safe, human-readable text for the UI and
    ``session.error_message``. ``log_detail`` is verbose technical detail for the
    server logs only (status, code, and the provider's raw ``details`` payload,
    which carries any field-level violations) and is never shown to users.

    Non-provider exceptions fall through to ``str(exc)`` so existing behavior and
    any already-friendly ``RuntimeError`` messages are preserved unchanged.
    """
    fields = {}
    for attr in ("code", "status", "message", "details"):
        value = getattr(exc, attr, None)
        if value is not None:
            fields[attr] = value

    log_detail = f"{type(exc).__name__}: {exc!r}"
    if fields:
        log_detail = f"{log_detail} | {fields}"

    if not _looks_like_provider_error(exc):
        return str(exc), log_detail

    text = str(exc).upper()
    status = str(fields.get("status") or "").upper()
    code = fields.get("code")

    def _mentions(token: str) -> bool:
        return token in status or token in text

    if _mentions("INVALID_ARGUMENT") or code == 400:
        user_message = (
            "The AI provider rejected the request (400 INVALID_ARGUMENT), usually "
            "an unsupported model parameter or an oversized request. Please try "
            "again; if it keeps happening, contact support."
        )
    elif _mentions("RESOURCE_EXHAUSTED") or code == 429:
        user_message = (
            "The AI provider is rate-limiting requests right now. Please wait a "
            "moment and try again."
        )
    elif _mentions("PERMISSION_DENIED") or _mentions("UNAUTHENTICATED") or code in (401, 403):
        user_message = (
            "The AI provider rejected the API key (authentication error). Please "
            "check the configured key and try again."
        )
    else:
        user_message = (
            "The AI provider returned an error while processing this request. "
            "Please try again; if it keeps happening, contact support."
        )

    return user_message, log_detail
