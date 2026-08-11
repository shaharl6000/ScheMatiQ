"""Tests for provider-error classification in the LLM retry loops.

Classification drives whether a failed call is retried, so it is checked here
against real ``google.genai`` exception objects rather than crafted strings.
"""

import pytest

from schematiq.core.llm_backends import (
    _MAX_RETRY_WAIT_SECONDS,
    _MIN_RETRY_WAIT_SECONDS,
    _extract_wait_time,
    _is_invalid_api_key_error,
    _is_rate_limit_error,
    _is_retryable_server_error,
    _status_code,
)

errors = pytest.importorskip("google.genai.errors")


def api_error(code: int, status: str, message: str = "boom", details=None):
    """Build the APIError subclass the SDK would raise for *code*."""
    error_body = {"code": code, "status": status, "message": message}
    if details is not None:
        error_body["details"] = details
    with pytest.raises(errors.APIError) as excinfo:
        errors.APIError.raise_error(code, {"error": error_body}, None)
    return excinfo.value


def rate_limited(retry_delay=None):
    """A 429 carrying google.rpc.RetryInfo, as the API returns it."""
    details = None
    if retry_delay is not None:
        details = [
            {"@type": "type.googleapis.com/google.rpc.QuotaFailure", "violations": []},
            {
                "@type": "type.googleapis.com/google.rpc.RetryInfo",
                "retryDelay": retry_delay,
            },
        ]
    return api_error(429, "RESOURCE_EXHAUSTED", "quota exceeded", details)


class TestStatusCode:
    def test_reads_code_from_genai_error(self):
        assert _status_code(api_error(429, "RESOURCE_EXHAUSTED")) == 429

    def test_reads_status_code_attribute(self):
        class OpenAIStyleError(Exception):
            status_code = 500

        assert _status_code(OpenAIStyleError("boom")) == 500

    def test_ignores_unrelated_code_attribute(self):
        # A non-HTTP ``code`` must not be mistaken for a status.
        class Weird(Exception):
            code = 7

        assert _status_code(Weird("boom")) is None
        assert _status_code(ValueError("boom")) is None


class TestRateLimit:
    def test_genai_429(self):
        assert _is_rate_limit_error(api_error(429, "RESOURCE_EXHAUSTED"))

    def test_text_fallback_without_status_code(self):
        assert _is_rate_limit_error(RuntimeError("429 quota exceeded"))
        assert _is_rate_limit_error(RuntimeError("RESOURCE_EXHAUSTED"))

    def test_other_codes_are_not_rate_limits(self):
        assert not _is_rate_limit_error(api_error(503, "UNAVAILABLE"))
        assert not _is_rate_limit_error(ValueError("bad schema"))


class TestRetryableServerError:
    @pytest.mark.parametrize(
        "code,status",
        [
            (408, "REQUEST_TIMEOUT"),
            (500, "INTERNAL"),
            (502, "BAD_GATEWAY"),
            (503, "UNAVAILABLE"),
            (504, "DEADLINE_EXCEEDED"),
        ],
    )
    def test_transient_codes_are_retryable(self, code, status):
        assert _is_retryable_server_error(api_error(code, status))

    def test_500_is_retryable_without_matching_wording(self):
        # Previously required the literal "503" in the message, so a 500 with
        # an unrecognised message was never retried.
        assert _is_retryable_server_error(api_error(500, "INTERNAL", "unexpected"))

    def test_text_fallback_without_status_code(self):
        assert _is_retryable_server_error(RuntimeError("503 model is overloaded"))

    def test_transport_failures_are_retryable(self):
        httpx = pytest.importorskip("httpx")
        assert _is_retryable_server_error(httpx.ReadTimeout("timed out"))
        assert _is_retryable_server_error(httpx.ConnectError("refused"))

    def test_client_errors_are_not_retryable(self):
        assert not _is_retryable_server_error(api_error(400, "INVALID_ARGUMENT"))
        assert not _is_retryable_server_error(api_error(429, "RESOURCE_EXHAUSTED"))


class TestInvalidApiKey:
    @pytest.mark.parametrize("code", [401, 403])
    def test_auth_codes(self, code):
        assert _is_invalid_api_key_error(api_error(code, "PERMISSION_DENIED"))

    def test_text_fallback_for_400_with_bad_key(self):
        assert _is_invalid_api_key_error(
            api_error(400, "INVALID_ARGUMENT", "API key not valid. Pass a valid key.")
        )

    def test_malformed_key_without_status_code(self):
        assert _is_invalid_api_key_error(ValueError("Illegal header value b'key\\n'"))

    def test_transient_errors_are_not_key_errors(self):
        assert not _is_invalid_api_key_error(api_error(503, "UNAVAILABLE"))


class TestExtractWaitTime:
    def test_uses_provider_retry_delay(self):
        # 54s + a 5-10s buffer.
        wait = _extract_wait_time(rate_limited("54s"))
        assert 59 <= wait <= 64

    def test_fractional_duration(self):
        wait = _extract_wait_time(rate_limited("0.5s"))
        # Below the floor, so clamped up rather than retried almost immediately.
        assert wait == _MIN_RETRY_WAIT_SECONDS

    def test_extreme_delay_is_capped(self):
        assert _extract_wait_time(rate_limited("86400s")) == _MAX_RETRY_WAIT_SECONDS

    def test_unparseable_delay_falls_back_to_default(self):
        wait = _extract_wait_time(rate_limited("later"))
        assert 50 <= wait <= 60

    def test_default_when_no_details(self):
        wait = _extract_wait_time(rate_limited())
        assert 50 <= wait <= 60

    def test_per_minute_message_waits_longer(self):
        wait = _extract_wait_time(RuntimeError("429 quota exceeded per minute"))
        assert 95 <= wait <= 105

    def test_text_retry_hint(self):
        wait = _extract_wait_time(RuntimeError("Please retry in 30s"))
        assert 35 <= wait <= 40

    def test_malformed_text_hint_does_not_raise(self):
        wait = _extract_wait_time(RuntimeError("retry in 1.2.3s"))
        assert 50 <= wait <= 60
