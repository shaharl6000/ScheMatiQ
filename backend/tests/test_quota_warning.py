"""Tests for the approaching-limit quota warning email.

check_global_quota should send a one-time warning when usage crosses the
threshold (but is still under the limit), block when at/over the limit, and do
nothing when well under.
"""

import pytest

import app.core.email_alerts as email_alerts
import app.services.schematiq_runner as sr
from app.services.schematiq_runner import ScheMatiQRunner
from schematiq.core.llm_call_tracker import QuotaExceededError


class _FakeUsage:
    """Duck-typed stand-in exposing only get_quota_usage."""

    def __init__(self, used: int):
        self._used = used

    def get_quota_usage(self, limit: int):
        return {"used": self._used, "limit": limit, "window_days": 0,
                "remaining": max(limit - self._used, 0)}


@pytest.fixture
def captured_alerts(monkeypatch):
    warns: list[tuple[int, int]] = []
    monkeypatch.setattr(email_alerts, "send_quota_warning_alert",
                        lambda used, limit: warns.append((used, limit)))
    monkeypatch.setattr(sr, "LLM_CALL_WARN_THRESHOLD", 0.8)
    return warns


def test_warning_fires_when_crossing_threshold(captured_alerts, monkeypatch):
    # 850 / 1000 = 85% -> warn, not blocked
    ScheMatiQRunner.check_global_quota(_FakeUsage(850), 1000)
    assert captured_alerts == [(850, 1000)]


def test_no_warning_below_threshold(captured_alerts):
    # 500 / 1000 = 50% -> nothing
    ScheMatiQRunner.check_global_quota(_FakeUsage(500), 1000)
    assert captured_alerts == []


def test_at_limit_blocks_and_does_not_warn(captured_alerts):
    with pytest.raises(QuotaExceededError):
        ScheMatiQRunner.check_global_quota(_FakeUsage(1000), 1000)
    assert captured_alerts == []  # blocked path does not send the warning


def test_threshold_zero_disables_warning(captured_alerts, monkeypatch):
    monkeypatch.setattr(sr, "LLM_CALL_WARN_THRESHOLD", 0.0)
    ScheMatiQRunner.check_global_quota(_FakeUsage(999), 1000)
    assert captured_alerts == []


def test_warning_email_sent_once(monkeypatch):
    # The one-time guard means repeated crossings send a single email.
    email_alerts._quota_warning_sent = False
    sent: list[str] = []
    monkeypatch.setattr(email_alerts, "_send_email", lambda subject, body: sent.append(subject))
    email_alerts.send_quota_warning_alert(used=850, limit=1000)
    email_alerts.send_quota_warning_alert(used=900, limit=1000)
    assert len(sent) == 1
    email_alerts._quota_warning_sent = False  # reset for other tests
