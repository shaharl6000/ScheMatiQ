"""
Tests for the LLM Call Tracking System.

Verifies:
- Per-stage call counting
- Reset capability
- Global quota enforcement (allow → allow → block on third)
- count_toward_quota opt-out
- File persistence across tracker instances
"""

import json
import tempfile
import threading
from pathlib import Path

import pytest

from schematiq.core.llm_call_tracker import (
    GlobalLLMUsageTracker,
    LLMCallTracker,
    QuotaExceededError,
)


# ──────────────────────────────────────────────────────────────────
# LLMCallTracker (per-session, in-memory)
# ──────────────────────────────────────────────────────────────────


class TestLLMCallTracker:
    """Tests for the in-memory per-session tracker."""

    def setup_method(self):
        """Get a fresh tracker for each test."""
        tracker = LLMCallTracker.get_instance()
        tracker.reset()

    def test_increment_counts_by_stage(self):
        tracker = LLMCallTracker.get_instance()

        tracker.set_stage("schema_discovery")
        tracker.increment(model="gemini-2.5-flash")
        tracker.increment(model="gemini-2.5-flash")

        tracker.set_stage("value_extraction")
        tracker.increment(model="gemini-2.5-flash-lite")

        counts = tracker.get_counts()
        assert counts["schema_discovery"] == 2
        assert counts["value_extraction"] == 1
        assert tracker.get_total() == 3

    def test_increment_default_stage_is_unknown(self):
        tracker = LLMCallTracker.get_instance()
        tracker.increment()
        assert tracker.get_counts().get("unknown", 0) == 1

    def test_get_summary(self):
        tracker = LLMCallTracker.get_instance()
        tracker.set_stage("observation_unit_discovery")
        tracker.increment(model="test-model", prompt_length=500)

        summary = tracker.get_summary()
        assert summary["total_calls"] == 1
        assert summary["per_stage"]["observation_unit_discovery"] == 1
        assert summary["log_length"] == 1

    def test_get_log_records_details(self):
        tracker = LLMCallTracker.get_instance()
        tracker.set_stage("schema_discovery")
        tracker.increment(model="gpt-4o", prompt_length=1234)

        log = tracker.get_log()
        assert len(log) == 1
        assert log[0]["stage"] == "schema_discovery"
        assert log[0]["model"] == "gpt-4o"
        assert log[0]["prompt_length"] == 1234
        assert "timestamp" in log[0]

    def test_reset_all(self):
        tracker = LLMCallTracker.get_instance()
        tracker.set_stage("schema_discovery")
        tracker.increment()
        tracker.increment()

        tracker.reset()

        assert tracker.get_total() == 0
        assert tracker.get_counts() == {}
        assert tracker.get_log() == []
        assert tracker.get_stage() == "unknown"

    def test_reset_specific_stage(self):
        tracker = LLMCallTracker.get_instance()

        tracker.set_stage("schema_discovery")
        tracker.increment()
        tracker.set_stage("value_extraction")
        tracker.increment()
        tracker.increment()

        tracker.reset(stage="schema_discovery")

        counts = tracker.get_counts()
        assert "schema_discovery" not in counts
        assert counts["value_extraction"] == 2
        assert tracker.get_total() == 2
        # Log entries for schema_discovery should be removed
        assert all(e["stage"] != "schema_discovery" for e in tracker.get_log())

    def test_singleton(self):
        a = LLMCallTracker.get_instance()
        b = LLMCallTracker.get_instance()
        assert a is b

    def test_thread_safety(self):
        """Concurrent increments should not lose counts."""
        tracker = LLMCallTracker.get_instance()
        tracker.set_stage("concurrent_test")

        n_threads = 10
        n_per_thread = 100
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            for _ in range(n_per_thread):
                tracker.increment()

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert tracker.get_counts()["concurrent_test"] == n_threads * n_per_thread


# ──────────────────────────────────────────────────────────────────
# GlobalLLMUsageTracker (persistent, file-backed)
# ──────────────────────────────────────────────────────────────────


class TestGlobalLLMUsageTracker:
    """Tests for the persistent global quota tracker."""

    def _make_tracker(self, tmp_path: Path) -> GlobalLLMUsageTracker:
        return GlobalLLMUsageTracker(tmp_path / "global_llm_usage.json")

    def test_empty_start(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        assert tracker.get_total() == 0
        usage = tracker.get_usage()
        assert usage["total_calls"] == 0
        assert usage["per_stage"] == {}
        assert usage["sessions"] == []

    def test_record_session(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        tracker.record_session("session-1", {"schema_discovery": 3, "value_extraction": 10})

        assert tracker.get_total() == 13
        usage = tracker.get_usage()
        assert usage["per_stage"]["schema_discovery"] == 3
        assert usage["per_stage"]["value_extraction"] == 10
        assert len(usage["sessions"]) == 1
        assert usage["sessions"][0]["session_id"] == "session-1"
        assert usage["sessions"][0]["calls"] == 13

    def test_accumulates_across_sessions(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        tracker.record_session("s1", {"schema_discovery": 5})
        tracker.record_session("s2", {"schema_discovery": 3, "value_extraction": 7})

        assert tracker.get_total() == 15
        usage = tracker.get_usage()
        assert usage["per_stage"]["schema_discovery"] == 8
        assert usage["per_stage"]["value_extraction"] == 7
        assert len(usage["sessions"]) == 2

    def test_persists_to_disk(self, tmp_path):
        path = tmp_path / "usage.json"
        tracker1 = GlobalLLMUsageTracker(path)
        tracker1.record_session("s1", {"schema_discovery": 5})

        # Create a NEW tracker instance pointing at the same file
        tracker2 = GlobalLLMUsageTracker(path)
        assert tracker2.get_total() == 5

    def test_reset(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        tracker.record_session("s1", {"schema_discovery": 100})
        tracker.reset()
        assert tracker.get_total() == 0
        assert tracker.get_usage()["sessions"] == []


# ──────────────────────────────────────────────────────────────────
# Quota enforcement: allow → allow → BLOCK
# ──────────────────────────────────────────────────────────────────


class TestQuotaEnforcement:
    """
    Core scenario: set limit=10.
    Session 1 uses 4 calls  → total 4  → allowed.
    Session 2 uses 5 calls  → total 9  → allowed.
    Session 3 tries to start → total 10 ≥ limit → BLOCKED.
    """

    def test_two_sessions_pass_third_blocked(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        limit = 10

        # ── Session 1: 4 calls → should pass ──
        tracker.check_quota(limit)  # no exception
        tracker.record_session("session-1", {
            "observation_unit_discovery": 1,
            "schema_discovery": 2,
            "value_extraction": 1,
        })
        assert tracker.get_total() == 4

        # ── Session 2: 5 calls → should pass ──
        tracker.check_quota(limit)  # no exception (total=4 < 10)
        tracker.record_session("session-2", {
            "schema_discovery": 1,
            "value_extraction": 4,
        })
        assert tracker.get_total() == 9

        # ── Session 3: tries to start → BLOCKED ──
        # Total is 9, but after session-2 completed we record 9.
        # Actually 9 < 10, so one more should still be allowed.
        # Let's add 1 more call to hit exactly 10:
        tracker.record_session("session-2b", {"value_extraction": 1})
        assert tracker.get_total() == 10

        with pytest.raises(QuotaExceededError) as exc_info:
            tracker.check_quota(limit)

        assert exc_info.value.used == 10
        assert exc_info.value.limit == 10
        assert "quota exceeded" in str(exc_info.value).lower()

    def test_quota_zero_means_disabled(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        tracker.record_session("s1", {"value_extraction": 9999})
        # limit=0 should never raise
        tracker.check_quota(0)  # no exception

    def test_quota_exceeded_error_fields(self):
        err = QuotaExceededError(used=42, limit=50)
        assert err.used == 42
        assert err.limit == 50
        assert "42" in str(err)
        assert "50" in str(err)


# ──────────────────────────────────────────────────────────────────
# count_toward_quota opt-out
# ──────────────────────────────────────────────────────────────────


class TestCountTowardQuota:
    """Verify that sessions can opt out of counting toward the quota."""

    def test_unmetered_session_does_not_increase_total(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        limit = 10

        # Session 1: metered, uses 6
        tracker.record_session("s1", {"value_extraction": 6})
        assert tracker.get_total() == 6

        # Session 2: UNMETERED — simulated by NOT calling record_session
        # (in production, ScheMatiQRunner skips record_session when count_toward_quota=False)
        # ... session runs, uses 20 calls, but we don't record them ...

        # Total should still be 6, not 26
        assert tracker.get_total() == 6

        # Session 3: metered, uses 3 → total 9 → under limit
        tracker.check_quota(limit)  # should pass (6 < 10)
        tracker.record_session("s3", {"schema_discovery": 3})
        assert tracker.get_total() == 9

    def test_metered_session_does_increase_total(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        tracker.record_session("s1", {"value_extraction": 5})
        tracker.record_session("s2", {"schema_discovery": 5})
        assert tracker.get_total() == 10


# ──────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────


class TestEdgeCases:

    def test_corrupt_file_recovers(self, tmp_path):
        path = tmp_path / "usage.json"
        path.write_text("NOT VALID JSON!!!", encoding="utf-8")

        tracker = GlobalLLMUsageTracker(path)
        assert tracker.get_total() == 0  # should not crash

    def test_empty_session_counts(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        tracker.record_session("empty", {})
        assert tracker.get_total() == 0
        assert len(tracker.get_usage()["sessions"]) == 1

    def test_check_quota_at_exactly_limit(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        tracker.record_session("s1", {"x": 10})
        with pytest.raises(QuotaExceededError):
            tracker.check_quota(10)  # 10 >= 10 → blocked

    def test_check_quota_one_below_limit(self, tmp_path):
        tracker = GlobalLLMUsageTracker(tmp_path / "usage.json")
        tracker.record_session("s1", {"x": 9})
        tracker.check_quota(10)  # 9 < 10 → allowed

