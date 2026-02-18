#!/usr/bin/env python3
"""
Full end-to-end quota flow test (local, no Google Sheets needed).

Simulates the exact sequence that ScheMatiQRunner does:
  1. Reset tracker
  2. Check quota
  3. Run LLM calls (mocked — just increments)
  4. Save stats to file
  5. Record session in global usage

Then verifies:
  - Session 1 (4 calls) → allowed, total=4
  - Session 2 (5 calls) → allowed, total=9
  - Session 3 → BLOCKED (total=9 ≥ limit=8)

Also tests:
  - count_toward_quota=False (unmetered session doesn't block future ones)
  - reset() clears everything
  - Persistence: new tracker instance reads same file
"""

import json
import sys
from pathlib import Path

from schematiq.core.llm_call_tracker import (
    GlobalLLMUsageTracker,
    LLMCallTracker,
    QuotaExceededError,
)

# ── Config ──
LIMIT = 8  # small limit to trigger quickly
WORK_DIR = Path("/tmp/schematiq_quota_test")


def setup():
    """Clean start."""
    if WORK_DIR.exists():
        import shutil
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True)
    LLMCallTracker.get_instance().reset()
    print(f"Work dir: {WORK_DIR}")
    print(f"Quota limit: {LIMIT}")
    print()


def simulate_session(
    session_id: str,
    global_tracker: GlobalLLMUsageTracker,
    stages: dict,
    count_toward_quota: bool = True,
) -> bool:
    """Simulate one ScheMatiQ session. Returns True if allowed, False if blocked."""
    tracker = LLMCallTracker.get_instance()
    tracker.reset()

    # 1. Check quota
    try:
        global_tracker.check_quota(LIMIT)
    except QuotaExceededError as e:
        print(f"  BLOCKED: {e}")
        return False

    # 2. Simulate LLM calls per stage
    total_calls = 0
    for stage, count in stages.items():
        tracker.set_stage(stage)
        for _ in range(count):
            tracker.increment(model="test-model", prompt_length=100)
        total_calls += count

    # 3. Save session stats to file (like ScheMatiQRunner does)
    session_dir = WORK_DIR / session_id
    session_dir.mkdir(exist_ok=True)
    summary = tracker.get_summary()
    summary["log"] = tracker.get_log()
    stats_file = session_dir / "llm_call_stats.json"
    stats_file.write_text(json.dumps(summary, indent=2))

    # 4. Record in global usage (unless opted out)
    if count_toward_quota:
        global_tracker.record_session(session_id, tracker.get_counts())
    else:
        print(f"  (unmetered — not counting toward quota)")

    print(f"  Session calls: {total_calls} | Global total: {global_tracker.get_total()}")
    return True


def main():
    setup()
    global_tracker = GlobalLLMUsageTracker(WORK_DIR / "global_llm_usage.json")

    # ── Session 1: 4 calls → should pass ──
    print("SESSION 1 (4 calls):")
    ok = simulate_session("session-1", global_tracker, {
        "observation_unit_discovery": 1,
        "schema_discovery": 2,
        "value_extraction": 1,
    })
    assert ok, "Session 1 should have been allowed"
    assert global_tracker.get_total() == 4
    print("  ✅ Allowed\n")

    # ── Session 2: 4 calls → should pass (total=8 after, but checked before) ──
    print("SESSION 2 (4 calls):")
    ok = simulate_session("session-2", global_tracker, {
        "schema_discovery": 1,
        "value_extraction": 3,
    })
    assert ok, "Session 2 should have been allowed"
    assert global_tracker.get_total() == 8
    print("  ✅ Allowed\n")

    # ── Session 3: should be BLOCKED (total=8 ≥ limit=8) ──
    print("SESSION 3 (blocked before running):")
    ok = simulate_session("session-3", global_tracker, {
        "schema_discovery": 1,
    })
    assert not ok, "Session 3 should have been BLOCKED"
    assert global_tracker.get_total() == 8  # unchanged
    print("  ✅ Correctly blocked\n")

    # ── Session 4: unmetered → runs but doesn't count ──
    # First reset to allow it to run (since we're at limit)
    # Actually, unmetered sessions still get quota-checked in the real flow.
    # But the user asked for "run without counting" — let's show that:
    print("RESET (simulating admin raising limit or resetting):")
    global_tracker.reset()
    assert global_tracker.get_total() == 0
    print("  ✅ Reset to 0\n")

    print("SESSION 4 — metered (3 calls):")
    ok = simulate_session("session-4", global_tracker, {
        "value_extraction": 3,
    })
    assert ok
    assert global_tracker.get_total() == 3
    print("  ✅ Allowed\n")

    print("SESSION 5 — UNMETERED (10 calls, doesn't count):")
    ok = simulate_session("session-5", global_tracker, {
        "value_extraction": 10,
    }, count_toward_quota=False)
    assert ok
    assert global_tracker.get_total() == 3  # still 3, not 13
    print("  ✅ Allowed, total unchanged\n")

    print("SESSION 6 — metered (4 calls, total→7):")
    ok = simulate_session("session-6", global_tracker, {
        "schema_discovery": 2,
        "value_extraction": 2,
    })
    assert ok
    assert global_tracker.get_total() == 7
    print("  ✅ Allowed\n")

    print("SESSION 7 — metered (2 calls → total=9 ≥ 8 after, but check is before):")
    ok = simulate_session("session-7", global_tracker, {
        "value_extraction": 2,
    })
    assert ok  # 7 < 8, so check passes; after recording, total=9
    assert global_tracker.get_total() == 9
    print("  ✅ Allowed (checked at 7, now 9)\n")

    print("SESSION 8 — should be BLOCKED (total=9 ≥ limit=8):")
    ok = simulate_session("session-8", global_tracker, {
        "schema_discovery": 1,
    })
    assert not ok
    print("  ✅ Correctly blocked\n")

    # ── Verify persistence: new tracker instance reads the same file ──
    print("PERSISTENCE TEST:")
    tracker2 = GlobalLLMUsageTracker(WORK_DIR / "global_llm_usage.json")
    assert tracker2.get_total() == 9
    print(f"  New tracker instance reads total={tracker2.get_total()} ✅\n")

    # ── Verify saved files ──
    print("SAVED FILES:")
    for d in sorted(WORK_DIR.iterdir()):
        if d.is_dir():
            stats = d / "llm_call_stats.json"
            if stats.exists():
                data = json.loads(stats.read_text())
                print(f"  {d.name}/llm_call_stats.json → {data['total_calls']} calls")
    global_file = WORK_DIR / "global_llm_usage.json"
    if global_file.exists():
        data = json.loads(global_file.read_text())
        print(f"\n  global_llm_usage.json:")
        print(f"    total_calls: {data['total_calls']}")
        print(f"    per_stage: {data['per_stage']}")
        print(f"    sessions: {len(data['sessions'])} recorded")

    print("\n" + "=" * 50)
    print("ALL TESTS PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())

