"""
LLM API Call Tracker
====================
Thread-safe singleton that counts and logs all LLM API calls,
broken down by pipeline stage.

Also provides ``GlobalLLMUsageTracker`` — a persistent, file-backed
aggregator that accumulates call counts across sessions and enforces
a configurable global quota.

Usage
-----
    from qbsd.core.llm_call_tracker import LLMCallTracker, GlobalLLMUsageTracker

    tracker = LLMCallTracker.get_instance()
    tracker.set_stage("schema_discovery")
    # ... LLM generate() calls will be counted automatically ...
    print(tracker.get_summary())
    tracker.reset()  # reset all counters

    # Global quota check
    global_tracker = GlobalLLMUsageTracker("./qbsd_work/global_llm_usage.json")
    global_tracker.check_quota(limit=1000)  # raises QuotaExceededError if over
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class LLMCallTracker:
    """Thread-safe singleton that tracks LLM API calls per pipeline stage."""

    _instance: Optional[LLMCallTracker] = None
    _init_lock = threading.Lock()

    # -- Singleton access -------------------------------------------------- #

    @classmethod
    def get_instance(cls) -> LLMCallTracker:
        """Return the global tracker instance (created on first call)."""
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # -- Construction ------------------------------------------------------ #

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = {}
        self._log: List[Dict[str, Any]] = []
        self._current_stage: str = "unknown"

    # -- Public API -------------------------------------------------------- #

    def set_stage(self, stage: str) -> None:
        """Set the current pipeline stage (e.g. ``"schema_discovery"``)."""
        with self._lock:
            self._current_stage = stage

    def get_stage(self) -> str:
        """Return the current pipeline stage."""
        with self._lock:
            return self._current_stage

    def increment(self, *, model: str = "", prompt_length: int = 0) -> None:
        """Record one LLM API call under the current stage.

        Parameters
        ----------
        model : str, optional
            Model identifier (e.g. ``"gemini-2.5-flash-lite"``).
        prompt_length : int, optional
            Approximate character length of the prompt sent to the LLM.
        """
        with self._lock:
            stage = self._current_stage
            self._counts[stage] = self._counts.get(stage, 0) + 1
            self._log.append({
                "timestamp": time.time(),
                "stage": stage,
                "model": model,
                "prompt_length": prompt_length,
            })

    def get_counts(self) -> Dict[str, int]:
        """Return a copy of per-stage call counts."""
        with self._lock:
            return dict(self._counts)

    def get_total(self) -> int:
        """Return the total number of LLM calls across all stages."""
        with self._lock:
            return sum(self._counts.values())

    def get_log(self) -> List[Dict[str, Any]]:
        """Return a copy of the detailed call log."""
        with self._lock:
            return list(self._log)

    def get_summary(self) -> Dict[str, Any]:
        """Return a full summary dictionary suitable for JSON serialisation."""
        with self._lock:
            return {
                "total_calls": sum(self._counts.values()),
                "per_stage": dict(self._counts),
                "log_length": len(self._log),
            }

    def reset(self, stage: Optional[str] = None) -> None:
        """Reset counters and log.

        Parameters
        ----------
        stage : str, optional
            If provided, only reset the counter for that stage.
            If ``None``, reset everything.
        """
        with self._lock:
            if stage is None:
                self._counts.clear()
                self._log.clear()
                self._current_stage = "unknown"
            else:
                self._counts.pop(stage, None)
                self._log = [
                    entry for entry in self._log if entry["stage"] != stage
                ]


##############################################################################
# Global quota / budget tracking                                             #
##############################################################################


class QuotaExceededError(Exception):
    """Raised when the global LLM call quota has been exceeded."""

    def __init__(self, used: int, limit: int):
        self.used = used
        self.limit = limit
        super().__init__(
            f"Global LLM call quota exceeded: {used}/{limit} calls used. "
            "Please contact an administrator to raise the limit."
        )


class GlobalLLMUsageTracker:
    """Persistent, file-backed tracker of cumulative LLM calls across sessions.

    Stores a simple JSON file with the structure::

        {
            "total_calls": 123,
            "per_stage": {"schema_discovery": 40, "value_extraction": 83},
            "sessions": [
                {"session_id": "abc", "calls": 47, "timestamp": 1700000000},
                ...
            ]
        }

    Thread-safe: multiple sessions can update concurrently.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        # Ensure the parent directory exists
        self._path.parent.mkdir(parents=True, exist_ok=True)

    # -- Reading ----------------------------------------------------------- #

    def _load(self) -> Dict[str, Any]:
        """Load the usage file, returning defaults if missing or corrupt."""
        if not self._path.exists():
            return {"total_calls": 0, "per_stage": {}, "sessions": []}
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read global usage file %s: %s", self._path, exc)
            return {"total_calls": 0, "per_stage": {}, "sessions": []}

    def _save(self, data: Dict[str, Any]) -> None:
        """Atomically write the usage file."""
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self._path)  # atomic on POSIX

    # -- Public API -------------------------------------------------------- #

    def get_total(self) -> int:
        """Return the cumulative total of LLM calls across all sessions."""
        with self._lock:
            return self._load().get("total_calls", 0)

    def get_usage(self) -> Dict[str, Any]:
        """Return the full usage data."""
        with self._lock:
            return self._load()

    def check_quota(self, limit: int) -> None:
        """Raise ``QuotaExceededError`` if cumulative calls >= *limit*.

        A *limit* of ``0`` disables the check (no quota enforced).
        """
        if limit <= 0:
            return  # quota disabled
        total = self.get_total()
        if total >= limit:
            raise QuotaExceededError(used=total, limit=limit)

    def record_session(
        self,
        session_id: str,
        session_counts: Dict[str, int],
    ) -> Dict[str, Any]:
        """Add a completed session's LLM call counts to the global totals.

        Parameters
        ----------
        session_id : str
            Unique identifier of the session that just finished.
        session_counts : dict
            Per-stage call counts from ``LLMCallTracker.get_counts()``.

        Returns
        -------
        dict
            Updated global usage data.
        """
        session_total = sum(session_counts.values())
        with self._lock:
            data = self._load()
            data["total_calls"] = data.get("total_calls", 0) + session_total

            # Merge per-stage counts
            global_stages = data.get("per_stage", {})
            for stage, count in session_counts.items():
                global_stages[stage] = global_stages.get(stage, 0) + count
            data["per_stage"] = global_stages

            # Append session entry
            sessions = data.get("sessions", [])
            sessions.append({
                "session_id": session_id,
                "calls": session_total,
                "per_stage": session_counts,
                "timestamp": time.time(),
            })
            data["sessions"] = sessions

            self._save(data)
            logger.info(
                "Global LLM usage updated: session %s added %d calls (new total: %d)",
                session_id[:8], session_total, data["total_calls"],
            )
            return data

    def sync_from_external(self, external_total: int) -> None:
        """Sync local file with an externally-sourced cumulative total.

        If the local file has a lower total (e.g. after a redeploy wiped it),
        the local total is updated to match the external source.
        If the local total is already >= the external total, no change is made.

        Parameters
        ----------
        external_total : int
            Cumulative LLM call count from an external source
            (e.g. Google Sheets).
        """
        with self._lock:
            data = self._load()
            local_total = data.get("total_calls", 0)
            if external_total > local_total:
                logger.info(
                    "Syncing global usage from external source: local=%d → external=%d",
                    local_total, external_total,
                )
                data["total_calls"] = external_total
                self._save(data)

    def reset(self) -> None:
        """Reset all global usage data (e.g. new billing cycle)."""
        with self._lock:
            self._save({"total_calls": 0, "per_stage": {}, "sessions": []})

