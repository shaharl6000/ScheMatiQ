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
    from schematiq.core.llm_call_tracker import LLMCallTracker, GlobalLLMUsageTracker

    tracker = LLMCallTracker.get_instance()
    tracker.set_stage("schema_discovery")
    # ... LLM generate() calls will be counted automatically ...
    print(tracker.get_summary())
    tracker.reset()  # reset all counters

    # Global quota check
    global_tracker = GlobalLLMUsageTracker("./schematiq_work/global_llm_usage.json")
    global_tracker.check_quota(limit=1000)  # raises QuotaExceededError if over
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import cost estimator for pricing logic
try:
    from schematiq.core.cost_estimator import get_model_pricing, calculate_cost
except ImportError:
    # Fallback if cost_estimator not available (e.g. during tests)
    def get_model_pricing(provider, model, as_of=None): return {"input": 0.0, "output": 0.0}
    def calculate_cost(input_tokens, output_tokens, pricing): return 0.0

logger = logging.getLogger(__name__)


def _provider_for_model(model: str) -> str:
    """Best-effort provider name for pricing lookup."""
    name = (model or "").lower()
    if "gemini" in name:
        return "gemini"
    if "gpt" in name:
        return "openai"
    if "llama" in name or "mistral" in name:
        return "together"
    return "unknown"


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

    def increment(
        self,
        *,
        model: str = "",
        prompt_length: int = 0,
        completion_length: int = 0,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        thinking_tokens: Optional[int] = None,
        cached_tokens: Optional[int] = None,
        count_call: bool = True,
    ) -> None:
        """Record one LLM API call under the current stage.

        Parameters
        ----------
        model : str, optional
            Model identifier (e.g. ``"gemini-2.5-flash-lite"``).
        prompt_length, completion_length : int, optional
            Character lengths of the prompt and the returned text. Used only
            to estimate cost when the provider reported no token usage.
        input_tokens : int, optional
            Billed prompt tokens as reported by the provider (Gemini
            ``prompt_token_count``, which already includes cached tokens).
        output_tokens : int, optional
            Billed response tokens, excluding thinking tokens.
        thinking_tokens : int, optional
            Thinking/reasoning tokens. Gemini bills these at the output rate
            but reports them separately from ``candidates_token_count``.
        cached_tokens : int, optional
            Part of ``input_tokens`` served from a context cache (billed at
            the cheaper cached-input rate).
        count_call : bool, optional
            ``False`` records the usage for cost only, without counting a call
            toward the per-stage counts (and so the quota). Used for responses
            that were billed but returned no usable text (empty / blocked).
        """
        with self._lock:
            stage = self._current_stage
            if count_call:
                self._counts[stage] = self._counts.get(stage, 0) + 1
            self._log.append({
                "timestamp": time.time(),
                "stage": stage,
                "model": model,
                "prompt_length": prompt_length,
                "completion_length": completion_length,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "thinking_tokens": thinking_tokens,
                "cached_tokens": cached_tokens,
                "counted": count_call,
            })

    def record_cache_storage(self, *, model: str, tokens: int, seconds: float) -> None:
        """Record context-cache storage (billed per token per hour, not a call)."""
        with self._lock:
            self._log.append({
                "timestamp": time.time(),
                "stage": self._current_stage,
                "model": model,
                "kind": "cache_storage",
                "cache_tokens": int(tokens or 0),
                "cache_seconds": float(seconds or 0.0),
                "counted": False,
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
            
    def calculate_current_cost(self) -> Dict[str, Any]:
        """Calculate the cost of the calls tracked so far.

        Uses the provider-reported token usage when present: input tokens at
        the input rate (cached part at the cached-input rate), output plus
        thinking tokens at the output rate. Entries without usage (older
        callers, providers that report none) fall back to ``chars // 4`` and
        are counted in ``estimated_calls`` so the result says how much of it
        is a guess. Context-cache storage entries are priced per token-hour.

        Returns:
            Dict with total cost and token breakdown.
        """
        with self._lock:
            logs = list(self._log)

        total_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        total_thinking_tokens = 0
        total_cached_tokens = 0
        cache_storage_cost = 0.0
        estimated_calls = 0

        for entry in logs:
            model = entry.get("model", "") or ""
            pricing = get_model_pricing(_provider_for_model(model), model)
            input_rate = pricing.get("input", 1.0)
            output_rate = pricing.get("output", 3.0)

            if entry.get("kind") == "cache_storage":
                hourly = pricing.get("cache_storage_per_hour", 0.0)
                cost = (
                    entry.get("cache_tokens", 0) / 1_000_000
                    * hourly
                    * entry.get("cache_seconds", 0.0) / 3600
                )
                cache_storage_cost += cost
                total_cost += cost
                continue

            if entry.get("input_tokens") is not None:
                input_tokens = int(entry.get("input_tokens") or 0)
                output_tokens = int(entry.get("output_tokens") or 0)
                thinking_tokens = int(entry.get("thinking_tokens") or 0)
                cached_tokens = min(int(entry.get("cached_tokens") or 0), input_tokens)
            else:
                # No provider usage: the backends pass character lengths.
                input_tokens = entry.get("prompt_length", 0) // 4
                output_tokens = entry.get("completion_length", 0) // 4
                thinking_tokens = 0
                cached_tokens = 0
                estimated_calls += 1

            cached_rate = pricing.get("cached_input", input_rate)
            total_cost += (
                (input_tokens - cached_tokens) / 1_000_000 * input_rate
                + cached_tokens / 1_000_000 * cached_rate
                + (output_tokens + thinking_tokens) / 1_000_000 * output_rate
            )
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens + thinking_tokens
            total_thinking_tokens += thinking_tokens
            total_cached_tokens += cached_tokens

        return {
            "total_cost_usd": round(total_cost, 6),
            "total_input_tokens": total_input_tokens,
            # Includes thinking tokens: they are billed as output.
            "total_output_tokens": total_output_tokens,
            "total_thinking_tokens": total_thinking_tokens,
            "total_cached_tokens": total_cached_tokens,
            "cache_storage_cost_usd": round(cache_storage_cost, 6),
            "estimated_calls": estimated_calls,
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

    def get_windowed_total(self, window_days: int) -> int:
        """Return total calls from sessions recorded within the last *window_days*.

        Only locally recorded sessions carry timestamps; totals synced from an
        external source via ``sync_from_external`` adjust the lifetime scalar
        without session entries and are therefore invisible to the window.
        Callers that need redeploy-safe windowed totals should also consult
        the external source directly and take the max.
        """
        if window_days <= 0:
            return self.get_total()
        cutoff = time.time() - window_days * 86400
        with self._lock:
            data = self._load()
            total = 0
            for entry in data.get("sessions", []):
                try:
                    if float(entry.get("timestamp", 0)) >= cutoff:
                        total += int(entry.get("calls", 0))
                except (TypeError, ValueError):
                    continue
            return total

    def check_quota(self, limit: int) -> None:
        """Raise ``QuotaExceededError`` if cumulative calls >= *limit*.

        A *limit* of ``0`` disables the check (no quota enforced).
        """
        if limit <= 0:
            return  # quota disabled
        total = self.get_total()
        if total >= limit:
            # Visible on the uvicorn terminal even when logging is minimal
            print(
                f"[LLM quota] blocked: cumulative_calls={total} quota_limit={limit} "
                f"(set env LLM_CALL_GLOBAL_LIMIT higher than {total}, or LLM_CALL_GLOBAL_LIMIT=0 to disable)",
                flush=True,
            )
            logger.warning(
                "Global LLM quota exceeded: %s/%s calls — adjust LLM_CALL_GLOBAL_LIMIT or set to 0",
                total,
                limit,
            )
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
