"""Usage tracking and per-user budget enforcement."""
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import DEFAULT_DATA_DIR, USER_BUDGET_USD


class UsageTracker:
    """Simple JSON-backed usage tracker for per-user spend."""

    def __init__(self, data_path: Optional[Path] = None, budget_usd: float = USER_BUDGET_USD):
        self.data_path = data_path or Path(DEFAULT_DATA_DIR) / "usage_limits.json"
        self.budget_usd = budget_usd
        self._lock = threading.Lock()
        self.data_path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> Dict[str, Any]:
        if not self.data_path.exists():
            return {"users": {}}
        try:
            return json.loads(self.data_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"users": {}}

    def _save(self, data: Dict[str, Any]) -> None:
        self.data_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def get_usage(self, user_id: str) -> Dict[str, Any]:
        with self._lock:
            data = self._load()
            user_entry = data.get("users", {}).get(user_id, {})
            total_spent = float(user_entry.get("total_spent_usd", 0.0))
            return {
                "total_spent_usd": total_spent,
                "budget_usd": self.budget_usd,
                "remaining_usd": max(self.budget_usd - total_spent, 0.0),
            }

    def can_spend(self, user_id: str, amount_usd: float) -> Dict[str, Any]:
        usage = self.get_usage(user_id)
        remaining = usage["remaining_usd"]
        return {
            "allowed": amount_usd <= remaining,
            "remaining_usd": remaining,
            "budget_usd": usage["budget_usd"],
            "total_spent_usd": usage["total_spent_usd"],
        }

    def add_spend(self, user_id: str, amount_usd: float, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        amount_usd = float(max(amount_usd, 0.0))
        with self._lock:
            data = self._load()
            users = data.setdefault("users", {})
            user_entry = users.setdefault(user_id, {"total_spent_usd": 0.0, "charges": []})

            user_entry["total_spent_usd"] = round(float(user_entry.get("total_spent_usd", 0.0)) + amount_usd, 6)
            user_entry.setdefault("charges", []).append({
                "amount_usd": amount_usd,
                "created_at": datetime.utcnow().isoformat(),
                "metadata": metadata or {},
            })

            self._save(data)

        return self.get_usage(user_id)

