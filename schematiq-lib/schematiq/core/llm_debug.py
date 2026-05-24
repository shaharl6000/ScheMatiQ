"""Persist raw LLM responses when SCHEMATIQ_DEBUG_DIR is set.

Set SCHEMATIQ_DEBUG_DIR to a directory path to enable (zero cost when unset).
Optional SCHEMATIQ_DEBUG_INCLUDE_INPUT=1 saves full prompt messages alongside metadata.

Each call writes:
  <slug>__<stage>__<label>__batch<n>__<timestamp_ms>.response.txt  (full raw output)
  <slug>__<stage>__<label>__batch<n>__<timestamp_ms>.meta.json     (stage, label, parsed summary, extra)
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DEBUG_DIR: Optional[Path] = None
_INCLUDE_INPUT = False

_debug_env = os.environ.get("SCHEMATIQ_DEBUG_DIR")
if _debug_env:
    _DEBUG_DIR = Path(_debug_env)
    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    _INCLUDE_INPUT = os.environ.get("SCHEMATIQ_DEBUG_INCLUDE_INPUT", "").lower() in (
        "1",
        "true",
        "yes",
    )
    logging.info("Debug dump enabled -> %s", _DEBUG_DIR)


def is_enabled() -> bool:
    return _DEBUG_DIR is not None


def _safe_slug(text: str, max_len: int = 60) -> str:
    slug = "".join(c if c.isalnum() or c in "-_ " else "_" for c in (text or ""))
    return (slug[:max_len].strip() or "unknown")


def dump_llm_call(
    stage: str,
    *,
    label: str = "",
    raw_response: Optional[str] = None,
    parsed: Optional[Any] = None,
    extra: Optional[Dict[str, Any]] = None,
    prompt_messages: Optional[List[Dict[str, str]]] = None,
    batch_idx: int = 0,
) -> None:
    """Write raw LLM output and metadata to SCHEMATIQ_DEBUG_DIR. No-op when disabled."""
    if _DEBUG_DIR is None:
        return

    slug = _safe_slug(label or stage)
    stage_part = _safe_slug(stage, max_len=40)
    label_part = _safe_slug(label, max_len=40) if label else ""
    ts = int(time.time() * 1000)
    parts = [slug, stage_part]
    if label_part and label_part != slug:
        parts.append(label_part)
    parts.extend([f"batch{batch_idx}", str(ts)])
    stem = "__".join(parts)

    meta: Dict[str, Any] = {
        "stage": stage,
        "label": label or None,
        "batch_idx": batch_idx,
        "timestamp_ms": ts,
        "raw_response_len": len(raw_response) if raw_response else 0,
    }
    if parsed is not None:
        meta["parsed"] = parsed
    if extra:
        meta.update(extra)
    if _INCLUDE_INPUT and prompt_messages:
        meta["prompt_messages"] = prompt_messages
    elif prompt_messages:
        meta["prompt_user_len"] = sum(
            len(m.get("content", "")) for m in prompt_messages if m.get("role") == "user"
        )
        system_msgs = [m for m in prompt_messages if m.get("role") == "system"]
        if system_msgs:
            content = system_msgs[0].get("content", "")
            meta["prompt_system_prefix"] = content[:500] + ("..." if len(content) > 500 else "")

    try:
        if raw_response is not None:
            (_DEBUG_DIR / f"{stem}.response.txt").write_text(
                raw_response, encoding="utf-8", errors="replace"
            )
        (_DEBUG_DIR / f"{stem}.meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning("Debug dump failed for %s: %s", stem, e)
