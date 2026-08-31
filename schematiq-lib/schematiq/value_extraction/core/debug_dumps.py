"""Debug-dump utilities for value extraction.

All debug output for ``PaperProcessor`` lives here instead of being scattered
inside the extraction pipeline, so the production code path stays readable
and every debug knob is controlled from one place.

Env vars:
  SCHEMATIQ_DEBUG_DIR      Directory to write per-call / per-unit JSON dumps
                           to. Unset (default) -> no file dumps are written.
  SCHEMATIQ_DEBUG_DISABLE  Master kill-switch. Set to "1"/"true" to silence
                           ALL debug output from this module - file dumps AND
                           the always-on empty-unit diagnostic log below -
                           even if SCHEMATIQ_DEBUG_DIR is set.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _truthy(value: Optional[str]) -> bool:
    return bool(value) and value.strip().lower() not in ("", "0", "false", "no")


# Master switch: when set, every function below becomes a no-op regardless
# of SCHEMATIQ_DEBUG_DIR.
DEBUG_DISABLED: bool = _truthy(os.environ.get("SCHEMATIQ_DEBUG_DISABLE"))

_DEBUG_DIR: Optional[Path] = None
_debug_env = os.environ.get("SCHEMATIQ_DEBUG_DIR")
if _debug_env and not DEBUG_DISABLED:
    _DEBUG_DIR = Path(_debug_env)
    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Debug dump enabled -> %s", _DEBUG_DIR)


def _safe_title(paper_title: str) -> str:
    return "".join(c if c.isalnum() or c in "-_ " else "_" for c in paper_title)[:60]


def dump_llm_call(
    pass_name: str,
    paper_title: str,
    batch_idx: int,
    columns_requested: List[str],
    prompt_msgs: List[Dict[str, str]],
    raw_response: str,
    parsed: Dict[str, Any],
    cleaned: Dict[str, Any],
    already_extracted: Optional[Dict[str, str]] = None,
) -> None:
    """Save a debug snapshot of one LLM call to SCHEMATIQ_DEBUG_DIR.

    Each call writes a JSON file named:
      <title>__<pass>__batch<n>__<timestamp_ms>.json
    No-op unless SCHEMATIQ_DEBUG_DIR is set and SCHEMATIQ_DEBUG_DISABLE is not.
    """
    if _DEBUG_DIR is None:
        return
    ts = int(time.time() * 1000)
    fname = f"{_safe_title(paper_title)}__{pass_name}__batch{batch_idx}__{ts}.json"
    payload = {
        "pass": pass_name,
        "paper_title": paper_title,
        "batch_idx": batch_idx,
        "columns_requested": columns_requested,
        "columns_filled": list(cleaned.keys()),
        "columns_missing": [c for c in columns_requested if c not in cleaned],
        "prompt_system": prompt_msgs[0]["content"][:500] + "..." if prompt_msgs else None,
        "prompt_user_len": len(prompt_msgs[1]["content"]) if len(prompt_msgs) > 1 else 0,
        "raw_response": raw_response[:5000] if raw_response else None,
        "parsed": {k: v for k, v in (parsed or {}).items()},
        "cleaned": {k: v for k, v in (cleaned or {}).items()},
    }
    if already_extracted:
        payload["already_extracted_context"] = already_extracted
    try:
        (_DEBUG_DIR / fname).write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    except Exception as e:
        logger.warning("Debug dump failed: %s", e)


def dump_unit_split(
    paper_title: str,
    paper_text: str,
    units: List[Dict[str, Any]],
) -> None:
    """Save a debug snapshot of the final unit identification split.

    Writes one JSON file per document showing, for each observation unit,
    its relevant_passages verbatim (not truncated) and how much of the
    document text they cover.

    NOTE: while DISABLE_RETRIEVER is True (defined in config.constants),
    extract_values_for_unit always re-substitutes the full document text
    for relevant_passages regardless of what unit identification returned,
    so is_full_text_fallback / coverage_pct_of_document below will read
    as "full document" for every unit in normal operation. That is
    expected, not a bug. These fields become meaningful again only if
    DISABLE_RETRIEVER is set back to False and passage-narrowing is
    reintroduced upstream.
    No-op unless SCHEMATIQ_DEBUG_DIR is set and SCHEMATIQ_DEBUG_DISABLE is not.
    """
    if _DEBUG_DIR is None:
        return
    ts = int(time.time() * 1000)
    fname = f"{_safe_title(paper_title)}__unit_split__{ts}.json"

    doc_len = len(paper_text) if paper_text else 0
    unit_summaries = []
    for u in units:
        passages = u.get("relevant_passages") or []
        passages_len = sum(len(p) for p in passages)
        is_full_text_fallback = passages == [paper_text] and doc_len > 0
        unit_summaries.append({
            "unit_name": u.get("unit_name"),
            "confidence": u.get("confidence"),
            "num_passages": len(passages),
            "passages_char_count": passages_len,
            "coverage_pct_of_document": (
                round(100 * passages_len / doc_len, 1) if doc_len else None
            ),
            "is_full_text_fallback": is_full_text_fallback,
            "relevant_passages": passages,
        })

    payload = {
        "paper_title": paper_title,
        "document_char_count": doc_len,
        "num_units": len(units),
        "units": unit_summaries,
    }
    try:
        (_DEBUG_DIR / fname).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        )
    except Exception as e:
        logger.warning("Unit split debug dump failed: %s", e)


def log_empty_unit_diagnostics(
    paper_title: str,
    unit_name: str,
    batch_diags: List[Dict[str, Any]],
) -> None:
    """Log full per-batch diagnostics when a unit extracted to a fully empty row.

    Unlike dump_llm_call / dump_unit_split, this runs on the anomaly path
    regardless of SCHEMATIQ_DEBUG_DIR (it stays quiet in normal operation
    since it only fires when every column blanked), so we can see WHY from
    the logs (Railway-visible) without re-running. Silenced entirely by
    SCHEMATIQ_DEBUG_DISABLE.
    """
    if DEBUG_DISABLED:
        return
    for d in batch_diags:
        logger.warning(
            "[%s] EMPTY-UNIT batch %d/%d '%s': finish_reason=%s empty_signal=%s "
            "truncated=%s resp_chars=%s parse_error=%s empty_reason=%s\n"
            "columns_requested=%s\nFULL raw response:\n%s",
            paper_title,
            d["batch_idx"] + 1,
            len(batch_diags),
            unit_name,
            d.get("finish_reason"),
            d.get("is_empty_signal"),
            d.get("truncated"),
            d.get("response_chars"),
            d.get("parse_error"),
            d.get("empty_reason"),
            d.get("columns_requested"),
            d.get("raw_response"),
        )
