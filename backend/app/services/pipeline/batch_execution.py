"""Resilience for schema-discovery batches that exceed model token limits.

Two failure modes exist when a batch is too large (verified against
``schematiq/core/llm_backends.py``):

1. **Input overflow** — the assembled prompt exceeds the model context window.
   The provider SDK raises (Gemini: ``400 INVALID_ARGUMENT``). In the base
   pipeline this propagates out of ``generate_schema`` and crashes the entire
   discovery run, losing all progress.

2. **Output truncation** — the response hits ``max_output_tokens`` and is cut
   off silently (Gemini logs ``finish_reason=MAX_TOKENS`` but does not raise).
   This module does not detect mode 2 directly (it produces no exception), but
   splitting a batch shrinks the input, which in turn reduces the schema the
   model must emit, indirectly lowering mode-2 risk.

Strategy: when a batch fails with a token-limit error, split it in half and
retry each half independently (recursively). A batch of a single document that
still fails cannot be split — it is skipped for schema discovery, logged, and
recorded as an artifact so nothing fails silently and we retain a record for
later improvement. Value extraction is a separate phase and is unaffected: a
document skipped here is still available downstream.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Substrings that identify a token / context-window overflow across providers.
# Matched case-insensitively against the string form of the raised exception.
# Kept deliberately broad; a false positive only triggers an (harmless) split
# retry, while a false negative would let the original crash through.
_TOKEN_LIMIT_MARKERS: Tuple[str, ...] = (
    "context length",
    "context window",
    "maximum context",
    "context_length_exceeded",
    "too many tokens",
    "input token count",
    "exceeds the maximum number of tokens",
    "request payload size",
    "reduce the length",
    "string too long",
    "token count",
    "prompt is too long",
)

# Provider error codes that, combined with token-ish wording, indicate overflow.
_INVALID_ARG_MARKERS: Tuple[str, ...] = (
    "invalid_argument",
    "400",
    "invalidrequesterror",
    "bad request",
)


def is_token_limit_error(err: BaseException) -> bool:
    """Return True if an exception looks like an input/context token overflow.

    Detection is heuristic and provider-agnostic: it inspects the stringified
    exception for known overflow wording, or for a generic invalid-argument /
    400 error that also mentions tokens. Rate-limit and auth errors are
    explicitly excluded so we never misclassify them as splittable overflow.
    """
    text = str(err).lower()

    # Never treat quota / rate-limit / auth as splittable overflow.
    if any(m in text for m in ("rate limit", "resource_exhausted", "quota", "429", "api key", "unauthenticated")):
        return False

    if any(m in text for m in _TOKEN_LIMIT_MARKERS):
        return True

    # Generic 400 / invalid argument that also references tokens.
    if any(m in text for m in _INVALID_ARG_MARKERS) and "token" in text:
        return True

    return False


def _log_skipped_batch(
    work_dir: Optional[Path],
    session_id: str,
    filenames: List[str],
    reason: str,
    error: str,
) -> None:
    """Append a skipped-batch record to a per-session JSON artifact.

    Writes ``schema_discovery_skips.json`` under ``work_dir/session_id``. The
    file holds a list of records; each has the filenames, a reason, the raw
    error string, and a timestamp. Failure to write the artifact is logged but
    never raised — resilience code must not itself become a failure source.
    """
    logger.warning(
        "Schema discovery skipped batch (%s): %s | error: %s",
        reason, filenames, error[:500],
    )

    if not work_dir:
        return

    try:
        session_dir = work_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        skip_file = session_dir / "schema_discovery_skips.json"

        records: List[Dict[str, Any]] = []
        if skip_file.exists():
            try:
                with open(skip_file) as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        records = loaded
            except Exception:
                # Corrupt or unreadable prior file: start fresh rather than fail.
                records = []

        records.append({
            "timestamp": datetime.now().isoformat(),
            "filenames": filenames,
            "reason": reason,
            "error": error[:2000],
        })

        with open(skip_file, "w") as f:
            json.dump(records, f, indent=2)
        logger.debug("Recorded skipped batch artifact (%d total) at %s", len(records), skip_file)
    except Exception as e:
        logger.warning("Could not write skipped-batch artifact: %s", e)


async def generate_schema_with_split(
    *,
    session_id: str,
    batch_docs: List[str],
    batch_names: List[str],
    run_generate: Callable[[List[str]], Any],
    is_stop_requested: Callable[[str], bool],
    work_dir: Optional[Path] = None,
    notify: Optional[Callable[[str, str], Any]] = None,
    _depth: int = 0,
) -> Optional[Any]:
    """Run schema generation for a batch, splitting on token-limit failure.

    Args:
        session_id: Session identifier (for stop checks and logging).
        batch_docs: Documents in this batch.
        batch_names: Filenames aligned with ``batch_docs``.
        run_generate: Async callable that takes a list of documents and returns
            the generated schema (or raises). This wraps content selection +
            ScheMatiQ.generate_schema for the given documents. It must be safe
            to call repeatedly with document subsets.
        is_stop_requested: Stop-signal check.
        work_dir: Working directory for skip artifacts.
        notify: Optional async callable ``(level, message)`` used to surface a
            user-facing note (e.g. in the monitor) when a batch is split or a
            document is skipped. Failures in the callback are swallowed.
        _depth: Internal recursion depth (for logging).

    Returns:
        The generated schema for the batch, a merged schema across split halves,
        or ``None`` if the entire batch had to be skipped.
    """
    async def _notify(level: str, message: str) -> None:
        if notify is None:
            return
        try:
            result = notify(level, message)
            if hasattr(result, "__await__"):
                await result
        except Exception as notify_err:
            logger.debug("notify callback failed: %s", notify_err)

    if is_stop_requested(session_id):
        return None

    try:
        return await run_generate(batch_docs)
    except Exception as e:
        if not is_token_limit_error(e):
            # Not an overflow — preserve original behavior: propagate.
            raise

        # Single document that still overflows: cannot split. Skip it.
        if len(batch_docs) <= 1:
            _log_skipped_batch(
                work_dir, session_id, batch_names,
                reason="single_document_exceeds_token_limit",
                error=str(e),
            )
            skipped_name = batch_names[0] if batch_names else "a document"
            await _notify(
                "warning",
                f"Skipped '{skipped_name}' during schema discovery: it exceeds the "
                f"model's token limit and cannot be split further. It will still be "
                f"available for value extraction.",
            )
            return None

        # Split in half and retry each half independently.
        mid = len(batch_docs) // 2
        logger.warning(
            "Batch of %d docs hit token limit (depth=%d); splitting into %d + %d and retrying",
            len(batch_docs), _depth, mid, len(batch_docs) - mid,
        )
        if _depth == 0:
            await _notify(
                "warning",
                f"A batch of {len(batch_docs)} documents exceeded the model's token "
                f"limit and was split into smaller batches automatically. Consider "
                f"lowering the fixed batch size if this recurs.",
            )

        left = await generate_schema_with_split(
            session_id=session_id,
            batch_docs=batch_docs[:mid],
            batch_names=batch_names[:mid],
            run_generate=run_generate,
            is_stop_requested=is_stop_requested,
            work_dir=work_dir,
            notify=notify,
            _depth=_depth + 1,
        )
        right = await generate_schema_with_split(
            session_id=session_id,
            batch_docs=batch_docs[mid:],
            batch_names=batch_names[mid:],
            run_generate=run_generate,
            is_stop_requested=is_stop_requested,
            work_dir=work_dir,
            notify=notify,
            _depth=_depth + 1,
        )

        if left is None:
            return right
        if right is None:
            return left

        # Merge the two half-schemas. Schema objects expose .merge(other).
        try:
            return left.merge(right)
        except Exception as merge_err:
            logger.error("Failed to merge split-batch schemas: %s", merge_err)
            # Fall back to the left half rather than losing everything.
            return left
