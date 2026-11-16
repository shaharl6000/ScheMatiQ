"""Resilience for schema-discovery batches that exceed model token limits."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from schematiq.core.schema import Schema

logger = logging.getLogger(__name__)

# Substrings that identify a token / context-window overflow across providers.
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

    if any(m in text for m in ("rate limit", "resource_exhausted", "quota", "429", "api key", "unauthenticated")):
        return False

    if any(m in text for m in _TOKEN_LIMIT_MARKERS):
        return True

    if any(m in text for m in _INVALID_ARG_MARKERS) and "token" in text:
        return True

    return False


def generate_schema_with_split(
    *,
    batch_docs: List[str],
    batch_names: List[str],
    run_generate: Callable[[List[str]], Any],
    should_stop: Callable[[], bool],
    on_warning: Optional[Callable[[str, str, Optional[Dict[str, Any]]], None]] = None,
    on_subbatch_outcome: Optional[Callable[[List[str], Optional[Schema], bool], None]] = None,
    _depth: int = 0,
) -> Optional[Schema]:
    """Run schema generation for a batch, splitting on token-limit failure.

    Args:
        batch_docs: Documents in this batch.
        batch_names: Filenames aligned with ``batch_docs``.
        run_generate: Synchronous callable taking a list of documents and
            returning the generated ``Schema`` or a tuple whose first two elements
            are ``(Schema, document_helpful)``.
        should_stop: Stop-signal check returning True when discovery should stop.
        on_warning: Optional callback ``(level, message, skip_info)`` invoked
            when a batch is split or a single document is skipped.
        on_subbatch_outcome: Optional callback ``(filenames, schema_or_none, document_helpful)``
            invoked for each resolved leaf batch (or skipped single document).
        _depth: Internal recursion depth.

    Returns:
        The generated ``Schema`` for the batch, a merged schema across split
        halves, or ``None`` if the batch was skipped or stopped.
    """
    def _notify(
        level: str,
        message: str,
        skip_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        if on_warning is None:
            return
        try:
            on_warning(level, message, skip_info)
        except Exception as notify_err:
            logger.debug("on_warning callback failed: %s", notify_err)

    if should_stop():
        return None

    try:
        raw = run_generate(batch_docs)
    except Exception as e:
        if not is_token_limit_error(e):
            raise

        if len(batch_docs) <= 1:
            error_str = str(e)
            logger.warning(
                "Schema discovery skipped batch (%s): %s | error: %s",
                "single_document_exceeds_token_limit",
                batch_names,
                error_str[:500],
            )
            skipped_name = batch_names[0] if batch_names else "a document"
            _notify(
                "warning",
                f"Skipped '{skipped_name}' during schema discovery: it exceeds the "
                f"model's token limit and cannot be split further. It will still be "
                f"available for value extraction.",
                {
                    "filenames": list(batch_names),
                    "reason": "single_document_exceeds_token_limit",
                    "error": error_str[:2000],
                },
            )
            if on_subbatch_outcome is not None:
                on_subbatch_outcome(list(batch_names), None, False)
            return None

        mid = len(batch_docs) // 2
        logger.warning(
            "Batch of %d docs hit token limit (depth=%d); splitting into %d + %d and retrying",
            len(batch_docs), _depth, mid, len(batch_docs) - mid,
        )
        if _depth == 0:
            _notify(
                "warning",
                f"A batch of {len(batch_docs)} documents exceeded the model's token "
                f"limit and was split into smaller batches automatically. Consider "
                f"lowering the fixed batch size if this recurs.",
                None,
            )

        left = generate_schema_with_split(
            batch_docs=batch_docs[:mid],
            batch_names=batch_names[:mid],
            run_generate=run_generate,
            should_stop=should_stop,
            on_warning=on_warning,
            on_subbatch_outcome=on_subbatch_outcome,
            _depth=_depth + 1,
        )
        right = generate_schema_with_split(
            batch_docs=batch_docs[mid:],
            batch_names=batch_names[mid:],
            run_generate=run_generate,
            should_stop=should_stop,
            on_warning=on_warning,
            on_subbatch_outcome=on_subbatch_outcome,
            _depth=_depth + 1,
        )

        if left is None:
            return right
        if right is None:
            return left

        try:
            return left.merge(right)
        except Exception as merge_err:
            logger.error("Failed to merge split-batch schemas: %s", merge_err)
            return left

    if isinstance(raw, tuple):
        sub_schema = raw[0]
        doc_helpful = bool(raw[1]) if len(raw) > 1 else True
    else:
        sub_schema = raw
        doc_helpful = True

    if on_subbatch_outcome is not None:
        on_subbatch_outcome(list(batch_names), sub_schema, doc_helpful)
    return sub_schema
