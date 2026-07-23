"""Batch planning for schema discovery.

Schema discovery processes documents in batches. Historically batches were
built by naive fixed-size slicing in original order:

    batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]

That ignores document length. A single long document can dominate an LLM
context window while a batch of short documents wastes most of it, producing
more API calls than necessary and inconsistent per-batch content density.

This module packs documents into batches by estimated input token size using
First-Fit-Decreasing bin-packing, so each batch fills the available context
window budget without exceeding it. A per-batch document count cap keeps
individual batches from mixing too many documents (which can dilute schema
signal), and oversized single documents are placed alone in their own batch.

Two strategies are supported:

- ``"smart"`` (default): token-aware FFD bin-packing described above.
- ``"fixed"``: exact legacy behavior — fixed-size contiguous slices in the
  original order. Kept for reproducibility and as an escape hatch.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Conservative fallbacks used only when the config / model specs do not provide
# a value. These mirror the fallbacks already used in schema_discovery.py.
_DEFAULT_CONTEXT_WINDOW = 8192
_DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS = 700
_DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS = 50

# Reserve fraction of the context window for model output + drift so we never
# plan a batch that fills the whole window. Output for schema discovery is a
# JSON schema; reserving a third of the window is deliberately conservative.
_OUTPUT_RESERVE_FRACTION = 0.35

# Default ceiling on how many documents may share a single batch even when they
# would all fit token-wise. Prevents diluting schema signal across too many
# short documents. Overridable via config ``documents_batch_size`` (used as the
# cap in smart mode).
_DEFAULT_MAX_DOCS_PER_BATCH = 12


@dataclass
class PlannedBatch:
    """A single planned batch of documents.

    ``documents`` and ``filenames`` are aligned lists. ``estimated_tokens`` is
    the summed input-token estimate for the batch (documents only, excluding
    fixed prompt overhead).
    """

    documents: List[str]
    filenames: List[str]
    estimated_tokens: int


def _count_tokens(text: str) -> int:
    """Count tokens for a single document.

    Reuses the tiktoken-based counter from schematiq-lib when importable so the
    estimate matches the cost estimator. Falls back to a chars/4 heuristic,
    identical to the library's own fallback.
    """
    try:
        from schematiq.core.cost_estimator import count_tokens as _lib_count_tokens
        return _lib_count_tokens(text)
    except Exception:
        if not text:
            return 0
        return len(text) // 4


def _measured_prompt_overhead() -> int:
    """Fixed per-call prompt overhead (system prompt + user template).

    Uses measured values from schematiq-lib when available, otherwise
    conservative constants.
    """
    try:
        from schematiq.core.cost_estimator import _measure_prompt_tokens
        measured = _measure_prompt_tokens()
        return (
            measured.get("schema_system_prompt", _DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS)
            + measured.get("schema_user_template", _DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS)
        )
    except Exception:
        return _DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS + _DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS


def _resolve_context_window(schematiq_config: Dict[str, Any]) -> int:
    """Resolve the schema-creation context window from config, with fallback."""
    backend = schematiq_config.get("schema_creation_backend", {}) or {}
    ctx = backend.get("context_window_size")
    if isinstance(ctx, int) and ctx > 0:
        return ctx
    return _DEFAULT_CONTEXT_WINDOW


def _batch_token_budget(schematiq_config: Dict[str, Any]) -> int:
    """Compute the per-batch document-token budget.

    budget = context_window - prompt_overhead - output_reserve

    The result is floored at a small positive value so planning always
    terminates even with pathological configs.
    """
    context_window = _resolve_context_window(schematiq_config)
    prompt_overhead = _measured_prompt_overhead()
    output_reserve = int(context_window * _OUTPUT_RESERVE_FRACTION)
    budget = context_window - prompt_overhead - output_reserve
    return max(budget, 256)


def _fixed_batches(
    documents: List[str],
    filenames: List[str],
    batch_size: int,
) -> List[PlannedBatch]:
    """Legacy fixed-size contiguous slicing in original order."""
    batch_size = max(1, int(batch_size))
    planned: List[PlannedBatch] = []
    for i in range(0, len(documents), batch_size):
        batch_docs = documents[i:i + batch_size]
        batch_names = filenames[i:i + batch_size]
        planned.append(
            PlannedBatch(
                documents=batch_docs,
                filenames=batch_names,
                estimated_tokens=sum(_count_tokens(d) for d in batch_docs),
            )
        )
    return planned


def _smart_batches(
    documents: List[str],
    filenames: List[str],
    token_budget: int,
    max_docs_per_batch: int,
) -> List[PlannedBatch]:
    """First-Fit-Decreasing bin-packing by estimated token size.

    Documents larger than ``token_budget`` are placed alone in their own batch
    (they cannot be split here). Remaining documents are sorted descending by
    token count and placed into the first batch that has room for both the
    tokens and the document-count cap.
    """
    # Pair each document with its estimated token count and original filename.
    items: List[Tuple[int, str, str]] = [
        (_count_tokens(doc), doc, name)
        for doc, name in zip(documents, filenames)
    ]

    # Decreasing order: largest documents placed first (FFD).
    items.sort(key=lambda t: t[0], reverse=True)

    bins: List[PlannedBatch] = []

    for tokens, doc, name in items:
        # Oversized document: give it its own batch. It still gets processed;
        # downstream retrieval/truncation handles content that exceeds the
        # window. Packing it with others would only make things worse.
        if tokens >= token_budget:
            bins.append(PlannedBatch(documents=[doc], filenames=[name], estimated_tokens=tokens))
            continue

        placed = False
        for b in bins:
            fits_tokens = b.estimated_tokens + tokens <= token_budget
            fits_count = len(b.documents) < max_docs_per_batch
            # Skip bins already holding an oversized solo document.
            solo_oversized = len(b.documents) == 1 and b.estimated_tokens >= token_budget
            if fits_tokens and fits_count and not solo_oversized:
                b.documents.append(doc)
                b.filenames.append(name)
                b.estimated_tokens += tokens
                placed = True
                break

        if not placed:
            bins.append(PlannedBatch(documents=[doc], filenames=[name], estimated_tokens=tokens))

    return bins


def plan_batches(
    documents: List[str],
    filenames: List[str],
    schematiq_config: Dict[str, Any],
) -> List[PlannedBatch]:
    """Plan document batches for schema discovery.

    Args:
        documents: Document text contents.
        filenames: Corresponding filenames (aligned with ``documents``).
        schematiq_config: Full ScheMatiQ configuration dict. Reads
            ``batch_strategy`` ("smart" | "fixed"), ``documents_batch_size``,
            and ``schema_creation_backend.context_window_size``.

    Returns:
        List of PlannedBatch. Empty input yields an empty list.
    """
    if not documents:
        return []

    if len(documents) != len(filenames):
        # Defensive: never let misaligned inputs corrupt batching. Fall back to
        # the shorter length so downstream zip semantics are preserved.
        n = min(len(documents), len(filenames))
        logger.warning(
            "plan_batches received misaligned inputs (%d docs, %d names); truncating to %d",
            len(documents), len(filenames), n,
        )
        documents = documents[:n]
        filenames = filenames[:n]

    strategy = (schematiq_config.get("batch_strategy") or "smart").lower()
    batch_size = schematiq_config.get("documents_batch_size", 1) or 1

    if strategy == "fixed":
        planned = _fixed_batches(documents, filenames, batch_size)
        logger.debug(
            "Batch planning (fixed) - %d docs, batch_size=%d, %d batches",
            len(documents), batch_size, len(planned),
        )
        return planned

    # smart (default)
    token_budget = _batch_token_budget(schematiq_config)
    # In smart mode documents_batch_size is reinterpreted as the per-batch
    # document-count cap. A value <= 1 would defeat packing, so fall back to the
    # module default in that case.
    max_docs = int(batch_size) if int(batch_size) > 1 else _DEFAULT_MAX_DOCS_PER_BATCH
    planned = _smart_batches(documents, filenames, token_budget, max_docs)
    logger.debug(
        "Batch planning (smart) - %d docs, token_budget=%d, max_docs/batch=%d, %d batches",
        len(documents), token_budget, max_docs, len(planned),
    )
    return planned
