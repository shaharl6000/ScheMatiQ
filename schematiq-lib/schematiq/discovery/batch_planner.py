"""Batch planning for schema discovery.

Schema discovery processes documents in batches. Historically batches were
built by naive fixed-size slicing in original order:

    batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]

That ignores document length. A single long document can dominate an LLM
context window while a batch of short documents wastes most of it, producing
more API calls than necessary and inconsistent per-batch content density.

This module packs documents into batches by estimated input token size using
First-Fit-Decreasing bin-packing, so each batch fills the available context
window budget without exceeding it. Packing as many documents as fit gives the
model broader context per call, which tends to produce a richer schema. A high
safety cap on documents per batch guards only against pathological inputs (e.g.
thousands of tiny documents forming one unwieldy prompt); oversized single
documents are placed alone in their own batch.

Two strategies are supported:

- ``"smart"`` (default): token-aware FFD bin-packing described above.
- ``"fixed"``: exact legacy behavior — fixed-size contiguous slices in the
  original order. Kept for reproducibility and as an escape hatch.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

try:
    from schematiq.core.cost_estimator import count_tokens as _count_tokens
except Exception:  # pragma: no cover - defensive fallback
    def _count_tokens(text: str) -> int:
        return len(text) // 4 if text else 0

_DEFAULT_CONTEXT_WINDOW = 8192
_DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS = 700
_DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS = 50

# Reserve fraction of the context window for model output + drift so we never
# plan a batch that fills the whole window.
_OUTPUT_RESERVE_FRACTION = 0.35

# Safety ceiling on documents per batch in smart mode. Guards against
# pathologically large batches (e.g. thousands of tiny documents).
_SMART_SAFETY_MAX_DOCS_PER_BATCH = 30


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


def _measured_prompt_overhead() -> int:
    """Fixed per-call prompt overhead (system prompt + user template)."""
    try:
        from schematiq.core.cost_estimator import _measure_prompt_tokens
        measured = _measure_prompt_tokens()
        return (
            measured.get("schema_system_prompt", _DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS)
            + measured.get("schema_user_template", _DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS)
        )
    except Exception:
        return _DEFAULT_SCHEMA_SYSTEM_PROMPT_TOKENS + _DEFAULT_SCHEMA_USER_TEMPLATE_TOKENS


def _resolve_context_window(context_window_size: Optional[int]) -> int:
    """Resolve the schema-creation context window, with fallback."""
    if isinstance(context_window_size, int) and context_window_size > 0:
        return context_window_size
    return _DEFAULT_CONTEXT_WINDOW


def _batch_token_budget(context_window_size: Optional[int]) -> int:
    """Compute the per-batch document-token budget.

    budget = context_window - prompt_overhead - output_reserve

    The result is floored at a small positive value so planning always
    terminates even with pathological configs.
    """
    context_window = _resolve_context_window(context_window_size)
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
    """First-Fit-Decreasing bin-packing by estimated token size."""
    items: List[Tuple[int, str, str]] = [
        (_count_tokens(doc), doc, name)
        for doc, name in zip(documents, filenames)
    ]

    # Decreasing order: largest documents placed first (FFD).
    items.sort(key=lambda t: t[0], reverse=True)

    bins: List[PlannedBatch] = []

    for tokens, doc, name in items:
        if tokens >= token_budget:
            bins.append(PlannedBatch(documents=[doc], filenames=[name], estimated_tokens=tokens))
            continue

        placed = False
        for b in bins:
            fits_tokens = b.estimated_tokens + tokens <= token_budget
            fits_count = len(b.documents) < max_docs_per_batch
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
    documents: Sequence[str],
    filenames: Sequence[str],
    *,
    batch_strategy: str = "smart",
    batch_size: int = 1,
    context_window_size: Optional[int] = None,
) -> List[PlannedBatch]:
    """Plan document batches for schema discovery.

    Args:
        documents: Document text contents.
        filenames: Corresponding filenames (aligned with ``documents``).
        batch_strategy: ``"smart"`` (token-aware FFD bin-packing) or ``"fixed"``.
        batch_size: Documents per batch when ``batch_strategy == "fixed"``.
        context_window_size: Model context window size in tokens (optional).

    Returns:
        List of PlannedBatch. Empty input yields an empty list.
    """
    if not documents:
        return []

    docs_list = list(documents)
    names_list = list(filenames)

    if len(docs_list) != len(names_list):
        n = min(len(docs_list), len(names_list))
        logger.warning(
            "plan_batches received misaligned inputs (%d docs, %d names); truncating to %d",
            len(docs_list), len(names_list), n,
        )
        docs_list = docs_list[:n]
        names_list = names_list[:n]

    strategy = (batch_strategy or "smart").lower()
    effective_batch_size = batch_size or 1

    if strategy == "fixed":
        planned = _fixed_batches(docs_list, names_list, effective_batch_size)
        logger.debug(
            "Batch planning (fixed) - %d docs, batch_size=%d, %d batches",
            len(docs_list), effective_batch_size, len(planned),
        )
        return planned

    token_budget = _batch_token_budget(context_window_size)
    planned = _smart_batches(docs_list, names_list, token_budget, _SMART_SAFETY_MAX_DOCS_PER_BATCH)
    logger.debug(
        "Batch planning (smart) - %d docs, token_budget=%d, safety_max_docs/batch=%d, %d batches",
        len(docs_list), token_budget, _SMART_SAFETY_MAX_DOCS_PER_BATCH, len(planned),
    )
    return planned
