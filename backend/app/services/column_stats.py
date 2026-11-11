"""Shared column statistics helpers for session data rows."""

from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterable, List

from app.models.session import ColumnInfo
from app.services.data_utils import normalize_row_data

_INVALID_STRING_ANSWERS = frozenset({"not found", "n/a", "none", "unknown"})
_INVALID_STRINGS_WITH_EMPTY = _INVALID_STRING_ANSWERS | {""}


def is_valid_value(value: Any) -> bool:
    """Return True when a cell value carries a non-null, meaningful answer."""
    if value is None:
        return False
    if isinstance(value, dict):
        answer = value.get("answer")
        if answer is None or answer == "None" or answer == "" or answer == "[]":
            return False
        if isinstance(answer, str) and answer.strip().lower() in _INVALID_STRING_ANSWERS:
            return False
        return True
    if isinstance(value, str) and value.strip().lower() in _INVALID_STRINGS_WITH_EMPTY:
        return False
    return value != "None" and value != "" and value != "[]"


def _canonical_value_key(canonical: Any) -> Any:
    """Return a hashable key for deduplicating cell answer values."""
    if canonical is None or isinstance(canonical, (str, int, float, bool)):
        return (type(canonical), canonical)
    if isinstance(canonical, tuple) and all(
        item is None or isinstance(item, (str, int, float, bool)) for item in canonical
    ):
        return (tuple, tuple((type(item), item) for item in canonical))
    try:
        return (dict, json.dumps(canonical, sort_keys=True))
    except (TypeError, ValueError):
        return (object, str(canonical))


def compute_column_stats(
    columns: Iterable[ColumnInfo],
    data_rows: List[Dict[str, Any]],
) -> List[ColumnInfo]:
    """Compute per-column non-null and unique value counts over ``data_rows``."""
    normalized_rows = [normalize_row_data(row) for row in data_rows]
    stats: List[ColumnInfo] = []

    for col in columns:
        non_null_count = 0
        seen_canonical = set()

        for row_data in normalized_rows:
            if col.name in row_data:
                value = row_data[col.name]
                if is_valid_value(value):
                    non_null_count += 1
                canonical = value.get("answer") if isinstance(value, dict) else value
                seen_canonical.add(_canonical_value_key(canonical))

        stats.append(
            ColumnInfo(
                name=col.name,
                display_name=col.display_name,
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",
                non_null_count=non_null_count,
                unique_count=len(seen_canonical),
                source_document=col.source_document,
                discovery_iteration=col.discovery_iteration,
                allowed_values=col.allowed_values,
                auto_expand_threshold=col.auto_expand_threshold,
            )
        )

    return stats


def count_unique_documents(data_rows: List[Dict[str, Any]]) -> int:
    """Count unique source document names referenced in rows' ``papers`` fields."""
    unique_documents = set()
    for row in data_rows:
        papers = row.get("papers", row.get("_papers", []))
        if isinstance(papers, list):
            unique_documents.update(papers)
        elif isinstance(papers, str) and papers:
            unique_documents.add(papers)
    return len(unique_documents) if unique_documents else len(data_rows)


def compute_completeness_percentage(
    row_count: int,
    columns: List[ColumnInfo],
) -> float:
    """Compute overall cell completeness percentage in [0, 100]."""
    total_cells = row_count * len(columns)
    non_null_cells = sum(col.non_null_count or 0 for col in columns)
    completeness = (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0
    if math.isnan(completeness) or math.isinf(completeness):
        return 0.0
    return completeness
