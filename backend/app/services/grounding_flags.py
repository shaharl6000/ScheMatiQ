"""Helpers for cells flagged ``_ungrounded`` (a value with no supporting
excerpt in the document, kept instead of deleted). Used by the CSV exports."""

from typing import Any, Iterable, Mapping

from app.services.file_parser import format_column_header

UNGROUNDED_COLUMNS_FIELD = "_ungrounded_columns"


def is_ungrounded_cell(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get("_ungrounded"))


def ungrounded_columns(cells: Mapping[str, Any], columns: Iterable[str]) -> str:
    """Display names (as in the CSV headers) of the flagged columns of one
    row, in ``columns`` order, joined by '; '."""
    return "; ".join(
        format_column_header(c) for c in columns if is_ungrounded_cell(cells.get(c))
    )


def any_ungrounded(rows_cells: Iterable[Mapping[str, Any]]) -> bool:
    return any(is_ungrounded_cell(v) for cells in rows_cells for v in cells.values())
