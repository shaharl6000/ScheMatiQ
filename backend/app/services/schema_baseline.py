"""Shared schema-baseline construction and checksum helpers."""

import hashlib
from datetime import datetime
from typing import Iterable

from app.models.session import ColumnBaseline, ColumnInfo, SchemaBaseline


def calculate_column_checksum(column: ColumnInfo) -> str:
    """Return the stable checksum used for schema-change detection.

    The default document strategy deliberately keeps the legacy checksum so
    persisted baselines do not make every existing column look edited.
    """
    content = f"{column.definition or ''}{column.rationale or ''}"
    if column.extraction_strategy != "document":
        content += f"|strategy:{column.extraction_strategy}"
    if column.allowed_values:
        content += "|".join(sorted(column.allowed_values))
    return hashlib.md5(content.encode()).hexdigest()


def build_column_baseline(column: ColumnInfo) -> ColumnBaseline:
    """Create the persisted baseline representation of one column."""
    return ColumnBaseline(
        name=column.name,
        definition=column.definition or "",
        rationale=column.rationale or "",
        allowed_values=column.allowed_values,
        extraction_strategy=column.extraction_strategy,
        checksum=calculate_column_checksum(column),
    )


def build_schema_baseline(columns: Iterable[ColumnInfo]) -> SchemaBaseline:
    """Create a baseline for all real data columns in a schema."""
    return SchemaBaseline(
        columns={
            column.name: build_column_baseline(column)
            for column in columns
            if column.name and not column.name.lower().endswith("_excerpt")
        },
        captured_at=datetime.now(),
    )
