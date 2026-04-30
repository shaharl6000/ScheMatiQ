"""Build Gemini response_schema for controlled generation during value extraction."""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Gemini response_schema property names must match this pattern.
# Names with hyphens, spaces, or other special characters cause a 400 INVALID_ARGUMENT.
_GEMINI_PROP_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _all_names_valid(columns) -> bool:
    """Return True if every column name is a valid Gemini response_schema property name."""
    return all(_GEMINI_PROP_NAME_RE.match(col.name) for col in columns)


def build_extraction_response_schema(columns) -> Optional[dict]:
    """Build Gemini response_schema for value extraction output.

    Creates a JSON Schema-compatible dict where each column is an optional
    top-level property containing answer (STRING) and excerpts (ARRAY).

    Returns None if any column name contains characters that Gemini rejects as
    response_schema property keys (e.g. hyphens like "IssueCourt-1"), which
    would cause a 400 INVALID_ARGUMENT error. Callers should skip controlled
    generation when None is returned and let the model output free-form JSON.

    Args:
        columns: List of Column objects with .name attribute.

    Returns:
        Dict suitable for Gemini's response_schema parameter, or None if any
        column name is invalid for use as a Gemini schema property.
    """
    if not _all_names_valid(columns):
        invalid = [col.name for col in columns if not _GEMINI_PROP_NAME_RE.match(col.name)]
        # Column names with hyphens or other special characters (e.g. "IssueCourt-1")
        # cause Gemini to return 400 INVALID_ARGUMENT. Fall back to free-form JSON output.
        logger.warning(
            "⚠️  Controlled generation disabled: %d column name(s) contain characters "
            "not allowed in Gemini response_schema properties: %s. "
            "Falling back to free-form JSON output.",
            len(invalid), invalid,
        )
        return None

    column_schema = {
        "type": "OBJECT",
        "properties": {
            "answer": {"type": "STRING"},
            "excerpts": {"type": "ARRAY", "items": {"type": "STRING"}},
            "suggested_for_allowed_values": {"type": "BOOLEAN"},
        },
        "required": ["answer", "excerpts"],
    }

    properties = {}
    for col in columns:
        properties[col.name] = column_schema

    return {
        "type": "OBJECT",
        "properties": properties,
        # No "required" key — all columns are optional so the model can omit unfound ones
    }
