"""Build Gemini response_schema for controlled generation during value extraction."""

import re
from typing import Dict, Optional, Tuple

# Gemini response_schema property names must match this pattern.
# Names with hyphens, spaces, or other special characters cause a 400 INVALID_ARGUMENT.
_GEMINI_PROP_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Characters not allowed in Gemini property names, replaced with underscore.
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_name(name: str) -> str:
    """Replace characters invalid for Gemini response_schema property names with '_'."""
    return _SANITIZE_RE.sub("_", name)


def build_extraction_response_schema(
    columns,
) -> Tuple[dict, Dict[str, str]]:
    """Build Gemini response_schema for value extraction output.

    Sanitizes column names that contain characters Gemini rejects as property
    keys (e.g. hyphens in "IssueCourt-1" → "IssueCourt_1") so controlled
    generation always works.  Returns the schema together with a reverse mapping
    from sanitized name → original name so callers can restore the original keys
    after parsing the response.

    Args:
        columns: List of Column objects with .name attribute.

    Returns:
        (schema_dict, key_map) where:
          - schema_dict is suitable for Gemini's response_schema parameter.
          - key_map maps each sanitized property name back to the original
            column name.  Empty when no sanitization was needed.
    """
    key_map: Dict[str, str] = {}
    for col in columns:
        sanitized = _sanitize_name(col.name)
        if sanitized != col.name:
            key_map[sanitized] = col.name

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
        properties[_sanitize_name(col.name)] = column_schema

    schema = {
        "type": "OBJECT",
        "properties": properties,
        # No "required" key — all columns are optional so the model can omit unfound ones
    }
    return schema, key_map
