"""Build Gemini response_schema for controlled generation during value extraction."""

from typing import List


def build_extraction_response_schema(columns) -> dict:
    """Build Gemini response_schema for value extraction output.

    Creates a JSON Schema-compatible dict where each column is an optional
    top-level property containing answer (STRING) and excerpts (ARRAY).

    Args:
        columns: List of Column objects with .name attribute.

    Returns:
        Dict suitable for Gemini's response_schema parameter.
    """
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
