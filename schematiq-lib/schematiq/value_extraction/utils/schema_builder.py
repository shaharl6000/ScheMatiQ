"""Build Gemini response_schema for controlled generation during value extraction."""

import logging
import re
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Characters not allowed in Gemini response_schema property names → replaced with '_'.
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")

# Gemini rejects response_schema with 400 INVALID_ARGUMENT when total schema
# complexity is too high.  Each column is an OBJECT with 3 sub-fields (answer,
# excerpts, suggested_for_allowed_values), so N columns ≈ 4N schema properties.
# Empirically on gemini-3.1-flash-lite the limit is ~48 columns with
# typical property names; 40 provides a safe margin for longer names.
# Callers (extract_values_for_unit, extract_values_for_paper) pre-batch columns
# into chunks of this size so every call gets controlled generation.  The check
# inside build_extraction_response_schema acts as a safety net.
_MAX_COLUMNS_FOR_CONTROLLED_GENERATION = 40


def _sanitize_name(name: str) -> str:
    """Replace characters invalid for Gemini response_schema property names with '_'."""
    return _SANITIZE_RE.sub("_", name)


def build_extraction_response_schema(
    columns,
) -> Tuple[Optional[dict], Dict[str, str]]:
    """Build Gemini response_schema for value extraction output.

    Returns (None, {}) when the number of columns exceeds the safe limit for
    Gemini controlled generation.  Callers should skip controlled generation
    when None is returned and let the model output free-form JSON.

    Sanitizes column names that contain characters Gemini rejects as property
    keys (e.g. hyphens in "IssueCourt-1" → "IssueCourt_1") and returns a
    reverse key_map so callers can restore original names after parsing.

    Args:
        columns: List of Column objects with .name attribute.

    Returns:
        (schema_dict, key_map) where schema_dict is None when too many columns,
        or a dict suitable for Gemini's response_schema parameter otherwise.
        key_map maps sanitized names back to original column names.
    """
    if len(columns) > _MAX_COLUMNS_FOR_CONTROLLED_GENERATION:
        logger.warning(
            "Controlled generation disabled: %d columns exceeds limit of %d",
            len(columns),
            _MAX_COLUMNS_FOR_CONTROLLED_GENERATION,
        )
        return None, {}

    key_map: Dict[str, str] = {}
    sanitized_to_original: Dict[str, str] = {}
    for col in columns:
        sanitized = _sanitize_name(col.name)
        if sanitized in sanitized_to_original and sanitized_to_original[sanitized] != col.name:
            logger.warning(
                "Controlled generation disabled: %r and %r both sanitize to %r",
                sanitized_to_original[sanitized], col.name, sanitized,
            )
            return None, {}
        sanitized_to_original[sanitized] = col.name
        if sanitized != col.name:
            key_map[sanitized] = col.name

    column_schema = {
        "type": "OBJECT",
        "properties": {
            "answer": {"type": "STRING", "nullable": True},
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
        "required": [_sanitize_name(col.name) for col in columns],
    }
    return schema, key_map
