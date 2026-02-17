"""Dedicated parser for observation unit identification responses.

This parser is separate from the value extraction JSON parser because:
1. The response format is different (list of units vs column->value mapping)
2. The LLM often confuses the two formats, requiring specialized detection
3. We need retry logic with format-specific error feedback
"""

import json
import re
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple


@dataclass
class UnitParseResult:
    """Result of parsing a unit identification response."""
    units: List[Dict[str, Any]] = field(default_factory=list)
    success: bool = False
    error: Optional[str] = None
    detected_format: str = "unknown"  # "correct", "value_extraction", "malformed", "empty"
    warnings: List[str] = field(default_factory=list)


class UnitIdentificationParser:
    """Dedicated parser for observation unit identification LLM responses.

    Unlike the value extraction parser, this expects:
    {
        "observation_units": [
            {"unit_name": "...", "relevant_passages": [...], "confidence": "..."},
            ...
        ],
        "total_units_found": N,
        "notes": "..."
    }

    NOT:
    {
        "column_name": {"answer": "...", "excerpts": [...]}
    }
    """

    # Suspicious keys that indicate value extraction format
    VALUE_EXTRACTION_KEYS = {"answer", "excerpts", "normalized_to_allowed", "suggested_for_allowed_values"}

    # Suspicious unit names that indicate format confusion
    SUSPICIOUS_UNIT_NAMES = {"answer", "excerpts", "confidence", "relevant_passages", "observation_units"}

    def __init__(self):
        self.json_fence_re = re.compile(r"```json(.*?)```", re.S)
        self.last_json_re = re.compile(r"\{[\s\S]*\}\s*$", re.S)

    def parse_response(self, text: str) -> UnitParseResult:
        """Parse an LLM response for observation unit identification.

        Args:
            text: Raw LLM response text

        Returns:
            UnitParseResult with parsed units and diagnostic info
        """
        result = UnitParseResult()

        # Extract JSON from response
        try:
            raw_json = self._extract_json_str(text)
        except ValueError as e:
            result.error = f"No JSON found in response: {e}"
            result.detected_format = "malformed"
            return result

        # Parse JSON
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as e:
            # Try to fix common issues
            try:
                # Truncate after last complete object
                raw_json = raw_json.split("}\n", 1)[0] + "}"
                data = json.loads(raw_json)
            except json.JSONDecodeError:
                result.error = f"Invalid JSON: {e}"
                result.detected_format = "malformed"
                return result

        if not isinstance(data, dict):
            result.error = "Top-level JSON is not an object"
            result.detected_format = "malformed"
            return result

        # Detect the format
        result.detected_format = self._detect_format(data)

        if result.detected_format == "value_extraction":
            result.error = "LLM returned value extraction format instead of unit identification format"
            result.warnings.append("Response contains 'answer' and 'excerpts' keys typical of value extraction")
            return result

        if result.detected_format == "malformed":
            result.error = "Response format is malformed - neither unit identification nor value extraction"
            return result

        # Normalize and validate units
        units, warnings = self._normalize_units(data)
        result.units = units
        result.warnings.extend(warnings)

        if not units:
            result.detected_format = "empty"
            result.success = True  # Empty is valid (no units found in document)
        else:
            result.success = True

        return result

    def _extract_json_str(self, text: str) -> str:
        """Extract JSON string from LLM output."""
        # Try to find JSON in code fence first
        m = self.json_fence_re.search(text)
        if m:
            return m.group(1).strip()

        # Fall back to finding last JSON object
        m = self.last_json_re.search(text)
        if m:
            return m.group(0)

        raise ValueError("No JSON object found in output")

    def _detect_format(self, data: Dict[str, Any]) -> str:
        """Detect if the response is in correct format, value extraction format, or malformed.

        Returns:
            "correct" - Proper unit identification format
            "value_extraction" - Wrong format (looks like value extraction response)
            "malformed" - Neither format, something else wrong
        """
        # Check for value extraction format indicators
        # Case 1: Top-level has answer/excerpts keys (single column extraction)
        if self.VALUE_EXTRACTION_KEYS & set(data.keys()):
            return "value_extraction"

        # Case 2: observation_units is a dict with answer/excerpts (confused format)
        obs_units = data.get("observation_units")
        if isinstance(obs_units, dict):
            if self.VALUE_EXTRACTION_KEYS & set(obs_units.keys()):
                return "value_extraction"

        # Case 3: observation_units is a list but items look like value extraction
        if isinstance(obs_units, list):
            for item in obs_units:
                if isinstance(item, dict):
                    # Check if item has answer/excerpts instead of unit_name
                    if self.VALUE_EXTRACTION_KEYS & set(item.keys()):
                        if "unit_name" not in item:
                            return "value_extraction"

        # Check for correct format indicators
        if "observation_units" in data:
            if isinstance(obs_units, list):
                return "correct"
            elif isinstance(obs_units, dict) and "unit_name" in obs_units:
                # Single unit as dict (we'll wrap it)
                return "correct"

        # No observation_units key at all - might be direct list of units or malformed
        if any(isinstance(v, dict) and "unit_name" in v for v in data.values()):
            return "malformed"  # Has unit-like objects but wrong structure

        return "malformed"

    def _normalize_units(self, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Normalize and validate the units from a correctly-formatted response.

        Args:
            data: Parsed JSON that passed format detection

        Returns:
            Tuple of (list of validated units, list of warnings)
        """
        warnings = []
        validated_units = []

        obs_units = data.get("observation_units", [])

        # Handle case where observation_units is a single dict instead of a list
        if isinstance(obs_units, dict):
            if "unit_name" in obs_units:
                obs_units = [obs_units]
            else:
                warnings.append("observation_units is a dict but missing unit_name")
                return [], warnings

        if not isinstance(obs_units, list):
            warnings.append(f"observation_units is not a list: {type(obs_units)}")
            return [], warnings

        for i, unit in enumerate(obs_units):
            if isinstance(unit, str):
                # LLM returned just a string name
                unit_name_lower = unit.lower().strip()
                if unit_name_lower in self.SUSPICIOUS_UNIT_NAMES:
                    warnings.append(f"Skipped suspicious unit name: '{unit}'")
                    continue
                validated_units.append({
                    "unit_name": unit,
                    "relevant_passages": [],  # Will use full text as fallback
                    "confidence": "low"
                })
                warnings.append(f"Unit {i+1} was just a string, wrapped with minimal info")

            elif isinstance(unit, dict):
                # Check for suspicious unit names
                unit_name = unit.get("unit_name", "")
                if isinstance(unit_name, str):
                    if unit_name.lower().strip() in self.SUSPICIOUS_UNIT_NAMES:
                        warnings.append(f"Skipped suspicious unit name: '{unit_name}'")
                        continue

                # Validate required fields
                if "unit_name" in unit:
                    validated_unit = {
                        "unit_name": unit["unit_name"],
                        "relevant_passages": unit.get("relevant_passages", unit.get("passages", [])),
                        "confidence": unit.get("confidence", "medium")
                    }

                    # Ensure relevant_passages is a list
                    if not isinstance(validated_unit["relevant_passages"], list):
                        validated_unit["relevant_passages"] = [str(validated_unit["relevant_passages"])]

                    validated_units.append(validated_unit)

                elif "name" in unit or "unit" in unit:
                    # Try alternate field names
                    name = unit.get("name") or unit.get("unit") or f"Unit {len(validated_units) + 1}"
                    validated_units.append({
                        "unit_name": name,
                        "relevant_passages": unit.get("relevant_passages", unit.get("passages", [])),
                        "confidence": unit.get("confidence", "medium")
                    })
                    warnings.append(f"Unit used alternate field name instead of 'unit_name'")

                else:
                    # Dict but no recognizable unit name field
                    # Check if it looks like value extraction (skip silently)
                    if self.VALUE_EXTRACTION_KEYS & set(unit.keys()):
                        warnings.append(f"Skipped unit {i+1}: looks like value extraction format")
                        continue
                    warnings.append(f"Skipped unit {i+1}: no unit_name or recognizable field")
            else:
                warnings.append(f"Skipped unit {i+1}: unexpected type {type(unit)}")

        return validated_units, warnings


def create_retry_prompt_addition(previous_error: str, detected_format: str) -> str:
    """Create additional prompt text to help LLM fix format errors on retry.

    Args:
        previous_error: Error message from previous attempt
        detected_format: What format was detected ("value_extraction", "malformed", etc.)

    Returns:
        Additional instruction text to append to the prompt
    """
    additions = []

    additions.append("\n\n" + "="*50)
    additions.append("IMPORTANT: YOUR PREVIOUS RESPONSE HAD FORMAT ERRORS")
    additions.append("="*50)

    if detected_format == "value_extraction":
        additions.append("""
You returned a VALUE EXTRACTION format like:
{"answer": "...", "excerpts": [...]}

This is WRONG for this task. You need to return OBSERVATION UNITS like:
{
  "observation_units": [
    {"unit_name": "GPT-4 on MMLU", "relevant_passages": ["..."], "confidence": "high"}
  ]
}

DO NOT include "answer" or "excerpts" keys. Those are for a different task.
""")
    elif detected_format == "malformed":
        additions.append(f"""
Your response was malformed: {previous_error}

Please ensure your response is valid JSON with this exact structure:
{{
  "observation_units": [
    {{"unit_name": "Name here", "relevant_passages": ["passage 1", "passage 2"], "confidence": "high"}}
  ],
  "total_units_found": 1,
  "notes": "optional"
}}
""")
    else:
        additions.append(f"Previous error: {previous_error}")

    additions.append("="*50 + "\n")

    return "\n".join(additions)
