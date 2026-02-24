"""JSON response parsing and validation for LLM outputs."""

import difflib
import json
import re
from typing import Dict, Any, List, Tuple, Optional


def _flatten_answer(answer: Any) -> str:
    """Convert a non-string answer (dict/list) to a clean readable string.

    - dict: extract all non-null values, join with ", "
    - list of dicts: flatten each dict, join items with "; "
    - list of primitives: join with ", "
    - other: str(answer)
    """
    if isinstance(answer, str):
        return answer
    if isinstance(answer, dict):
        values = [
            str(v) for v in answer.values()
            if v is not None and str(v).strip()
        ]
        return ', '.join(values) if values else str(answer)
    if isinstance(answer, list):
        if answer and isinstance(answer[0], dict):
            parts = []
            for item in answer:
                if isinstance(item, dict):
                    vals = [
                        str(v) for v in item.values()
                        if v is not None and str(v).strip()
                    ]
                    parts.append(', '.join(vals) if vals else str(item))
                else:
                    parts.append(str(item))
            return '; '.join(parts)
        return ', '.join(str(v) for v in answer)
    return str(answer)


class JSONResponseParser:
    """Handles parsing and validation of LLM JSON responses."""

    def __init__(self):
        self.json_fence_re = re.compile(r"```json(.*?)```", re.S)
        self.last_js_re = re.compile(r"\{[\s\S]*\}\s*$", re.S)
        self.placeholder_re = re.compile(
            r"\b(not\s+provided|not\s+specified|not\s+mentioned|no\s+(information|data)|"
            r"cannot\s+be\s+determined|unknown|n/?a|no\s+answer|insufficient\s+information)\b",
            re.I,
        )
    
    def extract_json_str(self, text: str) -> str:
        """Pull the *text* of the JSON object out of the LLM output."""
        m = self.json_fence_re.search(text)
        candidate = m.group(1).strip() if m else None

        if candidate is None:
            m = self.last_js_re.search(text)
            if not m:
                raise ValueError("No JSON object found in output.")
            candidate = m.group(0)

        return candidate
    
    def parse_response(self, text: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse ValueLLM output into a normalised dict::

            {
              "<column>": { "answer": <str>, "excerpts": <list[str]> },
              ...
            }

        • Missing "answer" / "excerpts" keys are filled with "" / []
        • If the model returned just a string for a column, it is wrapped
          into the {"answer": "..."} structure for consistency.
        """
        raw_json = self.extract_json_str(text)

        # First pass – try as‑is
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            # Common failure: trailing tokens after the last } – truncate
            raw_json = raw_json.split("}\n", 1)[0] + "}"
            data = json.loads(raw_json)

        if not isinstance(data, dict):
            raise ValueError("Top‑level JSON is not an object.")

        # Normalise each column entry
        norm: Dict[str, Dict[str, Any]] = {}
        for col, val in data.items():
            # Case 1: already the expected dict but possibly missing keys
            if isinstance(val, dict):
                answer = val.get("answer", "")
                excerpts = val.get("excerpts", [])
                # Guarantee correct types
                if not isinstance(answer, str):
                    answer = _flatten_answer(answer)
                if not isinstance(excerpts, list):
                    excerpts = [str(excerpts)]
                norm[col] = {"answer": answer, "excerpts": excerpts}

            # Case 2: the model emitted a bare string / number
            else:
                norm[col] = {"answer": str(val), "excerpts": []}

        return norm
    
    def _is_placeholder(self, answer: str, excerpts: List[str]) -> bool:
        """Check if answer appears to be a placeholder/non-answer."""
        if not isinstance(answer, str):
            return False
        if excerpts:  # if they gave evidence, allow it (paper may literally say "unknown")
            return False
        return bool(self.placeholder_re.search(answer.strip()))

    def _parse_numeric_range(self, constraint: str) -> Optional[Tuple[float, float]]:
        """
        Parse a numeric range constraint like "0-100" or "0.0-1.0".
        Returns (min_val, max_val) or None if not a valid range.
        """
        constraint = constraint.strip()
        if "-" not in constraint:
            return None

        # Handle negative numbers by checking if it starts with a negative
        # For ranges like "-10-10", we need to be careful
        parts = constraint.split("-")

        # Simple case: "0-100" -> ["0", "100"]
        if len(parts) == 2 and parts[0] and parts[1]:
            try:
                return float(parts[0]), float(parts[1])
            except ValueError:
                return None

        # Handle negative start: "-10-10" -> ["", "10", "10"]
        if len(parts) == 3 and not parts[0]:
            try:
                return -float(parts[1]), float(parts[2])
            except ValueError:
                return None

        return None

    def _normalize_to_allowed_values(
        self,
        answer: str,
        allowed_values: List[str],
        threshold: float = 0.8
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Normalize extracted answer to closest allowed value using soft matching.
        Returns (normalized_answer, was_matched, unmatched_original).

        - unmatched_original: Set to the original answer when it didn't match (for schema evolution tracking)

        Soft enforcement strategy:
        1. Check for numeric constraints (single-item list with "number" or range)
        2. Case-insensitive exact match -> return allowed value
        3. Fuzzy match above threshold -> return allowed value
        4. No match -> keep original answer and flag for schema evolution
        """
        if not allowed_values or not answer:
            return answer, False, None

        # Check for numeric constraints (single-item list)
        if len(allowed_values) == 1:
            constraint = allowed_values[0].lower().strip()

            # Type constraint: "number" - accepts any numeric value
            if constraint == "number":
                # Clean the answer (remove common suffixes like %, etc.)
                clean_answer = answer.strip().rstrip("%").strip()
                try:
                    float(clean_answer)  # Validates it's numeric
                    return clean_answer, True, None  # Return cleaned value
                except ValueError:
                    return answer, False, answer  # Not a number, flag for review

            # Range constraint: "min-max" pattern (e.g., "0-100", "0.0-1.0")
            range_vals = self._parse_numeric_range(constraint)
            if range_vals is not None:
                min_val, max_val = range_vals
                # Clean the answer (remove common suffixes like %, etc.)
                clean_answer = answer.strip().rstrip("%").strip()
                try:
                    num_answer = float(clean_answer)
                    # Clamp to range (soft enforcement)
                    if num_answer < min_val:
                        return str(min_val), True, None
                    elif num_answer > max_val:
                        return str(max_val), True, None
                    return clean_answer, True, None
                except ValueError:
                    return answer, False, answer  # Can't parse, flag for review

        # Categorical matching: case-insensitive exact match
        answer_lower = answer.lower().strip()
        for av in allowed_values:
            if av.lower().strip() == answer_lower:
                return av, True, None

        # Fuzzy matching using difflib
        best_match: Optional[str] = None
        best_ratio: float = 0.0
        for av in allowed_values:
            ratio = difflib.SequenceMatcher(None, answer_lower, av.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = av

        if best_ratio >= threshold and best_match is not None:
            return best_match, True, None

        # No match - keep original (soft enforcement) and flag for schema evolution
        return answer, False, answer

    def postprocess(
        self,
        parsed: Dict[str, Dict[str, Any]],
        requested_cols: List[str],
        column_allowed_values: Optional[Dict[str, List[str]]] = None
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[str]]]:
        """
        Ensure missing columns are omitted or set to {} and clean placeholders.
        Optionally normalize values to allowed_values (closed set enforcement).

        Args:
            parsed: Parsed LLM response
            requested_cols: List of requested column names
            column_allowed_values: Dict mapping column_name to list of allowed values

        Returns:
            Tuple of:
            - out: Dict of column results with answer, excerpts, and normalization flags
            - unmatched_values: Dict mapping column_name to list of unmatched values for schema evolution
        """
        column_allowed_values = column_allowed_values or {}
        out: Dict[str, Dict[str, Any]] = {}
        unmatched_values: Dict[str, List[str]] = {}

        for col in requested_cols:
            entry = parsed.get(col)
            if not entry:
                continue  # omit missing column
            ans = entry.get("answer", "")
            exs = entry.get("excerpts", [])

            # Check if LLM flagged this as a suggested new value
            suggested_for_allowed = entry.get("suggested_for_allowed_values", False)

            if self._is_placeholder(ans, exs) or (isinstance(ans, str) and not ans.strip()):
                # treat as missing by omitting the column (conforms to prompt spec)
                continue
            # normalize types
            if not isinstance(ans, str):
                ans = _flatten_answer(ans)
            if not isinstance(exs, list):
                exs = [str(exs)]

            # Apply allowed_values normalization (soft enforcement)
            normalized = False
            unmatched_original = None
            if col in column_allowed_values and column_allowed_values[col]:
                ans, normalized, unmatched_original = self._normalize_to_allowed_values(ans, column_allowed_values[col])

            out[col] = {"answer": ans, "excerpts": exs}
            if normalized:
                out[col]["normalized_to_allowed"] = True

            # Track unmatched values for schema evolution
            if unmatched_original is not None or suggested_for_allowed:
                value_to_track = unmatched_original or ans
                if col not in unmatched_values:
                    unmatched_values[col] = []
                if value_to_track not in unmatched_values[col]:
                    unmatched_values[col].append(value_to_track)
                out[col]["unmatched_value"] = value_to_track

        return out, unmatched_values