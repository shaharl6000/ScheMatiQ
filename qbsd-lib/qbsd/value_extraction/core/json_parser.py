"""JSON response parsing and validation for LLM outputs."""

import difflib
import json
import re
from typing import Dict, Any, List, Tuple, Optional


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
                    answer = str(answer)
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

    def _normalize_to_allowed_values(
        self,
        answer: str,
        allowed_values: List[str],
        threshold: float = 0.8
    ) -> Tuple[str, bool]:
        """
        Normalize extracted answer to closest allowed value using soft matching.
        Returns (normalized_answer, was_matched).

        Soft enforcement strategy:
        1. Case-insensitive exact match -> return allowed value
        2. Fuzzy match above threshold -> return allowed value
        3. No match -> keep original answer
        """
        if not allowed_values or not answer:
            return answer, False

        answer_lower = answer.lower().strip()

        # Case-insensitive exact match
        for av in allowed_values:
            if av.lower().strip() == answer_lower:
                return av, True

        # Fuzzy matching using difflib
        best_match: Optional[str] = None
        best_ratio: float = 0.0
        for av in allowed_values:
            ratio = difflib.SequenceMatcher(None, answer_lower, av.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = av

        if best_ratio >= threshold and best_match is not None:
            return best_match, True

        # No match - keep original (soft enforcement)
        return answer, False

    def postprocess(
        self,
        parsed: Dict[str, Dict[str, Any]],
        requested_cols: List[str],
        column_allowed_values: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Ensure missing columns are omitted or set to {} and clean placeholders.
        Optionally normalize values to allowed_values (closed set enforcement).

        Args:
            parsed: Parsed LLM response
            requested_cols: List of requested column names
            column_allowed_values: Dict mapping column_name to list of allowed values
        """
        column_allowed_values = column_allowed_values or {}
        out: Dict[str, Dict[str, Any]] = {}
        for col in requested_cols:
            entry = parsed.get(col)
            if not entry:
                continue  # omit missing column
            ans = entry.get("answer", "")
            exs = entry.get("excerpts", [])
            if self._is_placeholder(ans, exs) or (isinstance(ans, str) and not ans.strip()):
                # treat as missing by omitting the column (conforms to prompt spec)
                continue
            # normalize types
            if not isinstance(ans, str):
                ans = str(ans)
            if not isinstance(exs, list):
                exs = [str(exs)]

            # Apply allowed_values normalization (soft enforcement)
            normalized = False
            if col in column_allowed_values and column_allowed_values[col]:
                ans, normalized = self._normalize_to_allowed_values(ans, column_allowed_values[col])

            out[col] = {"answer": ans, "excerpts": exs}
            if normalized:
                out[col]["normalized_to_allowed"] = True
        return out