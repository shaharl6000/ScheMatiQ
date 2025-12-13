"""JSON response parsing and validation for LLM outputs."""

import json
import re
from typing import Dict, Any, List


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
    
    def postprocess(self, parsed: Dict[str, Dict[str, Any]], 
                   requested_cols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Ensure missing columns are omitted or set to {} and clean placeholders."""
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
            out[col] = {"answer": ans, "excerpts": exs}
        return out