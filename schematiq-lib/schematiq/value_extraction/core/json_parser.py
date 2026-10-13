"""JSON response parsing and validation for LLM outputs."""

import difflib
import json
import re
from datetime import datetime
from typing import Callable, Dict, Any, List, Tuple, Optional

from dateutil import parser as date_parser


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
            r"^(not\s+provided|not\s+specified|not\s+mentioned|no\s+(information|data)|"
            r"cannot\s+be\s+determined|unknown|n/?a|no\s+answer|insufficient\s+information|"
            r"none|null|not\s+applicable|not\s+available|no\s+entry|empty)$",
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
        # Fast path: try direct JSON parse (works with controlled generation)
        try:
            data = json.loads(text.strip())
            if isinstance(data, dict):
                return self._normalize_parsed(data)
        except (json.JSONDecodeError, ValueError):
            pass

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

        return self._normalize_parsed(data)
    
    def _normalize_parsed(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Normalise a parsed JSON dict into the standard column format.

        Each column entry is guaranteed to have "answer" (str), "excerpts"
        (list), and "figure_refs" (list — figure_id strings the model cited
        for an image-backed answer; see schema_builder's response schema).
        """
        norm: Dict[str, Dict[str, Any]] = {}
        for col, val in data.items():
            if isinstance(val, dict):
                answer = val.get("answer", "")
                excerpts = val.get("excerpts", [])
                figure_refs = val.get("figure_refs", [])
                # Preserve explicit null — do NOT convert to the string "None".
                # postprocess() checks for None and treats it as confirmed-empty.
                if answer is not None and not isinstance(answer, str):
                    answer = _flatten_answer(answer)
                if not isinstance(excerpts, list):
                    excerpts = [str(excerpts)]
                if not isinstance(figure_refs, list):
                    figure_refs = [str(figure_refs)]
                entry: Dict[str, Any] = {
                    "answer": answer, "excerpts": excerpts, "figure_refs": figure_refs,
                }
                if val.get("suggested_for_allowed_values"):
                    entry["suggested_for_allowed_values"] = True
                norm[col] = entry
            else:
                norm[col] = {"answer": str(val), "excerpts": [], "figure_refs": []}
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

    def _strftime_format_from_date_constraint(self, raw_constraint: str) -> Optional[str]:
        """
        Single-item allowed_values like 'date', 'date:iso', 'date:%m/%d/%Y', 'date:us', 'date:eu', 'date:long'.
        Returns a strftime format string for output, or None if not a date constraint.
        """
        c = raw_constraint.strip()
        low = c.lower()
        if low == "date":
            return "%Y-%m-%d"
        if not low.startswith("date:"):
            return None
        spec = c[5:].strip()
        if not spec or spec.lower() == "iso":
            return "%Y-%m-%d"
        shortcuts = {
            "us": "%m/%d/%Y",
            "eu": "%d/%m/%Y",
            "long": "%B %d, %Y",
        }
        return shortcuts.get(spec.lower(), spec)

    def _dayfirst_for_date_constraint(self, raw_constraint: str) -> bool:
        return raw_constraint.strip().lower() == "date:eu"

    def _parse_answer_to_datetime(self, answer: str, dayfirst: bool) -> Optional[datetime]:
        s = answer.strip()
        if not s:
            return None
        try:
            return date_parser.parse(s, dayfirst=dayfirst, fuzzy=False)
        except (ValueError, OverflowError):
            pass
        try:
            return date_parser.parse(s, dayfirst=dayfirst, fuzzy=True)
        except (ValueError, OverflowError):
            return None

    def _normalize_to_allowed_values(
        self,
        answer: str,
        allowed_values: List[str],
        threshold: float = 0.8,
        semantic_matcher: Optional[Callable[[str, List[str]], Optional[str]]] = None,
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Normalize extracted answer to closest allowed value using soft matching.
        Returns (normalized_answer, was_matched, unmatched_original).

        - unmatched_original: Set to the original answer when it didn't match (for schema evolution tracking)

        Soft enforcement strategy:
        1. Date constraints (single-item "date" / "date:...") -> parse and format
        2. Numeric constraints (single-item "number" or min-max range)
        3. Case-insensitive exact match -> return allowed value
        4. semantic_matcher(answer, allowed_values), if provided -> return its match
           (an LLM-backed meaning check, since a fixed string-similarity ratio is a
           weak proxy for meaning and fails free-form, sentence-length answers)
        5. Otherwise, fuzzy string match above threshold -> return allowed value
           (kept as a dependency-free fallback for callers with no LLM access,
           e.g. isolated tests)
        6. No match -> keep original answer and flag for schema evolution
        """
        if not allowed_values or not answer:
            return answer, False, None

        # Check for numeric constraints (single-item list)
        if len(allowed_values) == 1:
            raw_constraint = allowed_values[0].strip()
            constraint = raw_constraint.lower()

            out_fmt = self._strftime_format_from_date_constraint(raw_constraint)
            if out_fmt is not None:
                dayfirst = self._dayfirst_for_date_constraint(raw_constraint)
                dt = self._parse_answer_to_datetime(answer, dayfirst=dayfirst)
                if dt is not None:
                    try:
                        formatted = dt.strftime(out_fmt)
                        return formatted, True, None
                    except (ValueError, OSError):
                        return answer, False, answer
                return answer, False, answer

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

        # Semantic (LLM-backed) equivalence check, when a matcher was supplied
        if semantic_matcher is not None:
            matched = semantic_matcher(answer, allowed_values)
            if matched is not None:
                return matched, True, None
            return answer, False, answer

        # Fuzzy matching using difflib (fallback when no semantic_matcher given)
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

    def is_type_constraint(self, allowed_values: List[str]) -> bool:
        """True if allowed_values is a single-item TYPE/format constraint
        (date / date:... / number / a min-max range) rather than a literal
        set of categorical values -- mirrors _normalize_to_allowed_values's
        single-item branch above. A caller deciding whether to gate
        enforcement on multi-document corroboration should treat this case
        specially: corroboration only makes sense for genuine categorical
        values that might have been borrowed from an unrelated document.
        A type constraint is a structural format rule the schema itself
        declared, not content, so it should always apply -- and in fact
        can never accumulate corroboration under the same key, since
        _normalize_to_allowed_values returns the ANSWER's own parsed/
        clamped value (e.g. "100.0"), never the literal constraint token
        ("number"/"0-100"/"date"), so treating it like a categorical value
        would silently disable it forever.
        """
        if not allowed_values or len(allowed_values) != 1 or not isinstance(allowed_values[0], str):
            return False
        raw = allowed_values[0].strip()
        if self._strftime_format_from_date_constraint(raw) is not None:
            return True
        if raw.lower() == "number":
            return True
        return self._parse_numeric_range(raw.lower()) is not None

    def matches_allowed_value(self, answer: str, allowed_values: List[str]) -> Optional[str]:
        """Public read-only check: does *answer* match one of allowed_values
        (same exact/case-insensitive/fuzzy rules postprocess's soft
        enforcement uses)? Returns the canonical matched value, or None --
        callers that only need to know "would this match," without applying
        the replacement postprocess does, should use this instead of
        reaching into _normalize_to_allowed_values directly.

        Defensively typed at this public boundary (unlike the private
        method it wraps, which trusts postprocess's own upstream
        None/type/placeholder checks): a non-string answer or falsy
        allowed_values just means "no match" rather than an AttributeError.
        """
        if not isinstance(answer, str) or not allowed_values:
            return None
        matched_value, matched, _ = self._normalize_to_allowed_values(answer, allowed_values)
        return matched_value if matched else None

    def postprocess(
        self,
        parsed: Dict[str, Dict[str, Any]],
        requested_cols: List[str],
        column_allowed_values: Optional[Dict[str, List[str]]] = None,
        semantic_matcher: Optional[Callable[[str, List[str]], Optional[str]]] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[str]]]:
        """
        Ensure missing columns are omitted or set to {} and clean placeholders.
        Optionally normalize values to allowed_values (closed set enforcement).

        Args:
            parsed: Parsed LLM response
            requested_cols: List of requested column names
            column_allowed_values: Dict mapping column_name to list of allowed values
            semantic_matcher: Optional LLM-backed equivalence check, passed
                through to _normalize_to_allowed_values (see its docstring).
                Callers with LLM access (PaperProcessor) should supply this;
                it falls back to difflib string-similarity when omitted.

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
            figs = entry.get("figure_refs", [])

            # Explicit null answer = LLM confirmed this column is empty/not applicable.
            # Keep it in the output so it counts as "filled" and won't be retried.
            if ans is None:
                out[col] = {"answer": "", "excerpts": [], "_confirmed_empty": True}
                continue

            # Check if LLM flagged this as a suggested new value
            suggested_for_allowed = entry.get("suggested_for_allowed_values", False)

            # A figure_refs citation counts as "they gave evidence" the same
            # way a text excerpt does — an image-backed answer legitimately
            # has no text excerpt, and shouldn't be treated as an unsupported
            # placeholder just because exs is empty.
            if self._is_placeholder(ans, exs or figs) or (isinstance(ans, str) and not ans.strip()):
                continue
            # normalize types
            if not isinstance(ans, str):
                ans = _flatten_answer(ans)
            if not isinstance(exs, list):
                exs = [str(exs)]
            if not isinstance(figs, list):
                figs = [str(figs)]

            # Apply allowed_values normalization (soft enforcement)
            normalized = False
            unmatched_original = None
            if col in column_allowed_values and column_allowed_values[col]:
                # Deliberately does NOT clear/replace exs when normalization
                # changes ans's content, even though exs was written to
                # support the pre-normalization answer: exs is verified for
                # real downstream, in _ground_and_enforce (paper_processor.py),
                # which nulls the WHOLE answer if excerpts end up empty and
                # this context isn't vision-derived -- so clearing a real,
                # source-grounded excerpt here (tried once; see git history)
                # doesn't trade "answer paired with an imperfect citation" for
                # "answer paired with no citation," it trades it for "answer
                # gone entirely," which is worse. Leaving exs as the model's
                # own real excerpt lets _ground_and_enforce's actual
                # source-text verification decide its fate instead.
                ans, normalized, unmatched_original = self._normalize_to_allowed_values(
                    ans, column_allowed_values[col], semantic_matcher=semantic_matcher
                )

            out[col] = {"answer": ans, "excerpts": exs}
            if figs:
                out[col]["figure_refs"] = figs
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