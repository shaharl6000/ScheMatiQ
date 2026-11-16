"""Ground extraction excerpts in attached reference documents.

During value extraction the model sees the source document and, when the user
attached one, an external reference document (supplementary lookup material,
e.g. a table mapping each judge to the president who appointed them). A value
the model takes from the reference is supported by a quote from the reference,
not from the source document.

``PaperProcessor._ground_and_enforce`` checks excerpts against the source text
only, so such a quote comes back ``not_found`` and the correct answer is
flagged ``_ungrounded`` (or nulled under the "drop" policy). This module gives
those excerpts a second chance: an excerpt the source grounding could not find
is searched in each reference document, and when found it is re-attributed to
that reference.

Matching is stricter than source grounding: only exact, case-insensitive and
whitespace/dash-normalized matches count (no fuzzy phase). A reference can be a
large table, where a fuzzy match would turn a paraphrase into a "grounded" one.
Short excerpts are accepted: in a lookup table the supporting quote is often a
single cell (e.g. a president's name).
"""

from __future__ import annotations

import re
import threading
from typing import Any, Dict, List, Optional, Tuple

from .excerpt_grounder import ExcerptGrounder, _PreparedSource, _strip_decorations

# Section header written by the backend's build_reference_context for each
# attached reference document. Keep in sync with
# backend/app/services/reference_context.py.
_SECTION_RE = re.compile(r"^--- Reference document: (?P<name>.+?) ---$", re.MULTILINE)

# build_reference_context joins sections with this separator.
_SECTION_SEPARATOR = "\n\n"


class ReferenceGrounder:
    """Locate excerpts that the source grounding missed in reference documents."""

    def __init__(self, reference_context: Optional[str]):
        self._grounder = ExcerptGrounder()
        self._sections: List[Tuple[str, _PreparedSource]] = []
        # The normalized index of a reference is built lazily and can be large;
        # build it once even when several worker threads need it at the same time.
        self._normalize_lock = threading.Lock()
        if reference_context:
            self._sections = self._parse_sections(reference_context)

    @property
    def active(self) -> bool:
        return bool(self._sections)

    @staticmethod
    def _parse_sections(reference_context: str) -> List[Tuple[str, _PreparedSource]]:
        headers = list(_SECTION_RE.finditer(reference_context))
        sections: List[Tuple[str, _PreparedSource]] = []
        for i, header in enumerate(headers):
            body_start = header.end()
            if reference_context.startswith("\n", body_start):
                body_start += 1
            if i + 1 < len(headers):
                body_end = headers[i + 1].start()
                body = reference_context[body_start:body_end]
                if body.endswith(_SECTION_SEPARATOR):
                    body = body[: -len(_SECTION_SEPARATOR)]
            else:
                body = reference_context[body_start:]
            if body.strip():
                sections.append((header.group("name").strip(), _PreparedSource(body)))
        return sections

    def locate(self, excerpt_text: str) -> Optional[Tuple[str, str]]:
        """Return (reference filename, grounding status) for an excerpt, or None."""
        if not excerpt_text:
            return None
        candidates = [excerpt_text]
        stripped = _strip_decorations(excerpt_text)
        if stripped and stripped != excerpt_text:
            candidates.append(stripped)
        for candidate in candidates:
            if not candidate.strip():
                continue
            candidate_lower = candidate.lower()
            for filename, prepared in self._sections:
                if prepared.source_text.find(candidate) >= 0:
                    return filename, "exact"
                if prepared.source_lower.find(candidate_lower) >= 0:
                    return filename, "case_insensitive"
            for filename, prepared in self._sections:
                with self._normalize_lock:
                    prepared.get_normalized()
                _, _, status = self._grounder._search_stripped(
                    candidate, prepared.source_text, _prepared=prepared
                )
                if status != "not_found":
                    return filename, status
        return None

    def ground_unmatched(self, extraction_result: Dict[str, Any]) -> int:
        """Re-attribute ``not_found`` excerpts that appear in a reference document.

        Run after ``ExcerptGrounder.ground_all_excerpts`` on the same result.
        Modifies excerpts in place and returns how many were re-attributed.

        A re-attributed excerpt gets ``source`` set to the reference filename and
        a real ``grounding_status``. Its ``char_start``/``char_end`` are cleared:
        offsets into the reference would point at unrelated text if the viewer
        applied them to the source document.
        """
        if not self._sections:
            return 0
        count = 0
        for col_name, col_data in extraction_result.items():
            if col_name.startswith("_") or not isinstance(col_data, dict):
                continue
            for exc in col_data.get("excerpts") or []:
                if not isinstance(exc, dict) or exc.get("type") == "figure":
                    continue
                if exc.get("grounding_status") != "not_found":
                    continue
                found = self.locate(str(exc.get("text") or ""))
                if found is None:
                    continue
                filename, status = found
                exc["source"] = filename
                exc["grounding_status"] = status
                exc["char_start"] = None
                exc["char_end"] = None
                count += 1
        return count
