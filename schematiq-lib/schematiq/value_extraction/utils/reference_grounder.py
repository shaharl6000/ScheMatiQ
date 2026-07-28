"""Attribute extraction excerpts that came from an external reference document.

During value extraction the model is given the source document *and* — when the
user attached one — an external reference document (supplementary lookup material,
e.g. a table mapping each judge to the president who appointed them). The model
returns excerpts as verbatim quotes supporting each value, but it does not tell us
*which document* a given excerpt came from.

The default excerpt attribution stamps every excerpt with the source-document
filename (see ``PaperProcessor._attach_source_to_excerpts``). That is wrong for a
value the model pulled from the reference: the excerpt text lives in the reference,
not the source document, so highlighting it against the source document would fail.

This module re-attributes such excerpts. Given the combined reference-context blob
(the same text injected into the extraction prompt, whose per-document sections are
labelled ``--- Reference document: <filename> ---``), it:

- locates each excerpt in the reference text (exact/case-insensitive/normalised),
- when found, sets the excerpt's ``source`` to the owning reference filename, and
- for tabular references, narrows the excerpt ``text`` to the single matching row
  (keeping the header line) so the highlight in the reference viewer is precise.

It is deliberately conservative: an excerpt that cannot be located in the reference
text is left untouched, so source-document excerpts keep their existing attribution.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Section header emitted by ``build_reference_context`` for each attached
# reference document. Kept in sync with backend reference_context.py.
_SECTION_RE = re.compile(r"^--- Reference document: (?P<name>.+?) ---$")

# Below this folded length, a free-text excerpt is too generic to safely re-attribute
# to a reference on a bare substring match (e.g. a single name or number that also
# appears in the source document). Excerpts that align to a full tabular row are
# re-attributed regardless of length, because a whole row is distinctive.
_MIN_FREETEXT_MATCH_CHARS = 12

_NUMERIC_RE = re.compile(r"^[+-]?[\d,.]*\d[\d,.]*\s*%?$")


def _looks_numeric(cell: str) -> bool:
    """True if a table cell is numeric-ish (int/float/percent), ignoring currency."""
    cell = cell.strip().lstrip("$€£").strip()
    return bool(cell) and bool(_NUMERIC_RE.match(cell))


def _fold(text: str) -> str:
    """Normalise whitespace and common unicode punctuation for robust matching.

    Mirrors the spirit of the frontend highlight folding (unicode quotes/dashes,
    collapsed whitespace) so an excerpt located here matches what the viewer can
    later highlight. Deliberately lossy: used only for locating, never stored.
    """
    if not text:
        return ""
    folded = (
        text.replace("\u2018", "'").replace("\u2019", "'")
        .replace("\u201c", '"').replace("\u201d", '"')
        .replace("\u2013", "-").replace("\u2014", "-")
        .replace("\u00a0", " ")
    )
    return re.sub(r"\s+", " ", folded).strip().lower()


@dataclass
class _ReferenceSection:
    """One attached reference document's text, plus tabular metadata."""

    filename: str
    text: str
    folded: str = ""
    is_tabular: bool = False
    header_line: str = ""
    # Physical lines of the section body (for single-row narrowing), with their
    # folded form precomputed. Blank lines are dropped so row matching is stable.
    lines: List[str] = field(default_factory=list)
    folded_lines: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.folded = _fold(self.text)
        raw_lines = [ln for ln in self.text.splitlines() if ln.strip()]
        self.lines = raw_lines
        self.folded_lines = [_fold(ln) for ln in raw_lines]
        self._detect_tabular()

    def _detect_tabular(self) -> None:
        if len(self.lines) < 3:
            return
        for delim in (",", "\t", "|"):
            counts = [ln.count(delim) for ln in self.lines[:20]]
            if not counts or counts[0] < 1:
                continue
            consistent = sum(c == counts[0] for c in counts)
            if consistent < max(3, int(0.7 * len(counts))):
                continue
            self.is_tabular = True
            # Treat the first row as a header only if it looks like column labels
            # (no numeric cells) while a later row has a numeric cell in the same
            # position. Otherwise the file is headerless and prepending the first
            # data row would corrupt the highlight.
            if self._first_row_is_header(delim):
                self.header_line = self.lines[0].strip()
            return

    def _first_row_is_header(self, delim: str) -> bool:
        first = [c.strip() for c in self.lines[0].split(delim)]
        if not first or any(_looks_numeric(c) for c in first):
            return False
        data_rows = [
            [c.strip() for c in ln.split(delim)] for ln in self.lines[1:30]
        ]
        if not data_rows:
            return False
        # Signal 1: a data row has a numeric cell while the all-text first row does
        # not — strong evidence the first row is a label row.
        for cells in data_rows:
            if any(_looks_numeric(c) for c in cells):
                return True
        # Signal 2: all-text table. Treat the first row as a header only if its
        # cells do not recur as values in their own column (labels are distinct
        # from the data below them), which distinguishes a "name,country" header
        # from a headerless table whose first row is just more data.
        width = len(first)
        for col in range(width):
            column_values = {
                cells[col] for cells in data_rows if len(cells) > col
            }
            if first[col] in column_values:
                return False
        return True


def _parse_sections(reference_context: str) -> List[_ReferenceSection]:
    """Split the combined reference blob into per-document sections.

    ``build_reference_context`` joins documents as::

        --- Reference document: a.csv ---
        <text of a>

        --- Reference document: b.txt ---
        <text of b>

    If no headers are present (unexpected, but be defensive) the whole blob is
    treated as a single unnamed section.
    """
    if not reference_context or not reference_context.strip():
        return []

    sections: List[Tuple[str, List[str]]] = []
    current_name: Optional[str] = None
    current_lines: List[str] = []
    for line in reference_context.splitlines():
        m = _SECTION_RE.match(line.strip())
        if m:
            if current_name is not None or current_lines:
                sections.append((current_name or "reference", current_lines))
            current_name = m.group("name").strip()
            current_lines = []
        else:
            current_lines.append(line)
    if current_name is not None or current_lines:
        sections.append((current_name or "reference", current_lines))

    result: List[_ReferenceSection] = []
    for name, lines in sections:
        text = "\n".join(lines).strip()
        if text:
            result.append(_ReferenceSection(filename=name, text=text))
    return result


class ReferenceGrounder:
    """Re-attributes reference-derived excerpts to the reference they came from.

    Build once from the combined reference-context blob, then call
    :meth:`reattribute` on each extraction result to fix up its excerpts in place.
    Cheap to build; matching is string search over the (small, per-document)
    reference text.
    """

    def __init__(self, reference_context: Optional[str]):
        self._sections = _parse_sections(reference_context or "")

    @property
    def active(self) -> bool:
        return bool(self._sections)

    def _match_row(self, section: _ReferenceSection, folded_excerpt: str) -> Optional[str]:
        """Return the single tabular row (header + row) containing the excerpt.

        Returns None if the excerpt does not map cleanly to one row, so the caller
        keeps the model's original excerpt text.
        """
    def _match_row(self, section: _ReferenceSection, folded_excerpt: str) -> Optional[str]:
        """Return the single tabular row (header + row) the excerpt maps to.

        A row matches when its folded form is contained in the folded excerpt, or
        the excerpt is contained in the folded row (the model may quote one cell of
        a row). Narrowing happens only when *exactly one* data row matches — a
        multi-row excerpt is left whole so no content is silently dropped.
        """
        if not section.is_tabular:
            return None
        folded_header = _fold(section.header_line) if section.header_line else ""
        matches: List[str] = []
        for raw, folded in zip(section.lines, section.folded_lines):
            if not folded or (folded_header and folded == folded_header):
                continue
            row_in_excerpt = folded in folded_excerpt
            excerpt_in_row = folded_excerpt in folded
            # Row fully quoted in the excerpt -> distinctive, always a match.
            # Excerpt is only a fragment of the row -> require it to be long enough
            # to be distinctive (a bare cell value likely also occurs in the source
            # document, so a short fragment must not flip attribution).
            if row_in_excerpt or (
                excerpt_in_row and len(folded_excerpt) >= _MIN_FREETEXT_MATCH_CHARS
            ):
                matches.append(raw.strip())
                if len(matches) > 1:
                    return None  # ambiguous / spans multiple rows -> don't narrow
        if len(matches) != 1:
            return None
        row = matches[0]
        if section.header_line and section.header_line != row:
            return f"{section.header_line}\n{row}"
        return row

    def _locate(self, excerpt_text: str) -> Optional[Tuple[_ReferenceSection, Optional[str]]]:
        """Find the reference section an excerpt came from.

        Returns ``(section, narrowed_text_or_None)``. Narrowed text is set only for
        a single-row tabular match. For free text, the excerpt must be locatable in
        the reference *and* distinctive enough (see ``_MIN_FREETEXT_MATCH_CHARS``)
        to avoid flipping a short value that also occurs in the source document.
        Returns None when the excerpt is not confidently from a reference.
        """
        folded = _fold(excerpt_text)
        if not folded:
            return None
        for section in self._sections:
            if folded not in section.folded:
                continue
            narrowed = self._match_row(section, folded)
            if narrowed is not None:
                return section, narrowed
            # No single-row narrowing: accept as a free-text reference match only
            # if the excerpt is long enough to be distinctive.
            if len(folded) >= _MIN_FREETEXT_MATCH_CHARS:
                return section, None
        return None

    def reattribute(self, extraction_result: Dict[str, Any]) -> None:
        """Fix excerpt attribution in an extraction result, in place.

        For each excerpt whose text is located in a reference document, set its
        ``source`` to that reference's filename and, for tabular references,
        narrow its ``text`` to the matching row. Excerpts not found in any
        reference are left untouched (they belong to the source document).
        """
        if not self._sections:
            return
        for col_name, col_value in extraction_result.items():
            if col_name.startswith("_"):
                continue
            if not isinstance(col_value, dict):
                continue
            excerpts = col_value.get("excerpts")
            if not isinstance(excerpts, list):
                continue
            for exc in excerpts:
                if not isinstance(exc, dict):
                    continue
                text = exc.get("text")
                if not isinstance(text, str) or not text.strip():
                    continue
                located = self._locate(text)
                if located is None:
                    continue
                section, narrowed = located
                exc["source"] = section.filename
                if narrowed:
                    exc["text"] = narrowed
