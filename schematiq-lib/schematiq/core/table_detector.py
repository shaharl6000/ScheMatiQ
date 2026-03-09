"""Detect and format tables in plain-text documents."""

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class DetectedTable:
    """A table region detected in plain text."""

    start_line: int
    end_line: int
    caption: Optional[str]
    raw_text: str


class TableDetector:
    """Find table regions in plain text and convert them to Markdown."""

    # Caption patterns: "Table 1:", "Table 1.", "Table 1 -", etc.
    CAPTION_RE = re.compile(r"^\s*(Table\s+\d+[\.\:\s\-].*)", re.IGNORECASE)

    # Minimum requirements for a valid table region
    MIN_TABLE_LINES = 3
    MIN_COLUMNS = 2

    def detect_tables(self, text: str) -> List[DetectedTable]:
        """Find table regions in plain text.

        Detection heuristics:
        1. Locate lines matching ``Table N:`` / ``Table N.`` captions.
        2. Scan forward for a tabular body (pipe delimiters, tabs, or
           consistent multi-space column alignment).
        3. Body ends at a blank line followed by non-tabular text, a
           section header, or the next table caption.
        4. Validate: region must have >= 3 lines with >= 2 columns.
        """
        lines = text.split("\n")
        tables: List[DetectedTable] = []
        i = 0

        while i < len(lines):
            caption_match = self.CAPTION_RE.match(lines[i])
            if caption_match:
                caption = caption_match.group(1).strip()
                body_start = i + 1

                # Skip blank lines between caption and body
                while body_start < len(lines) and not lines[body_start].strip():
                    body_start += 1

                body_end = self._find_table_end(lines, body_start)

                if body_end > body_start:
                    body_lines = lines[body_start:body_end]
                    if self._validate_table(body_lines):
                        raw = "\n".join(lines[i:body_end])
                        tables.append(
                            DetectedTable(
                                start_line=i,
                                end_line=body_end,
                                caption=caption,
                                raw_text=raw,
                            )
                        )
                        i = body_end
                        continue
            i += 1

        return tables

    def format_as_markdown(self, table: DetectedTable) -> str:
        """Convert a detected table region to a clean Markdown table."""
        # Extract body lines (skip caption)
        all_lines = table.raw_text.split("\n")
        body_lines = []
        past_caption = False
        for line in all_lines:
            if not past_caption:
                if self.CAPTION_RE.match(line) or not line.strip():
                    continue
                past_caption = True
            if past_caption and line.strip():
                body_lines.append(line)

        if not body_lines:
            return table.raw_text

        # Parse rows into cells
        rows = [self._split_row(line) for line in body_lines]
        if not rows:
            return table.raw_text

        # Normalise column count to the max detected
        max_cols = max(len(r) for r in rows)
        if max_cols < self.MIN_COLUMNS:
            return table.raw_text

        rows = [r + [""] * (max_cols - len(r)) for r in rows]

        # Build markdown
        parts: List[str] = []
        if table.caption:
            parts.append(f"**{table.caption}**")
            parts.append("")

        # Header row
        header = "| " + " | ".join(cell.strip() for cell in rows[0]) + " |"
        separator = "| " + " | ".join("---" for _ in rows[0]) + " |"
        parts.append(header)
        parts.append(separator)

        for row in rows[1:]:
            parts.append("| " + " | ".join(cell.strip() for cell in row) + " |")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_tabular_line(self, line: str) -> bool:
        """Return True if *line* looks like part of a table body."""
        stripped = line.strip()
        if not stripped:
            return False
        # Pipe-delimited
        if "|" in stripped:
            return True
        # Tab-delimited (2+ tabs)
        if stripped.count("\t") >= 1:
            return True
        # Multi-space alignment (2+ runs of 2+ spaces between non-space)
        if len(re.findall(r"\S\s{2,}\S", stripped)) >= 1:
            return True
        return False

    def _find_table_end(self, lines: List[str], start: int) -> int:
        """Find where the table body ends."""
        end = start
        blank_run = 0

        for j in range(start, len(lines)):
            line = lines[j]

            # Another table caption → stop
            if j > start and self.CAPTION_RE.match(line):
                break

            # Section header heuristic (e.g. "3. Methods", all-caps heading)
            stripped = line.strip()
            if stripped and not self._is_tabular_line(line):
                if re.match(r"^\d+\.\s+[A-Z]", stripped):
                    break
                if stripped.isupper() and len(stripped) > 3:
                    break

            if not stripped:
                blank_run += 1
                if blank_run >= 2:
                    break
                continue
            else:
                blank_run = 0

            if self._is_tabular_line(line):
                end = j + 1
            else:
                # Non-tabular, non-blank → end of table
                break

        return end

    def _validate_table(self, body_lines: List[str]) -> bool:
        """Check minimum size: >= 3 tabular lines with >= 2 columns."""
        tabular = [l for l in body_lines if self._is_tabular_line(l)]
        if len(tabular) < self.MIN_TABLE_LINES:
            return False
        # Check column count on first tabular line
        cols = self._split_row(tabular[0])
        return len(cols) >= self.MIN_COLUMNS

    def _split_row(self, line: str) -> List[str]:
        """Split a table row into cells, auto-detecting the delimiter."""
        stripped = line.strip()

        # Pipe-delimited
        if "|" in stripped:
            # Remove leading/trailing pipes and split
            stripped = stripped.strip("|")
            return [c.strip() for c in stripped.split("|")]

        # Tab-delimited
        if "\t" in stripped:
            return [c.strip() for c in stripped.split("\t")]

        # Multi-space delimited (2+ spaces)
        cells = re.split(r"\s{2,}", stripped)
        return cells
