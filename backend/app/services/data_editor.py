"""
Service for editing individual cells in data tables.
Handles updates to JSONL data files for both load and ScheMatiQ sessions.
"""

import copy
import json
import logging
from pathlib import Path
from typing import Any, Optional

from app.services.data_utils import (
    resolve_session_data_files,
    persist_session_data_file,
    rename_column_keys_in_row,
)

logger = logging.getLogger(__name__)


class DataEditor:
    """Handles cell-level data updates in JSONL data files."""

    def __init__(self, work_dir: str = "./schematiq_work", data_dir: str = "./data"):
        self.work_dir = Path(work_dir)
        self.data_dir = Path(data_dir)

    def _candidate_data_files(self, session_id: str) -> list[Path]:
        """All data JSONL paths that may exist for a session (before hydration)."""
        return [
            self.work_dir / session_id / "extracted_data.jsonl",
            self.work_dir / session_id / "data.jsonl",
            self.data_dir / session_id / "data.jsonl",
        ]

    async def _resolve_session_data_files(self, session_id: str) -> list[Path]:
        """Return existing local data files, hydrating from storage when needed."""
        return await resolve_session_data_files(
            session_id,
            work_dir=self.work_dir,
            data_dir=self.data_dir,
        )

    def _find_data_file(self, session_id: str) -> Optional[Path]:
        """
        Find the primary data file for a session (first match by priority).

        Used by single-file operations such as cell edits.
        """
        for path in self._candidate_data_files(session_id):
            if path.exists():
                return path
        return None

    async def update_cell(
        self, session_id: str, row_name: str, column: str, value: Any,
        restore: Any = None, source_document: str = None,
        row_index: Optional[int] = None, reference_source: Optional[str] = None,
    ) -> dict:
        """
        Update a specific cell value in the session's data file.

        Args:
            session_id: The session identifier
            row_name: The row_name field to identify the row
            column: The column name to update
            value: The new value for the cell
            source_document: Optional source document to disambiguate rows with the same name
            row_index: Optional absolute non-blank line position, used as a fallback
                identity when row_name is absent (e.g. generic CSV/JSON imports)

        Returns:
            dict with status and details

        Raises:
            FileNotFoundError: If no data file exists for the session
            ValueError: If the row is not found
        """
        # Resolve the *same* set of data files the reader (`get_data`) sees, in
        # the same priority order. Historically this method wrote to a single
        # file chosen by its own relative-path candidate list, which could
        # diverge from what `get_data` reads (e.g. dev.sh `.dev-data/instance-*`
        # work dirs, or a row duplicated across `extracted_data.jsonl` and the
        # additional-documents `data/data.jsonl`). When they diverged the edit
        # persisted to a file the reader ignored, so the value reverted on the
        # next refresh. Updating every file that contains the row keeps writer
        # and reader consistent and prevents a stale duplicate from winning the
        # reader's first-occurrence dedup.
        data_files = await self._resolve_session_data_files(session_id)
        if not data_files:
            single = self._find_data_file(session_id)
            if single:
                data_files = [single]
        if not data_files:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        from app.services.data_utils import _resolve_source_document

        match_by_index = (not row_name) and row_index is not None

        updated_any = False
        previous_value = None
        persisted_files: list = []

        for file_position, data_file in enumerate(data_files):
            # Read all rows for this file
            rows = []
            try:
                with open(data_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            rows.append(json.loads(line))
            except FileNotFoundError:
                continue

            file_updated = False
            for idx, row in enumerate(rows):
                if match_by_index:
                    # row_index is an absolute position in the reader's merged,
                    # deduped view, not a per-file line number. Only the first
                    # resolved file is authoritative for the index fallback
                    # (used for name-less generic imports, effectively
                    # single-file); do not attempt it against later files.
                    if file_position != 0 or idx != row_index:
                        continue
                else:
                    current_row_name = row.get("row_name") or row.get("_row_name")
                    if current_row_name != row_name:
                        continue
                    if source_document:
                        current_src = _resolve_source_document(row)
                        if current_src and current_src != source_document:
                            continue

                row_previous = self._apply_cell_update(
                    row, column, value, restore=restore,
                    reference_source=reference_source,
                )
                # Preserve the previous value from the first file that matched
                # (the reader's authoritative first-occurrence).
                if not updated_any:
                    previous_value = row_previous
                file_updated = True
                updated_any = True
                break

            if file_updated:
                with open(data_file, "w", encoding="utf-8") as f:
                    for row in rows:
                        f.write(json.dumps(row, ensure_ascii=False) + "\n")
                persisted_files.append(data_file)

            # Index-based match targets a single file only; stop after the first.
            if match_by_index and file_position == 0:
                break

        if not updated_any:
            if match_by_index:
                raise ValueError(f"Row at index {row_index} not found")
            raise ValueError(f"Row with row_name '{row_name}' not found")

        for data_file in persisted_files:
            await persist_session_data_file(session_id, data_file)

        return {
            "status": "success",
            "session_id": session_id,
            "row_name": row_name,
            "column": column,
            "value": value,
            "previous_value": previous_value,
        }

    def _apply_cell_update(
        self, row: dict, column: str, value: Any,
        restore: Any = None, reference_source: Optional[str] = None,
    ) -> Any:
        """Mutate a single row's cell in place and return its previous value.

        Handles both the nested ``data`` dict shape and the flat runtime JSONL
        shape, preserving the ScheMatiQ ``answer``/``excerpts`` cell object.
        """
        previous_value = None
        if "data" in row and isinstance(row["data"], dict):
            previous_value = copy.deepcopy(row["data"].get(column))
            if restore is not None:
                row["data"][column] = restore
            elif column in row["data"]:
                cell_value = row["data"][column]
                if isinstance(cell_value, dict) and "answer" in cell_value:
                    cell_value["answer"] = value
                    cell_value["excerpts"] = []
                    cell_value["manually_edited"] = True
                else:
                    row["data"][column] = {
                        "answer": value,
                        "excerpts": [],
                        "manually_edited": True,
                    }
            else:
                row["data"][column] = {
                    "answer": value,
                    "excerpts": [],
                    "manually_edited": True,
                }
        else:
            previous_value = copy.deepcopy(row.get(column))
            if restore is not None:
                row[column] = restore
            elif column in row and isinstance(row[column], dict) and "answer" in row[column]:
                row[column]["answer"] = value
                row[column]["excerpts"] = []
                row[column]["manually_edited"] = True
            else:
                row[column] = {
                    "answer": value,
                    "excerpts": [],
                    "manually_edited": True,
                }

        # Provenance: when the value came from an attached reference document,
        # mark the cell as externally sourced (reuses the external_source style).
        if reference_source and restore is None:
            self._mark_external_source(row, column, reference_source)

        return previous_value

    @staticmethod
    def _mark_external_source(row: dict, column: str, source_name: str) -> None:
        """Flag a cell as sourced from an external reference document.

        Sets ``_cell_status[column] = "external_source"`` (rendered distinctly in
        the table) and, when the cell holds an answer/excerpts object, attaches an
        excerpt attributing the value to ``source_name`` so the source is shown.
        """
        if "data" in row and isinstance(row["data"], dict):
            cell = row["data"].get(column)
        else:
            cell = row.get(column)
        if isinstance(cell, dict):
            cell["excerpts"] = [
                {
                    "text": f"Value taken from reference document '{source_name}'.",
                    "source": source_name,
                }
            ]
        # Rows may carry an explicit ``_cell_status: null`` (the field defaults to
        # None on the row model), so setdefault is unsafe here — it would return
        # None and the item assignment below would raise.
        status = row.get("_cell_status")
        if not isinstance(status, dict):
            status = {}
            row["_cell_status"] = status
        status[column] = "external_source"

    async def rename_column(
        self, session_id: str, old_name: str, new_name: str
    ) -> dict:
        """
        Rename a column key in all rows across every session data file.

        Args:
            session_id: The session identifier
            old_name: The current column name
            new_name: The new column name

        Returns:
            dict with status and count of updated rows

        Raises:
            FileNotFoundError: If no data file exists for the session
        """
        data_files = await self._resolve_session_data_files(session_id)
        if not data_files:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        files_updated = 0
        total_rows_updated = 0

        for data_file in data_files:
            rows = []
            with open(data_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        rows.append(json.loads(line))

            file_rows_updated = 0
            for row in rows:
                if rename_column_keys_in_row(row, old_name, new_name):
                    file_rows_updated += 1

            with open(data_file, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")

            await persist_session_data_file(session_id, data_file)
            files_updated += 1
            total_rows_updated += file_rows_updated

        if files_updated == 0:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        return {
            "status": "success",
            "session_id": session_id,
            "old_name": old_name,
            "new_name": new_name,
            "files_updated": files_updated,
            "rows_updated": total_rows_updated,
        }
