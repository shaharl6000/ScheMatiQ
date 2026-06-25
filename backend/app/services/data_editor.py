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
    remove_column_keys_in_row,
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
        row_index: Optional[int] = None,
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
        data_file = self._find_data_file(session_id)
        if not data_file:
            hydrated = await self._resolve_session_data_files(session_id)
            data_file = hydrated[0] if hydrated else None
        if not data_file:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        # Read all rows
        rows = []
        with open(data_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))

        # Find and update the target row
        from app.services.data_utils import _resolve_source_document

        match_by_index = (not row_name) and row_index is not None

        updated = False
        previous_value = None
        for idx, row in enumerate(rows):
            if match_by_index:
                if idx != row_index:
                    continue
            else:
                current_row_name = row.get("row_name") or row.get("_row_name")
                if current_row_name != row_name:
                    continue
                if source_document:
                    current_src = _resolve_source_document(row)
                    if current_src and current_src != source_document:
                        continue
            # Matched row:
            # Update the cell value
            if "data" in row and isinstance(row["data"], dict):
                # Capture previous value for undo support
                previous_value = copy.deepcopy(row["data"].get(column))

                if restore is not None:
                    # Full restore (undo): replace entire cell object
                    row["data"][column] = restore
                elif column in row["data"]:
                    cell_value = row["data"][column]
                    # Handle ScheMatiQ answer format
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
                # Flat runtime JSONL format — preserve answer/excerpts dict shape
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
            updated = True
            break

        if not updated:
            if match_by_index:
                raise ValueError(f"Row at index {row_index} not found")
            raise ValueError(f"Row with row_name '{row_name}' not found")

        # Write back all rows
        with open(data_file, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        await persist_session_data_file(session_id, data_file)

        return {
            "status": "success",
            "session_id": session_id,
            "row_name": row_name,
            "column": column,
            "value": value,
            "previous_value": previous_value,
        }

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
