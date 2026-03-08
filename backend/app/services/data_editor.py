"""
Service for editing individual cells in data tables.
Handles updates to JSONL data files for both load and ScheMatiQ sessions.
"""

import copy
import json
from pathlib import Path
from typing import Any, Optional


class DataEditor:
    """Handles cell-level data updates in JSONL data files."""

    def __init__(self, work_dir: str = "./schematiq_work", data_dir: str = "./data"):
        self.work_dir = Path(work_dir)
        self.data_dir = Path(data_dir)

    def _find_data_file(self, session_id: str) -> Optional[Path]:
        """
        Find the data file for a session.
        Checks multiple locations in priority order:
        1. schematiq_work/{session_id}/extracted_data.jsonl
        2. schematiq_work/{session_id}/data.jsonl
        3. data/{session_id}/data.jsonl
        """
        candidates = [
            self.work_dir / session_id / "extracted_data.jsonl",
            self.work_dir / session_id / "data.jsonl",
            self.data_dir / session_id / "data.jsonl",
        ]
        for path in candidates:
            if path.exists():
                return path
        return None

    async def update_cell(
        self, session_id: str, row_name: str, column: str, value: Any,
        restore: Any = None
    ) -> dict:
        """
        Update a specific cell value in the session's data file.

        Args:
            session_id: The session identifier
            row_name: The row_name field to identify the row
            column: The column name to update
            value: The new value for the cell

        Returns:
            dict with status and details

        Raises:
            FileNotFoundError: If no data file exists for the session
            ValueError: If the row is not found
        """
        data_file = self._find_data_file(session_id)
        if not data_file:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        # Read all rows
        rows = []
        with open(data_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))

        # Find and update the target row
        updated = False
        previous_value = None
        for row in rows:
            current_row_name = row.get("row_name") or row.get("_row_name")
            if current_row_name == row_name:
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
                    # Old flat format
                    previous_value = copy.deepcopy(row.get(column))
                    row[column] = value if restore is None else restore
                updated = True
                break

        if not updated:
            raise ValueError(f"Row with row_name '{row_name}' not found")

        # Write back all rows
        with open(data_file, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

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
        Rename a column key in all rows of the session's data file.

        Args:
            session_id: The session identifier
            old_name: The current column name
            new_name: The new column name

        Returns:
            dict with status and count of updated rows

        Raises:
            FileNotFoundError: If no data file exists for the session
        """
        data_file = self._find_data_file(session_id)
        if not data_file:
            raise FileNotFoundError(f"No data file found for session {session_id}")

        rows = []
        with open(data_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))

        updated_count = 0
        for row in rows:
            if "data" in row and isinstance(row["data"], dict):
                if old_name in row["data"]:
                    row["data"][new_name] = row["data"].pop(old_name)
                    updated_count += 1
                # Also rename the excerpt column if it exists
                old_excerpt = f"{old_name}_excerpt"
                new_excerpt = f"{new_name}_excerpt"
                if old_excerpt in row["data"]:
                    row["data"][new_excerpt] = row["data"].pop(old_excerpt)
            elif old_name in row:
                row[new_name] = row.pop(old_name)
                updated_count += 1

        with open(data_file, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        return {
            "status": "success",
            "session_id": session_id,
            "old_name": old_name,
            "new_name": new_name,
            "rows_updated": updated_count,
        }

