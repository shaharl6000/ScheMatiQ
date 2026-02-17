"""
Service for editing individual cells in data tables.
Handles updates to JSONL data files for both load and ScheMatiQ sessions.
"""

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
        self, session_id: str, row_name: str, column: str, value: Any
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
        for row in rows:
            current_row_name = row.get("row_name") or row.get("_row_name")
            if current_row_name == row_name:
                # Update the cell value
                if "data" in row and isinstance(row["data"], dict):
                    # New format with nested 'data' key
                    if column in row["data"]:
                        cell_value = row["data"][column]
                        # Handle ScheMatiQ answer format
                        if isinstance(cell_value, dict) and "answer" in cell_value:
                            cell_value["answer"] = value
                        else:
                            row["data"][column] = value
                    else:
                        row["data"][column] = value
                else:
                    # Old flat format
                    row[column] = value
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
        }

