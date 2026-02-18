"""
Service for managing observation units.
Handles adding, removing, and listing observation units (rows) in extraction results.
"""

import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

from app.models.session import VisualizationSession
from app.services.session_manager import SessionManager
from app.services.websocket_manager import WebSocketManager


class ObservationUnitManager:
    """Manager for observation unit operations."""

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        self.websocket_manager = websocket_manager
        self.session_manager = session_manager

    def get_observation_units(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Get all observation units for a session.

        Args:
            session_id: Session identifier

        Returns:
            List of observation unit dictionaries
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            return []

        # Get observation units from session metadata
        if session.metadata and hasattr(session.metadata, 'extracted_schema'):
            extracted_schema = session.metadata.extracted_schema or {}
            obs_unit = extracted_schema.get('observation_unit', {})

            # Get the actual instances from the data
            units = self._get_units_from_data(session_id)
            return units

        return []

    def _get_units_from_data(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Extract observation units from the actual data rows.

        Args:
            session_id: Session identifier

        Returns:
            List of observation unit dictionaries with metadata
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            return []

        units = []

        # Check schematiq_work directory first (primary location for extracted data)
        schematiq_work_dir = Path("./schematiq_work") / session_id
        data_dir = Path("./data") / session_id

        # Try schematiq_work/extracted_data.jsonl first, then data/data.jsonl
        jsonl_file = schematiq_work_dir / "extracted_data.jsonl"
        if not jsonl_file.exists():
            jsonl_file = data_dir / "data.jsonl"

        if jsonl_file.exists():
            with open(jsonl_file, 'r') as f:
                for line in f:
                    if line.strip():
                        try:
                            row = json.loads(line)
                            # Get the observation unit name (check _unit_name first, then fallbacks)
                            unit_name = (
                                row.get('_unit_name') or
                                row.get('unit_name') or
                                row.get('observation_unit') or
                                row.get('name') or
                                row.get(list(row.keys())[0] if row else '')
                            )
                            if unit_name:
                                units.append({
                                    "unit_name": unit_name,
                                    "document_id": row.get('document_id', row.get('source_document')),
                                    "confidence": row.get('confidence', 1.0)
                                })
                        except json.JSONDecodeError:
                            continue

        # Alternative: check CSV data file
        csv_file = data_dir / "data.csv"
        if not units and csv_file.exists():
            import csv
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Get the first column as the observation unit name
                    first_col = list(row.keys())[0] if row else None
                    if first_col:
                        units.append({
                            "unit_name": row.get(first_col, ''),
                            "document_id": row.get('document_id', row.get('source_document')),
                            "confidence": 1.0
                        })

        return units

    async def remove_observation_unit(
        self,
        session_id: str,
        unit_name: str
    ) -> Dict[str, Any]:
        """
        Remove an observation unit from the session.

        Args:
            session_id: Session identifier
            unit_name: Name of unit to remove

        Returns:
            Dict with updated observation_units and row_count

        Raises:
            ValueError: If unit doesn't exist
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Check schematiq_work directory first (primary location for extracted data)
        schematiq_work_dir = Path("./schematiq_work") / session_id
        data_dir = Path("./data") / session_id
        removed = False
        updated_rows = []

        # Try schematiq_work/extracted_data.jsonl first, then data/data.jsonl
        jsonl_file = schematiq_work_dir / "extracted_data.jsonl"
        if not jsonl_file.exists():
            jsonl_file = data_dir / "data.jsonl"

        if jsonl_file.exists():
            rows = []
            with open(jsonl_file, 'r') as f:
                for line in f:
                    if line.strip():
                        try:
                            row = json.loads(line)
                            row_unit = (
                                row.get('_unit_name') or
                                row.get('unit_name') or
                                row.get('observation_unit') or
                                row.get('name') or
                                row.get(list(row.keys())[0] if row else '')
                            )
                            if row_unit != unit_name:
                                rows.append(row)
                            else:
                                removed = True
                        except json.JSONDecodeError:
                            continue

            if removed:
                with open(jsonl_file, 'w') as f:
                    for row in rows:
                        f.write(json.dumps(row) + '\n')
                updated_rows = rows

        # Also check/remove from CSV file
        csv_file = data_dir / "data.csv"
        if csv_file.exists():
            import csv
            rows = []
            fieldnames = None

            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    first_col = list(row.keys())[0] if row else None
                    if first_col and row.get(first_col) != unit_name:
                        rows.append(row)
                    elif first_col and row.get(first_col) == unit_name:
                        removed = True

            if removed and fieldnames:
                with open(csv_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

        if not removed:
            raise ValueError(f"Observation unit '{unit_name}' not found")

        # Update session metadata
        session.metadata.last_modified = datetime.now()
        if session.statistics:
            session.statistics.total_rows = len(updated_rows) if updated_rows else session.statistics.total_rows - 1
        self.session_manager.update_session(session)

        # Broadcast update via WebSocket
        await self.websocket_manager.broadcast_to_session(session_id, {
            "type": "data_updated",
            "operation": "remove_observation_unit",
            "unit_name": unit_name,
            "row_count": len(updated_rows) if updated_rows else 0
        })

        return {
            "observation_units": self.get_observation_units(session_id),
            "row_count": len(updated_rows) if updated_rows else 0
        }

    async def add_observation_unit(
        self,
        session_id: str,
        unit_name: str,
        document_id: Optional[str],
        relevant_passages: List[str],
        confidence: float
    ) -> Dict[str, Any]:
        """
        Add a new observation unit to the session.

        Args:
            session_id: Session identifier
            unit_name: Name of the new unit
            document_id: Optional document ID
            relevant_passages: Relevant text passages
            confidence: Confidence score (0.0-1.0)

        Returns:
            Dict with updated observation_units and row_count

        Raises:
            ValueError: If unit already exists
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        data_dir = Path("./data") / session_id
        data_dir.mkdir(parents=True, exist_ok=True)

        # Check if unit already exists
        existing_units = self.get_observation_units(session_id)
        existing_names = [u.get("unit_name") for u in existing_units]
        if unit_name in existing_names:
            raise ValueError(f"Observation unit '{unit_name}' already exists")

        # Create new row with empty values for all columns
        new_row = {
            "observation_unit": unit_name,
            "name": unit_name,  # Also set as 'name' for compatibility
        }

        # Add document_id if provided
        if document_id:
            new_row["document_id"] = document_id
            new_row["source_document"] = document_id

        # Get column names from session and initialize empty values
        for col in session.columns:
            if col.name and col.name not in new_row:
                new_row[col.name] = ""

        row_count = len(existing_units) + 1

        # Add to JSONL file
        jsonl_file = data_dir / "data.jsonl"
        with open(jsonl_file, 'a') as f:
            f.write(json.dumps(new_row) + '\n')

        # Also update CSV if it exists
        csv_file = data_dir / "data.csv"
        if csv_file.exists():
            import csv

            # Read existing data to get fieldnames
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or list(new_row.keys())

            # Ensure new row has all fields
            for field in fieldnames:
                if field not in new_row:
                    new_row[field] = ""

            # Append new row
            with open(csv_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(new_row)

        # Update session metadata
        session.metadata.last_modified = datetime.now()
        if session.statistics:
            session.statistics.total_rows = row_count
        self.session_manager.update_session(session)

        # Broadcast update via WebSocket
        await self.websocket_manager.broadcast_to_session(session_id, {
            "type": "data_updated",
            "operation": "add_observation_unit",
            "unit_name": unit_name,
            "row_count": row_count
        })

        return {
            "observation_units": self.get_observation_units(session_id),
            "row_count": row_count
        }

    async def update_observation_unit_definition(
        self,
        session_id: str,
        name: str,
        definition: str,
        example_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Update the observation unit definition (schema-level concept).

        This updates what constitutes a single row, not the individual instances.

        Args:
            session_id: Session identifier
            name: Name of the observation unit type (e.g., "Protein", "Model-Benchmark Evaluation")
            definition: Definition of what constitutes one row
            example_names: Optional list of example names

        Returns:
            Dict with updated observation_unit

        Raises:
            ValueError: If session not found
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Import ObservationUnitInfo here to avoid circular imports
        from app.models.session import ObservationUnitInfo

        # Get current observation unit or create a new one
        old_observation_unit = session.observation_unit

        # Create updated observation unit, preserving read-only fields
        updated_observation_unit = ObservationUnitInfo(
            name=name,
            definition=definition,
            example_names=example_names,
            source_document=old_observation_unit.source_document if old_observation_unit else None,
            discovery_iteration=old_observation_unit.discovery_iteration if old_observation_unit else None
        )

        # Update session
        session.observation_unit = updated_observation_unit
        session.metadata.last_modified = datetime.now()
        self.session_manager.update_session(session)

        # Update schema JSON file on disk
        self._update_schema_file(session_id, updated_observation_unit)

        # Broadcast update via WebSocket
        await self.websocket_manager.broadcast_to_session(session_id, {
            "type": "observation_unit_definition_updated",
            "observation_unit": {
                "name": updated_observation_unit.name,
                "definition": updated_observation_unit.definition,
                "example_names": updated_observation_unit.example_names,
                "source_document": updated_observation_unit.source_document,
                "discovery_iteration": updated_observation_unit.discovery_iteration
            }
        })

        return {
            "observation_unit": {
                "name": updated_observation_unit.name,
                "definition": updated_observation_unit.definition,
                "example_names": updated_observation_unit.example_names,
                "source_document": updated_observation_unit.source_document,
                "discovery_iteration": updated_observation_unit.discovery_iteration
            }
        }

    def _update_schema_file(self, session_id: str, observation_unit) -> None:
        """
        Update the observation unit in the schema JSON file.

        Args:
            session_id: Session identifier
            observation_unit: Updated ObservationUnitInfo
        """
        schematiq_work_dir = Path("./schematiq_work") / session_id
        schema_file = schematiq_work_dir / "discovered_schema.json"

        if schema_file.exists():
            try:
                with open(schema_file, 'r') as f:
                    schema_data = json.load(f)

                # Update observation unit in schema
                schema_data['observation_unit'] = {
                    'name': observation_unit.name,
                    'definition': observation_unit.definition,
                    'example_names': observation_unit.example_names,
                    'source_document': observation_unit.source_document,
                    'discovery_iteration': observation_unit.discovery_iteration
                }

                with open(schema_file, 'w') as f:
                    json.dump(schema_data, f, indent=2)

            except (json.JSONDecodeError, IOError) as e:
                # Log error but don't fail the operation
                logger.warning("Failed to update schema file: %s", e)
