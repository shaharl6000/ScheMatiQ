"""
Schema editing API endpoints for QBSD visualization.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
import json
import asyncio
from pathlib import Path

from app.models.session import VisualizationSession, SessionStatus, ColumnInfo
from app.services.session_manager import SessionManager
from app.services.websocket_manager import WebSocketManager
from app.services.schema_manager import SchemaManager
from app.services import session_manager, websocket_manager

router = APIRouter(tags=["schema"])

# Create schema manager instance
schema_manager = SchemaManager(websocket_manager, session_manager)

# Request/Response Models
class ColumnEditRequest(BaseModel):
    old_name: str
    new_name: Optional[str] = None
    definition: Optional[str] = None
    rationale: Optional[str] = None
    allowed_values: Optional[List[str]] = None  # Closed set of valid values
    reprocess: bool = True

class ColumnAddRequest(BaseModel):
    name: str
    definition: str
    rationale: str
    allowed_values: Optional[List[str]] = None  # Closed set of valid values
    documents_path: Optional[str] = None
    data_type: str = "text"

class ColumnMergeRequest(BaseModel):
    source_columns: List[str]
    target_column: str
    merge_strategy: str = "CONCATENATE"  # CONCATENATE, COMBINE_UNIQUE, TAKE_FIRST, TAKE_LONGEST
    new_definition: Optional[str] = None
    separator: str = " | "

class ReprocessRequest(BaseModel):
    columns: Optional[List[str]] = None
    force_reprocess: bool = False
    incremental: bool = True

class SchemaValidationResponse(BaseModel):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    column_count: int
    missing_definitions: List[str]

class ReprocessingStatusResponse(BaseModel):
    is_running: bool
    progress: float
    current_column: Optional[str] = None
    columns_processed: int
    total_columns: int
    estimated_time_remaining: Optional[int] = None
    start_time: Optional[str] = None

@router.put("/edit-column/{session_id}")
async def edit_column(
    session_id: str, 
    edit_request: ColumnEditRequest,
    background_tasks: BackgroundTasks
):
    """Edit a column's properties and optionally reprocess documents."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Find the column to edit
        column_found = False
        for col in session.columns:
            if col.name == edit_request.old_name:
                # Update column properties
                if edit_request.new_name:
                    col.name = edit_request.new_name
                if edit_request.definition is not None:
                    col.definition = edit_request.definition
                if edit_request.rationale is not None:
                    col.rationale = edit_request.rationale
                if edit_request.allowed_values is not None:
                    # Empty list means clear allowed_values, otherwise set the list
                    col.allowed_values = edit_request.allowed_values if edit_request.allowed_values else None
                column_found = True
                break
        
        if not column_found:
            raise HTTPException(status_code=404, detail=f"Column '{edit_request.old_name}' not found")
        
        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        # Broadcast schema update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "edit_column",
            "old_name": edit_request.old_name,
            "new_name": edit_request.new_name,
            "columns": [col.model_dump() for col in session.columns]
        })
        
        # Schedule reprocessing if requested
        if edit_request.reprocess:
            background_tasks.add_task(
                schema_manager.reprocess_column, 
                session_id, 
                edit_request.new_name or edit_request.old_name
            )
        
        return {
            "status": "success",
            "message": f"Column '{edit_request.old_name}' updated successfully",
            "reprocessing": edit_request.reprocess
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-column/{session_id}/{column_name}")
async def delete_column(session_id: str, column_name: str):
    """Delete a column from the schema and existing data."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Find and remove the column
        column_found = False
        original_columns = session.columns.copy()
        session.columns = [col for col in session.columns if col.name != column_name]
        
        if len(session.columns) < len(original_columns):
            column_found = True
        
        if not column_found:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")
        
        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        # Remove column data from existing records
        print(f"DEBUG: Starting column data removal for '{column_name}' in session {session_id}")
        await schema_manager.remove_column_data(session_id, column_name)
        print(f"DEBUG: Column data removal completed for '{column_name}'")
        
        # Update statistics to reflect deleted column
        if session.statistics and session.statistics.column_stats:
            session.statistics.column_stats = [
                stat for stat in session.statistics.column_stats 
                if stat.name != column_name
            ]
            session.statistics.total_columns = len(session.columns)
            session_manager.update_session(session)
        
        # Broadcast schema update and data refresh trigger
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "delete_column",
            "column_name": column_name,
            "columns": [col.model_dump() for col in session.columns],
            "data_updated": True,  # Signal that data has changed
            "refresh_data": True   # Trigger data table refresh
        })
        
        return {
            "status": "success",
            "message": f"Column '{column_name}' deleted successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-column/{session_id}")
async def add_column(
    session_id: str, 
    add_request: ColumnAddRequest,
    background_tasks: BackgroundTasks
):
    """Add a new column to the schema and extract values from documents."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Check if column already exists
        for col in session.columns:
            if col.name == add_request.name:
                raise HTTPException(status_code=400, detail=f"Column '{add_request.name}' already exists")
        
        # Create new column
        new_column = ColumnInfo(
            name=add_request.name,
            definition=add_request.definition,
            rationale=add_request.rationale,
            data_type=add_request.data_type,
            allowed_values=add_request.allowed_values if add_request.allowed_values else None
        )
        
        session.columns.append(new_column)
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        # Broadcast schema update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "add_column",
            "column": new_column.model_dump(),
            "columns": [col.model_dump() for col in session.columns]
        })
        
        # Schedule value extraction for new column
        background_tasks.add_task(
            schema_manager.extract_values_for_new_column,
            session_id,
            new_column,
            add_request.documents_path
        )
        
        return {
            "status": "success",
            "message": f"Column '{add_request.name}' added successfully",
            "column": new_column.model_dump(),
            "extracting_values": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/merge-columns/{session_id}")
async def merge_columns(
    session_id: str, 
    merge_request: ColumnMergeRequest,
    background_tasks: BackgroundTasks
):
    """Merge multiple columns into a single column."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Validate source columns exist
        source_columns = []
        for col_name in merge_request.source_columns:
            col = next((c for c in session.columns if c.name == col_name), None)
            if not col:
                raise HTTPException(status_code=404, detail=f"Source column '{col_name}' not found")
            source_columns.append(col)
        
        # Check if target column already exists
        if any(col.name == merge_request.target_column for col in session.columns):
            raise HTTPException(status_code=400, detail=f"Target column '{merge_request.target_column}' already exists")
        
        # Create merged column
        merged_definition = merge_request.new_definition or f"Merged from: {', '.join(merge_request.source_columns)}"
        merged_column = ColumnInfo(
            name=merge_request.target_column,
            definition=merged_definition,
            rationale=f"Merged column using {merge_request.merge_strategy} strategy",
            data_type="text"
        )
        
        # Remove source columns and add merged column
        session.columns = [col for col in session.columns if col.name not in merge_request.source_columns]
        session.columns.append(merged_column)
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        # Broadcast schema update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "merge_columns",
            "source_columns": merge_request.source_columns,
            "target_column": merge_request.target_column,
            "columns": [col.model_dump() for col in session.columns]
        })
        
        # Schedule data merging
        background_tasks.add_task(
            schema_manager.merge_column_data,
            session_id,
            merge_request.source_columns,
            merge_request.target_column,
            merge_request.merge_strategy,
            merge_request.separator
        )
        
        return {
            "status": "success",
            "message": f"Columns {merge_request.source_columns} merged into '{merge_request.target_column}'",
            "merged_column": merged_column.model_dump(),
            "processing": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reprocess/{session_id}")
async def reprocess_documents(
    session_id: str, 
    reprocess_request: ReprocessRequest,
    background_tasks: BackgroundTasks
):
    """Trigger document reprocessing for specified columns or entire schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Validate columns if specified
        columns_to_process = reprocess_request.columns or [col.name for col in session.columns]
        for col_name in columns_to_process:
            if not any(col.name == col_name for col in session.columns):
                raise HTTPException(status_code=404, detail=f"Column '{col_name}' not found")
        
        # Schedule reprocessing
        background_tasks.add_task(
            schema_manager.reprocess_documents,
            session_id,
            columns_to_process,
            reprocess_request.incremental,
            reprocess_request.force_reprocess
        )
        
        return {
            "status": "success",
            "message": "Document reprocessing started",
            "columns": columns_to_process,
            "incremental": reprocess_request.incremental
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/reprocessing-status/{session_id}")
async def get_reprocessing_status(session_id: str) -> ReprocessingStatusResponse:
    """Get the current reprocessing status for a session."""
    try:
        status = await schema_manager.get_reprocessing_status(session_id)
        return ReprocessingStatusResponse(**status)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/validation/{session_id}")
async def validate_schema(session_id: str) -> SchemaValidationResponse:
    """Validate the schema for potential issues."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        errors = []
        warnings = []
        missing_definitions = []
        
        # Check for duplicate names
        column_names = [col.name for col in session.columns]
        if len(column_names) != len(set(column_names)):
            errors.append("Duplicate column names found")
        
        # Check for missing definitions
        for col in session.columns:
            if not col.definition or not col.definition.strip():
                missing_definitions.append(col.name)
                warnings.append(f"Column '{col.name}' has no definition")
        
        # Check for very short names
        for col in session.columns:
            if len(col.name) < 3:
                warnings.append(f"Column '{col.name}' has a very short name")
        
        return SchemaValidationResponse(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            column_count=len(session.columns),
            missing_definitions=missing_definitions
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/backup/{session_id}")
async def create_schema_backup(session_id: str):
    """Create a backup of the current schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        backup_data = {
            "timestamp": datetime.now().isoformat(),
            "query": session.schema_query,
            "columns": [col.model_dump() for col in session.columns]
        }
        
        # Save backup file
        session_dir = Path("./data") / session_id
        session_dir.mkdir(exist_ok=True)
        backup_file = session_dir / f"schema_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f, indent=2)
        
        return {
            "status": "success",
            "message": "Schema backup created successfully",
            "backup_file": str(backup_file),
            "timestamp": backup_data["timestamp"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))