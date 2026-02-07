"""
Schema editing API endpoints for QBSD visualization.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
import json
import asyncio
import logging
from pathlib import Path

from app.models.session import VisualizationSession, SessionStatus, ColumnInfo
from app.models.modification import ModificationAction
from app.models.qbsd import RetrieverConfig
from app.services.session_manager import SessionManager
from app.services.websocket_manager import WebSocketManager
from app.services.schema_manager import SchemaManager
from app.services.reextraction_service import ReextractionService
from app.services.continue_discovery_service import ContinueDiscoveryService
from app.services import session_manager, websocket_manager, concurrency_limiter
from app.core.exceptions import CapacityExceededError
from app.core.logging_utils import set_session_context

logger = logging.getLogger(__name__)
router = APIRouter(tags=["schema"])

# Create schema manager instance
schema_manager = SchemaManager(websocket_manager, session_manager)

# Create reextraction service instance
reextraction_service = ReextractionService(websocket_manager, session_manager)

# Create continue discovery service instance
continue_discovery_service = ContinueDiscoveryService(websocket_manager, session_manager)

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
    llm_config: Optional[Dict[str, Any]] = None  # User-provided LLM config with API key

class ColumnMergeRequest(BaseModel):
    source_columns: List[str]
    target_column: str
    merge_strategy: str = "concatenate"
    definition: Optional[str] = None
    rationale: Optional[str] = None
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
    slot_acquired = False
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

        # Track modification in history
        modification = ModificationAction(
            action_type="column_edited",
            column_name=edit_request.new_name or edit_request.old_name,
            details={
                "original_name": edit_request.old_name,
                "new_name": edit_request.new_name,
                "definition_changed": edit_request.definition is not None,
                "rationale_changed": edit_request.rationale is not None,
                "allowed_values_changed": edit_request.allowed_values is not None,
            }
        )
        session.modification_history.append(modification)

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

        # Schedule reprocessing if requested (requires concurrency slot)
        if edit_request.reprocess:
            await concurrency_limiter.acquire(session_id, "reprocess_column")
            slot_acquired = True
            background_tasks.add_task(
                schema_manager.reprocess_column,
                session_id,
                edit_request.new_name or edit_request.old_name
            )

        return {
            "status": "success",
            "message": f"Column '{edit_request.old_name}' updated successfully",
            "reprocessing": edit_request.reprocess,
            "columns": [col.model_dump() for col in session.columns]  # Return updated columns
        }

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        if slot_acquired:
            await concurrency_limiter.release(session_id)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-column/{session_id}/{column_name}")
async def delete_column(session_id: str, column_name: str):
    """Delete a column from the schema and existing data."""
    try:
        set_session_context(session_id)
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

        # Track modification in history
        modification = ModificationAction(
            action_type="column_deleted",
            column_name=column_name,
            details={
                "had_definition": any(c.definition for c in original_columns if c.name == column_name),
                "had_rationale": any(c.rationale for c in original_columns if c.name == column_name),
            }
        )
        session.modification_history.append(modification)

        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Remove column data from existing records
        logger.debug(f"Starting column data removal for '{column_name}'")
        await schema_manager.remove_column_data(session_id, column_name)
        logger.debug(f"Column data removal completed for '{column_name}'")
        
        # Update statistics to reflect deleted column
        if session.statistics and session.statistics.column_stats:
            session.statistics.column_stats = [
                stat for stat in session.statistics.column_stats
                if stat.name != column_name
            ]
            session.statistics.total_columns = len(session.columns)

            # Update schema_evolution
            if session.statistics.schema_evolution:
                # Remove from column_sources
                if column_name in session.statistics.schema_evolution.column_sources:
                    del session.statistics.schema_evolution.column_sources[column_name]
                # Update snapshots to reflect deletion
                for snapshot in session.statistics.schema_evolution.snapshots:
                    if column_name in snapshot.new_columns:
                        snapshot.new_columns.remove(column_name)
                    # Ensure total_columns doesn't exceed actual count
                    if snapshot.total_columns > len(session.columns):
                        snapshot.total_columns = len(session.columns)

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
    slot_acquired = False
    try:
        set_session_context(session_id)
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

        # Track modification in history
        modification = ModificationAction(
            action_type="column_added",
            column_name=add_request.name,
            details={
                "definition": add_request.definition,
                "rationale": add_request.rationale,
                "data_type": add_request.data_type,
            }
        )
        session.modification_history.append(modification)

        # Update statistics to include new column
        if session.statistics:
            # Add to column_stats
            new_col_info = ColumnInfo(
                name=add_request.name,
                definition=add_request.definition,
                rationale=add_request.rationale,
                data_type=add_request.data_type or "object",
                non_null_count=0,
                unique_count=0,
                allowed_values=add_request.allowed_values if add_request.allowed_values else None
            )
            session.statistics.column_stats.append(new_col_info)
            session.statistics.total_columns = len(session.columns)

            # Update schema_evolution
            if session.statistics.schema_evolution:
                # Add column source
                session.statistics.schema_evolution.column_sources[add_request.name] = "manual_addition"

        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Broadcast schema update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "add_column",
            "column": new_column.model_dump(),
            "columns": [col.model_dump() for col in session.columns]
        })

        # Save user-provided LLM config if provided (for value extraction)
        if add_request.llm_config:
            session_dir = Path("./data") / session_id
            session_dir.mkdir(parents=True, exist_ok=True)
            user_config_file = session_dir / "user_llm_config.json"
            with open(user_config_file, 'w') as f:
                json.dump(add_request.llm_config, f, indent=2)
            config_for_log = {k: v for k, v in add_request.llm_config.items() if k != 'api_key'}
            has_api_key = 'api_key' in add_request.llm_config and add_request.llm_config['api_key']
            logger.debug(f"Saved user LLM config for add-column: {config_for_log}, api_key={'present' if has_api_key else 'MISSING'}")

        # Reserve a concurrency slot for value extraction
        await concurrency_limiter.acquire(session_id, "add_column_extraction")
        slot_acquired = True

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
            "columns": [col.model_dump() for col in session.columns],  # Return all updated columns
            "extracting_values": True
        }

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        if slot_acquired:
            await concurrency_limiter.release(session_id)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/merge-columns/{session_id}")
async def merge_columns(
    session_id: str,
    merge_request: ColumnMergeRequest
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
        merged_definition = merge_request.definition or f"Merged from: {', '.join(merge_request.source_columns)}"
        merged_rationale = merge_request.rationale or f"Merged column using {merge_request.merge_strategy} strategy"
        merged_column = ColumnInfo(
            name=merge_request.target_column,
            definition=merged_definition,
            rationale=merged_rationale,
            data_type="text"
        )
        
        # Remove source columns and add merged column
        session.columns = [col for col in session.columns if col.name not in merge_request.source_columns]
        session.columns.append(merged_column)
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        # Merge column data synchronously (just file I/O, completes in milliseconds)
        await schema_manager.merge_column_data(
            session_id,
            merge_request.source_columns,
            merge_request.target_column,
            merge_request.merge_strategy,
            merge_request.separator
        )

        # Broadcast schema update with refresh flags (data is already merged)
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "merge_columns",
            "source_columns": merge_request.source_columns,
            "target_column": merge_request.target_column,
            "columns": [col.model_dump() for col in session.columns],
            "data_updated": True,
            "refresh_data": True
        })
        
        return {
            "status": "success",
            "message": f"Columns {merge_request.source_columns} merged into '{merge_request.target_column}'",
            "merged_column": merged_column.model_dump(),
            "columns": [col.model_dump() for col in session.columns],
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

        # Reserve a concurrency slot
        await concurrency_limiter.acquire(session_id, "reprocess")

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

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await concurrency_limiter.release(session_id)
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


# Schema Evolution Endpoints

class ApproveSuggestionRequest(BaseModel):
    column_name: str
    value: str

class RejectSuggestionRequest(BaseModel):
    column_name: str
    value: str

class SetThresholdRequest(BaseModel):
    threshold: int = 2  # 0 = disabled


@router.get("/suggestions/{session_id}")
async def get_schema_suggestions(session_id: str):
    """Get pending allowed_values suggestions for review."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        suggestions = []

        # Collect pending values from all columns
        for col in session.columns:
            if col.pending_values and len(col.pending_values) > 0:
                suggestions.append({
                    "column_name": col.name,
                    "pending_values": [pv.model_dump() for pv in col.pending_values],
                    "current_allowed_values": col.allowed_values or [],
                    "auto_expand_threshold": col.auto_expand_threshold
                })

        # Also include session-level suggestions if any
        if session.schema_suggestions:
            for sugg in session.schema_suggestions:
                # Avoid duplicates
                if not any(s["column_name"] == sugg.column_name for s in suggestions):
                    suggestions.append({
                        "column_name": sugg.column_name,
                        "suggested_values": sugg.suggested_values,
                        "value_details": {k: v.model_dump() for k, v in sugg.value_details.items()},
                        "auto_approved": sugg.auto_approved
                    })

        return {
            "session_id": session_id,
            "suggestions": suggestions,
            "total_pending": sum(len(s.get("pending_values", s.get("suggested_values", []))) for s in suggestions)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/approve-suggestion/{session_id}")
async def approve_suggestion(session_id: str, request: ApproveSuggestionRequest):
    """Approve adding a suggested value to allowed_values."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Find the column
        column_found = False
        for col in session.columns:
            if col.name == request.column_name:
                column_found = True

                # Add value to allowed_values
                if col.allowed_values is None:
                    col.allowed_values = []
                if request.value not in col.allowed_values:
                    col.allowed_values.append(request.value)

                # Remove from pending_values
                if col.pending_values:
                    col.pending_values = [pv for pv in col.pending_values if pv.value != request.value]
                    if not col.pending_values:
                        col.pending_values = None

                break

        if not column_found:
            raise HTTPException(status_code=404, detail=f"Column '{request.column_name}' not found")

        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Broadcast update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "approve_suggestion",
            "column_name": request.column_name,
            "approved_value": request.value,
            "columns": [col.model_dump() for col in session.columns]
        })

        return {
            "status": "success",
            "message": f"Value '{request.value}' approved and added to allowed_values for column '{request.column_name}'"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reject-suggestion/{session_id}")
async def reject_suggestion(session_id: str, request: RejectSuggestionRequest):
    """Reject a suggested value (removes from pending, won't be re-suggested in this session)."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Find the column and remove the pending value
        column_found = False
        for col in session.columns:
            if col.name == request.column_name:
                column_found = True

                if col.pending_values:
                    col.pending_values = [pv for pv in col.pending_values if pv.value != request.value]
                    if not col.pending_values:
                        col.pending_values = None

                break

        if not column_found:
            raise HTTPException(status_code=404, detail=f"Column '{request.column_name}' not found")

        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Broadcast update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "reject_suggestion",
            "column_name": request.column_name,
            "rejected_value": request.value,
            "columns": [col.model_dump() for col in session.columns]
        })

        return {
            "status": "success",
            "message": f"Value '{request.value}' rejected for column '{request.column_name}'"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/auto-expand-threshold/{session_id}/{column_name}")
async def set_auto_expand_threshold(
    session_id: str,
    column_name: str,
    request: SetThresholdRequest
):
    """Configure auto-expand threshold for a column (0 = disabled)."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Find and update the column
        column_found = False
        for col in session.columns:
            if col.name == column_name:
                col.auto_expand_threshold = request.threshold if request.threshold > 0 else None
                column_found = True
                break

        if not column_found:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Broadcast update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "set_threshold",
            "column_name": column_name,
            "threshold": request.threshold,
            "columns": [col.model_dump() for col in session.columns]
        })

        return {
            "status": "success",
            "message": f"Auto-expand threshold for '{column_name}' set to {request.threshold}",
            "threshold": request.threshold
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-approve/{session_id}")
async def bulk_approve_suggestions(session_id: str, column_name: Optional[str] = None):
    """Approve all pending suggestions for a column or all columns."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        approved_count = 0

        for col in session.columns:
            if column_name and col.name != column_name:
                continue

            if col.pending_values:
                if col.allowed_values is None:
                    col.allowed_values = []

                for pv in col.pending_values:
                    if pv.value not in col.allowed_values:
                        col.allowed_values.append(pv.value)
                        approved_count += 1

                col.pending_values = None

        # Update session
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        # Broadcast update
        await websocket_manager.broadcast_schema_updated(session_id, {
            "operation": "bulk_approve",
            "column_name": column_name,
            "approved_count": approved_count,
            "columns": [col.model_dump() for col in session.columns]
        })

        return {
            "status": "success",
            "message": f"Approved {approved_count} pending values",
            "approved_count": approved_count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Re-extraction Endpoints ====================

class ColumnChangeDetail(BaseModel):
    column_name: str
    change_type: str  # "definition", "rationale", "allowed_values", "new"
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    row_count_affected: int = 0


class SchemaChangeStatusResponse(BaseModel):
    has_changes: bool
    changed_columns: List[str]
    new_columns: List[str]
    column_changes: Dict[str, ColumnChangeDetail]
    can_reextract: bool
    missing_baseline: bool = False


class PaperDiscoveryResponse(BaseModel):
    total_rows: int
    rows_with_papers: int
    available_papers: List[str]
    missing_papers: List[str]
    paper_to_rows: Dict[str, List[str]]


class ReextractionRequest(BaseModel):
    columns: List[str]
    llm_config: Optional[Dict[str, Any]] = None  # User-provided LLM config with API key


class ReextractionResponse(BaseModel):
    status: str
    operation_id: str
    columns: List[str]
    estimated_papers: int
    rows_to_process: int
    missing_papers: List[str]


@router.get("/change-status/{session_id}")
async def get_schema_change_status(session_id: str) -> SchemaChangeStatusResponse:
    """Detect which columns have changed since the baseline."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        changes = reextraction_service.detect_schema_changes(session)

        # Count rows to determine affected row counts
        paper_discovery = await reextraction_service.discover_papers(session_id)
        row_count = paper_discovery["total_rows"]

        # Update row counts in changes
        for col_name in changes["column_changes"]:
            changes["column_changes"][col_name]["row_count_affected"] = row_count

        # Determine if we can reextract (need documents)
        changes["can_reextract"] = paper_discovery["rows_with_papers"] > 0

        return SchemaChangeStatusResponse(
            has_changes=changes["has_changes"],
            changed_columns=changes["changed_columns"],
            new_columns=changes["new_columns"],
            column_changes={
                k: ColumnChangeDetail(**v) for k, v in changes["column_changes"].items()
            },
            can_reextract=changes["can_reextract"],
            missing_baseline=changes["missing_baseline"]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discover-papers/{session_id}")
async def discover_papers(session_id: str) -> PaperDiscoveryResponse:
    """Find papers associated with table rows in storage."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        discovery = await reextraction_service.discover_papers(session_id)

        return PaperDiscoveryResponse(**discovery)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class DocumentAvailabilityRequest(BaseModel):
    operation_type: str = "reextraction"  # 'reextraction' or 'continue_discovery'
    columns: Optional[List[str]] = None


class DocumentInfo(BaseModel):
    name: str
    status: str  # 'local', 'cloud', 'missing'
    cloud_path: Optional[str] = None
    affected_rows: List[str] = []


class DocumentAvailabilityResponse(BaseModel):
    total_documents: int
    local_documents: List[DocumentInfo]
    cloud_documents: List[DocumentInfo]
    missing_documents: List[DocumentInfo]
    can_proceed: bool
    total_rows: int
    rows_with_missing_docs: int


@router.post("/precheck-documents/{session_id}")
async def precheck_document_availability(
    session_id: str,
    request: DocumentAvailabilityRequest
) -> DocumentAvailabilityResponse:
    """Pre-check document availability before extraction."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        result = await reextraction_service.precheck_document_availability(
            session_id,
            request.operation_type
        )

        return DocumentAvailabilityResponse(
            total_documents=result["total_documents"],
            local_documents=[DocumentInfo(**doc) for doc in result["local_documents"]],
            cloud_documents=[DocumentInfo(**doc) for doc in result["cloud_documents"]],
            missing_documents=[DocumentInfo(**doc) for doc in result["missing_documents"]],
            can_proceed=result["can_proceed"],
            total_rows=result["total_rows"],
            rows_with_missing_docs=result["rows_with_missing_docs"]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reextract/{session_id}")
async def start_reextraction(
    session_id: str,
    request: ReextractionRequest,
    background_tasks: BackgroundTasks
) -> ReextractionResponse:
    """Start selective re-extraction for specified columns."""
    try:
        set_session_context(session_id)
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        if not request.columns:
            raise HTTPException(status_code=400, detail="No columns specified for re-extraction")

        # Save user-provided LLM config if provided
        if request.llm_config:
            session_dir = Path("./data") / session_id
            session_dir.mkdir(parents=True, exist_ok=True)
            user_config_file = session_dir / "user_llm_config.json"
            with open(user_config_file, 'w') as f:
                json.dump(request.llm_config, f, indent=2)
            # Log without exposing full API key
            config_for_log = {k: v for k, v in request.llm_config.items() if k != 'api_key'}
            has_api_key = 'api_key' in request.llm_config and request.llm_config['api_key']
            logger.debug(f"Saved user LLM config for re-extraction: {config_for_log}, api_key={'present' if has_api_key else 'MISSING'}")

        # Reserve a concurrency slot
        await concurrency_limiter.acquire(session_id, "reextraction")

        try:
            result = await reextraction_service.start_reextraction(
                session_id,
                request.columns
            )
        except Exception:
            # Release slot if start_reextraction fails before creating its task
            await concurrency_limiter.release(session_id)
            raise

        return ReextractionResponse(**result)

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reextraction-status/{session_id}/{operation_id}")
async def get_reextraction_status(session_id: str, operation_id: str):
    """Get status of a re-extraction operation."""
    try:
        status = reextraction_service.get_operation_status(operation_id)
        if not status:
            raise HTTPException(status_code=404, detail="Operation not found")

        if status["session_id"] != session_id:
            raise HTTPException(status_code=403, detail="Operation does not belong to this session")

        return status

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop-reextraction/{session_id}/{operation_id}")
async def stop_reextraction(session_id: str, operation_id: str):
    """Stop a running re-extraction operation.

    Returns information about partial results saved.
    """
    try:
        result = await reextraction_service.stop_operation(operation_id)

        if not result["stopped"]:
            raise HTTPException(status_code=400, detail=result["message"])

        return {
            "status": "stopped",
            "message": result["message"],
            "processed_documents": result.get("processed_documents", 0),
            "total_documents": result.get("total_documents", 0)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/capture-baseline/{session_id}")
async def capture_schema_baseline(session_id: str):
    """Manually capture the current schema as the baseline."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        await reextraction_service.capture_and_save_baseline(session_id)

        return {
            "status": "success",
            "message": "Schema baseline captured successfully",
            "column_count": len([c for c in session.columns if not c.name.lower().endswith('_excerpt')])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload-missing-papers/{session_id}")
async def upload_missing_papers(
    session_id: str,
    files: List[Any] = None  # Will be UploadFile in actual use
):
    """Upload papers that are missing from storage."""
    from fastapi import UploadFile, File

    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        session_dir = Path("./data") / session_id
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)

        uploaded = []
        # Handle file uploads if provided
        if files:
            for file in files:
                if hasattr(file, 'filename') and hasattr(file, 'read'):
                    file_path = docs_dir / file.filename
                    content = await file.read()
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    uploaded.append(file.filename)

        return {
            "status": "success",
            "message": f"Uploaded {len(uploaded)} files",
            "uploaded_files": uploaded
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Continue Schema Discovery Endpoints ====================

class ContinueDiscoveryDocumentsResponse(BaseModel):
    original_documents: List[str]
    original_count: int
    cloud_datasets: List[str]
    original_cloud_dataset: Optional[str]
    can_use_original: bool
    query: str


class ContinueDiscoveryRequest(BaseModel):
    document_source: str  # 'original', 'upload', 'cloud'
    cloud_dataset: Optional[str] = None
    llm_config: Dict[str, Any]
    retriever_config: Optional[Dict[str, Any]] = None  # Retriever settings (empty = defaults)
    max_keys_schema: int = 100
    documents_batch_size: int = 1
    bypass_limit: bool = False  # Developer mode: bypass document limit


class ContinueDiscoveryResponse(BaseModel):
    status: str
    operation_id: str
    initial_column_count: int
    document_source: str


class NewColumnInfo(BaseModel):
    name: str
    definition: str
    rationale: str
    allowed_values: Optional[List[str]] = None
    source_document: Optional[str] = None
    discovery_iteration: Optional[int] = None


class ContinueDiscoveryStatus(BaseModel):
    operation_id: str
    session_id: str
    status: str  # pending, running, completed, failed, stopped
    phase: str   # discovery, extraction
    progress: float
    current_batch: int
    total_batches: int
    initial_columns: List[str]
    new_columns: List[NewColumnInfo]
    confirmed_columns: List[str]
    processed_documents: int
    total_documents: int
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None


class ConfirmColumnsRequest(BaseModel):
    selected_columns: List[str]
    row_selection: str  # 'all' or 'selected'
    selected_rows: Optional[List[str]] = None
    llm_config: Optional[Dict[str, Any]] = None


class ConfirmColumnsResponse(BaseModel):
    status: str
    operation_id: str
    columns: List[str]
    row_count: Any  # int or 'all'


@router.get("/continue-discovery/documents/{session_id}")
async def get_continue_discovery_documents(session_id: str) -> ContinueDiscoveryDocumentsResponse:
    """Get available document sources for continued schema discovery."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        result = await continue_discovery_service.get_available_documents(session_id)

        return ContinueDiscoveryDocumentsResponse(**result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/continue-discovery/start/{session_id}")
async def start_continue_discovery(
    session_id: str,
    request: ContinueDiscoveryRequest,
    background_tasks: BackgroundTasks
) -> ContinueDiscoveryResponse:
    """Start schema discovery continuation with current schema as initial schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        if not request.llm_config:
            raise HTTPException(status_code=400, detail="LLM configuration is required")

        # Reserve a concurrency slot
        await concurrency_limiter.acquire(session_id, "continue_discovery")

        try:
            result = await continue_discovery_service.start_continue_discovery(
                session_id=session_id,
                document_source=request.document_source,
                llm_config=request.llm_config,
                cloud_dataset=request.cloud_dataset,
                retriever_config=request.retriever_config,
                max_keys_schema=request.max_keys_schema,
                documents_batch_size=request.documents_batch_size,
                bypass_limit=request.bypass_limit
            )
        except Exception:
            # Release slot if start fails before creating its task
            await concurrency_limiter.release(session_id)
            raise

        return ContinueDiscoveryResponse(**result)

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/continue-discovery/status/{session_id}/{operation_id}")
async def get_continue_discovery_status(session_id: str, operation_id: str) -> ContinueDiscoveryStatus:
    """Get status of a continue discovery operation."""
    try:
        status = continue_discovery_service.get_operation_status(operation_id)
        if not status:
            raise HTTPException(status_code=404, detail="Operation not found")

        if status["session_id"] != session_id:
            raise HTTPException(status_code=403, detail="Operation does not belong to this session")

        # Convert new_columns dicts to NewColumnInfo
        new_columns = [NewColumnInfo(**col) for col in status.get("new_columns", [])]

        return ContinueDiscoveryStatus(
            operation_id=status["operation_id"],
            session_id=status["session_id"],
            status=status["status"],
            phase=status["phase"],
            progress=status["progress"],
            current_batch=status["current_batch"],
            total_batches=status["total_batches"],
            initial_columns=status["initial_columns"],
            new_columns=new_columns,
            confirmed_columns=status.get("confirmed_columns", []),
            processed_documents=status["processed_documents"],
            total_documents=status["total_documents"],
            started_at=status.get("started_at"),
            completed_at=status.get("completed_at"),
            error=status.get("error")
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/continue-discovery/confirm/{session_id}/{operation_id}")
async def confirm_new_columns(
    session_id: str,
    operation_id: str,
    request: ConfirmColumnsRequest,
    background_tasks: BackgroundTasks
) -> ConfirmColumnsResponse:
    """Confirm which new columns to add and start value extraction."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Reserve a concurrency slot for extraction phase
        await concurrency_limiter.acquire(session_id, "continue_discovery_extraction")

        try:
            result = await continue_discovery_service.confirm_and_start_extraction(
                operation_id=operation_id,
                selected_columns=request.selected_columns,
                row_selection=request.row_selection,
                selected_rows=request.selected_rows,
                llm_config=request.llm_config
            )
        except Exception:
            await concurrency_limiter.release(session_id)
            raise

        return ConfirmColumnsResponse(**result)

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/continue-discovery/stop/{session_id}/{operation_id}")
async def stop_continue_discovery(session_id: str, operation_id: str):
    """Stop a running continue discovery or extraction operation."""
    try:
        result = await continue_discovery_service.stop_operation(operation_id)

        if not result["stopped"]:
            raise HTTPException(status_code=400, detail=result["message"])

        return {
            "status": "stopped",
            "phase": result.get("phase", "unknown"),
            "message": result["message"]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))