"""ScheMatiQ API endpoints."""

import logging
import uuid
import asyncio
import json
from datetime import datetime
from pathlib import Path
from app.core.config import DEFAULT_DATA_DIR, DEFAULT_SCHEMATIQ_WORK_DIR
from typing import Any, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel

from app.models.session import VisualizationSession, SessionType, SessionStatus, SessionMetadata, FilterSortRequest
from app.services.session_capabilities import has_live_pipeline
from app.models.schematiq import ScheMatiQConfig, ScheMatiQStatus, CostEstimate, CostEstimateRequest, PhaseEstimate, DocumentStats
from app.services.pipeline.data_query import session_data_read_failed
from app.services.data_editor import DataEditor
from app.services import session_manager, concurrency_limiter
from app.core.exceptions import CapacityExceededError
from app.services.export_service import (
    export_complete_schematiq,
    export_schematiq_csv,
    export_schematiq_rich_csv_data,
    export_schematiq_schema_json,
)

from schematiq.core.cost_estimator import estimate_from_config

logger = logging.getLogger(__name__)
router = APIRouter()
# Shared ScheMatiQ runner singleton (see app.services.orchestration). The chat
# tools, the /load/rediscover route and app.main all bind this same instance, so
# a run started from any entry point can be stopped and resumed from any other.
from app.services.orchestration import schematiq_runner
from app.services.session_documents import pending_docs_dir
# Create data editor instance
data_editor = DataEditor()

# Project root for path resolution (backend/app/api/routes -> project root = 5 levels up)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

# User-facing message for the global LLM quota (LLM_CALL_GLOBAL_LIMIT).
# Deliberately distinct from the ConcurrencyLimiter's 503 message: hitting the
# quota is not a transient load issue, so "try again later" would mislead.
QUOTA_EXCEEDED_USER_MESSAGE = (
    "This deployment has reached its LLM usage quota, so new extractions are "
    "paused. This is a usage quota rather than a temporary load issue, so "
    "retrying will not help - please contact us to restore access."
)


def _resolve_docs_path(path: str, session_id: Optional[str] = None) -> Optional[Path]:
    """Resolve a document path to an existing directory.
    
    Tries multiple resolution strategies:
    - Direct path
    - Session's pending_documents directory
    - Relative paths from various locations
    - Cloud dataset name -> research/data/<name>
    """
    if not path:
        return None
    
    doc_path = Path(path)
    candidates = [
        doc_path,
        Path("..") / path,
        Path("../..") / path,
        PROJECT_ROOT / path,
        PROJECT_ROOT / "research" / "data" / Path(path).name,
        Path.cwd() / path,
        Path.cwd().parent / path,
    ]
    
    # Add session-specific path if session_id provided
    if session_id:
        candidates.insert(1, pending_docs_dir(Path(DEFAULT_DATA_DIR) / session_id))
    
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    
    return None


def _load_documents_from_path(resolved_path: Path) -> List[str]:
    """Load text documents from a directory."""
    documents = []
    for doc_file in sorted(resolved_path.iterdir()):
        if doc_file.is_file() and doc_file.suffix in ['.txt', '.md']:
            try:
                content = doc_file.read_text(encoding='utf-8')
                documents.append(content)
            except Exception:
                pass
    return documents


def _convert_estimate_result(result) -> CostEstimate:
    """Convert cost estimator result to Pydantic model."""
    return CostEstimate(
        schema_discovery=PhaseEstimate(
            input_tokens=result.schema_discovery.input_tokens,
            output_tokens=result.schema_discovery.output_tokens,
            api_calls=result.schema_discovery.api_calls,
            cost_usd=result.schema_discovery.cost_usd
        ),
        value_extraction=PhaseEstimate(
            input_tokens=result.value_extraction.input_tokens,
            output_tokens=result.value_extraction.output_tokens,
            api_calls=result.value_extraction.api_calls,
            cost_usd=result.value_extraction.cost_usd
        ),
        total_input_tokens=result.total_input_tokens,
        total_output_tokens=result.total_output_tokens,
        total_api_calls=result.total_api_calls,
        total_cost_usd=result.total_cost_usd,
        warnings=result.warnings,
        document_stats=DocumentStats(**result.document_stats)
    )

@router.post("/configure", response_model=dict)
async def configure_schematiq(config: ScheMatiQConfig):
    """Configure a new ScheMatiQ session."""
    from app.core.config import DEVELOPER_MODE
    try:
        logger.debug(f"Received ScheMatiQ config: {config}")
        
        # Validate configuration
        validation = await schematiq_runner.validate_config(config)
        
        logger.debug(f"Validation result: {validation}")
        
        if not validation["is_valid"]:
            raise HTTPException(status_code=400, detail=validation["errors"])
        
        # Create session
        session_id = str(uuid.uuid4())
        metadata = SessionMetadata(source=f"ScheMatiQ Query: {config.query[:50]}...")
        
        session = VisualizationSession(
            id=session_id,
            type=SessionType.SCHEMATIQ,
            metadata=metadata,
            schema_query=config.query,
            opt_out_data_collection=config.opt_out_data_collection,
            write_artifacts=config.write_artifacts if config.write_artifacts is not None else DEVELOPER_MODE,
        )
        
        # Store session and config
        session_manager.create_session(session)
        await schematiq_runner.save_config(session_id, config)
        
        return {
            "session_id": session_id,
            "message": "ScheMatiQ session configured successfully"
        }
        
    except Exception as e:
        logger.error(f"Exception in configure_schematiq: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/estimate-cost/{session_id}", response_model=CostEstimate)
async def estimate_schematiq_cost(session_id: str):
    """Estimate cost for ScheMatiQ execution before running.
    
    Returns estimated token counts, API calls, and costs broken down by phase
    (schema discovery + value extraction) as well as totals.
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Load the saved config for this session
        config_file = Path(DEFAULT_SCHEMATIQ_WORK_DIR) / session_id / "config.json"
        if not config_file.exists():
            raise HTTPException(
                status_code=400,
                detail="No configuration found for this session. Configure ScheMatiQ first."
            )
        
        with open(config_file) as f:
            config_data = json.load(f)
        
        # Load documents for token counting
        documents = []
        docs_path = config_data.get("docs_path")
        
        if docs_path:
            paths = [docs_path] if isinstance(docs_path, str) else docs_path
            for path in paths:
                resolved = _resolve_docs_path(path, session_id)
                if resolved:
                    documents.extend(_load_documents_from_path(resolved))
        
        # Also check for uploaded documents in data directory
        upload_dir = pending_docs_dir(Path(DEFAULT_DATA_DIR) / session_id)
        if upload_dir.exists() and not documents:
            documents.extend(_load_documents_from_path(upload_dir))
        
        # Run the estimation
        result = estimate_from_config(documents, config_data)
        return _convert_estimate_result(result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/estimate-cost-preview", response_model=CostEstimate)
async def estimate_schematiq_cost_preview(request: CostEstimateRequest):
    """Estimate cost for ScheMatiQ execution without saving a session.
    
    This is useful for getting a cost estimate before committing to a configuration.
    Takes the full ScheMatiQConfig directly in the request body.
    """
    try:
        config = request.config
        
        # Convert Pydantic model to dict for the estimator
        config_data = {
            "query": config.query,
            "docs_path": config.docs_path,
            "documents_batch_size": config.documents_batch_size,
            "skip_value_extraction": config.skip_value_extraction,
            "schema_creation_backend": {
                "provider": config.schema_creation_backend.provider,
                "model": config.schema_creation_backend.model,
                "max_output_tokens": config.schema_creation_backend.max_output_tokens,
            },
            "value_extraction_backend": {
                "provider": config.value_extraction_backend.provider,
                "model": config.value_extraction_backend.model,
                "max_output_tokens": config.value_extraction_backend.max_output_tokens,
            },
            "retriever": {
                "k": config.retriever.k if config.retriever else 8
            },
            "initial_schema": [col.model_dump() for col in config.initial_schema] if config.initial_schema else []
        }
        
        # Load documents for token counting
        documents = []
        document_token_counts = None
        
        # Check if uploaded file info was provided (estimate from size)
        if request.uploaded_files and len(request.uploaded_files) > 0:
            # Estimate tokens from file sizes: ~4 bytes per token for English text
            document_token_counts = [
                max(1, file_info.size // 4)
                for file_info in request.uploaded_files
            ]
        else:
            # Load from docs_path (for cloud datasets)
            docs_path = config.docs_path
            if docs_path:
                paths = [docs_path] if isinstance(docs_path, str) else docs_path
                for path in paths:
                    resolved = _resolve_docs_path(path)
                    if resolved:
                        documents.extend(_load_documents_from_path(resolved))
        
        # Run the estimation
        result = estimate_from_config(
            documents,
            config_data,
            document_token_counts=document_token_counts
        )
        return _convert_estimate_result(result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/{session_id}")
async def run_schematiq(session_id: str, background_tasks: BackgroundTasks):
    """Start ScheMatiQ execution."""
    try:
        session = session_manager.get_session(session_id)
        if not has_live_pipeline(session):
            raise HTTPException(status_code=404, detail="ScheMatiQ session not found")

        # Pre-check global LLM quota before starting background task.
        # This gives the user an immediate HTTP error instead of a delayed WebSocket error.
        from app.core.config import LLM_CALL_GLOBAL_LIMIT, DEVELOPER_MODE
        from schematiq.core.llm_call_tracker import QuotaExceededError
        if not DEVELOPER_MODE:
            try:
                schematiq_runner.check_global_quota(LLM_CALL_GLOBAL_LIMIT)
            except QuotaExceededError as exc:
                # Send email alert (once per process lifetime)
                from app.core.email_alerts import send_quota_exceeded_alert
                send_quota_exceeded_alert(total_used=exc.used)

                raise HTTPException(status_code=429, detail=QUOTA_EXCEEDED_USER_MESSAGE)

        # Reserve concurrency slot before starting background task
        await concurrency_limiter.acquire(session_id, "schematiq")

        # Start ScheMatiQ in background (its finally block releases the slot)
        background_tasks.add_task(schematiq_runner.run_schematiq, session_id)

        return {"message": "ScheMatiQ execution started", "session_id": session_id}

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await concurrency_limiter.release(session_id)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/resume/{session_id}")
async def resume_schematiq(session_id: str, background_tasks: BackgroundTasks):
    """Resume ScheMatiQ execution after observation unit review or rediscover schema.

    Called either:
    - After the user reviews/edits the observation unit during the review step
    - After the user edits the observation unit post-completion and wants to rediscover the schema

    The pipeline will re-run schema discovery using the (potentially edited) observation unit.
    """
    try:
        session = session_manager.get_session(session_id)
        if not has_live_pipeline(session):
            raise HTTPException(status_code=404, detail="ScheMatiQ session not found")

        allowed_statuses = {
            SessionStatus.OBSERVATION_UNIT_REVIEW,
            SessionStatus.COMPLETED,
            SessionStatus.STOPPED,
            SessionStatus.SCHEMA_READY,
            SessionStatus.PROCESSING,
        }
        if session.status not in allowed_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Session cannot be resumed from status '{session.status}'. "
                       f"Allowed: {', '.join(s.value for s in allowed_statuses)}"
            )
        if (
            session.status == SessionStatus.PROCESSING
            and not session.metadata.pending_observation_unit_rediscovery
        ):
            raise HTTPException(
                status_code=400,
                detail="Session is still processing. Stop the run before resuming.",
            )

        # Pre-check global LLM quota before starting background task
        from app.core.config import LLM_CALL_GLOBAL_LIMIT, DEVELOPER_MODE
        from schematiq.core.llm_call_tracker import QuotaExceededError
        if not DEVELOPER_MODE:
            try:
                schematiq_runner.check_global_quota(LLM_CALL_GLOBAL_LIMIT)
            except QuotaExceededError as exc:
                from app.core.email_alerts import send_quota_exceeded_alert
                send_quota_exceeded_alert(total_used=exc.used)
                raise HTTPException(status_code=429, detail=QUOTA_EXCEEDED_USER_MESSAGE)

        # Stop + prepare synchronously so resume cannot race the active pipeline.
        try:
            await schematiq_runner.prepare_resume(session_id)
        except RuntimeError as e:
            msg = str(e)
            if "already has an active operation" in msg or "Timed out waiting" in msg:
                raise HTTPException(
                    status_code=409,
                    detail="Pipeline is still stopping. Wait a few seconds and try Save & Rediscover again.",
                ) from e
            raise HTTPException(status_code=500, detail=msg) from e

        background_tasks.add_task(schematiq_runner.run_schematiq, session_id)

        return {"message": "ScheMatiQ execution resumed", "session_id": session_id}

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await concurrency_limiter.release(session_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{session_id}", response_model=ScheMatiQStatus)
async def get_schematiq_status(session_id: str):
    """Get ScheMatiQ execution status."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        status = await schematiq_runner.get_status(session_id)
        
        return status
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/{session_id}")
async def get_schematiq_schema(session_id: str):
    """Get discovered schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        schema = await schematiq_runner.get_schema(session_id)
        
        return schema
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data/{session_id}")
async def get_schematiq_data_with_filters(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
    documents: Optional[str] = Query(None, description="Comma-separated document names to filter by"),
    request: Optional[FilterSortRequest] = None
):
    """Get extracted data with optional filtering and sorting."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Extract filter/sort params from request body
        filters = None
        sort = None
        search = None

        if request:
            filters = [f.dict() for f in request.filters] if request.filters else None
            sort = [s.dict() for s in request.sort] if request.sort else None
            search = request.search

        # Parse comma-separated document names
        document_filter = [d.strip() for d in documents.split(',') if d.strip()] if documents else None

        data = await schematiq_runner.get_data(
            session_id, page, page_size,
            filters=filters, sort=sort, search=search,
            document_filter=document_filter
        )

        # An empty payload that contradicts the session's own row count means the
        # data could not be read, not that the table is empty. Reporting it as a
        # retryable error keeps clients from rendering an empty table (and, for
        # schema-driven grids, bare column headers) over data that still exists.
        if await session_data_read_failed(session_id, data, session):
            logger.error(
                "Session %s: data read returned 0 rows while statistics record %s — "
                "no local data file and the rows are present in storage",
                session_id,
                session.statistics.total_rows,
            )
            raise HTTPException(
                status_code=503,
                detail="Table data could not be loaded right now. Please retry in a moment.",
            )

        return data

    except HTTPException:
        # Preserve intended status codes (404 for a missing session, 503 above);
        # the generic handler below would otherwise report them all as 500.
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{session_id}")
async def get_schematiq_data(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
    documents: Optional[str] = Query(None, description="Comma-separated document names to filter by"),
):
    """Get extracted data (backward compatible, no filtering)."""
    return await get_schematiq_data_with_filters(session_id, page, page_size, documents, None)


class CellRestoreBody(BaseModel):
    """Optional body for restoring a full cell object (undo)."""
    restore: Any = None

@router.put("/cell/{session_id}")
async def update_cell(session_id: str, column: str, value: str,
                      row_name: str = "",
                      row_index: Optional[int] = None,
                      source_document: Optional[str] = None,
                      body: Optional[CellRestoreBody] = None):
    """Update a single cell value in the data table.

    Rows are identified by ``row_name`` (preferred). When the row has no name
    (e.g. generic CSV/JSON imports in load mode), ``row_index`` — the absolute
    non-blank line position from the paginated data — is used as a fallback.
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # A body means the caller is restoring a cell (undo/redo), even when
        # its `restore` value is itself None (the cell had no value before
        # the operation being undone) -- distinct from a plain edit, which
        # never sends a body at all. See DataEditor._apply_cell_update.
        restore = body.restore if body else None
        is_restore = body is not None
        result = await data_editor.update_cell(
            session_id, row_name, column, value,
            restore=restore, is_restore=is_restore, source_document=source_document,
            row_index=row_index,
        )
        return result

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Upper bound on cells per batch request. The frontend chunks below this; the
# cap keeps one request from holding the session write lock for an unbounded
# time on an arbitrarily large payload (CWE-770).
MAX_CELL_BATCH = 2000


class CellBatchItem(BaseModel):
    """One cell write inside a batch. Mirrors update_cell's query params.

    ``is_restore`` replaces the single-cell endpoint's "body present" signal:
    when true, ``restore`` (possibly null) is written back verbatim.
    """
    column: str
    value: str = ""
    row_name: str = ""
    row_index: Optional[int] = None
    source_document: Optional[str] = None
    is_restore: bool = False
    restore: Any = None


class CellBatchBody(BaseModel):
    updates: List[CellBatchItem]


@router.put("/cells/{session_id}")
async def update_cells(session_id: str, body: CellBatchBody):
    """Update many cells in one request (column clear, multi-cell paste, undo).

    The data file is read, rewritten and persisted once for the whole batch
    instead of once per cell. A row that cannot be found fails only its own
    entry; the response lists a result per update, in request order.
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        if len(body.updates) > MAX_CELL_BATCH:
            raise HTTPException(
                status_code=400,
                detail=f"Too many cell updates in one request (max {MAX_CELL_BATCH})",
            )

        results = await data_editor.update_cells(
            session_id,
            [item.model_dump() for item in body.updates],
        )
        failed = sum(1 for r in results if not r["ok"])
        return {
            "status": "success",
            "session_id": session_id,
            "succeeded": len(results) - failed,
            "failed": failed,
            "results": [
                {"ok": r["ok"], "error": r.get("error")} for r in results
            ],
        }

    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Batch cell update failed for session %s", session_id)
        raise HTTPException(status_code=500, detail="Batch cell update failed")


@router.post("/stop/{session_id}")
async def stop_schematiq(session_id: str):
    """Request ScheMatiQ execution to stop. Returns immediately."""
    try:
        result = await schematiq_runner.request_stop(session_id)
        if not result["accepted"]:
            raise HTTPException(status_code=404, detail=result["message"])

        # Background safety net: force-cancels task if it doesn't stop within 60s
        asyncio.create_task(schematiq_runner.stop_execution(session_id))

        return {"status": "stopping", "message": result["message"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# NOTE: the unauthenticated "list every session" endpoint was removed here.
# See the matching note in routes/load.py — the listing exposed every project
# id on the deployment to any caller, and the frontend never used it.


@router.get("/directories")
async def list_document_directories():
    """List available document directories from research/data folder.

    Returns a list of directories that can be used as document paths for ScheMatiQ.
    Each directory includes its name and the relative path for the backend.
    """
    try:
        # Try multiple path resolution strategies
        candidates = [
            Path("../research/data"),  # When running from backend/
            Path(__file__).parent.parent.parent.parent.parent / "research" / "data",  # Absolute from file location
            Path.cwd().parent / "research" / "data",  # From current working directory
            Path.cwd() / "research" / "data",  # If running from project root
        ]

        research_data_path = None
        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                research_data_path = candidate
                break

        if research_data_path is None:
            logger.warning(f"Could not find research/data directory. Tried: {[str(c) for c in candidates]}")
            return []

        directories = []
        for item in sorted(research_data_path.iterdir()):
            if item.is_dir():
                directories.append({
                    "value": f"../research/data/{item.name}",
                    "label": item.name
                })

        return directories

    except Exception as e:
        logger.error(f"Error listing directories: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema-files")
async def list_schema_files():
    """List available initial schema files from research/experiments/configurations folder.

    Returns a list of JSON files that contain valid schema arrays (with name, definition, rationale).
    Each file can optionally include allowed_values for columns.
    """
    try:
        # Try multiple path resolution strategies
        candidates = [
            Path("../research/experiments/configurations"),  # When running from backend/
            Path(__file__).parent.parent.parent.parent.parent / "research" / "experiments" / "configurations",
            Path.cwd().parent / "research" / "experiments" / "configurations",
            Path.cwd() / "research" / "experiments" / "configurations",
        ]

        config_path = None
        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                config_path = candidate
                break

        if config_path is None:
            logger.warning(f"Could not find research/experiments/configurations directory. Tried: {[str(c) for c in candidates]}")
            return []

        schema_files = []
        for item in sorted(config_path.iterdir()):
            if item.is_file() and item.suffix == '.json':
                # Try to validate it's a schema file (array of columns)
                try:
                    data = json.loads(item.read_text(encoding="utf-8"))
                    # Check if it's a list of column definitions
                    if isinstance(data, list) and len(data) > 0:
                        first_item = data[0]
                        if isinstance(first_item, dict) and "name" in first_item and "definition" in first_item:
                            # Valid schema file
                            columns_preview = [col.get("name", "?") for col in data[:3]]
                            preview = ", ".join(columns_preview)
                            if len(data) > 3:
                                preview += f" (+{len(data) - 3} more)"

                            schema_files.append({
                                "value": f"research/experiments/configurations/{item.name}",
                                "label": item.stem,  # Filename without extension
                                "columns_count": len(data),
                                "preview": preview,
                                "columns": [
                                    {
                                        "name": col.get("name", ""),
                                        "definition": col.get("definition", ""),
                                        "rationale": col.get("rationale", ""),
                                        "allowed_values": col.get("allowed_values")
                                    }
                                    for col in data
                                ]
                            })
                except (json.JSONDecodeError, KeyError):
                    # Skip invalid JSON or non-schema files
                    continue

        return schema_files

    except Exception as e:
        logger.error(f"Error listing schema files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/{session_id}")
async def export_schematiq_data(
    session_id: str,
    column_order: Optional[str] = None,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export ScheMatiQ data as CSV with excerpts in separate columns.

    Args:
        session_id: The session ID to export
        column_order: Optional comma-separated list of column names in desired order
        tz_offset: Timezone offset in minutes from UTC (for filename timestamp)
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        data = await schematiq_runner.get_data(session_id, page=0, page_size=10000)
        return export_schematiq_csv(
            session, data, column_order=column_order, tz_offset=tz_offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-complete/{session_id}")
async def export_complete_schematiq_data(
    session_id: str,
    format: str = "json",
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export complete ScheMatiQ data with schema metadata in multiple formats."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        data = await schematiq_runner.get_data(session_id, page=0, page_size=10000)
        return await export_complete_schematiq(
            session, session_id, data, format=format, tz_offset=tz_offset
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in export_complete_schematiq_data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-rich-csv/{session_id}")
async def export_schematiq_rich_csv(
    session_id: str,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export ScheMatiQ data as metadata-rich CSV with definition and rationale columns."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        data = await schematiq_runner.get_data(session_id, page=0, page_size=10000)
        return export_schematiq_rich_csv_data(
            session, session_id, data, tz_offset=tz_offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-schema/{session_id}")
async def export_schematiq_schema_only(
    session_id: str,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export only the ScheMatiQ schema metadata."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        return export_schematiq_schema_json(session, session_id, tz_offset=tz_offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in export_schematiq_schema_only: {e}")
        raise HTTPException(status_code=500, detail=str(e))