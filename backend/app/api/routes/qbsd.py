"""QBSD API endpoints."""

import uuid
import asyncio
import csv
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.models.session import VisualizationSession, SessionType, SessionMetadata, FilterSortRequest
from app.models.qbsd import QBSDConfig, QBSDStatus, CostEstimate, CostEstimateRequest, PhaseEstimate, DocumentStats
from app.services.qbsd_runner import QBSDRunner
from app.services.data_editor import DataEditor
from app.services import websocket_manager, session_manager
from app.storage import get_storage
from app.services.usage_tracker import UsageTracker
from app.core.config import USER_BUDGET_USD

from qbsd.core.cost_estimator import estimate_from_config

router = APIRouter()
# Create shared QBSD runner instance with shared managers
qbsd_runner = QBSDRunner(websocket_manager=websocket_manager, session_manager=session_manager)
# Create data editor instance
data_editor = DataEditor()
usage_tracker = UsageTracker()

# Project root for path resolution (backend/app/api/routes -> project root = 5 levels up)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent


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
        candidates.insert(1, Path("./data") / session_id / "pending_documents")
    
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


_SERVER_KEY_ENV_MAP = {
    "openai": "OPENAI_API_KEY",
    "together": "TOGETHER_API_KEY",
    "gemini": "GEMINI_API_KEY",
}


def _get_client_id(request: Request, session_id: str) -> str:
    return request.headers.get("X-Client-Id") or session_id


def _server_key_available(provider: str) -> bool:
    env_key = _SERVER_KEY_ENV_MAP.get(provider.lower())
    return bool(env_key and os.getenv(env_key))


def _load_documents_for_config(config_data: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    """Load documents for cost estimation. Always reads file content so tiktoken can be used."""
    documents: List[str] = []

    docs_path = config_data.get("docs_path")
    if docs_path:
        paths = [docs_path] if isinstance(docs_path, str) else docs_path
        for path in paths:
            resolved = _resolve_docs_path(path, session_id)
            if resolved:
                documents.extend(_load_documents_from_path(resolved))

    # Always load from pending_documents if they exist
    upload_dir = Path("./data") / session_id / "pending_documents"
    if upload_dir.exists():
        documents.extend(_load_documents_from_path(upload_dir))

    # tiktoken will be used by estimate_from_config when documents are provided
    return {"documents": documents, "document_token_counts": None}


def _calculate_server_cost(config: QBSDConfig, estimate: CostEstimate) -> float:
    total = 0.0
    if not (config.schema_creation_backend.api_key or "").strip():
        total += estimate.schema_discovery.cost_usd
    if not config.skip_value_extraction and not (config.value_extraction_backend.api_key or "").strip():
        total += estimate.value_extraction.cost_usd
    return round(total, 6)


@router.get("/llm-availability")
async def get_llm_availability():
    """Return which providers have server-side API keys configured."""
    providers = {
        name: _server_key_available(name)
        for name in _SERVER_KEY_ENV_MAP.keys()
    }
    available = [name for name, enabled in providers.items() if enabled]
    return {
        "providers": providers,
        "available_providers": available,
        "budget_usd": USER_BUDGET_USD,
    }


@router.get("/usage")
async def get_usage(request: Request):
    """Return usage info for the current client ID."""
    user_id = _get_client_id(request, "anonymous")
    return usage_tracker.get_usage(user_id)

@router.post("/configure", response_model=dict)
async def configure_qbsd(config: QBSDConfig):
    """Configure a new QBSD session."""
    try:
        print(f"DEBUG: Received QBSD config: {config}")
        
        # Validate configuration
        validation = await qbsd_runner.validate_config(config)
        
        print(f"DEBUG: Validation result: {validation}")
        
        if not validation["is_valid"]:
            raise HTTPException(status_code=400, detail=validation["errors"])
        
        # Create session
        session_id = str(uuid.uuid4())
        metadata = SessionMetadata(source=f"QBSD Query: {config.query[:50]}...")
        
        session = VisualizationSession(
            id=session_id,
            type=SessionType.QBSD,
            metadata=metadata,
            schema_query=config.query
        )
        
        # Store session and config
        session_manager.create_session(session)
        await qbsd_runner.save_config(session_id, config)
        
        return {
            "session_id": session_id,
            "message": "QBSD session configured successfully"
        }
        
    except Exception as e:
        print(f"DEBUG: Exception in configure_qbsd: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/estimate-cost/{session_id}", response_model=CostEstimate)
async def estimate_qbsd_cost(session_id: str):
    """Estimate cost for QBSD execution before running.
    
    Returns estimated token counts, API calls, and costs broken down by phase
    (schema discovery + value extraction) as well as totals.
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Load the saved config for this session
        config_file = Path("./qbsd_work") / session_id / "config.json"
        if not config_file.exists():
            raise HTTPException(
                status_code=400,
                detail="No configuration found for this session. Configure QBSD first."
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
        upload_dir = Path("./data") / session_id / "pending_documents"
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
async def estimate_qbsd_cost_preview(request: CostEstimateRequest):
    """Estimate cost for QBSD execution without saving a session.
    
    This is useful for getting a cost estimate before committing to a configuration.
    Takes the full QBSDConfig directly in the request body.
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
            # Load from docs_path (local or cloud datasets)
            docs_path = config.docs_path
            if docs_path:
                paths = [docs_path] if isinstance(docs_path, str) else docs_path
                unresolved_paths = []
                for path in paths:
                    resolved = _resolve_docs_path(path)
                    if resolved:
                        documents.extend(_load_documents_from_path(resolved))
                    else:
                        unresolved_paths.append(path)

                # If nothing was loaded locally, try cloud storage sizes
                if not documents and unresolved_paths:
                    storage = get_storage()
                    cloud_token_counts = []
                    for path in unresolved_paths:
                        dataset_name = Path(path).name
                        files = await storage.list_dataset_files(dataset_name)
                        if files:
                            cloud_token_counts.extend(
                                max(1, file_info.size // 4) for file_info in files
                            )
                    if cloud_token_counts:
                        document_token_counts = cloud_token_counts
        
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
async def run_qbsd(session_id: str, background_tasks: BackgroundTasks, request: Request):
    """Start QBSD execution."""
    try:
        session = session_manager.get_session(session_id)
        if not session or session.type != SessionType.QBSD:
            raise HTTPException(status_code=404, detail="QBSD session not found")

        config_file = Path("./qbsd_work") / session_id / "config.json"
        if not config_file.exists():
            raise HTTPException(status_code=400, detail="No configuration found for this session")

        with open(config_file) as f:
            config_data = json.load(f)
        config = QBSDConfig(**config_data)

        # Validate server key availability when user did not provide a key
        schema_provider = (config.schema_creation_backend.provider or "").lower()
        value_provider = (config.value_extraction_backend.provider or "").lower()
        if not (config.schema_creation_backend.api_key or "").strip() and not _server_key_available(schema_provider):
            raise HTTPException(
                status_code=400,
                detail=f"No server API key configured for {schema_provider}. Please provide your own API key."
            )
        if not config.skip_value_extraction and not (config.value_extraction_backend.api_key or "").strip():
            if not _server_key_available(value_provider):
                raise HTTPException(
                    status_code=400,
                    detail=f"No server API key configured for {value_provider}. Please provide your own API key."
                )

        # Enforce per-user budget when using server keys
        estimate_inputs = _load_documents_for_config(config_data, session_id)
        estimate_result = estimate_from_config(
            estimate_inputs["documents"],
            config_data,
            document_token_counts=estimate_inputs["document_token_counts"],
        )
        estimate = _convert_estimate_result(estimate_result)
        server_cost = _calculate_server_cost(config, estimate)

        if server_cost > 0:
            user_id = _get_client_id(request, session_id)
            allowance = usage_tracker.can_spend(user_id, server_cost)
            if not allowance["allowed"]:
                remaining = allowance["remaining_usd"]
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"Budget exceeded. Remaining ${remaining:.2f}, "
                        f"attempted ${server_cost:.2f}."
                    ),
                )

            usage_tracker.add_spend(
                user_id,
                server_cost,
                metadata={
                    "session_id": session_id,
                    "schema_provider": schema_provider,
                    "value_provider": value_provider,
                    "estimated_cost_usd": estimate.total_cost_usd,
                },
            )
        
        # Start QBSD in background
        background_tasks.add_task(qbsd_runner.run_qbsd, session_id)
        
        return {"message": "QBSD execution started", "session_id": session_id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{session_id}", response_model=QBSDStatus)
async def get_qbsd_status(session_id: str):
    """Get QBSD execution status."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        status = await qbsd_runner.get_status(session_id)
        
        return status
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/{session_id}")
async def get_qbsd_schema(session_id: str):
    """Get discovered schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        schema = await qbsd_runner.get_schema(session_id)
        
        return schema
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data/{session_id}")
async def get_qbsd_data_with_filters(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
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

        data = await qbsd_runner.get_data(
            session_id, page, page_size,
            filters=filters, sort=sort, search=search
        )

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{session_id}")
async def get_qbsd_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get extracted data (backward compatible, no filtering)."""
    return await get_qbsd_data_with_filters(session_id, page, page_size, None)


@router.put("/cell/{session_id}")
async def update_cell(session_id: str, row_name: str, column: str, value: str):
    """Update a single cell value in the data table."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        result = await data_editor.update_cell(session_id, row_name, column, value)
        return result

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop/{session_id}")
async def stop_qbsd(session_id: str):
    """Stop QBSD execution gracefully.

    Returns information about what partial results were saved.
    """
    try:
        result = await qbsd_runner.stop_execution(session_id)

        if not result["stopped"]:
            raise HTTPException(status_code=404, detail=result["message"])

        return {
            "status": "stopped",
            "message": result["message"],
            "schema_saved": result["schema_saved"],
            "data_rows_saved": result["data_rows_saved"]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sessions", response_model=List[VisualizationSession])
async def list_qbsd_sessions():
    """List all QBSD sessions."""
    return session_manager.list_sessions(SessionType.QBSD)


@router.get("/directories")
async def list_document_directories():
    """List available document directories from research/data folder.

    Returns a list of directories that can be used as document paths for QBSD.
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
            print(f"DEBUG: Could not find research/data directory. Tried: {[str(c) for c in candidates]}")
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
        print(f"DEBUG: Error listing directories: {e}")
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
            print(f"DEBUG: Could not find research/experiments/configurations directory. Tried: {[str(c) for c in candidates]}")
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
        print(f"DEBUG: Error listing schema files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/{session_id}")
async def export_qbsd_data(
    session_id: str,
    column_order: Optional[str] = None,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export QBSD data as CSV with excerpts in separate columns.

    Args:
        session_id: The session ID to export
        column_order: Optional comma-separated list of column names in desired order
        tz_offset: Timezone offset in minutes from UTC (for filename timestamp)
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get all data
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)  # Get all data

        # Schema-only mode: no rows is valid, we'll export just the schema
        is_schema_only = not data.rows
        
        # Prepare CSV data with metadata
        output = io.StringIO()
        
        # Add schema metadata as CSV comments
        output.write("# QBSD Export with Schema Metadata\n")
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        output.write(f"# Generated: {user_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
        # Include observation_unit if available
        if session.observation_unit:
            obs_unit_json = json.dumps(session.observation_unit.model_dump())
            output.write(f"# Observation Unit: {obs_unit_json}\n")

        # Load and include LLM configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    if "schema_creation_backend" in qbsd_config:
                        backend = qbsd_config["schema_creation_backend"]
                        output.write(f"# Schema Creation: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
                    if "value_extraction_backend" in qbsd_config:
                        backend = qbsd_config["value_extraction_backend"]
                        output.write(f"# Value Extraction: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
                    elif "backend" in qbsd_config:  # Legacy support
                        backend = qbsd_config["backend"]
                        output.write(f"# AI Model: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
            except Exception as e:
                output.write(f"# LLM Config: Error loading ({e})\n")
        
        output.write("#\n")
        output.write("# Column Definitions:\n")
        
        # Add column metadata for each schema column
        for col in session.columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                output.write(f"# {col.name}: {col.definition or 'No definition available'}\n")
                if col.rationale:
                    output.write(f"#   Rationale: {col.rationale}\n")
                if col.allowed_values:
                    output.write(f"#   Allowed Values: {', '.join(col.allowed_values)}\n")

        # Add special column explanations
        output.write("# row_name: Identifier for this data row\n")
        output.write("# papers: Source documents used for extraction\n")
        output.write("# {column}_excerpt: Supporting evidence from documents for {column}\n")
        output.write("#\n")

        # Add schema evolution summary if available
        if session.statistics and session.statistics.schema_evolution:
            evolution = session.statistics.schema_evolution
            output.write("# Schema Evolution:\n")
            for snapshot in evolution.snapshots:
                # Include document names if available
                docs_str = ", ".join(snapshot.documents_processed[:3]) if snapshot.documents_processed else ""
                if len(snapshot.documents_processed) > 3:
                    docs_str += f"... (+{len(snapshot.documents_processed) - 3} more)"

                if snapshot.new_columns:
                    cols_str = ", ".join(snapshot.new_columns[:5])
                    if len(snapshot.new_columns) > 5:
                        cols_str += f"... (+{len(snapshot.new_columns) - 5} more)"
                    if docs_str:
                        output.write(f"# Iteration {snapshot.iteration}: +{len(snapshot.new_columns)} columns [{cols_str}] from [{docs_str}] (total: {snapshot.total_columns})\n")
                    else:
                        output.write(f"# Iteration {snapshot.iteration}: +{len(snapshot.new_columns)} columns [{cols_str}] (total: {snapshot.total_columns})\n")
                else:
                    # Write iterations with no new columns too (for accurate iteration count)
                    if docs_str:
                        output.write(f"# Iteration {snapshot.iteration}: +0 columns from [{docs_str}] (total: {snapshot.total_columns})\n")
                    else:
                        output.write(f"# Iteration {snapshot.iteration}: +0 columns (total: {snapshot.total_columns})\n")
            output.write(f"# Total: {len(evolution.column_sources)} columns from {len(evolution.snapshots)} iterations\n")
            # Write column sources mapping
            if evolution.column_sources:
                output.write("# Column Sources:\n")
                for col_name, source in evolution.column_sources.items():
                    output.write(f"#   {col_name}: {source}\n")
            output.write("#\n")

        # Write document processing metadata if present
        if session.statistics:
            if session.statistics.total_documents:
                output.write(f"# Total Documents: {session.statistics.total_documents}\n")
            if session.statistics.skipped_documents:
                skipped_docs = session.statistics.skipped_documents
                output.write(f"# Skipped Documents: {json.dumps(skipped_docs)}\n")
            if session.statistics.total_documents or session.statistics.skipped_documents:
                output.write("#\n")
        
        # Determine all column names including excerpt columns
        all_columns = set()
        if is_schema_only:
            # Schema-only mode: use column names from schema
            output.write("# NOTE: Schema-only export (no value extraction performed)\n")
            output.write("#\n")
            all_columns.add('row_name')
            all_columns.add('papers')
            for col in session.columns:
                if col.name:
                    all_columns.add(col.name)
        else:
            for row in data.rows:
                if row.row_name:
                    all_columns.add('row_name')
                if row.papers:
                    all_columns.add('papers')
                # Include observation unit fields if present
                if row.unit_name:
                    all_columns.add('_unit_name')
                if row.source_document:
                    all_columns.add('_source_document')
                for col_name in row.data.keys():
                    all_columns.add(col_name)
                    # Add excerpt column for QBSD data
                    if isinstance(row.data[col_name], dict) and 'excerpts' in row.data[col_name]:
                        all_columns.add(f"{col_name}_excerpt")
        
        # Determine column order - use user-specified order if provided
        if column_order:
            # Parse user-specified column order
            requested_order = [col.strip() for col in column_order.split(',')]
            # Filter to only include columns that exist, preserving requested order
            column_names = [col for col in requested_order if col in all_columns]
            # Add any remaining columns not in the requested order
            remaining_columns = sorted([col for col in all_columns if col not in column_names])
            column_names.extend(remaining_columns)
        else:
            # Default: sorted alphabetically
            column_names = sorted(list(all_columns))

        writer = csv.DictWriter(output, fieldnames=column_names)
        writer.writeheader()
        
        # Write data rows
        for row in data.rows:
            csv_row = {}

            # Add standard columns
            if row.row_name:
                csv_row['row_name'] = row.row_name
            if row.papers:
                csv_row['papers'] = '; '.join(row.papers) if isinstance(row.papers, list) else str(row.papers)
            # Add observation unit fields
            if row.unit_name:
                csv_row['_unit_name'] = row.unit_name
            if row.source_document:
                csv_row['_source_document'] = row.source_document

            # Process data columns
            for col_name, value in row.data.items():
                if isinstance(value, dict) and 'answer' in value:
                    # QBSD format: extract answer and excerpts
                    csv_row[col_name] = value['answer']
                    if 'excerpts' in value and value['excerpts']:
                        excerpt_col = f"{col_name}_excerpt"
                        if isinstance(value['excerpts'], list):
                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                        else:
                            csv_row[excerpt_col] = str(value['excerpts'])
                else:
                    # Regular data
                    if isinstance(value, (list, dict)):
                        csv_row[col_name] = str(value)  # Convert complex types to string
                    else:
                        csv_row[col_name] = value
            
            writer.writerow(csv_row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename with datetime in user's timezone
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{'schema_only_' if is_schema_only else ''}{timestamp}.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-complete/{session_id}")
async def export_complete_qbsd_data(
    session_id: str,
    format: str = "json",
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export complete QBSD data with schema metadata in multiple formats."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get schema and data
        schema = await qbsd_runner.get_schema(session_id)
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)
        
        # Load QBSD configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        llm_configuration = None
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    llm_configuration = {
                        "schema_creation_backend": qbsd_config.get("schema_creation_backend"),
                        "value_extraction_backend": qbsd_config.get("value_extraction_backend")
                    }
            except Exception as e:
                print(f"DEBUG: Could not load LLM configuration for complete export: {e}")
        
        # Prepare complete export data structure
        export_data = {
            "session_id": session_id,
            "session_type": session.type.value,
            "created": session.metadata.created.isoformat(),
            "last_modified": session.metadata.last_modified.isoformat(),
            "query": session.schema_query,
            "schema": {
                "columns": [
                    {
                        "name": col.name,
                        "definition": col.definition or "",
                        "rationale": col.rationale or "",
                        "data_type": col.data_type,
                        "source_document": col.source_document,
                        "discovery_iteration": col.discovery_iteration
                    }
                    for col in session.columns
                    if col.name and not col.name.lower().endswith('_excerpt')
                ]
            },
            "metadata": {
                "total_rows": data.total_count,
                "total_columns": len([col for col in session.columns if not col.name.lower().endswith('_excerpt')]),
                "source": session.metadata.source,
                "schema_discovery_completed": session.metadata.schema_discovery_completed,
                "total_documents": session.metadata.total_documents,
                "processed_documents": session.metadata.processed_documents
            },
            "data": [
                {
                    "row_name": row.row_name,
                    "papers": row.papers,
                    "data": row.data
                }
                for row in data.rows
            ]
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            export_data["schema_evolution"] = session.statistics.schema_evolution.model_dump()

        # Include observation_unit if available
        if session.observation_unit:
            export_data["observation_unit"] = session.observation_unit.model_dump()

        # Include LLM configuration if available
        if llm_configuration and any(llm_configuration.values()):
            export_data["llm_configuration"] = llm_configuration

        # Handle different export formats
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        if format.lower() == "json":
            # JSON format with complete metadata
            content = json.dumps(export_data, indent=2, ensure_ascii=False, default=str)
            timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"QBSD_{timestamp}_complete.json"
            
            return StreamingResponse(
                io.BytesIO(content.encode('utf-8')),
                media_type='application/json',
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
            
        elif format.lower() == "zip":
            # ZIP package with separate files
            import zipfile
            import tempfile
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                with zipfile.ZipFile(tmp_file.name, 'w') as zip_file:
                    # Add schema file
                    schema_data = {
                        "query": export_data["query"],
                        "schema": [
                            {
                                "name": col["name"],
                                "definition": col["definition"],
                                "rationale": col["rationale"]
                            }
                            for col in export_data["schema"]["columns"]
                        ]
                    }
                    
                    # Include LLM configuration if available
                    if llm_configuration and any(llm_configuration.values()):
                        schema_data["llm_configuration"] = llm_configuration
                    
                    zip_file.writestr("schema.json", json.dumps(schema_data, indent=2))
                    
                    # Add separate column metadata CSV for easy reference
                    metadata_output = io.StringIO()
                    metadata_writer = csv.writer(metadata_output)
                    metadata_writer.writerow(["Column Name", "Definition", "Rationale", "Data Type"])
                    
                    for col in export_data["schema"]["columns"]:
                        metadata_writer.writerow([
                            col["name"],
                            col["definition"],
                            col["rationale"],
                            col.get("data_type", "text")
                        ])
                    
                    # Add special columns documentation
                    metadata_writer.writerow(["row_name", "Identifier for this data row", "Standard QBSD metadata field", "text"])
                    metadata_writer.writerow(["papers", "Source documents used for extraction", "Standard QBSD metadata field", "text"])
                    metadata_writer.writerow(["{column}_excerpt", "Supporting evidence from documents", "Generated for each QBSD data column", "text"])
                    
                    zip_file.writestr("column_metadata.csv", metadata_output.getvalue())
                    
                    # Add data file as CSV with QBSD format handling
                    if export_data["data"]:
                        output = io.StringIO()
                        
                        # Determine all column names including excerpt columns
                        all_columns = set()
                        for row in export_data["data"]:
                            if row["row_name"]:
                                all_columns.add('row_name')
                            if row["papers"]:
                                all_columns.add('papers')
                            for col_name in row["data"].keys():
                                all_columns.add(col_name)
                                # Add excerpt column for QBSD data
                                if isinstance(row["data"][col_name], dict) and 'excerpts' in row["data"][col_name]:
                                    all_columns.add(f"{col_name}_excerpt")
                        
                        column_names = sorted(list(all_columns))
                        writer = csv.DictWriter(output, fieldnames=column_names)
                        writer.writeheader()
                        
                        # Write data rows with QBSD format handling
                        for row in export_data["data"]:
                            csv_row = {}
                            
                            # Add standard columns
                            if row["row_name"]:
                                csv_row['row_name'] = row["row_name"]
                            if row["papers"]:
                                csv_row['papers'] = '; '.join(row["papers"]) if isinstance(row["papers"], list) else str(row["papers"])
                            
                            # Process data columns
                            for col_name, value in row["data"].items():
                                if isinstance(value, dict) and 'answer' in value:
                                    # QBSD format: extract answer and excerpts
                                    csv_row[col_name] = value['answer']
                                    if 'excerpts' in value and value['excerpts']:
                                        excerpt_col = f"{col_name}_excerpt"
                                        if isinstance(value['excerpts'], list):
                                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                                        else:
                                            csv_row[excerpt_col] = str(value['excerpts'])
                                else:
                                    # Regular data
                                    if isinstance(value, (list, dict)):
                                        csv_row[col_name] = str(value)
                                    else:
                                        csv_row[col_name] = value
                            
                            writer.writerow(csv_row)
                        
                        zip_file.writestr("data.csv", output.getvalue())
                    
                    # Add metadata file
                    zip_file.writestr("metadata.json", json.dumps(export_data["metadata"], indent=2, default=str))
                
                # Read and return zip file
                with open(tmp_file.name, 'rb') as f:
                    zip_content = f.read()
                
                # Clean up temp file
                Path(tmp_file.name).unlink()

                timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"QBSD_{timestamp}_complete.zip"
                return StreamingResponse(
                    io.BytesIO(zip_content),
                    media_type='application/zip',
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
                
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'zip'")
        
    except Exception as e:
        print(f"DEBUG: Exception in export_complete_qbsd_data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-rich-csv/{session_id}")
async def export_qbsd_rich_csv(
    session_id: str,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export QBSD data as metadata-rich CSV with definition and rationale columns."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get all data
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)
        
        if not data.rows:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Prepare metadata-rich CSV
        output = io.StringIO()
        
        # Create extended column set with metadata
        base_columns = set()
        for row in data.rows:
            if row.row_name:
                base_columns.add('row_name')
            if row.papers:
                base_columns.add('papers')
            # Include observation unit fields if present
            if row.unit_name:
                base_columns.add('_unit_name')
            if row.source_document:
                base_columns.add('_source_document')
            for col_name in row.data.keys():
                base_columns.add(col_name)
                # Add excerpt column for QBSD data
                if isinstance(row.data[col_name], dict) and 'excerpts' in row.data[col_name]:
                    base_columns.add(f"{col_name}_excerpt")
        
        # Build enhanced column list with metadata columns
        enhanced_columns = []
        for col_name in sorted(base_columns):
            enhanced_columns.append(col_name)
            # Add metadata columns for schema columns (not for standard or excerpt columns)
            if (col_name not in ['row_name', 'papers', '_unit_name', '_source_document'] and
                not col_name.endswith('_excerpt')):
                enhanced_columns.append(f"{col_name}_definition")
                enhanced_columns.append(f"{col_name}_rationale")
                enhanced_columns.append(f"{col_name}_allowed_values")
        
        writer = csv.DictWriter(output, fieldnames=enhanced_columns)
        
        # Write metadata header rows first
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        output.write("# Metadata-Rich CSV Export\n")
        output.write(f"# Generated: {user_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
        # Include observation_unit if available
        if session.observation_unit:
            obs_unit_json = json.dumps(session.observation_unit.model_dump())
            output.write(f"# Observation Unit: {obs_unit_json}\n")
        output.write("# Format: Each data column has corresponding _definition and _rationale columns\n")
        output.write("#\n")
        
        # Write CSV headers
        writer.writeheader()
        
        # Create column metadata lookup
        column_metadata = {}
        for col in session.columns:
            if col.name:
                column_metadata[col.name] = {
                    'definition': col.definition or '',
                    'rationale': col.rationale or '',
                    'allowed_values': ', '.join(col.allowed_values) if col.allowed_values else ''
                }

        # Write data rows with metadata
        for row in data.rows:
            csv_row = {}

            # Add standard columns
            if row.row_name:
                csv_row['row_name'] = row.row_name
            if row.papers:
                csv_row['papers'] = '; '.join(row.papers) if isinstance(row.papers, list) else str(row.papers)
            # Add observation unit fields
            if row.unit_name:
                csv_row['_unit_name'] = row.unit_name
            if row.source_document:
                csv_row['_source_document'] = row.source_document

            # Process data columns with metadata
            for col_name, value in row.data.items():
                if isinstance(value, dict) and 'answer' in value:
                    # QBSD format: extract answer and excerpts
                    csv_row[col_name] = value['answer']
                    if 'excerpts' in value and value['excerpts']:
                        excerpt_col = f"{col_name}_excerpt"
                        if isinstance(value['excerpts'], list):
                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                        else:
                            csv_row[excerpt_col] = str(value['excerpts'])
                else:
                    # Regular data
                    if isinstance(value, (list, dict)):
                        csv_row[col_name] = str(value)
                    else:
                        csv_row[col_name] = value

                # Add metadata columns for this data column
                if col_name in column_metadata:
                    csv_row[f"{col_name}_definition"] = column_metadata[col_name]['definition']
                    csv_row[f"{col_name}_rationale"] = column_metadata[col_name]['rationale']
                    csv_row[f"{col_name}_allowed_values"] = column_metadata[col_name]['allowed_values']

            writer.writerow(csv_row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename with datetime in user's timezone
        timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{timestamp}_rich.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-schema/{session_id}")
async def export_qbsd_schema_only(
    session_id: str,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export only the QBSD schema metadata."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Load QBSD configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        llm_configuration = None
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    llm_configuration = {
                        "schema_creation_backend": qbsd_config.get("schema_creation_backend"),
                        "value_extraction_backend": qbsd_config.get("value_extraction_backend")
                    }
            except Exception as e:
                print(f"DEBUG: Could not load LLM configuration: {e}")

        # Create QBSD schema export
        schema_columns = []
        for col in session.columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                col_export = {
                    "name": col.name,
                    "definition": col.definition or "",
                    "rationale": col.rationale or "",
                    "source_document": col.source_document,
                    "discovery_iteration": col.discovery_iteration
                }
                if col.allowed_values:
                    col_export["allowed_values"] = col.allowed_values
                schema_columns.append(col_export)

        schema_export = {
            "query": session.schema_query or "",
            "schema": schema_columns,
            "metadata": {
                "session_id": session_id,
                "session_type": session.type.value,
                "created": session.metadata.created.isoformat(),
                "source": session.metadata.source,
                "total_columns": len([col for col in session.columns if not col.name.lower().endswith('_excerpt')]),
                "schema_discovery_completed": session.metadata.schema_discovery_completed,
                "export_timestamp": datetime.now().isoformat()
            }
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            schema_export["schema_evolution"] = session.statistics.schema_evolution.model_dump()

        # Include LLM configuration if available
        if llm_configuration and any(llm_configuration.values()):
            schema_export["llm_configuration"] = llm_configuration

        # Include observation_unit if available
        if session.observation_unit:
            schema_export["observation_unit"] = session.observation_unit.model_dump()

        content = json.dumps(schema_export, indent=2, ensure_ascii=False, default=str)

        # Generate filename with datetime in user's timezone
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{timestamp}_schema.json"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='application/json',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"DEBUG: Exception in export_qbsd_schema_only: {e}")
        raise HTTPException(status_code=500, detail=str(e))