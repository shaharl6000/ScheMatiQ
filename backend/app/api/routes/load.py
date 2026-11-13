"""Upload API endpoints."""

import asyncio
import logging
import uuid
import json
import random
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path
from app.core.config import DEFAULT_DATA_DIR
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel

from app.models.session import VisualizationSession, SessionType, SessionMetadata, PaginatedData, SessionStatus, FilterSortRequest
from app.services.session_capabilities import has_live_pipeline, is_imported
from app.models.upload import ColumnMappingRequest
from app.services.file_parser import FileParser, is_system_file
from app.services.export_service import (
    export_complete_upload,
    export_upload_csv,
    export_upload_rich_csv_data,
    export_upload_schema_json,
)
from app.services.pipeline.data_query import session_data_read_failed
from app.services.data_utils import (
    ensure_session_data_file_local,
    persist_session_data_file,
)
from app.services import session_manager, websocket_manager, concurrency_limiter
from app.services.upload_document_processor import UploadDocumentProcessor
from app.storage import get_storage
from app.core.exceptions import CapacityExceededError
from app.core.logging_utils import set_session_context

logger = logging.getLogger(__name__)

router = APIRouter()

# Module-level singleton (same pattern as schematiq_runner, reextraction_service, etc.)
from app.services import pubmed_enrichment_service, uniprot_enrichment_service
from app.services.session_documents import (
    committed_docs_dir,
    pending_docs_dir,
    remove_document_artifacts,
)
upload_processor = UploadDocumentProcessor(
    websocket_manager=websocket_manager,
    session_manager=session_manager,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)


# ==================
# Template Loading
# ==================

@router.post("/template/{template_name}", response_model=dict)
async def load_template(template_name: str):
    """Load a pre-made template table and create a session from it.

    This endpoint allows users to load pre-uploaded example tables
    without having to upload files manually.

    Args:
        template_name: Name of the template to load

    Returns:
        Session ID and parsing results
    """
    try:
        storage = get_storage()

        # Get template content
        template_content = await storage.download_template(template_name)
        if not template_content:
            # Get available templates for error message
            templates = await storage.list_templates()
            available = [t.name for t in templates]
            raise HTTPException(
                status_code=404,
                detail=f"Template '{template_name}' not found. Available: {available}"
            )

        # Determine file type from templates list
        templates = await storage.list_templates()
        template_info = next((t for t in templates if t.name == template_name), None)
        file_type = template_info.file_type if template_info else "csv"
        filename = f"{template_name}.{file_type}"

        # Create session
        session_id = str(uuid.uuid4())
        set_session_context(session_id)
        metadata = SessionMetadata(
            source=f"template:{template_name}",
            file_size=len(template_content)
        )

        from app.core.config import DEVELOPER_MODE
        session = VisualizationSession(
            id=session_id,
            type=SessionType.UPLOAD,
            metadata=metadata,
            write_artifacts=DEVELOPER_MODE,
        )

        # Save template content as uploaded file
        session_dir = Path(DEFAULT_DATA_DIR) / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        # Write template content to file
        file_path = session_dir / filename
        with open(file_path, 'wb') as f:
            f.write(template_content)

        # Store session
        session_manager.create_session(session)

        # Parse the template file
        parser = FileParser()
        result = await parser.parse_file(session_id, None)

        # Extract query and observation_unit from metadata if available
        if "extracted_metadata" in result:
            metadata = result["extracted_metadata"]
            if metadata.get('query'):
                session.schema_query = metadata['query']
            # Restore observation_unit from CSV comments
            if metadata.get('observation_unit'):
                from app.models.session import ObservationUnitInfo
                session.observation_unit = ObservationUnitInfo(**metadata['observation_unit'])
                logger.debug(f"Restored observation unit from template CSV: {session.observation_unit.name}")
            # Restore cloud_dataset if present
            if metadata.get('cloud_dataset'):
                session.metadata.cloud_dataset = metadata['cloud_dataset']
            # Restore document_metadata (external URLs for source documents) if present
            if metadata.get('document_metadata'):
                session.metadata.document_metadata = metadata['document_metadata']
                logger.debug(f"Restored document_metadata from template: {len(session.metadata.document_metadata)} documents with URLs")

        # Restore observation_unit from JSON parse result if present (and not already set)
        if result.get("observation_unit") and not session.observation_unit:
            from app.models.session import ObservationUnitInfo
            session.observation_unit = ObservationUnitInfo(**result["observation_unit"])
            logger.debug(f"Restored observation unit from template JSON: {session.observation_unit.name}")

        # Update session with parsed data
        session.columns = result["columns"]
        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED
        session_manager.update_session(session)

        # Capture schema baseline for re-extraction change detection
        session_manager.capture_schema_baseline(session_id)

        # Get row count from statistics (handle both dict and Pydantic object)
        stats = result["statistics"]
        row_count = stats.total_rows if hasattr(stats, 'total_rows') else stats.get("total_rows", 0) if isinstance(stats, dict) else 0

        return {
            "session_id": session_id,
            "template_name": template_name,
            "status": "success",
            "message": f"Template '{template_name}' loaded successfully",
            "row_count": row_count,
            "column_count": len(result["columns"])
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading template {template_name}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/file", response_model=dict)
async def upload_file(file: UploadFile = File(...)):
    """Upload and validate a file."""
    try:
        session_id = str(uuid.uuid4())
        set_session_context(session_id)
        logger.debug(f"Received file upload: {file.filename}, size: {file.size}")

        # Validate file
        parser = FileParser()
        validation = await parser.validate_file(file)

        logger.debug(f"File validation result: {validation}")

        if not validation.is_valid:
            raise HTTPException(status_code=400, detail=validation.errors)

        # Create session
        metadata = SessionMetadata(
            source=file.filename,
            file_size=file.size
        )

        from app.core.config import DEVELOPER_MODE
        session = VisualizationSession(
            id=session_id,
            type=SessionType.UPLOAD,
            metadata=metadata,
            write_artifacts=DEVELOPER_MODE,
        )

        logger.debug(f"Created session: {session_id}")

        # Save file content for processing
        await parser.save_uploaded_file(session_id, file)
        logger.debug(f"File saved for session: {session_id}")

        # Store session
        session_manager.create_session(session)
        logger.debug("Session stored")

        return {
            "session_id": session_id,
            "validation": validation,
            "requires_column_mapping": validation.detected_format == "csv"
        }

    except Exception as e:
        logger.error(f"Exception in upload_file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/parse/{session_id}", response_model=dict)
async def parse_file(session_id: str, mapping: Optional[ColumnMappingRequest] = None):
    """Parse uploaded file with optional column mapping."""
    try:
        set_session_context(session_id)
        logger.debug(f"Parsing file for session: {session_id}")

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        logger.debug(f"Found session: {session}")

        parser = FileParser()
        result = await parser.parse_file(session_id, mapping)

        logger.debug(f"Parse result: columns={len(result['columns'])}, statistics={result['statistics']}")

        # Check for extracted metadata from CSV comments
        if "extracted_metadata" in result:
            metadata = result["extracted_metadata"]
            logger.debug(f"Found CSV metadata - query: {metadata.get('query')}, LLM config: {bool(metadata.get('llm_config'))}")

            # Store extracted query
            if metadata.get('query'):
                session.schema_query = metadata['query']

            # Restore observation_unit if present (from CSV comments)
            if metadata.get('observation_unit'):
                from app.models.session import ObservationUnitInfo
                session.observation_unit = ObservationUnitInfo(**metadata['observation_unit'])
                logger.debug(f"Restored observation unit from CSV: {session.observation_unit.name}")

            # Restore cloud_dataset if present
            if metadata.get('cloud_dataset'):
                session.metadata.cloud_dataset = metadata['cloud_dataset']
                logger.debug(f"Restored cloud_dataset: {session.metadata.cloud_dataset}")

            # Restore document_metadata (external URLs for source documents) if present
            if metadata.get('document_metadata'):
                session.metadata.document_metadata = metadata['document_metadata']
                logger.debug(f"Restored document_metadata: {len(session.metadata.document_metadata)} documents with URLs")

            # Create a parsed schema file with the extracted LLM configuration
            if metadata.get('llm_config'):
                session_dir = Path(DEFAULT_DATA_DIR) / session_id
                parsed_schema_file = session_dir / "parsed_schema.json"
                
                schema_data = {
                    "query": metadata.get('query', ''),
                    "schema": [
                        {
                            "name": col.name,
                            "definition": col.definition or '',
                            "rationale": col.rationale or ''
                        }
                        for col in result["columns"]
                    ],
                    "llm_configuration": metadata['llm_config'],
                    "metadata": {
                        "imported_from_csv": True,
                        "original_session_id": metadata.get('original_session_id'),
                        "generated_timestamp": metadata.get('generated_timestamp'),
                        "import_timestamp": datetime.now().isoformat()
                    }
                }
                
                with open(parsed_schema_file, 'w') as f:
                    json.dump(schema_data, f, indent=2)

                # Also populate the session's extracted_schema for frontend access
                session.metadata.extracted_schema = schema_data

                logger.debug("Saved parsed schema with extracted LLM configuration")

        # Restore observation_unit from JSON parse result if present (and not already set from CSV metadata)
        if result.get("observation_unit") and not session.observation_unit:
            from app.models.session import ObservationUnitInfo
            session.observation_unit = ObservationUnitInfo(**result["observation_unit"])
            logger.debug(f"Restored observation unit from JSON: {session.observation_unit.name}")

        # Update session with parsed data
        session.columns = result["columns"]
        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED  # Mark as completed after successful parsing
        session_manager.update_session(session)

        # Capture schema baseline for re-extraction change detection
        session_manager.capture_schema_baseline(session_id)

        # Persist parsed rows to durable storage. FileParser writes data.jsonl
        # only to the container-local data dir, which is ephemeral on Railway;
        # without this upload the rows are lost on the next redeploy while the
        # separately-persisted session metadata survives, leaving the schema
        # visible above an empty table. Mirrors the run-completion persist (#407)
        # for the import path. Best-effort: the helper logs and swallows failures.
        await persist_session_data_file(
            session_id, parser.data_dir / session_id / "data.jsonl"
        )

        logger.debug("Session updated successfully")
        
        # Include metadata info in response if available
        response = {"status": "success", "message": "File parsed successfully"}
        if "extracted_metadata" in result:
            response["extracted_metadata"] = {
                "has_metadata": True,
                "query_found": bool(result["extracted_metadata"].get('query')),
                "llm_config_found": bool(result["extracted_metadata"].get('llm_config')),
                "columns_with_metadata": result["extracted_metadata"].get('column_count_with_metadata', 0)
            }
        
        return response

    except Exception as e:
        logger.error(f"Exception in parse_file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data/{session_id}", response_model=PaginatedData)
async def get_data_with_filters(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
    documents: Optional[str] = Query(None, description="Comma-separated document names to filter by"),
    request: Optional[FilterSortRequest] = None
):
    """Get paginated data with optional filtering and sorting."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        parser = FileParser()

        # Imported/load-mode rows live in data.jsonl, which is container-local and
        # does not survive a Railway redeploy. Re-download it from storage when the
        # local copy is gone before reading, mirroring how the schematiq read path
        # hydrates. Without this the table renders empty (columns from the persisted
        # schema, no rows) after every redeploy. No-op when the file already exists.
        await ensure_session_data_file_local(
            session_id, parser.data_dir / session_id / "data.jsonl"
        )

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

        data = await parser.get_paginated_data(
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
        logger.error(f"Exception in get_data_with_filters: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{session_id}", response_model=PaginatedData)
async def get_data(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
    documents: Optional[str] = Query(None, description="Comma-separated document names to filter by"),
):
    """Get paginated data for a session (backward compatible, no filtering)."""
    return await get_data_with_filters(session_id, page, page_size, documents, None)

# NOTE: the unauthenticated "list every session" endpoint was removed here.
# These routes have no auth (main.py installs only CORSMiddleware, which is
# browser-only), so the listing let any caller enumerate every project id on
# the deployment and then read any project's data, documents and chat. The
# frontend never called it: frontend/src/utils/recentProjects.ts keeps a
# per-browser registry of ids precisely because the server has no user scoping.
# Restoring a listing endpoint requires an auth/ownership model first.

@router.get("/sessions/{session_id}", response_model=VisualizationSession)
async def get_session(session_id: str):
    """Get session details."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session

@router.get("/export/{session_id}")
async def export_upload_data(
    session_id: str,
    column_order: Optional[str] = None,
    tz_offset: int = Query(default=0, description="Timezone offset in minutes from UTC")
):
    """Export uploaded data as CSV.

    Args:
        session_id: The session ID to export
        column_order: Optional comma-separated list of column names in desired order
        tz_offset: Timezone offset in minutes from UTC (for filename timestamp)
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"
        return export_upload_csv(
            session, data_file, column_order=column_order, tz_offset=tz_offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-documents/{session_id}", response_model=dict)
async def add_documents(session_id: str, files: List[UploadFile] = File(...), bypass_limit: bool = Query(False)):
    """Upload documents for processing with extracted schema."""
    from app.core.config import MAX_DOCUMENTS, DEVELOPER_MODE

    try:
        set_session_context(session_id)
        logger.debug(f"Adding {len(files)} documents to session: {session_id}")

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Allow both upload sessions (various states) and ScheMatiQ sessions (created or completed)
        is_valid_upload_session = (
            is_imported(session) and
            session.status in [SessionStatus.SCHEMA_EXTRACTED, SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )
        is_valid_schematiq_session = (
            has_live_pipeline(session) and
            session.status in [SessionStatus.CREATED, SessionStatus.COMPLETED, SessionStatus.STOPPED]
        )

        if not (is_valid_upload_session or is_valid_schematiq_session):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot upload documents for session type '{session.type}' in status '{session.status}'. Upload sessions need schema extracted/completed, ScheMatiQ sessions must be created or completed."
            )

        # Document count: separate valid files from system files
        valid_files = [f for f in files if not _is_system_file(f.filename)]
        system_files = [f for f in files if _is_system_file(f.filename)]

        existing_count = len(session.metadata.uploaded_documents or [])
        new_count = len(valid_files)
        total_count = existing_count + new_count

        # Ignore bypass_limit if DEVELOPER_MODE is not enabled
        if bypass_limit and not DEVELOPER_MODE:
            logger.warning(f"bypass_limit requested but DEVELOPER_MODE is disabled (session: {session_id})")
            bypass_limit = False

        # If over limit and not bypassed, randomly select files to fit within limit
        limit_applied = False
        enforce_limit = not (DEVELOPER_MODE and bypass_limit)
        # Don't enforce document limit for completed sessions — these are complementary
        # source documents for re-extraction/continue discovery, not initial ScheMatiQ uploads
        if session.status == SessionStatus.COMPLETED:
            enforce_limit = False
        available_slots = MAX_DOCUMENTS - existing_count
        if enforce_limit and new_count > MAX_DOCUMENTS:
            # Session is full or new batch alone exceeds limit — sample from new uploads to MAX_DOCUMENTS
            random.shuffle(valid_files)
            valid_files = valid_files[:MAX_DOCUMENTS]
            limit_applied = True
            logger.debug(f"Document limit applied. Selected {len(valid_files)} of {new_count} files (max {MAX_DOCUMENTS}, existing {existing_count})")
        elif enforce_limit and available_slots > 0 and new_count > available_slots:
            # There's room but not enough — sample to fit
            random.shuffle(valid_files)
            valid_files = valid_files[:available_slots]
            limit_applied = True
            logger.debug(f"Document limit applied. Selected {len(valid_files)} of {new_count} files (max {MAX_DOCUMENTS}, existing {existing_count})")

        # Reconstruct files list: valid (possibly trimmed) + system files
        files = valid_files + system_files

        logger.debug(f"Session status: {session.status}, extracted schema available")
        
        # Validate files
        errors = []
        warnings = []
        uploaded_filenames = []
        failed_files = []
        document_extraction = {}
        
        # Create directories for this session
        # New documents go to pending_documents/ first, then moved to documents/ after processing
        parser = FileParser()
        session_dir = parser.data_dir / session_id
        pending_dir = pending_docs_dir(session_dir)
        pending_dir.mkdir(parents=True, exist_ok=True)
        docs_dir = committed_docs_dir(session_dir)
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each uploaded file
        total_size = 0
        for i, file in enumerate(files):
            logger.debug(f"Processing file {i+1}/{len(files)}: {file.filename}")

            # Skip system files that shouldn't be processed
            if _is_system_file(file.filename):
                logger.debug(f"Skipping system file: {file.filename}")
                continue
            
            # Validate file size (25MB limit per file)
            if file.size > 25 * 1024 * 1024:
                errors.append(f"File '{file.filename}' exceeds 25MB limit")
                continue
            
            total_size += file.size
            
            # Validate file type (text files, PDFs, docs, etc.)
            allowed_extensions = {'.txt', '.md', '.pdf', '.doc', '.docx', '.rtf', '.json'}
            file_ext = Path(file.filename).suffix.lower()
            
            if file_ext not in allowed_extensions:
                warnings.append(f"File '{file.filename}' has unsupported extension '{file_ext}'. Supported: {', '.join(allowed_extensions)}")
                # Continue processing - might still be text content
            
            try:
                file_path = pending_dir / file.filename
                with open(file_path, 'wb') as f:
                    content = await file.read()
                    f.write(content)

                # to_thread rather than run_in_executor: it copies the context,
                # so preprocessing logs keep the session id.
                display_name, error = await asyncio.to_thread(
                    _preprocess_and_record,
                    file_path, file.filename, docs_dir, session, document_extraction,
                )
                if error:
                    if "already processed" in error:
                        errors.append(error)
                    else:
                        failed_files.append({"filename": file.filename, "status": error})
                        errors.append(error)
                    continue

                uploaded_filenames.append(display_name)
                logger.debug(f"Saved file to pending: {file_path}")

            except Exception as e:
                errors.append(f"Failed to save file '{file.filename}': {str(e)}")
                failed_files.append({"filename": file.filename, "status": f"failed: {e}"})
        
        # Check total size limit (100MB total)
        if total_size > 100 * 1024 * 1024:
            errors.append("Total upload size exceeds 100MB limit")
        
        if not uploaded_filenames and errors:
            raise HTTPException(status_code=400, detail={"errors": errors, "warnings": warnings, "failed_files": failed_files})
        
        # Update session metadata — append new filenames to any already queued
        existing_docs = session.metadata.uploaded_documents or []
        session.metadata.uploaded_documents = existing_docs + uploaded_filenames
        # Track stems so PubMed enrichment skips user-uploaded documents
        existing_stems = set(session.metadata.pubmed_link_suppressed_stems)
        for fn in uploaded_filenames:
            existing_stems.add(Path(fn).stem)
        session.metadata.pubmed_link_suppressed_stems = sorted(existing_stems)
        session.status = SessionStatus.DOCUMENTS_UPLOADED
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        logger.debug(f"Updated session with {len(uploaded_filenames)} uploaded documents")
        
        response = {
            "status": "success",
            "message": f"Successfully uploaded {len(uploaded_filenames)} documents",
            "uploaded_files": uploaded_filenames,
            "warnings": warnings,
            "documents_directory": str(docs_dir),
            "document_extraction": document_extraction,
            "failed_files": failed_files if failed_files else None,
            "errors": errors if failed_files else None,
        }
        if limit_applied:
            response["limit_applied"] = True
            response["message"] = f"Document limit applied: randomly selected {len(uploaded_filenames)} of {new_count} uploaded documents (maximum {MAX_DOCUMENTS})."
            response["warnings"] = warnings + [f"Only {MAX_DOCUMENTS} documents are allowed. {new_count - len(uploaded_filenames)} documents were excluded by random selection."]

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in add_documents: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rediscover/{session_id}", response_model=dict)
async def rediscover_imported_session(session_id: str, background_tasks: BackgroundTasks):
    """Run schema rediscovery for an imported (UPLOAD-type) session.

    Mirrors POST /schematiq/resume/{session_id}, which is restricted to
    SessionType.SCHEMATIQ sessions and expects a config.json already written
    by /schematiq/configure. An imported project (dual-file/zip) never goes
    through that flow, so this endpoint synthesizes an equivalent config.json
    from the session's existing query/observation_unit and runs it through the
    same schematiq_runner used by every other run. Only reachable when the
    session's rows already reference real source documents (see
    precheck_document_availability below) — editing the observation unit alone
    does not make rediscovery possible for a session with no documents behind
    its data.
    """
    from app.api.routes.schematiq import QUOTA_EXCEEDED_USER_MESSAGE
    from app.services.orchestration import reextraction_service, schematiq_runner
    from app.services.rediscovery_service import (
        prepare_rediscovery,
        RediscoverySessionNotFound,
        RediscoveryNotImported,
        RediscoveryNoObservationUnit,
        RediscoveryDocumentsUnavailable,
        RediscoveryQuotaExceeded,
        RediscoveryPipelineBusy,
        RediscoveryPrepareFailed,
    )

    try:
        await prepare_rediscovery(
            session_id,
            runner=schematiq_runner,
            reextraction_service=reextraction_service,
            require_imported=True,
        )
        background_tasks.add_task(schematiq_runner.run_schematiq, session_id)

        return {"message": "Schema rediscovery started", "session_id": session_id}

    except RediscoverySessionNotFound:
        raise HTTPException(status_code=404, detail="Session not found")
    except RediscoveryNotImported as e:
        raise HTTPException(
            status_code=400,
            detail=f"Rediscovery via this endpoint is only for imported sessions (type='{e.session_type}').",
        )
    except RediscoveryNoObservationUnit:
        raise HTTPException(
            status_code=400,
            detail="Session has no observation unit configured. Set one before rediscovering.",
        )
    except RediscoveryDocumentsUnavailable:
        raise HTTPException(
            status_code=400,
            detail="No source documents are available for this project. "
                   "Add the original source documents from the Documents tab, then try again.",
        )
    except RediscoveryQuotaExceeded:
        raise HTTPException(status_code=429, detail=QUOTA_EXCEEDED_USER_MESSAGE)
    except RediscoveryPipelineBusy:
        raise HTTPException(
            status_code=409,
            detail="Pipeline is still stopping. Wait a few seconds and try again.",
        )
    except RediscoveryPrepareFailed as e:
        raise HTTPException(status_code=500, detail=e.message)

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await concurrency_limiter.release(session_id)
        logger.error(f"Exception in rediscover_imported_session: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


class CloudDocumentRequest(BaseModel):
    """Request model for adding cloud documents to a session."""
    dataset: str
    files: List[str]  # List of filenames to add from the dataset


class RemoveDocumentRequest(BaseModel):
    """Request model for removing a specific uploaded document."""
    filename: str


@router.delete("/remove-document/{session_id}", response_model=dict)
async def remove_uploaded_document(session_id: str, request: RemoveDocumentRequest):
    """Remove an uploaded document from a session before processing.

    This endpoint allows users to remove a document that was uploaded
    but not yet processed.

    Args:
        session_id: Session ID containing the document
        request: Document filename to remove

    Returns:
        Status and updated list of uploaded documents
    """
    try:
        set_session_context(session_id)
        logger.debug(f"Removing document '{request.filename}' from session: {session_id}")

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Only allow removal for sessions with documents uploaded but not yet processed
        if session.status not in [SessionStatus.DOCUMENTS_UPLOADED, SessionStatus.SCHEMA_EXTRACTED, SessionStatus.COMPLETED]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot remove documents for session in status '{session.status}'. Documents can only be removed before processing."
            )

        # Check if document is in the uploaded list
        if not session.metadata.uploaded_documents:
            raise HTTPException(status_code=404, detail="No uploaded documents found")

        if request.filename not in session.metadata.uploaded_documents:
            raise HTTPException(
                status_code=404,
                detail=f"Document '{request.filename}' not found in uploaded documents"
            )

        # Remove from metadata
        session.metadata.uploaded_documents.remove(request.filename)
        stem = Path(request.filename).stem
        session.metadata.pubmed_link_suppressed_stems = [
            s for s in session.metadata.pubmed_link_suppressed_stems if s != stem
        ]

        # Remove the actual file from pending_documents directory
        parser = FileParser()
        session_dir = parser.data_dir / session_id
        pending_file = pending_docs_dir(session_dir) / request.filename
        docs_file = committed_docs_dir(session_dir) / request.filename

        files_removed = []
        if pending_file.exists():
            pending_file.unlink()
            files_removed.append(str(pending_file))
            logger.debug(f"Removed file from pending: {pending_file}")
        if docs_file.exists():
            docs_file.unlink()
            files_removed.append(str(docs_file))
            logger.debug(f"Removed file from documents: {docs_file}")

        # Remove the document's per-document artifacts (figures, ...) from both
        # pending and committed, so nothing is orphaned and a re-upload of the
        # same filename cannot inherit stale artifacts.
        remove_document_artifacts(session_dir, stem)

        # Update session status if no more documents
        if not session.metadata.uploaded_documents:
            # Revert to previous state
            if is_imported(session):
                session.status = SessionStatus.COMPLETED if session.statistics else SessionStatus.SCHEMA_EXTRACTED
            else:
                session.status = SessionStatus.COMPLETED

        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        logger.debug(f"Document removed. Remaining: {session.metadata.uploaded_documents}")

        return {
            "status": "success",
            "message": f"Document '{request.filename}' removed successfully",
            "remaining_documents": session.metadata.uploaded_documents,
            "files_removed": files_removed
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in remove_uploaded_document: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-cloud-documents/{session_id}", response_model=dict)
async def add_cloud_documents(session_id: str, request: CloudDocumentRequest):
    """Add documents from cloud storage to a session.

    This endpoint allows users to add documents from pre-uploaded
    cloud datasets without uploading files manually.

    Args:
        session_id: Session ID to add documents to
        request: Dataset name and list of filenames to add

    Returns:
        Status and list of added files
    """
    try:
        set_session_context(session_id)
        logger.debug(f"Adding cloud documents from '{request.dataset}' to session: {session_id}")

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Validate session status
        is_valid_upload_session = (
            is_imported(session) and
            session.status in [SessionStatus.SCHEMA_EXTRACTED, SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )
        is_valid_schematiq_session = (
            has_live_pipeline(session) and
            session.status == SessionStatus.COMPLETED
        )

        if not (is_valid_upload_session or is_valid_schematiq_session):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot add documents for session type '{session.type}' in status '{session.status}'."
            )

        storage = get_storage()

        # Verify dataset exists
        datasets = await storage.list_datasets()
        dataset_names = [d.name for d in datasets]
        if request.dataset not in dataset_names:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{request.dataset}' not found. Available: {dataset_names}"
            )

        # Create directories for this session
        parser = FileParser()
        session_dir = parser.data_dir / session_id
        pending_dir = pending_docs_dir(session_dir)
        pending_dir.mkdir(parents=True, exist_ok=True)
        docs_dir = committed_docs_dir(session_dir)
        docs_dir.mkdir(parents=True, exist_ok=True)

        # Download requested files
        downloaded_files = []
        errors = []
        failed_files = []
        document_extraction = {}

        for filename in request.files:
            try:
                content = await storage.download_dataset_file(request.dataset, filename)
                if not content:
                    errors.append(f"Could not download file: {filename}")
                    failed_files.append({"filename": filename, "status": "failed: could not download"})
                    continue

                file_path = pending_dir / filename
                with open(file_path, 'wb') as f:
                    f.write(content)

                # to_thread rather than run_in_executor: it copies the context,
                # so preprocessing logs keep the session id.
                display_name, error = await asyncio.to_thread(
                    _preprocess_and_record,
                    file_path, filename, docs_dir, session, document_extraction,
                )
                if error:
                    if "already processed" in error:
                        errors.append(error)
                    else:
                        failed_files.append({"filename": filename, "status": error})
                        errors.append(error)
                    continue

                downloaded_files.append(display_name)
                logger.debug(f"Downloaded and converted cloud file: {filename} -> {display_name}")
            except Exception as e:
                errors.append(f"Error downloading {filename}: {str(e)}")
                failed_files.append({"filename": filename, "status": f"failed: {e}"})

        if not downloaded_files:
            raise HTTPException(
                status_code=400,
                detail={"errors": errors, "message": "No files were downloaded", "failed_files": failed_files}
            )

        # Update session metadata
        existing_docs = session.metadata.uploaded_documents or []
        session.metadata.uploaded_documents = existing_docs + downloaded_files
        session.metadata.cloud_dataset = request.dataset  # Store original cloud dataset name
        session.status = SessionStatus.DOCUMENTS_UPLOADED
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        logger.debug(f"Added {len(downloaded_files)} cloud documents to session")

        return {
            "status": "success",
            "message": f"Successfully added {len(downloaded_files)} documents from '{request.dataset}'",
            "added_files": downloaded_files,
            "errors": errors if failed_files else None,
            "failed_files": failed_files if failed_files else None,
            "document_extraction": document_extraction,
            "documents_directory": str(docs_dir)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in add_cloud_documents: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


class DocumentProcessingRequest(BaseModel):
    """Request model for document processing with optional LLM configuration."""
    llm_config: Optional[Dict[str, Any]] = None


@router.post("/sessions/{session_id}/confirm-websocket")
async def confirm_websocket_ready(session_id: str):
    """Confirm WebSocket is connected before starting document processing.

    Frontend calls this after WebSocket connects to verify the connection
    is registered on the backend. This prevents race conditions where
    cell extraction starts before WebSocket is ready.
    """
    from app.services import websocket_manager

    set_session_context(session_id)
    conn_count = websocket_manager.get_connection_count(session_id)
    logger.info(f"WebSocket confirmation request for {session_id}: {conn_count} connections")

    if conn_count == 0:
        raise HTTPException(
            status_code=400,
            detail="No WebSocket connection found. Please ensure WebSocket is connected."
        )

    return {"status": "ready", "connections": conn_count, "session_id": session_id}


@router.post("/process-documents/{session_id}", response_model=dict)
async def process_documents(session_id: str, background_tasks: BackgroundTasks, request: Optional[DocumentProcessingRequest] = None):
    """Start processing uploaded documents with extracted schema using ScheMatiQ pipeline."""
    try:
        set_session_context(session_id)
        logger.debug(f"Starting document processing for session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Allow both upload sessions (with documents) and ScheMatiQ sessions (completed or with documents uploaded)
        is_valid_upload_session = (
            is_imported(session) and
            session.status in [SessionStatus.DOCUMENTS_UPLOADED, SessionStatus.COMPLETED]
        )
        is_valid_schematiq_session = (
            has_live_pipeline(session) and
            session.status in [SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )

        if not (is_valid_upload_session or is_valid_schematiq_session):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot process documents for session type '{session.type}' in status '{session.status}'. Sessions need documents uploaded or be completed."
            )

        # Check for schema - either extracted_schema (enhanced upload) or columns (regular upload/ScheMatiQ)
        if not session.metadata.extracted_schema and not session.columns:
            raise HTTPException(status_code=400, detail="No schema found for processing")

        # If session has columns but no extracted_schema, create it (for regular upload or ScheMatiQ sessions)
        if not session.metadata.extracted_schema and session.columns:
            logger.debug(f"Converting session columns to extracted_schema format (type: {session.type})")
            # Use schema_query for ScheMatiQ sessions, fallback for upload sessions
            query = session.schema_query if session.schema_query else f"Data processing for {session.metadata.source}"
            extracted_schema = {
                "query": query,
                "schema": []
            }

            # Convert columns to schema format, excluding excerpt columns
            for col in session.columns:
                if col.name and not col.name.lower().endswith('_excerpt'):
                    schema_col = {
                        "name": col.name,
                        "definition": col.definition or f"Column containing {col.name} data",
                        "rationale": col.rationale or "Extracted from uploaded data structure"
                    }
                    extracted_schema["schema"].append(schema_col)

            # Include observation_unit if session has one (critical for multi-row extraction)
            if session.observation_unit:
                extracted_schema["observation_unit"] = session.observation_unit.model_dump()

            # Store the converted schema in session metadata for processing
            session.metadata.extracted_schema = extracted_schema
            session_manager.update_session(session)
            logger.debug(f"Created extracted_schema with {len(extracted_schema['schema'])} columns")

        if not session.metadata.uploaded_documents:
            raise HTTPException(status_code=400, detail="No uploaded documents found for processing")

        schema_count = len(session.metadata.extracted_schema['schema']) if session.metadata.extracted_schema else 0
        logger.debug(f"Session ready for processing - {len(session.metadata.uploaded_documents)} documents, {schema_count} schema columns")
        
        # Update session status
        session.status = SessionStatus.PROCESSING_DOCUMENTS
        session.metadata.last_modified = datetime.now()
        session.metadata.original_row_count = session.statistics.total_rows if session.statistics else 0
        
        # Store user-provided LLM config if available
        user_llm_config = None
        if request and request.llm_config:
            user_llm_config = request.llm_config
            # Log config without exposing full API key
            config_for_log = {k: v for k, v in user_llm_config.items() if k != 'api_key'}
            has_api_key = 'api_key' in user_llm_config and user_llm_config['api_key']
            api_key_info = f"api_key={'present ('+str(len(user_llm_config['api_key']))+' chars)' if has_api_key else 'MISSING'}"
            logger.debug(f"Using user-provided LLM config: {config_for_log}, {api_key_info}")

            # Store the user configuration in session directory for processing
            session_dir = Path(DEFAULT_DATA_DIR) / session_id
            session_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
            user_config_file = session_dir / "user_llm_config.json"

            with open(user_config_file, 'w') as f:
                json.dump(user_llm_config, f, indent=2)

        session_manager.update_session(session)

        # Reserve a concurrency slot
        await concurrency_limiter.acquire(session_id, "upload_extraction")

        # Start processing in background
        background_tasks.add_task(upload_processor.process_documents, session_id)

        logger.debug(f"Document processing started in background for session: {session_id}")

        return {
            "status": "success",
            "message": "Document processing started",
            "session_id": session_id,
            "total_documents": len(session.metadata.uploaded_documents),
            "schema_columns": len(session.metadata.extracted_schema['schema'])
        }

    except CapacityExceededError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await concurrency_limiter.release(session_id)
        logger.error(f"Exception in process_documents: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop-processing/{session_id}", response_model=dict)
async def stop_document_processing(session_id: str):
    """Request document processing to stop. Returns immediately."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        if session.status != SessionStatus.PROCESSING_DOCUMENTS:
            return {
                "status": "not_processing",
                "message": f"Session is not processing documents (status: {session.status})",
                "stopped": False
            }

        result = upload_processor.request_stop(session_id)
        if not result["accepted"]:
            raise HTTPException(status_code=404, detail=result["message"])

        # Background: wait for processing to actually stop, then update status
        asyncio.create_task(_finalize_upload_stop(session_id))

        return {"status": "stopping", "message": result["message"], "stopped": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in stop_document_processing: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def _finalize_upload_stop(session_id: str):
    """Background task: wait for upload processing to finish, update session status, broadcast."""
    try:
        # Wait up to 30s for the task to notice the stop flag
        for _ in range(60):
            await asyncio.sleep(0.5)
            if not upload_processor.is_running(session_id):
                break

        session = session_manager.get_session(session_id)
        if session and session.status not in (SessionStatus.STOPPED, SessionStatus.COMPLETED):
            session.status = SessionStatus.STOPPED
            session.metadata.last_modified = datetime.now()
            session_manager.update_session(session)

            processed = session.metadata.processed_documents or 0
            total = len(session.metadata.uploaded_documents) if session.metadata.uploaded_documents else 0
            rows_added = session.metadata.additional_rows_added or 0

            await websocket_manager.broadcast_to_session(session_id, {
                "type": "stopped",
                "data": {
                    "message": "Document processing stopped by user",
                    "processed_documents": processed,
                    "total_documents": total,
                    "data_rows_saved": rows_added
                }
            })
    except Exception:
        logger.error("Background _finalize_upload_stop failed for session %s", session_id, exc_info=True)


@router.get("/processing-status/{session_id}", response_model=dict)
async def get_processing_status(session_id: str):
    """Get document processing status and progress."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        if not is_imported(session):
            raise HTTPException(status_code=400, detail="Processing status only available for upload sessions")
        
        # Build status response
        status_info = {
            "session_id": session_id,
            "status": session.status,
            "total_documents": len(session.metadata.uploaded_documents) if session.metadata.uploaded_documents else 0,
            "processed_documents": session.metadata.processed_documents,
            "original_row_count": session.metadata.original_row_count or 0,
            "additional_rows_added": session.metadata.additional_rows_added,
            "processing_stats": session.metadata.processing_stats,
            "last_modified": session.metadata.last_modified.isoformat(),
        }
        
        # Calculate progress
        if status_info["total_documents"] > 0:
            progress = session.metadata.processed_documents / status_info["total_documents"]
            status_info["progress"] = min(progress, 1.0)
        else:
            status_info["progress"] = 0.0
        
        return status_info

    except Exception as e:
        logger.error(f"Exception in get_processing_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-complete/{session_id}")
async def export_complete_data(session_id: str, format: str = "json"):
    """Export complete data with schema metadata in multiple formats."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"
        return await export_complete_upload(session, session_id, data_file, format=format)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in export_complete_data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-rich-csv/{session_id}")
async def export_upload_rich_csv(session_id: str):
    """Export upload data as metadata-rich CSV with definition and rationale columns."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"
        return export_upload_rich_csv_data(session, session_id, data_file)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in export_upload_rich_csv: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-schema/{session_id}")
async def export_schema_only(session_id: str):
    """Export only the schema metadata in ScheMatiQ format."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        return export_upload_schema_json(session, session_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Exception in export_schema_only: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _preprocess_and_record(
    file_path: Path,
    filename: str,
    docs_dir: Path,
    session,
    document_extraction: dict,
) -> tuple[str | None, str | None]:
    """Preprocess a saved file and record metadata on the session.

    Returns (display_name, None) on success, or (None, error_message) on failure.
    The caller is responsible for writing file_path to disk before calling this.
    """
    from app.services.document_preprocessor import preprocess_uploaded_file

    txt_stem_name = Path(filename).stem + ".txt"
    if (docs_dir / txt_stem_name).exists() or (docs_dir / filename).exists():
        return None, (
            f"'{filename}' was already processed in this session. "
            "Remove the existing rows first or use a different filename."
        )

    # Extract figures next to the .txt in pending_documents/ (documents_dir=None
    # resolves to source_path.parent) so they ride the pending -> committed
    # commit together with the text (see SchematiqRunner._move_pending_documents).
    result = preprocess_uploaded_file(file_path, original_filename=filename, documents_dir=None)
    if not result.success:
        file_path.unlink(missing_ok=True)
        return None, f"{filename}: {result.status}"

    safe_filename = result.display_name
    if not session.metadata.document_metadata:
        session.metadata.document_metadata = {}
    session.metadata.document_metadata[safe_filename] = {
        "extraction_status": result.status,
        "extraction_method": result.method,
        "original_filename": result.original_filename or filename,
    }
    document_extraction[safe_filename] = session.metadata.document_metadata[safe_filename]
    return safe_filename, None


_is_system_file = is_system_file