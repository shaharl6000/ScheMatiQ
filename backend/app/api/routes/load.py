"""Upload API endpoints."""

import uuid
import json
import csv
import io
import tempfile
import zipfile
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from app.models.session import VisualizationSession, SessionType, SessionMetadata, PaginatedData, SessionStatus, FilterSortRequest
from app.models.upload import (
    FileValidationResult, ColumnMappingRequest, DataPreviewRequest,
    SchemaValidationResult, DualFileUploadResult, CompatibilityCheck
)
from app.services.file_parser import FileParser
from app.services.session_manager import SessionManager
from app.services import session_manager
from app.storage import get_storage

router = APIRouter()


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
        metadata = SessionMetadata(
            source=f"template:{template_name}",
            file_size=len(template_content)
        )

        session = VisualizationSession(
            id=session_id,
            type=SessionType.UPLOAD,
            metadata=metadata
        )

        # Save template content as uploaded file
        session_dir = Path("./data") / session_id
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
                print(f"DEBUG: Restored observation unit from template CSV: {session.observation_unit.name}")

        # Restore observation_unit from JSON parse result if present (and not already set)
        if result.get("observation_unit") and not session.observation_unit:
            from app.models.session import ObservationUnitInfo
            session.observation_unit = ObservationUnitInfo(**result["observation_unit"])
            print(f"DEBUG: Restored observation unit from template JSON: {session.observation_unit.name}")

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
        print(f"Error loading template {template_name}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/file", response_model=dict)
async def upload_file(file: UploadFile = File(...)):
    """Upload and validate a file."""
    try:
        print(f"DEBUG: Received file upload: {file.filename}, size: {file.size}")
        
        # Validate file
        parser = FileParser()
        validation = await parser.validate_file(file)
        
        print(f"DEBUG: File validation result: {validation}")
        
        if not validation.is_valid:
            raise HTTPException(status_code=400, detail=validation.errors)
        
        # Create session
        session_id = str(uuid.uuid4())
        metadata = SessionMetadata(
            source=file.filename,
            file_size=file.size
        )
        
        session = VisualizationSession(
            id=session_id,
            type=SessionType.UPLOAD,
            metadata=metadata
        )
        
        print(f"DEBUG: Created session: {session_id}")
        
        # Save file content for processing
        await parser.save_uploaded_file(session_id, file)
        print(f"DEBUG: File saved for session: {session_id}")
        
        # Store session
        session_manager.create_session(session)
        print(f"DEBUG: Session stored")
        
        return {
            "session_id": session_id,
            "validation": validation,
            "requires_column_mapping": validation.detected_format == "csv"
        }
        
    except Exception as e:
        print(f"DEBUG: Exception in upload_file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/parse/{session_id}", response_model=dict)
async def parse_file(session_id: str, mapping: Optional[ColumnMappingRequest] = None):
    """Parse uploaded file with optional column mapping."""
    try:
        print(f"DEBUG: Parsing file for session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        print(f"DEBUG: Found session: {session}")
        
        parser = FileParser()
        result = await parser.parse_file(session_id, mapping)
        
        print(f"DEBUG: Parse result: columns={len(result['columns'])}, statistics={result['statistics']}")
        
        # Check for extracted metadata from CSV comments
        if "extracted_metadata" in result:
            metadata = result["extracted_metadata"]
            print(f"DEBUG: Found CSV metadata - query: {metadata.get('query')}, LLM config: {bool(metadata.get('llm_config'))}")
            
            # Store extracted query
            if metadata.get('query'):
                session.schema_query = metadata['query']

            # Restore observation_unit if present (from CSV comments)
            if metadata.get('observation_unit'):
                from app.models.session import ObservationUnitInfo
                session.observation_unit = ObservationUnitInfo(**metadata['observation_unit'])
                print(f"DEBUG: Restored observation unit from CSV: {session.observation_unit.name}")

            # Create a parsed schema file with the extracted LLM configuration
            if metadata.get('llm_config'):
                session_dir = Path("./data") / session_id
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

                print(f"DEBUG: Saved parsed schema with extracted LLM configuration")
        
        # Restore observation_unit from JSON parse result if present (and not already set from CSV metadata)
        if result.get("observation_unit") and not session.observation_unit:
            from app.models.session import ObservationUnitInfo
            session.observation_unit = ObservationUnitInfo(**result["observation_unit"])
            print(f"DEBUG: Restored observation unit from JSON: {session.observation_unit.name}")

        # Update session with parsed data
        session.columns = result["columns"]
        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED  # Mark as completed after successful parsing
        session_manager.update_session(session)

        # Capture schema baseline for re-extraction change detection
        session_manager.capture_schema_baseline(session_id)

        print(f"DEBUG: Session updated successfully")
        
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
        print(f"DEBUG: Exception in parse_file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/data/{session_id}", response_model=PaginatedData)
async def get_data_with_filters(
    session_id: str,
    page: int = 0,
    page_size: int = 50,
    request: Optional[FilterSortRequest] = None
):
    """Get paginated data with optional filtering and sorting."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        parser = FileParser()

        # Extract filter/sort params from request body
        filters = None
        sort = None
        search = None

        if request:
            filters = [f.dict() for f in request.filters] if request.filters else None
            sort = [s.dict() for s in request.sort] if request.sort else None
            search = request.search

        data = await parser.get_paginated_data(
            session_id, page, page_size,
            filters=filters, sort=sort, search=search
        )

        return data

    except Exception as e:
        print(f"DEBUG: Exception in get_data_with_filters: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{session_id}", response_model=PaginatedData)
async def get_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get paginated data for a session (backward compatible, no filtering)."""
    return await get_data_with_filters(session_id, page, page_size, None)

@router.get("/sessions", response_model=List[VisualizationSession])
async def list_sessions():
    """List all upload sessions."""
    return session_manager.list_sessions(SessionType.UPLOAD)

@router.get("/sessions/{session_id}", response_model=VisualizationSession)
async def get_session(session_id: str):
    """Get session details."""
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session

@router.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session and its data."""
    success = session_manager.delete_session(session_id)
    if not success:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"message": "Session deleted successfully"}

@router.post("/dual-file", response_model=dict)
async def upload_dual_files(
    schema_file: UploadFile = File(..., description="QBSD schema JSON file"),
    data_file: UploadFile = File(..., description="Data file (CSV/JSON/JSONL)")
):
    """Upload and validate both schema and data files."""
    try:
        print(f"DEBUG: Received dual file upload - schema: {schema_file.filename}, data: {data_file.filename}")
        
        parser = FileParser()
        
        # Validate schema file
        print("DEBUG: Validating schema file...")
        schema_validation = await parser.validate_schema_file(schema_file)
        print(f"DEBUG: Schema validation result: {schema_validation}")
        
        # Validate data file  
        print("DEBUG: Validating data file...")
        data_validation = await parser.validate_file(data_file)
        print(f"DEBUG: Data validation result: {data_validation}")
        
        # Create session
        session_id = str(uuid.uuid4())
        metadata = SessionMetadata(
            source=f"Dual Upload: {schema_file.filename} + {data_file.filename}"
        )
        
        session = VisualizationSession(
            id=session_id,
            type=SessionType.UPLOAD,
            metadata=metadata
        )
        
        print(f"DEBUG: Created session: {session_id}")
        
        # Save files
        await parser.save_schema_file(session_id, schema_file, schema_validation)
        await parser.save_uploaded_file(session_id, data_file)
        print(f"DEBUG: Files saved for session: {session_id}")
        
        # Check compatibility if both files are valid
        compatibility = CompatibilityCheck(is_compatible=False)
        if schema_validation.is_valid and data_validation.is_valid:
            print("DEBUG: Checking schema-data compatibility...")
            
            # Get data columns from sample data or parse file preview
            data_columns = []
            if data_validation.sample_data:
                # Extract columns from sample data
                for sample in data_validation.sample_data:
                    if isinstance(sample, dict):
                        data_columns.extend(sample.keys())
                        break
            
            # Remove duplicates and clean column names
            data_columns = list(set(data_columns))
            print(f"DEBUG: Data columns: {data_columns}")
            print(f"DEBUG: Schema columns: {schema_validation.detected_columns}")
            
            compatibility = parser.check_schema_data_compatibility(
                schema_validation, data_validation, data_columns
            )
            print(f"DEBUG: Compatibility result: {compatibility}")
        
        # Store session
        session_manager.create_session(session)
        print(f"DEBUG: Session stored")
        
        return {
            "session_id": session_id,
            "schema_validation": schema_validation.model_dump(),
            "data_validation": data_validation.model_dump(),
            "compatibility": compatibility.model_dump(),
            "requires_column_mapping": data_validation.detected_format == "csv"
        }
        
    except Exception as e:
        print(f"DEBUG: Exception in upload_dual_files: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-dual/{session_id}", response_model=dict)
async def process_dual_files(session_id: str, mapping: Optional[ColumnMappingRequest] = None):
    """Process dual uploaded files with optional column mapping."""
    try:
        print(f"DEBUG: Processing dual files for session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        print(f"DEBUG: Found session: {session}")
        
        parser = FileParser()
        
        # Parse the data file (schema is already parsed during upload)
        result = await parser.parse_file(session_id, mapping)
        
        # Load parsed schema
        session_dir = Path("./data") / session_id
        schema_file = session_dir / "parsed_schema.json"
        
        if schema_file.exists():
            with open(schema_file) as f:
                schema_data = json.load(f)
                
            # Convert schema to ColumnInfo format for session
            from app.models.session import ColumnInfo
            
            schema_columns = []
            for col in schema_data.get('schema', []):
                col_info = ColumnInfo(
                    name=col['name'],
                    definition=col.get('definition'),
                    rationale=col.get('rationale'),
                    data_type="object"  # Will be updated from data analysis
                )
                schema_columns.append(col_info)
            
            # Update session with enhanced schema info and query
            session.columns = schema_columns
            session.schema_query = schema_data.get('query')

            # Restore observation_unit from parsed schema if present
            if schema_data.get('observation_unit'):
                from app.models.session import ObservationUnitInfo
                session.observation_unit = ObservationUnitInfo(**schema_data['observation_unit'])
                print(f"DEBUG: Restored observation unit from parsed schema: {session.observation_unit.name}")
        else:
            # Fallback to basic column info from data
            session.columns = result["columns"]

        # Also check result dict for observation_unit (from JSON file parsing)
        if result.get("observation_unit") and not session.observation_unit:
            from app.models.session import ObservationUnitInfo
            session.observation_unit = ObservationUnitInfo(**result["observation_unit"])
            print(f"DEBUG: Restored observation unit from data file: {session.observation_unit.name}")

        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED
        session_manager.update_session(session)

        # Capture schema baseline for re-extraction change detection
        session_manager.capture_schema_baseline(session_id)

        print(f"DEBUG: Dual file processing completed successfully")

        return {"status": "success", "message": "Files processed successfully"}

    except Exception as e:
        print(f"DEBUG: Exception in process_dual_files: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

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
        
        # Get the original parsed data
        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"

        if not data_file.exists():
            raise HTTPException(status_code=404, detail="No data found for export")

        # Helper function to flatten QBSD answer format to plain values
        def flatten_cell_value(value):
            """Convert QBSD answer format {'answer': ..., 'excerpts': [...]} to plain value."""
            if isinstance(value, dict) and 'answer' in value:
                return value['answer']
            return value

        # Read JSONL format and flatten DataRow objects
        # First pass: collect all column names to ensure consistency
        all_columns = set()
        raw_rows = []
        with open(data_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    row_data = json.loads(line)
                    raw_rows.append(row_data)
                    if 'data' in row_data:
                        all_columns.update(row_data['data'].keys())
                    else:
                        all_columns.update(row_data.keys())

        # Check if the original data structure includes row_name or papers as data columns
        # We only include these if they're part of the data schema to maintain consistency
        has_row_name_column = 'row_name' in all_columns
        has_papers_column = 'papers' in all_columns

        # Second pass: flatten rows with consistent columns
        rows = []
        for row_data in raw_rows:
            if 'data' in row_data:
                # Flatten each cell value (convert QBSD format to plain values)
                flat_row = {}
                for key, value in row_data['data'].items():
                    flat_row[key] = flatten_cell_value(value)
                # Only add row_name/papers if they're already expected columns in the data schema
                # This ensures new rows don't introduce extra columns that original rows don't have
                if has_row_name_column:
                    flat_row['row_name'] = row_data.get('row_name', '')
                if has_papers_column:
                    flat_row['papers'] = str(row_data.get('papers', ''))
                rows.append(flat_row)
            else:
                # Flatten values for non-DataRow format too
                flat_row = {}
                for key, value in row_data.items():
                    flat_row[key] = flatten_cell_value(value)
                rows.append(flat_row)

        # Ensure all rows have the same columns (fill missing with empty string)
        if rows:
            final_columns = set()
            for row in rows:
                final_columns.update(row.keys())
            for row in rows:
                for col in final_columns:
                    if col not in row:
                        row[col] = ''
        
        if not rows:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Prepare CSV with metadata
        output = io.StringIO()
        
        # Add schema metadata as CSV comments
        output.write("# Upload Data Export with Schema Metadata\n")
        output.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Source: {session.metadata.source}\n")
        
        # Include schema query if available
        if session.schema_query:
            output.write(f"# Query: {session.schema_query}\n")

        # Include observation_unit if available
        if session.observation_unit:
            obs_unit_json = json.dumps(session.observation_unit.model_dump())
            output.write(f"# Observation Unit: {obs_unit_json}\n")

        # Load preserved LLM configuration from parsed schema if available
        session_dir = Path("./data") / session_id
        parsed_schema_file = session_dir / "parsed_schema.json"
        
        if parsed_schema_file.exists():
            try:
                with open(parsed_schema_file) as f:
                    parsed_schema = json.load(f)
                    if "llm_configuration" in parsed_schema:
                        llm_config = parsed_schema["llm_configuration"]
                        if llm_config.get("schema_creation_backend"):
                            backend = llm_config["schema_creation_backend"]
                            output.write(f"# Schema Creation: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
                        if llm_config.get("value_extraction_backend"):
                            backend = llm_config["value_extraction_backend"]
                            output.write(f"# Value Extraction: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
            except Exception as e:
                output.write(f"# LLM Config: Error loading ({e})\n")
        
        output.write("#\n")
        output.write("# Column Definitions:\n")
        
        # Add column metadata for each schema column
        for col in session.columns:
            if col.name:
                output.write(f"# {col.name}: {col.definition or 'No definition available'}\n")
                if col.rationale:
                    output.write(f"#   Rationale: {col.rationale}\n")
                if col.allowed_values:
                    output.write(f"#   Allowed Values: {', '.join(col.allowed_values)}\n")

        output.write("#\n")
        
        # Get column names - use user-specified order if provided
        if rows:
            available_columns = set(rows[0].keys())

            if column_order:
                # Parse user-specified column order
                requested_order = [col.strip() for col in column_order.split(',')]
                # Filter to only include columns that exist, preserving requested order
                column_names = [col for col in requested_order if col in available_columns]
                # Add any remaining columns not in the requested order
                remaining_columns = [col for col in available_columns if col not in column_names]
                column_names.extend(remaining_columns)
            else:
                # Default: use first row's column order
                column_names = list(rows[0].keys())

            writer = csv.DictWriter(output, fieldnames=column_names)
            writer.writeheader()

            for row in rows:
                writer.writerow(row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename with timestamp in user's timezone
        source_name = session.metadata.source or "uploaded_data"
        # Strip file extension to avoid double extensions (e.g., file.csv_timestamp.csv)
        if source_name.lower().endswith(('.csv', '.json', '.jsonl')):
            source_name = Path(source_name).stem
        safe_name = "".join(c for c in source_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        # Apply timezone offset (offset is minutes from UTC, negative means ahead of UTC)
        user_time = datetime.utcnow() - timedelta(minutes=tz_offset)
        timestamp = user_time.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{safe_name}_{timestamp}.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/extract-schema/{session_id}", response_model=dict)
async def extract_schema(session_id: str, query: str = ""):
    """Extract schema from uploaded data and convert to QBSD format."""
    try:
        print(f"DEBUG: Extracting schema for session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        if session.type != SessionType.UPLOAD:
            raise HTTPException(status_code=400, detail="Schema extraction only available for upload sessions")
        
        # Ensure session data is processed
        if session.status != SessionStatus.COMPLETED:
            raise HTTPException(status_code=400, detail="Session must be completed before schema extraction. Please parse the file first.")
        
        print(f"DEBUG: Session status: {session.status}, type: {session.type}")
        
        parser = FileParser()
        
        # Extract schema from parsed data
        extracted_schema = await parser.extract_schema_from_data(
            session_id, 
            query if query.strip() else None
        )
        
        print(f"DEBUG: Extracted schema with {len(extracted_schema['schema'])} columns")
        
        # Update session with extracted schema
        session.status = SessionStatus.SCHEMA_EXTRACTED
        session.metadata.extracted_schema = extracted_schema
        session.metadata.last_modified = datetime.now()
        
        # Convert extracted schema columns to ColumnInfo format for session
        from app.models.session import ColumnInfo
        
        schema_columns = []
        for col in extracted_schema['schema']:
            col_info = ColumnInfo(
                name=col['name'],
                definition=col['definition'],
                rationale=col['rationale'],
                data_type="extracted"  # Mark as extracted
            )
            schema_columns.append(col_info)
        
        session.columns = schema_columns
        session.schema_query = extracted_schema['query']
        session_manager.update_session(session)
        
        print(f"DEBUG: Session updated with extracted schema, status: {session.status}")
        
        return {
            "status": "success",
            "message": "Schema extracted successfully",
            "schema": extracted_schema,
            "total_columns": len(extracted_schema['schema'])
        }
        
    except Exception as e:
        print(f"DEBUG: Exception in extract_schema: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-documents/{session_id}", response_model=dict)
async def add_documents(session_id: str, files: List[UploadFile] = File(...)):
    """Upload documents for processing with extracted schema."""
    try:
        print(f"DEBUG: Adding {len(files)} documents to session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Allow both upload sessions (various states) and QBSD sessions (created or completed)
        is_valid_upload_session = (
            session.type == SessionType.UPLOAD and
            session.status in [SessionStatus.SCHEMA_EXTRACTED, SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )
        is_valid_qbsd_session = (
            session.type == SessionType.QBSD and
            session.status in [SessionStatus.CREATED, SessionStatus.COMPLETED]
        )

        if not (is_valid_upload_session or is_valid_qbsd_session):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot upload documents for session type '{session.type}' in status '{session.status}'. Upload sessions need schema extracted/completed, QBSD sessions must be created or completed."
            )
        
        print(f"DEBUG: Session status: {session.status}, extracted schema available")
        
        # Validate files
        errors = []
        warnings = []
        uploaded_filenames = []
        
        # Create directories for this session
        # New documents go to pending_documents/ first, then moved to documents/ after processing
        parser = FileParser()
        session_dir = parser.data_dir / session_id
        pending_dir = session_dir / "pending_documents"
        pending_dir.mkdir(parents=True, exist_ok=True)
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each uploaded file
        total_size = 0
        for i, file in enumerate(files):
            print(f"DEBUG: Processing file {i+1}/{len(files)}: {file.filename}")
            
            # Skip system files that shouldn't be processed
            if _is_system_file(file.filename):
                print(f"DEBUG: Skipping system file: {file.filename}")
                continue
            
            # Validate file size (10MB limit per file)
            if file.size > 10 * 1024 * 1024:
                errors.append(f"File '{file.filename}' exceeds 10MB limit")
                continue
            
            total_size += file.size
            
            # Validate file type (text files, PDFs, docs, etc.)
            allowed_extensions = {'.txt', '.md', '.pdf', '.doc', '.docx', '.rtf'}
            file_ext = Path(file.filename).suffix.lower()
            
            if file_ext not in allowed_extensions:
                warnings.append(f"File '{file.filename}' has unsupported extension '{file_ext}'. Supported: {', '.join(allowed_extensions)}")
                # Continue processing - might still be text content
            
            # Save file to pending_documents/ (will be moved to documents/ after processing)
            try:
                # Use original filename - handle potential duplicates by checking existence
                safe_filename = file.filename
                file_path = pending_dir / safe_filename

                # If file already exists in pending or documents, add a numeric suffix
                docs_file_path = docs_dir / safe_filename
                if file_path.exists() or docs_file_path.exists():
                    base_name = Path(file.filename).stem
                    extension = Path(file.filename).suffix
                    counter = 1
                    while file_path.exists() or (docs_dir / safe_filename).exists():
                        safe_filename = f"{base_name}_{counter}{extension}"
                        file_path = pending_dir / safe_filename
                        counter += 1

                with open(file_path, 'wb') as f:
                    content = await file.read()
                    f.write(content)

                # If PDF, convert to text
                if file_ext == '.pdf':
                    from app.services.pdf_utils import convert_pdf_to_txt
                    try:
                        txt_path = convert_pdf_to_txt(file_path)
                        # Remove original PDF after conversion
                        file_path.unlink()
                        file_path = txt_path
                        safe_filename = txt_path.name
                        print(f"DEBUG: Converted PDF to text: {txt_path}")
                    except Exception as e:
                        errors.append(f"Failed to convert PDF '{file.filename}': {str(e)}")
                        continue

                uploaded_filenames.append(safe_filename)
                print(f"DEBUG: Saved file to pending: {file_path}")
                
            except Exception as e:
                errors.append(f"Failed to save file '{file.filename}': {str(e)}")
        
        # Check total size limit (100MB total)
        if total_size > 100 * 1024 * 1024:
            errors.append("Total upload size exceeds 100MB limit")
        
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors, "warnings": warnings})
        
        # Update session metadata
        session.status = SessionStatus.DOCUMENTS_UPLOADED
        session.metadata.uploaded_documents = uploaded_filenames
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)
        
        print(f"DEBUG: Updated session with {len(uploaded_filenames)} uploaded documents")
        
        return {
            "status": "success",
            "message": f"Successfully uploaded {len(uploaded_filenames)} documents",
            "uploaded_files": uploaded_filenames,
            "warnings": warnings,
            "documents_directory": str(docs_dir)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"DEBUG: Exception in add_documents: {e}")
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
        print(f"DEBUG: Removing document '{request.filename}' from session: {session_id}")

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

        # Remove the actual file from pending_documents directory
        parser = FileParser()
        session_dir = parser.data_dir / session_id
        pending_file = session_dir / "pending_documents" / request.filename
        docs_file = session_dir / "documents" / request.filename

        files_removed = []
        if pending_file.exists():
            pending_file.unlink()
            files_removed.append(str(pending_file))
            print(f"DEBUG: Removed file from pending: {pending_file}")
        if docs_file.exists():
            docs_file.unlink()
            files_removed.append(str(docs_file))
            print(f"DEBUG: Removed file from documents: {docs_file}")

        # Update session status if no more documents
        if not session.metadata.uploaded_documents:
            # Revert to previous state
            if session.type == SessionType.UPLOAD:
                session.status = SessionStatus.COMPLETED if session.statistics else SessionStatus.SCHEMA_EXTRACTED
            else:
                session.status = SessionStatus.COMPLETED

        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        print(f"DEBUG: Document removed. Remaining: {session.metadata.uploaded_documents}")

        return {
            "status": "success",
            "message": f"Document '{request.filename}' removed successfully",
            "remaining_documents": session.metadata.uploaded_documents,
            "files_removed": files_removed
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"DEBUG: Exception in remove_uploaded_document: {e}")
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
        print(f"DEBUG: Adding cloud documents from '{request.dataset}' to session: {session_id}")

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Validate session status
        is_valid_upload_session = (
            session.type == SessionType.UPLOAD and
            session.status in [SessionStatus.SCHEMA_EXTRACTED, SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )
        is_valid_qbsd_session = (
            session.type == SessionType.QBSD and
            session.status == SessionStatus.COMPLETED
        )

        if not (is_valid_upload_session or is_valid_qbsd_session):
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
        pending_dir = session_dir / "pending_documents"
        pending_dir.mkdir(parents=True, exist_ok=True)
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)

        # Download requested files
        downloaded_files = []
        errors = []

        for filename in request.files:
            try:
                content = await storage.download_dataset_file(request.dataset, filename)
                if content:
                    file_path = pending_dir / filename
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    downloaded_files.append(filename)
                    print(f"DEBUG: Downloaded cloud file: {filename}")
                else:
                    errors.append(f"Could not download file: {filename}")
            except Exception as e:
                errors.append(f"Error downloading {filename}: {str(e)}")

        if not downloaded_files:
            raise HTTPException(
                status_code=400,
                detail={"errors": errors, "message": "No files were downloaded"}
            )

        # Update session metadata
        existing_docs = session.metadata.uploaded_documents or []
        session.metadata.uploaded_documents = existing_docs + downloaded_files
        session.metadata.cloud_dataset = request.dataset  # Store original cloud dataset name
        session.status = SessionStatus.DOCUMENTS_UPLOADED
        session.metadata.last_modified = datetime.now()
        session_manager.update_session(session)

        print(f"DEBUG: Added {len(downloaded_files)} cloud documents to session")

        return {
            "status": "success",
            "message": f"Successfully added {len(downloaded_files)} documents from '{request.dataset}'",
            "added_files": downloaded_files,
            "errors": errors if errors else None,
            "documents_directory": str(docs_dir)
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"DEBUG: Exception in add_cloud_documents: {e}")
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
    from services import websocket_manager

    conn_count = websocket_manager.get_connection_count(session_id)
    print(f"🔍 WebSocket confirmation request for {session_id}: {conn_count} connections")

    if conn_count == 0:
        raise HTTPException(
            status_code=400,
            detail="No WebSocket connection found. Please ensure WebSocket is connected."
        )

    return {"status": "ready", "connections": conn_count, "session_id": session_id}


@router.post("/process-documents/{session_id}", response_model=dict)
async def process_documents(session_id: str, background_tasks: BackgroundTasks, request: Optional[DocumentProcessingRequest] = None):
    """Start processing uploaded documents with extracted schema using QBSD pipeline."""
    try:
        print(f"DEBUG: Starting document processing for session: {session_id}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Allow both upload sessions (with documents) and QBSD sessions (completed or with documents uploaded)
        is_valid_upload_session = (
            session.type == SessionType.UPLOAD and
            session.status in [SessionStatus.DOCUMENTS_UPLOADED, SessionStatus.COMPLETED]
        )
        is_valid_qbsd_session = (
            session.type == SessionType.QBSD and
            session.status in [SessionStatus.COMPLETED, SessionStatus.DOCUMENTS_UPLOADED]
        )

        if not (is_valid_upload_session or is_valid_qbsd_session):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot process documents for session type '{session.type}' in status '{session.status}'. Sessions need documents uploaded or be completed."
            )

        # Check for schema - either extracted_schema (enhanced upload) or columns (regular upload/QBSD)
        if not session.metadata.extracted_schema and not session.columns:
            raise HTTPException(status_code=400, detail="No schema found for processing")

        # If session has columns but no extracted_schema, create it (for regular upload or QBSD sessions)
        if not session.metadata.extracted_schema and session.columns:
            print(f"DEBUG: Converting session columns to extracted_schema format (type: {session.type})")
            # Use schema_query for QBSD sessions, fallback for upload sessions
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
                        "rationale": col.rationale or f"Extracted from uploaded data structure"
                    }
                    extracted_schema["schema"].append(schema_col)
            
            # Store the converted schema in session metadata for processing
            session.metadata.extracted_schema = extracted_schema
            session_manager.update_session(session)
            print(f"DEBUG: Created extracted_schema with {len(extracted_schema['schema'])} columns")
        
        if not session.metadata.uploaded_documents:
            raise HTTPException(status_code=400, detail="No uploaded documents found for processing")
        
        schema_count = len(session.metadata.extracted_schema['schema']) if session.metadata.extracted_schema else 0
        print(f"DEBUG: Session ready for processing - {len(session.metadata.uploaded_documents)} documents, {schema_count} schema columns")
        
        # Create UploadDocumentProcessor and start processing in background
        from app.services.upload_document_processor import UploadDocumentProcessor
        from app.services import websocket_manager
        
        processor = UploadDocumentProcessor(
            websocket_manager=websocket_manager,
            session_manager=session_manager
        )
        
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
            print(f"DEBUG: Using user-provided LLM config: {config_for_log}, {api_key_info}")

            # Store the user configuration in session directory for processing
            session_dir = Path("./data") / session_id
            session_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
            user_config_file = session_dir / "user_llm_config.json"

            with open(user_config_file, 'w') as f:
                json.dump(user_llm_config, f, indent=2)
        
        session_manager.update_session(session)
        
        # Start processing in background
        background_tasks.add_task(processor.process_documents, session_id)
        
        print(f"DEBUG: Document processing started in background for session: {session_id}")
        
        return {
            "status": "success",
            "message": "Document processing started",
            "session_id": session_id,
            "total_documents": len(session.metadata.uploaded_documents),
            "schema_columns": len(session.metadata.extracted_schema['schema'])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"DEBUG: Exception in process_documents: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop-processing/{session_id}", response_model=dict)
async def stop_document_processing(session_id: str):
    """Stop document processing gracefully.

    Returns information about what partial results were saved.
    """
    try:
        from app.services.upload_document_processor import UploadDocumentProcessor
        from app.services import websocket_manager

        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        # Check if session is actually processing
        if session.status != SessionStatus.PROCESSING_DOCUMENTS:
            return {
                "status": "not_processing",
                "message": f"Session is not processing documents (status: {session.status})",
                "stopped": False
            }

        # Create processor and call stop
        processor = UploadDocumentProcessor(
            websocket_manager=websocket_manager,
            session_manager=session_manager
        )

        stopped = processor.stop_processing(session_id)

        if stopped:
            # Update session status
            session.status = SessionStatus.STOPPED
            session.metadata.last_modified = datetime.now()
            session_manager.update_session(session)

            # Get current progress info
            processed = session.metadata.processed_documents or 0
            total = len(session.metadata.uploaded_documents) if session.metadata.uploaded_documents else 0
            rows_added = session.metadata.additional_rows_added or 0

            # Broadcast stopped message via WebSocket
            await websocket_manager.broadcast_message(
                session_id,
                {
                    "type": "stopped",
                    "data": {
                        "message": "Document processing stopped by user",
                        "processed_documents": processed,
                        "total_documents": total,
                        "data_rows_saved": rows_added
                    }
                }
            )

            return {
                "status": "stopped",
                "message": f"Processing stopped. {processed}/{total} documents processed, {rows_added} rows extracted.",
                "stopped": True,
                "processed_documents": processed,
                "total_documents": total,
                "data_rows_saved": rows_added
            }
        else:
            return {
                "status": "not_found",
                "message": "No active processing found for this session",
                "stopped": False
            }

    except Exception as e:
        print(f"DEBUG: Exception in stop_document_processing: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/processing-status/{session_id}", response_model=dict)
async def get_processing_status(session_id: str):
    """Get document processing status and progress."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        if session.type != SessionType.UPLOAD:
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
        print(f"DEBUG: Exception in get_processing_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-complete/{session_id}")
async def export_complete_data(session_id: str, format: str = "json"):
    """Export complete data with schema metadata in multiple formats."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
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
                ]
            },
            "metadata": {
                "total_rows": session.statistics.total_rows if session.statistics else 0,
                "total_columns": len(session.columns),
                "source": session.metadata.source,
                "file_size": session.metadata.file_size
            },
            "data": []
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            export_data["schema_evolution"] = session.statistics.schema_evolution.model_dump()

        # Include observation_unit if available
        if session.observation_unit:
            export_data["observation_unit"] = session.observation_unit.model_dump()

        # Include documents_batch_size from qbsd_config if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    if "documents_batch_size" in qbsd_config:
                        export_data["metadata"]["documents_batch_size"] = qbsd_config["documents_batch_size"]
            except Exception:
                pass  # Continue without batch size if there's an error

        # Get all data
        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"
        
        if data_file.exists():
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        row_data = json.loads(line)
                        export_data["data"].append(row_data)
        
        # Handle different export formats
        if format.lower() == "json":
            # JSON format with complete metadata
            content = json.dumps(export_data, indent=2, ensure_ascii=False)
            filename = f"{session_id[:8]}_complete_export.json"
            
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
                        "schema": export_data["schema"]["columns"]
                    }
                    
                    # Include LLM configuration if available from parsed schema
                    session_dir = Path("./data") / session_id
                    parsed_schema_file = session_dir / "parsed_schema.json"
                    
                    if parsed_schema_file.exists():
                        try:
                            with open(parsed_schema_file) as f:
                                parsed_schema = json.load(f)
                                if "llm_configuration" in parsed_schema:
                                    schema_data["llm_configuration"] = parsed_schema["llm_configuration"]
                        except Exception:
                            pass  # Continue without LLM config if there's an error
                    
                    zip_file.writestr("schema.json", json.dumps(schema_data, indent=2))
                    
                    # Add separate column metadata CSV for easy reference
                    metadata_output = io.StringIO()
                    metadata_writer = csv.writer(metadata_output)
                    metadata_writer.writerow(["Column Name", "Definition", "Rationale", "Data Type"])
                    
                    for col in export_data["schema"]["columns"]:
                        metadata_writer.writerow([
                            col["name"],
                            col.get("definition", ""),
                            col.get("rationale", ""),
                            col.get("data_type", "text")
                        ])
                    
                    # Add special columns documentation
                    metadata_writer.writerow(["row_name", "Identifier for this data row", "Standard upload metadata field", "text"])
                    metadata_writer.writerow(["papers", "Source documents (if applicable)", "Standard upload metadata field", "text"])
                    
                    zip_file.writestr("column_metadata.csv", metadata_output.getvalue())
                    
                    # Add data file as CSV
                    if export_data["data"]:
                        output = io.StringIO()
                        # Get all column names
                        all_columns = set()
                        for row in export_data["data"]:
                            if "data" in row:
                                all_columns.update(row["data"].keys())
                            if "row_name" in row:
                                all_columns.add("row_name")
                            if "papers" in row:
                                all_columns.add("papers")
                        
                        column_names = sorted(list(all_columns))
                        writer = csv.DictWriter(output, fieldnames=column_names)
                        writer.writeheader()
                        
                        for row in export_data["data"]:
                            csv_row = {}
                            if "row_name" in row:
                                csv_row["row_name"] = row["row_name"]
                            if "papers" in row:
                                csv_row["papers"] = row["papers"]
                            if "data" in row:
                                for col, value in row["data"].items():
                                    csv_row[col] = str(value) if value is not None else ""
                            writer.writerow(csv_row)
                        
                        zip_file.writestr("data.csv", output.getvalue())
                    
                    # Add metadata file
                    zip_file.writestr("metadata.json", json.dumps(export_data["metadata"], indent=2))
                
                # Read and return zip file
                with open(tmp_file.name, 'rb') as f:
                    zip_content = f.read()
                
                # Clean up temp file
                Path(tmp_file.name).unlink()
                
                filename = f"{session_id[:8]}_complete_export.zip"
                return StreamingResponse(
                    io.BytesIO(zip_content),
                    media_type='application/zip',
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
                
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'zip'")
        
    except Exception as e:
        print(f"DEBUG: Exception in export_complete_data: {e}")
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
        
        # Get all data
        parser = FileParser()
        data_file = parser.data_dir / session_id / "data.jsonl"
        
        if not data_file.exists():
            raise HTTPException(status_code=404, detail="No data found for export")
        
        # Load all data rows
        rows = []
        with open(data_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    row_data = json.loads(line)
                    if 'data' in row_data:
                        # Extract data from DataRow format
                        flat_row = row_data['data'].copy()
                        if row_data.get('row_name'):
                            flat_row['row_name'] = row_data['row_name']
                        if row_data.get('papers'):
                            flat_row['papers'] = row_data['papers']
                        rows.append(flat_row)
                    else:
                        rows.append(row_data)
        
        if not rows:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Prepare metadata-rich CSV
        output = io.StringIO()
        
        # Get base columns from data
        base_columns = set()
        for row in rows:
            base_columns.update(row.keys())
        
        # Build enhanced column list with metadata columns
        enhanced_columns = []
        for col_name in sorted(base_columns):
            enhanced_columns.append(col_name)
            # Add metadata columns for schema columns (not for standard columns)
            if col_name not in ['row_name', 'papers']:
                enhanced_columns.append(f"{col_name}_definition")
                enhanced_columns.append(f"{col_name}_rationale")
                enhanced_columns.append(f"{col_name}_allowed_values")
        
        writer = csv.DictWriter(output, fieldnames=enhanced_columns)
        
        # Write metadata header rows first
        output.write("# Metadata-Rich CSV Export\n")
        output.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Source: {session.metadata.source}\n")
        if session.schema_query:
            output.write(f"# Query: {session.schema_query}\n")
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
        for row in rows:
            csv_row = {}
            
            # Process all columns
            for col_name, value in row.items():
                # Add the actual data value
                csv_row[col_name] = str(value) if value is not None else ""
                
                # Add metadata columns for schema columns (not for standard columns)
                if col_name not in ['row_name', 'papers'] and col_name in column_metadata:
                    csv_row[f"{col_name}_definition"] = column_metadata[col_name]['definition']
                    csv_row[f"{col_name}_rationale"] = column_metadata[col_name]['rationale']
                    csv_row[f"{col_name}_allowed_values"] = column_metadata[col_name]['allowed_values']
            
            writer.writerow(csv_row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename
        source_name = session.metadata.source or "uploaded_data"
        # Strip file extension to avoid double extensions (e.g., file.csv_timestamp.csv)
        if source_name.lower().endswith(('.csv', '.json', '.jsonl')):
            source_name = Path(source_name).stem
        safe_name = "".join(c for c in source_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{safe_name}_{timestamp}_rich.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"DEBUG: Exception in export_upload_rich_csv: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-schema/{session_id}")
async def export_schema_only(session_id: str):
    """Export only the schema metadata in QBSD format."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Create QBSD-compatible schema export
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
                "export_timestamp": datetime.now().isoformat()
            }
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            schema_export["schema_evolution"] = session.statistics.schema_evolution.model_dump()

        # Include observation_unit if available
        if session.observation_unit:
            schema_export["observation_unit"] = session.observation_unit.model_dump()

        content = json.dumps(schema_export, indent=2, ensure_ascii=False)

        # Generate filename
        source_name = session.metadata.source or "schema"
        # Strip file extension to avoid double extensions (e.g., file.csv_schema.json)
        if source_name.lower().endswith(('.csv', '.json', '.jsonl')):
            source_name = Path(source_name).stem
        safe_name = "".join(c for c in source_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"{safe_name}_schema_{session_id[:8]}.json"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='application/json',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"DEBUG: Exception in export_schema_only: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _is_system_file(filename: str) -> bool:
    """Check if a filename is a system file that should be ignored."""
    if not filename:
        return True
        
    system_files = {'.DS_Store', '._.DS_Store', 'Thumbs.db', '.gitkeep', '.gitignore'}
    system_prefixes = ('._', '.tmp', '~$')
    
    # Check exact matches
    if filename in system_files:
        return True
        
    # Check prefixes
    for prefix in system_prefixes:
        if filename.startswith(prefix):
            return True
            
    # Check for temporary Office files
    if filename.startswith('~$') or filename.startswith('.~'):
        return True
        
    return False