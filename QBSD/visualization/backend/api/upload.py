"""Upload API endpoints."""

import uuid
import json
from typing import List, Optional
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse

from models.session import VisualizationSession, SessionType, SessionMetadata, PaginatedData, SessionStatus
from models.upload import (
    FileValidationResult, ColumnMappingRequest, DataPreviewRequest,
    SchemaValidationResult, DualFileUploadResult, CompatibilityCheck
)
from services.file_parser import FileParser
from services.session_manager import SessionManager

router = APIRouter()
session_manager = SessionManager()

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
        
        # Update session with parsed data
        session.columns = result["columns"]
        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED  # Mark as completed after successful parsing
        session_manager.update_session(session)
        
        print(f"DEBUG: Session updated successfully")
        
        return {"status": "success", "message": "File parsed successfully"}
        
    except Exception as e:
        print(f"DEBUG: Exception in parse_file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data/{session_id}", response_model=PaginatedData)
async def get_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get paginated data for a session."""
    try:
        print(f"DEBUG: Getting data for session: {session_id}, page: {page}, page_size: {page_size}")
        
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        print(f"DEBUG: Found session: {session.id}, status: {session.status}")
        
        parser = FileParser()
        data = await parser.get_paginated_data(session_id, page, page_size)
        
        print(f"DEBUG: Retrieved data: {len(data.rows)} rows, total: {data.total_count}")
        
        return data
        
    except Exception as e:
        print(f"DEBUG: Exception in get_data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

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
            from models.session import ColumnInfo
            
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
        else:
            # Fallback to basic column info from data
            session.columns = result["columns"]
            
        session.statistics = result["statistics"]
        session.status = SessionStatus.COMPLETED
        session_manager.update_session(session)
        
        print(f"DEBUG: Dual file processing completed successfully")
        
        return {"status": "success", "message": "Files processed successfully"}
        
    except Exception as e:
        print(f"DEBUG: Exception in process_dual_files: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))