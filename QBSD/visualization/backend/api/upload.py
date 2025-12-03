"""Upload API endpoints."""

import uuid
import json
from typing import List, Optional
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse

from models.session import VisualizationSession, SessionType, SessionMetadata, PaginatedData
from models.upload import FileValidationResult, ColumnMappingRequest, DataPreviewRequest
from services.file_parser import FileParser
from services.session_manager import SessionManager

router = APIRouter()
session_manager = SessionManager()

@router.post("/file", response_model=dict)
async def upload_file(file: UploadFile = File(...)):
    """Upload and validate a file."""
    try:
        # Validate file
        parser = FileParser()
        validation = await parser.validate_file(file)
        
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
        
        # Save file content for processing
        await parser.save_uploaded_file(session_id, file)
        
        # Store session
        session_manager.create_session(session)
        
        return {
            "session_id": session_id,
            "validation": validation,
            "requires_column_mapping": validation.detected_format == "csv"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/parse/{session_id}", response_model=dict)
async def parse_file(session_id: str, mapping: Optional[ColumnMappingRequest] = None):
    """Parse uploaded file with optional column mapping."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        parser = FileParser()
        result = await parser.parse_file(session_id, mapping)
        
        # Update session with parsed data
        session.columns = result["columns"]
        session.statistics = result["statistics"]
        session_manager.update_session(session)
        
        return {"status": "success", "message": "File parsed successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data/{session_id}", response_model=PaginatedData)
async def get_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get paginated data for a session."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        parser = FileParser()
        data = await parser.get_paginated_data(session_id, page, page_size)
        
        return data
        
    except Exception as e:
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