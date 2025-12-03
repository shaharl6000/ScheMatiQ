"""QBSD API endpoints."""

import uuid
import asyncio
from typing import List
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from models.session import VisualizationSession, SessionType, SessionMetadata
from models.qbsd import QBSDConfig, QBSDStatus
from services.qbsd_runner import QBSDRunner
from services.session_manager import SessionManager

router = APIRouter()
session_manager = SessionManager()

@router.post("/configure", response_model=dict)
async def configure_qbsd(config: QBSDConfig):
    """Configure a new QBSD session."""
    try:
        # Validate configuration
        runner = QBSDRunner()
        validation = await runner.validate_config(config)
        
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
        await runner.save_config(session_id, config)
        
        return {
            "session_id": session_id,
            "message": "QBSD session configured successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/run/{session_id}")
async def run_qbsd(session_id: str, background_tasks: BackgroundTasks):
    """Start QBSD execution."""
    try:
        session = session_manager.get_session(session_id)
        if not session or session.type != SessionType.QBSD:
            raise HTTPException(status_code=404, detail="QBSD session not found")
        
        runner = QBSDRunner()
        
        # Start QBSD in background
        background_tasks.add_task(runner.run_qbsd, session_id)
        
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
        
        runner = QBSDRunner()
        status = await runner.get_status(session_id)
        
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
        
        runner = QBSDRunner()
        schema = await runner.get_schema(session_id)
        
        return schema
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data/{session_id}")
async def get_qbsd_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get extracted data."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        runner = QBSDRunner()
        data = await runner.get_data(session_id, page, page_size)
        
        return data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop/{session_id}")
async def stop_qbsd(session_id: str):
    """Stop QBSD execution."""
    try:
        runner = QBSDRunner()
        success = await runner.stop_execution(session_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="No running QBSD session found")
        
        return {"message": "QBSD execution stopped"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sessions", response_model=List[VisualizationSession])
async def list_qbsd_sessions():
    """List all QBSD sessions."""
    return session_manager.list_sessions(SessionType.QBSD)