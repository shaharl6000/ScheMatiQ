"""WebSocket endpoints for real-time updates."""

from typing import Dict, Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio
from services import websocket_manager

router = APIRouter()

@router.websocket("/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time progress updates."""
    await websocket.accept()
    
    try:
        # Add connection to manager
        websocket_manager.add_connection(session_id, websocket)
        
        # Send initial connection confirmation
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
            "message": "Connected to progress updates"
        })
        
        # Keep connection alive
        while True:
            try:
                # Wait for messages from client (like ping/pong)
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle ping/pong for connection health
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                    
            except WebSocketDisconnect:
                break
            except Exception as e:
                await websocket.send_json({
                    "type": "error",
                    "message": f"WebSocket error: {str(e)}"
                })
                
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.remove_connection(session_id, websocket)

@router.websocket("/logs/{session_id}")
async def websocket_logs(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time log streaming."""
    await websocket.accept()
    
    try:
        websocket_manager.add_log_connection(session_id, websocket)
        
        await websocket.send_json({
            "type": "log_connected",
            "session_id": session_id,
            "message": "Connected to log stream"
        })
        
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                    
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.remove_log_connection(session_id, websocket)