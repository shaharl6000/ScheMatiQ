"""WebSocket connection management."""

import json
import asyncio
from typing import Dict, Set, List, Any
from fastapi import WebSocket
from datetime import datetime

class WebSocketManager:
    """Manages WebSocket connections for real-time updates."""
    
    def __init__(self):
        # Session ID -> Set of WebSocket connections
        self.connections: Dict[str, Set[WebSocket]] = {}
        self.log_connections: Dict[str, Set[WebSocket]] = {}
    
    def add_connection(self, session_id: str, websocket: WebSocket):
        """Add a WebSocket connection for a session."""
        if session_id not in self.connections:
            self.connections[session_id] = set()
        self.connections[session_id].add(websocket)
    
    def remove_connection(self, session_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if session_id in self.connections:
            self.connections[session_id].discard(websocket)
            if not self.connections[session_id]:
                del self.connections[session_id]
    
    def add_log_connection(self, session_id: str, websocket: WebSocket):
        """Add a WebSocket connection for log streaming."""
        if session_id not in self.log_connections:
            self.log_connections[session_id] = set()
        self.log_connections[session_id].add(websocket)
    
    def remove_log_connection(self, session_id: str, websocket: WebSocket):
        """Remove a log WebSocket connection."""
        if session_id in self.log_connections:
            self.log_connections[session_id].discard(websocket)
            if not self.log_connections[session_id]:
                del self.log_connections[session_id]
    
    async def broadcast_progress(self, session_id: str, progress_data: Dict[str, Any]):
        """Broadcast progress update to all connected clients."""
        if session_id not in self.connections:
            return
        
        message = {
            "type": "progress",
            "timestamp": datetime.now().isoformat(),
            "data": progress_data
        }
        
        # Send to all connections for this session
        dead_connections = []
        for websocket in self.connections[session_id]:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)
        
        # Remove dead connections
        for websocket in dead_connections:
            self.remove_connection(session_id, websocket)
    
    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]):
        """Broadcast log message to all log connections."""
        if session_id not in self.log_connections:
            return
        
        message = {
            "type": "log",
            "timestamp": datetime.now().isoformat(),
            "data": log_data
        }
        
        # Send to all log connections for this session
        dead_connections = []
        for websocket in self.log_connections[session_id]:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)
        
        # Remove dead connections
        for websocket in dead_connections:
            self.remove_log_connection(session_id, websocket)
    
    async def broadcast_error(self, session_id: str, error_message: str):
        """Broadcast error message to all connections."""
        error_data = {
            "type": "error",
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        
        await self.broadcast_progress(session_id, error_data)
        await self.broadcast_log(session_id, error_data)
    
    async def broadcast_completion(self, session_id: str, result_data: Dict[str, Any]):
        """Broadcast completion message."""
        completion_data = {
            "type": "completed",
            "timestamp": datetime.now().isoformat(),
            "result": result_data
        }
        
        await self.broadcast_progress(session_id, completion_data)
    
    async def broadcast_schema_completed(self, session_id: str, schema_data: Dict[str, Any]):
        """Broadcast schema discovery completion."""
        schema_completion_data = {
            "type": "schema_completed",
            "timestamp": datetime.now().isoformat(),
            "schema": schema_data
        }
        
        await self.broadcast_progress(session_id, schema_completion_data)
    
    async def broadcast_row_completed(self, session_id: str, row_data: Dict[str, Any]):
        """Broadcast individual row completion during value extraction."""
        row_completion_data = {
            "type": "row_completed", 
            "timestamp": datetime.now().isoformat(),
            "row": row_data
        }
        
        await self.broadcast_progress(session_id, row_completion_data)
    
    def get_connection_count(self, session_id: str) -> int:
        """Get number of active connections for a session."""
        return len(self.connections.get(session_id, set()))
    
    def has_connections(self, session_id: str) -> bool:
        """Check if session has any active connections."""
        return session_id in self.connections and len(self.connections[session_id]) > 0