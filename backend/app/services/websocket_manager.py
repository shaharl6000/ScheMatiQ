"""WebSocket connection management."""

import json
import asyncio
import logging
from typing import Dict, Set, Any, List
from fastapi import WebSocket
from datetime import datetime

logger = logging.getLogger(__name__)

class WebSocketManager:
    """Manages WebSocket connections for real-time updates."""

    def __init__(self):
        # Session ID -> Set of WebSocket connections
        self.connections: Dict[str, Set[WebSocket]] = {}
        self.log_connections: Dict[str, Set[WebSocket]] = {}
        # Buffer for cell events when no connections exist (handles race condition)
        self.pending_cell_events: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def add_connection(self, session_id: str, websocket: WebSocket):
        """Add a WebSocket connection for a session and flush any buffered events."""
        async with self._lock:
            if session_id not in self.connections:
                self.connections[session_id] = set()
            self.connections[session_id].add(websocket)

        # Schedule flush of any buffered cell events (outside lock)
        asyncio.create_task(self._flush_buffered_events(session_id))

    async def remove_connection(self, session_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        async with self._lock:
            if session_id in self.connections:
                self.connections[session_id].discard(websocket)
                if not self.connections[session_id]:
                    del self.connections[session_id]

    async def add_log_connection(self, session_id: str, websocket: WebSocket):
        """Add a WebSocket connection for log streaming."""
        async with self._lock:
            if session_id not in self.log_connections:
                self.log_connections[session_id] = set()
            self.log_connections[session_id].add(websocket)

    async def remove_log_connection(self, session_id: str, websocket: WebSocket):
        """Remove a log WebSocket connection."""
        async with self._lock:
            if session_id in self.log_connections:
                self.log_connections[session_id].discard(websocket)
                if not self.log_connections[session_id]:
                    del self.log_connections[session_id]
    
    async def broadcast_progress(self, session_id: str, progress_data: Dict[str, Any]):
        """Broadcast progress update to all connected clients."""
        # Snapshot connections under lock
        async with self._lock:
            ws_set = self.connections.get(session_id)
            if not ws_set:
                return
            snapshot = list(ws_set)

        message = {
            "type": "progress",
            "timestamp": datetime.now().isoformat(),
            "data": progress_data
        }

        # Send to all connections (outside lock to avoid blocking during I/O)
        dead_connections = []
        for websocket in snapshot:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)

        # Remove dead connections
        for websocket in dead_connections:
            await self.remove_connection(session_id, websocket)
    
    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]):
        """Broadcast log message to all log connections."""
        # Snapshot connections under lock
        async with self._lock:
            ws_set = self.log_connections.get(session_id)
            if not ws_set:
                return
            snapshot = list(ws_set)

        message = {
            "type": "log",
            "timestamp": datetime.now().isoformat(),
            "data": log_data
        }

        # Send to all log connections (outside lock)
        dead_connections = []
        for websocket in snapshot:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)

        # Remove dead connections
        for websocket in dead_connections:
            await self.remove_log_connection(session_id, websocket)
    
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
        # Snapshot connections under lock
        async with self._lock:
            ws_set = self.connections.get(session_id)
            if not ws_set:
                return
            snapshot = list(ws_set)

        message = {
            "type": "completion",
            "timestamp": datetime.now().isoformat(),
            "data": result_data
        }

        # Send to all connections (outside lock)
        dead_connections = []
        for websocket in snapshot:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)

        # Remove dead connections
        for websocket in dead_connections:
            await self.remove_connection(session_id, websocket)
    
    # Schema editing specific broadcast methods
    async def broadcast_schema_updated(self, session_id: str, update_data: Dict[str, Any]):
        """Broadcast schema update notification."""
        message = {
            "type": "schema_updated",
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "data": update_data
        }
        
        await self.broadcast_to_session(session_id, message)
    
    async def broadcast_reprocessing_progress(self, session_id: str, progress_data: Dict[str, Any]):
        """Broadcast reprocessing progress updates."""
        message = {
            "type": "reprocessing_progress",
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "data": progress_data
        }
        
        await self.broadcast_to_session(session_id, message)
    
    async def broadcast_reprocessing_completed(self, session_id: str, completion_data: Dict[str, Any]):
        """Broadcast reprocessing completion."""
        message = {
            "type": "reprocessing_completed",
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "data": completion_data
        }
        
        await self.broadcast_to_session(session_id, message)
    
    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]):
        """Generic method to broadcast a message to all connections for a session."""
        # Snapshot connections under lock
        async with self._lock:
            ws_set = self.connections.get(session_id)
            if not ws_set:
                logger.warning(f"No connections for session {session_id}")
                logger.warning(f"Active sessions: {list(self.connections.keys())}")
                return
            snapshot = list(ws_set)

        dead_connections = []
        for websocket in snapshot:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send message to websocket: {e}")
                dead_connections.append(websocket)

        # Remove dead connections
        for websocket in dead_connections:
            await self.remove_connection(session_id, websocket)
    
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
        # Snapshot connections under lock
        async with self._lock:
            ws_set = self.connections.get(session_id)
            if not ws_set:
                return
            snapshot = list(ws_set)

        message = {
            "type": "row_completed",
            "timestamp": datetime.now().isoformat(),
            "data": row_data
        }

        dead_connections = []
        for websocket in snapshot:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_connections.append(websocket)

        # Remove dead connections
        for websocket in dead_connections:
            await self.remove_connection(session_id, websocket)
    
    def get_connection_count(self, session_id: str) -> int:
        """Get number of active connections for a session."""
        return len(self.connections.get(session_id, set()))

    def has_connections(self, session_id: str) -> bool:
        """Check if session has any active connections."""
        return session_id in self.connections and len(self.connections[session_id]) > 0

    async def buffer_or_broadcast_cell(self, session_id: str, message: Dict[str, Any]):
        """Broadcast cell event or buffer if no connections exist.

        This handles the race condition where cell extraction starts
        before the WebSocket connection is fully registered.
        """
        async with self._lock:
            has_conns = session_id in self.connections and len(self.connections[session_id]) > 0
            if not has_conns:
                # Buffer the event for later delivery
                if session_id not in self.pending_cell_events:
                    self.pending_cell_events[session_id] = []
                self.pending_cell_events[session_id].append(message)
                cell_info = message.get('data', {})
                logger.debug(f"Buffered cell event: {cell_info.get('row_name')}/{cell_info.get('column')} (will flush when connected)")
                return

        # Connection exists, broadcast immediately (broadcast_to_session handles its own lock)
        await self.broadcast_to_session(session_id, message)

    async def _flush_buffered_events(self, session_id: str):
        """Send any buffered cell events to newly connected client."""
        async with self._lock:
            events = self.pending_cell_events.pop(session_id, [])
        if events:
            logger.info(f"Flushing {len(events)} buffered cell events for {session_id}")
            for event in events:
                await self.broadcast_to_session(session_id, event)