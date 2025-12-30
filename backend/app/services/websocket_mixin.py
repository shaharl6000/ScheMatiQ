"""WebSocket broadcasting mixin for services."""

from typing import Dict, Any
from datetime import datetime
from .websocket_manager import WebSocketManager


class WebSocketBroadcasterMixin:
    """Mixin class providing common WebSocket broadcasting functionality."""
    
    def __init__(self, websocket_manager: WebSocketManager):
        self.websocket_manager = websocket_manager
    
    async def broadcast_progress(
        self, 
        session_id: str, 
        message: str, 
        progress: float,
        status: str = "processing"
    ):
        """Broadcast progress update via WebSocket."""
        await self.websocket_manager.broadcast_progress(session_id, {
            "session_id": session_id,
            "status": status,
            "progress": progress,
            "message": message,
            "timestamp": datetime.now().isoformat()
        })
    
    async def broadcast_completion(
        self,
        session_id: str,
        message: str,
        result_data: Dict[str, Any] = None
    ):
        """Broadcast completion message via WebSocket."""
        data = {
            "session_id": session_id,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        if result_data:
            data.update(result_data)

        # Debug: Log WebSocket connection status
        conn_count = self.websocket_manager.get_connection_count(session_id)
        print(f"🔌 DEBUG: WebSocket connections for {session_id}: {conn_count} active")
        print(f"🔌 DEBUG: Broadcasting completion with data: {data}")

        await self.websocket_manager.broadcast_completion(session_id, data)
    
    async def broadcast_error(
        self, 
        session_id: str, 
        error_message: str,
        error_details: Dict[str, Any] = None
    ):
        """Broadcast error message via WebSocket."""
        data = {
            "session_id": session_id,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        if error_details:
            data.update(error_details)
        
        await self.websocket_manager.broadcast_error(session_id, error_message)
    
    async def broadcast_status_update(
        self, 
        session_id: str, 
        status: str, 
        message: str = None,
        additional_data: Dict[str, Any] = None
    ):
        """Broadcast general status update via WebSocket."""
        data = {
            "session_id": session_id,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        if message:
            data["message"] = message
        if additional_data:
            data.update(additional_data)
        
        await self.websocket_manager.broadcast_progress(session_id, data)
    
    async def broadcast_step_progress(
        self,
        session_id: str,
        step_name: str,
        step_number: int,
        total_steps: int,
        step_progress: float = None,
        message: str = None,
        details: Dict[str, Any] = None
    ):
        """Broadcast step-based progress update."""
        overall_progress = (step_number - 1 + (step_progress or 0)) / total_steps

        data = {
            "session_id": session_id,
            "status": "processing",
            "progress": overall_progress,
            "current_step": step_name,
            "step_number": step_number,
            "total_steps": total_steps,
            "step_progress": step_progress,
            "timestamp": datetime.now().isoformat()
        }
        if message:
            data["message"] = message
        if details:
            data["details"] = details

        await self.websocket_manager.broadcast_progress(session_id, data)
    
    async def broadcast_schema_completed(
        self, 
        session_id: str, 
        schema_data: Dict[str, Any]
    ):
        """Broadcast schema completion event via WebSocket."""
        await self.websocket_manager.broadcast_schema_completed(session_id, schema_data)
    
    async def broadcast_row_completed(
        self,
        session_id: str,
        row_data: Dict[str, Any]
    ):
        """Broadcast row completion event via WebSocket."""
        await self.websocket_manager.broadcast_row_completed(session_id, row_data)

    async def broadcast_cell_extracted(
        self,
        session_id: str,
        cell_data: Dict[str, Any]
    ):
        """Broadcast individual cell value extraction via WebSocket.

        Used for real-time streaming of values to the UI as they're extracted.
        Uses buffering to handle race condition where extraction starts
        before WebSocket connection is fully registered.

        Args:
            cell_data: Dict with row_name, column, and value keys
        """
        # Debug: Log WebSocket connection status
        conn_count = self.websocket_manager.get_connection_count(session_id)
        print(f"📡 CELL BROADCAST: {session_id} has {conn_count} connections")
        print(f"📡 CELL BROADCAST: {cell_data.get('row_name')}/{cell_data.get('column')}")

        # Use buffer_or_broadcast_cell to handle race condition
        message = {
            "type": "cell_extracted",
            "timestamp": datetime.now().isoformat(),
            "data": cell_data
        }
        await self.websocket_manager.buffer_or_broadcast_cell(session_id, message)