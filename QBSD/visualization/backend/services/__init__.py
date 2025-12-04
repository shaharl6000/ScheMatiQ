"""Shared service instances."""

from .websocket_manager import WebSocketManager
from .session_manager import SessionManager

# Create singleton instances
websocket_manager = WebSocketManager()
session_manager = SessionManager()