"""WebSocket endpoints for real-time updates."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio
import logging
from app.services import websocket_manager
from app.core.logging_utils import set_session_context

logger = logging.getLogger(__name__)
router = APIRouter()

# Server-side heartbeat interval (seconds) - keeps connection alive on Railway
SERVER_HEARTBEAT_INTERVAL = 20


@router.websocket("/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time progress updates."""
    await websocket.accept()
    set_session_context(session_id)
    logger.info("WebSocket connected")

    # Flag to control heartbeat task
    connection_active = True

    async def server_heartbeat():
        """Send periodic heartbeats from server to keep connection alive."""
        while connection_active:
            try:
                await asyncio.sleep(SERVER_HEARTBEAT_INTERVAL)
                if connection_active:
                    await websocket.send_json({"type": "heartbeat"})
            except Exception:
                break

    # Start server-side heartbeat task
    heartbeat_task = asyncio.create_task(server_heartbeat())

    try:
        # Add connection to manager
        await websocket_manager.add_connection(session_id, websocket)
        logger.info(f"WebSocket registered (total: {websocket_manager.get_connection_count(session_id)})")

        # Send initial connection confirmation
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
            "message": "Connected to progress updates"
        })

        # Keep connection alive
        while True:
            try:
                # Wait for messages from client with timeout
                # Timeout is 2x heartbeat interval to allow for network delays
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=SERVER_HEARTBEAT_INTERVAL * 3
                )
                message = json.loads(data)

                # Handle ping/pong for connection health
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                # No message received, but that's OK - server heartbeat keeps connection alive
                # Just continue the loop
                continue
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                try:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"WebSocket error: {str(e)}"
                    })
                except Exception:
                    break

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    finally:
        connection_active = False
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await websocket_manager.remove_connection(session_id, websocket)
        logger.info(f"WebSocket removed (remaining: {websocket_manager.get_connection_count(session_id)})")


@router.websocket("/logs/{session_id}")
async def websocket_logs(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time log streaming."""
    await websocket.accept()
    set_session_context(session_id)
    logger.info("Log WebSocket connected")

    # Flag to control heartbeat task
    connection_active = True

    async def server_heartbeat():
        """Send periodic heartbeats from server to keep connection alive."""
        while connection_active:
            try:
                await asyncio.sleep(SERVER_HEARTBEAT_INTERVAL)
                if connection_active:
                    await websocket.send_json({"type": "heartbeat"})
            except Exception:
                break

    # Start server-side heartbeat task
    heartbeat_task = asyncio.create_task(server_heartbeat())

    try:
        await websocket_manager.add_log_connection(session_id, websocket)

        await websocket.send_json({
            "type": "log_connected",
            "session_id": session_id,
            "message": "Connected to log stream"
        })

        while True:
            try:
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=SERVER_HEARTBEAT_INTERVAL * 3
                )
                message = json.loads(data)

                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                continue
            except WebSocketDisconnect:
                break

    except WebSocketDisconnect:
        pass
    finally:
        connection_active = False
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await websocket_manager.remove_log_connection(session_id, websocket)
        logger.info("Log WebSocket removed")
