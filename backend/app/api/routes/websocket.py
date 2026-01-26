"""WebSocket endpoints for real-time updates."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio
from app.services import websocket_manager

router = APIRouter()

# Server-side heartbeat interval (seconds) - keeps connection alive on Railway
SERVER_HEARTBEAT_INTERVAL = 20


@router.websocket("/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time progress updates."""
    await websocket.accept()
    print(f"🔌 WebSocket CONNECTED: {session_id}")

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
        websocket_manager.add_connection(session_id, websocket)
        print(f"🔌 WebSocket REGISTERED: {session_id} (total: {websocket_manager.get_connection_count(session_id)})")

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
                print(f"🔌 WebSocket error for {session_id}: {e}")
                try:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"WebSocket error: {str(e)}"
                    })
                except Exception:
                    break

    except WebSocketDisconnect:
        print(f"🔌 WebSocket DISCONNECTED: {session_id}")
    finally:
        connection_active = False
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        websocket_manager.remove_connection(session_id, websocket)
        print(f"🔌 WebSocket REMOVED: {session_id} (remaining: {websocket_manager.get_connection_count(session_id)})")


@router.websocket("/logs/{session_id}")
async def websocket_logs(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time log streaming."""
    await websocket.accept()
    print(f"🔌 Log WebSocket CONNECTED: {session_id}")

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
        websocket_manager.add_log_connection(session_id, websocket)

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
        websocket_manager.remove_log_connection(session_id, websocket)
        print(f"🔌 Log WebSocket REMOVED: {session_id}")
