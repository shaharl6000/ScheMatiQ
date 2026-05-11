"""WebSocket callback factories and heartbeat for pipeline operations."""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


def create_value_extracted_callback(ws_mixin, session_id: str, loop: asyncio.AbstractEventLoop):
    """Create a callback that streams extracted cell values via WebSocket.

    The callback bridges sync extraction code to async WebSocket broadcasting.
    """
    def on_value_extracted(row_name: str, column_name: str, value: Any):
        try:
            asyncio.run_coroutine_threadsafe(
                ws_mixin.broadcast_cell_extracted(session_id, {
                    "row_name": row_name,
                    "column": column_name,
                    "value": value
                }),
                loop
            )
        except Exception as e:
            logger.warning("Failed to broadcast cell %s for %s: %s", column_name, row_name, e)

    return on_value_extracted


def create_warning_callback(ws_manager, session_id: str, loop: asyncio.AbstractEventLoop):
    """Create a callback that broadcasts warnings via WebSocket.

    The callback bridges sync extraction code to async WebSocket broadcasting.
    Used to surface issues like observation unit parsing failures to the UI.
    """
    def on_warning(paper_title: str, warning_type: str, message: str):
        try:
            asyncio.run_coroutine_threadsafe(
                ws_manager.broadcast_log(session_id, {
                    "level": "warning",
                    "message": f"[{paper_title}] {warning_type}: {message}",
                    "paper_title": paper_title,
                    "warning_type": warning_type,
                    "details": message
                }),
                loop
            )
        except Exception as e:
            logger.warning("Failed to broadcast warning for %s: %s", paper_title, e)

    return on_warning


async def start_heartbeat(ws_manager, session_id: str, interval: float = 15.0) -> asyncio.Task:
    """Start a background heartbeat to keep WebSocket alive during long operations.

    Args:
        ws_manager: WebSocket manager instance
        session_id: The session to send heartbeats to
        interval: Seconds between heartbeat messages (default 15s)

    Returns:
        The heartbeat task (caller should cancel when done)
    """
    async def heartbeat_loop():
        while True:
            await asyncio.sleep(interval)
            try:
                await ws_manager.broadcast_log(session_id, {
                    "level": "info",
                    "message": "Processing... (still working)"
                })
            except Exception as e:
                logger.warning("Heartbeat failed: %s", e)
                break

    return asyncio.create_task(heartbeat_loop())
