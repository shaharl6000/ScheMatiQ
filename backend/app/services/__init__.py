"""Shared service instances."""

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Optional, Tuple

from .websocket_manager import WebSocketManager
from .session_manager import SessionManager

from app.core.config import MAX_CONCURRENT_SESSIONS, QBSD_THREAD_POOL_SIZE
from app.core.exceptions import CapacityExceededError

logger = logging.getLogger(__name__)

# ── Shared thread pool for blocking QBSD operations ──────────────────
# Bounded pool prevents unbounded thread growth under concurrent load.
# 6 workers on 8 vCPU leaves headroom for the event loop and OS.
qbsd_thread_pool = ThreadPoolExecutor(
    max_workers=QBSD_THREAD_POOL_SIZE,
    thread_name_prefix="qbsd-worker",
)
logger.info("[concurrency] Thread pool initialized: %d workers (QBSD_THREAD_POOL_SIZE)", QBSD_THREAD_POOL_SIZE)


# ── Concurrency limiter for long-running operations ──────────────────
class ConcurrencyLimiter:
    """Tracks active long-running operations across all services.

    All LLM-heavy operations (QBSD creation, reextraction, continue discovery,
    document processing) share a single counter so the server never exceeds
    its capacity.
    """

    def __init__(self, max_concurrent: int):
        self._lock = asyncio.Lock()
        self._max = max_concurrent
        # session_id -> (operation_type, start_time)
        self._active: Dict[str, Tuple[str, float]] = {}

    async def acquire(self, session_id: str, operation: str) -> None:
        """Reserve a slot. Raises CapacityExceededError or RuntimeError."""
        async with self._lock:
            if session_id in self._active:
                existing_op = self._active[session_id][0]
                raise RuntimeError(
                    f"Session {session_id} already has an active operation: {existing_op}"
                )
            if len(self._active) >= self._max:
                logger.warning(
                    "[concurrency] REJECTED %s (%s) - at capacity. Active: %d/%d",
                    session_id[:8], operation, len(self._active), self._max,
                )
                raise CapacityExceededError(len(self._active), self._max)
            self._active[session_id] = (operation, time.monotonic())
            logger.info(
                "[concurrency] Acquired slot for %s (%s). Active: %d/%d",
                session_id[:8], operation, len(self._active), self._max,
            )

    async def release(self, session_id: str) -> None:
        """Release a slot. Safe to call even if not acquired."""
        async with self._lock:
            entry = self._active.pop(session_id, None)
            if entry:
                operation, start_time = entry
                duration = time.monotonic() - start_time
                minutes, seconds = divmod(int(duration), 60)
                logger.info(
                    "[concurrency] Released slot for %s (%s). Duration: %dm %ds. Active: %d/%d",
                    session_id[:8], operation, minutes, seconds,
                    len(self._active), self._max,
                )

    async def get_active_count(self) -> int:
        """Return the number of currently active operations."""
        async with self._lock:
            return len(self._active)


concurrency_limiter = ConcurrencyLimiter(MAX_CONCURRENT_SESSIONS)
logger.info("[concurrency] Concurrency limiter initialized: max %d sessions", MAX_CONCURRENT_SESSIONS)


def find_session_data_file(session_id: str) -> Optional[Path]:
    """Find the primary data file for a session (QBSD or load).

    QBSD sessions store extracted data in ./qbsd_work/{session_id}/extracted_data.jsonl.
    Load sessions store data in ./data/{session_id}/data.jsonl.
    """
    qbsd_file = Path("./qbsd_work") / session_id / "extracted_data.jsonl"
    if qbsd_file.exists():
        return qbsd_file
    load_file = Path("./data") / session_id / "data.jsonl"
    if load_file.exists():
        return load_file
    return None


# Create singleton instances
websocket_manager = WebSocketManager()
session_manager = SessionManager()

# ── Research data collection (Google Drive archival) ─────────────
# Gracefully disabled when credentials are not configured or
# google packages are not installed.
from app.core.config import DATA_COLLECTION_ENABLED

if DATA_COLLECTION_ENABLED:
    try:
        from app.storage.google_drive import GoogleDriveUploader
        from app.storage.google_sheets import GoogleSheetsLogger
        from app.services.data_collection_service import DataCollectionService

        _drive_uploader = GoogleDriveUploader.get_instance()
        _sheets_logger = GoogleSheetsLogger.get_instance()
        data_collection_service = DataCollectionService(
            session_manager=session_manager,
            uploader=_drive_uploader,
            sheets_logger=_sheets_logger,
        )
        if data_collection_service.is_enabled:
            logger.info("[data-collection] Service enabled — sessions will be archived to Google Drive")
        else:
            logger.info("[data-collection] Credentials invalid or missing — archival disabled")
            data_collection_service = None
    except Exception as e:
        logger.debug("[data-collection] Could not initialize: %s", e)
        data_collection_service = None
else:
    data_collection_service = None
