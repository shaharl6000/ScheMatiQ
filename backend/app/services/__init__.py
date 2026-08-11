"""Shared service instances."""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from .websocket_manager import WebSocketManager
from .session_manager import SessionManager

from app.core.config import MAX_CONCURRENT_SESSIONS, SCHEMATIQ_THREAD_POOL_SIZE
from app.core.exceptions import CapacityExceededError
from app.core.logging_utils import ContextPropagatingThreadPoolExecutor

logger = logging.getLogger(__name__)

# ── Shared thread pool for blocking ScheMatiQ operations ─────────────
# Bounded pool prevents unbounded thread growth under concurrent load.
# 6 workers on 8 vCPU leaves headroom for the event loop and OS.
# Context-propagating so that logs from offloaded work still carry the
# session id; a plain pool loses it and everything reads as no-session.
schematiq_thread_pool = ContextPropagatingThreadPoolExecutor(
    max_workers=SCHEMATIQ_THREAD_POOL_SIZE,
    thread_name_prefix="schematiq-worker",
)
logger.info("[concurrency] Thread pool initialized: %d workers (SCHEMATIQ_THREAD_POOL_SIZE)", SCHEMATIQ_THREAD_POOL_SIZE)


# ── Concurrency limiter for long-running operations ──────────────────
class ConcurrencyLimiter:
    """Tracks active long-running operations across all services.

    All LLM-heavy operations (ScheMatiQ creation, reextraction, continue discovery,
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

    def is_session_active(self, session_id: str) -> bool:
        """Return True if this session currently holds a concurrency slot."""
        return session_id in self._active


concurrency_limiter = ConcurrencyLimiter(MAX_CONCURRENT_SESSIONS)
logger.info("[concurrency] Concurrency limiter initialized: max %d sessions", MAX_CONCURRENT_SESSIONS)


# ── Serializes access to the process-global LLMCallTracker singleton ─
# Verified from schematiq-lib source (schematiq/core/llm_call_tracker.py):
# LLMCallTracker.get_instance() returns one instance per process, and
# _current_stage plus _counts live directly on that instance with no
# per-session or contextvar scoping. set_stage() changes which stage
# increment() credits for *every* concurrent caller, and get_counts() reads
# the same shared dict. Reextraction and continue-discovery record their
# LLM usage by snapshotting get_counts() before and after their run and
# recording the delta (see reextraction_service.py / continue_discovery_
# service.py). MAX_CONCURRENT_SESSIONS defaults to 5 and concurrency_limiter
# actually permits that many simultaneous runs, so without this lock two
# overlapping runs could both mislabel each other's calls (via set_stage)
# and corrupt each other's before/after delta. This lock makes each
# snapshot-run-snapshot sequence exclusive across the whole process for
# these two services.
llm_call_tracker_lock = asyncio.Lock()


def find_session_data_file_sync(session_id: str) -> Optional[Path]:
    """Find the primary data file for a session, hydrating from storage when needed.

    For use from sync/thread-pool code only. Async callers should use
    ``find_session_data_file`` instead.
    """
    from app.services.data_utils import resolve_primary_session_data_file_sync

    return resolve_primary_session_data_file_sync(session_id)


async def find_session_data_file(session_id: str) -> Optional[Path]:
    """Find the primary data file for a session, hydrating from storage when needed."""
    from app.services.data_utils import resolve_primary_session_data_file

    return await resolve_primary_session_data_file(session_id)


# ── Shared EmbeddingRetriever singleton ────────────────────────────
# The SentenceTransformer model load is expensive (~2s + HuggingFace
# HEAD requests).  All services use the same model, so we share one
# instance.  Per-query parameters (k) are overridden at call sites.
_shared_retriever = None
_shared_retriever_lock = __import__("threading").Lock()


def get_shared_retriever():
    """Return the process-wide EmbeddingRetriever, creating it on first call."""
    global _shared_retriever
    if _shared_retriever is None:
        with _shared_retriever_lock:
            if _shared_retriever is None:
                from schematiq.core.retrievers import EmbeddingRetriever
                logger.info("[retriever] Loading shared EmbeddingRetriever (all-MiniLM-L6-v2)")
                _shared_retriever = EmbeddingRetriever(
                    model_name="all-MiniLM-L6-v2",
                    k=10,
                    max_words=768,
                    enable_dynamic_k=True,
                    dynamic_k_threshold=0.65,
                    dynamic_k_minimum=3,
                )
                logger.info("[retriever] Shared EmbeddingRetriever ready")
    return _shared_retriever


# Create singleton instances
websocket_manager = WebSocketManager()
session_manager = SessionManager()

# ── Research data collection (Google Drive archival) ─────────────
# Gracefully disabled when credentials are not configured or
# google packages are not installed.
from app.core.config import (
    DATA_COLLECTION_ENABLED, DEVELOPER_MODE,
    GOOGLE_DRIVE_FOLDER_ID, GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_OAUTH_CREDENTIALS_JSON,
)

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
        logger.warning("[data-collection] Could not initialize: %s", e, exc_info=True)
        data_collection_service = None
else:
    data_collection_service = None
    # Log exactly why data collection is disabled so users can diagnose easily
    _reasons = []
    if DEVELOPER_MODE:
        _reasons.append("DEVELOPER_MODE=true")
    if not GOOGLE_DRIVE_FOLDER_ID:
        _reasons.append("GOOGLE_DRIVE_FOLDER_ID not set")
    if not (GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_OAUTH_CREDENTIALS_JSON):
        _reasons.append("no Google credentials set (need GOOGLE_OAUTH_CREDENTIALS_JSON or GOOGLE_SERVICE_ACCOUNT_JSON)")
    logger.info("[data-collection] Disabled — %s", "; ".join(_reasons) if _reasons else "DATA_COLLECTION_ENABLED=False")

# ── PubMed document enrichment (DOI link lookup) ──────────────────
from app.services.pubmed_enrichment_service import PubMedEnrichmentService
pubmed_enrichment_service = PubMedEnrichmentService(session_manager=session_manager)
logger.info("[pubmed-enrichment] Service initialized")

# ── UniProt row enrichment (protein data lookup) ───────────────────
from app.services.uniprot_enrichment_service import UniProtEnrichmentService
uniprot_enrichment_service = UniProtEnrichmentService(session_manager=session_manager)
logger.info("[uniprot-enrichment] Service initialized")
