"""Shared service instances."""

import asyncio
import json
import logging
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from .websocket_manager import WebSocketManager
from .session_manager import SessionManager

from app.core.config import MAX_CONCURRENT_SESSIONS, SCHEMATIQ_THREAD_POOL_SIZE
from app.core.exceptions import CapacityExceededError
from app.core.logging_utils import ContextPropagatingThreadPoolExecutor
from app.models.schematiq import RetrieverConfig

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


# ── Shared EmbeddingRetriever & model caches ───────────────────────
# SentenceTransformer weights are heavy (~hundreds of MB + HF load calls), so
# we cache at most one loaded model per `model_name` in `_shared_embedding_models`
# (bounded LRU so distinct model names cannot grow memory without limit), and
# cache lightweight `EmbeddingRetriever` wrappers (and their per-chunking-config
# passage embedding cache) per full RetrieverConfig tuple in `_shared_retrievers`
# (bounded LRU so parameter sweeps evict old wrappers without reloading weights).
_MAX_SHARED_MODELS = 8
_MAX_SHARED_RETRIEVERS = 32
_shared_embedding_models: "OrderedDict[str, Any]" = OrderedDict()
_shared_retrievers: "OrderedDict[Tuple[Any, ...], Any]" = OrderedDict()
_shared_retriever_lock = threading.Lock()


def _normalize_retriever_config(
    retriever_config: Optional[Union[RetrieverConfig, Mapping[str, Any]]] = None,
    *,
    model_name: Optional[str] = None,
) -> RetrieverConfig:
    """Normalize dict/Pydantic/None retriever configs into a RetrieverConfig."""
    if isinstance(retriever_config, RetrieverConfig):
        data = retriever_config.model_dump()
    elif isinstance(retriever_config, Mapping):
        raw = {k: v for k, v in retriever_config.items() if v is not None}
        allowed_fields = set(RetrieverConfig.model_fields) | {
            "passage_chars",
            "max_words",
            "overlap_words",
        }
        data = {k: v for k, v in raw.items() if k in allowed_fields}
    elif retriever_config is None:
        data = {}
    else:
        raise TypeError(
            f"retriever_config must be RetrieverConfig, mapping, or None; "
            f"got {type(retriever_config).__name__}"
        )

    raw_model = data.get("model_name")
    if isinstance(raw_model, str):
        if raw_model.strip():
            data["model_name"] = raw_model.strip()
        else:
            data.pop("model_name")

    if model_name is not None and model_name.strip():
        data["model_name"] = model_name.strip()

    cfg = RetrieverConfig(**data)
    if cfg.type.lower() != "embedding":
        raise ValueError(f"Unsupported shared retriever type: {cfg.type!r}")
    return cfg


def get_shared_retriever(
    retriever_config: Optional[Union[RetrieverConfig, Mapping[str, Any]]] = None,
    *,
    model_name: Optional[str] = None,
):
    """Return a cached EmbeddingRetriever matching the given retriever config.

    Multiple configs sharing the same ``model_name`` share a single loaded model
    instance while keeping independent retrieval/chunking parameters and passage
    caches. Both the model cache and wrapper cache use bounded LRU eviction.
    """
    cfg = _normalize_retriever_config(retriever_config, model_name=model_name)
    cache_key = (
        cfg.type.lower(),
        cfg.model_name,
        cfg.passage_words,
        cfg.overlap,
        cfg.k,
        cfg.enable_dynamic_k,
        float(cfg.dynamic_k_threshold),
        cfg.dynamic_k_minimum,
    )
    with _shared_retriever_lock:
        cached = _shared_retrievers.get(cache_key)
        if cached is not None:
            _shared_retrievers.move_to_end(cache_key)
            if cfg.model_name in _shared_embedding_models:
                _shared_embedding_models.move_to_end(cfg.model_name)
            return cached

        from schematiq.core.retrievers import EmbeddingRetriever

        shared_model = _shared_embedding_models.get(cfg.model_name)
        if shared_model is not None:
            _shared_embedding_models.move_to_end(cfg.model_name)

        logger.info(
            "[retriever] %s EmbeddingRetriever "
            "(model=%s, k=%s, passage_words=%s, overlap=%s, dynamic_k=%s)",
            "Creating" if shared_model is not None else "Loading",
            cfg.model_name,
            cfg.k,
            cfg.passage_words,
            cfg.overlap,
            cfg.enable_dynamic_k,
        )
        init_kwargs: Dict[str, Any] = {
            "model_name": cfg.model_name,
            "k": cfg.k,
            "passage_words": cfg.passage_words,
            "overlap_words": cfg.overlap,
            "enable_dynamic_k": cfg.enable_dynamic_k,
            "dynamic_k_threshold": cfg.dynamic_k_threshold,
            "dynamic_k_minimum": cfg.dynamic_k_minimum,
        }
        if shared_model is not None:
            init_kwargs["model"] = shared_model

        cached = EmbeddingRetriever(**init_kwargs)

        if shared_model is None and getattr(cached, "model", None) is not None:
            _shared_embedding_models[cfg.model_name] = cached.model
            while len(_shared_embedding_models) > _MAX_SHARED_MODELS:
                _shared_embedding_models.popitem(last=False)

        _shared_retrievers[cache_key] = cached
        while len(_shared_retrievers) > _MAX_SHARED_RETRIEVERS:
            _shared_retrievers.popitem(last=False)

        logger.info("[retriever] Shared EmbeddingRetriever ready (%s)", cfg.model_name)
        return cached


def _extract_retriever_mapping(payload: Any) -> Optional[Dict[str, Any]]:
    """Extract a non-empty retriever mapping from a saved session/schema JSON dict."""
    if not isinstance(payload, Mapping):
        return None
    for key in ("retriever", "retriever_config"):
        candidate = payload.get(key)
        if isinstance(candidate, Mapping) and candidate:
            return dict(candidate)
    return None


def resolve_retriever_config_for_session(
    session_id: str,
    session_manager: Any = None,
    *,
    extracted_schema: Optional[Mapping[str, Any]] = None,
    data_dir: Optional[Union[str, Path]] = None,
    work_dir: Optional[Union[str, Path]] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve the saved retriever config dict for ``session_id``, if any.

    Checks session config sources in priority order (mirroring
    ``resolve_llm_for_session``):
      1. ``{work_dir}/{session_id}/schematiq_config.json``
      2. ``{work_dir}/{session_id}/config.json``
      3. ``extracted_schema`` argument or ``session.metadata.extracted_schema``
      4. ``{data_dir}/{session_id}/parsed_schema.json``
      5. ``{data_dir}/{session_id}/schematiq_config.json``
    Returns ``None`` when no custom retriever config was stored so
    ``get_shared_retriever(None)`` uses default ``RetrieverConfig()`` values.
    """
    from app.core.config import DEFAULT_DATA_DIR, DEFAULT_SCHEMATIQ_WORK_DIR

    resolved_work_dir = Path(work_dir if work_dir is not None else DEFAULT_SCHEMATIQ_WORK_DIR)
    resolved_data_dir = Path(data_dir if data_dir is not None else DEFAULT_DATA_DIR)
    schematiq_session_dir = resolved_work_dir / session_id
    data_session_dir = resolved_data_dir / session_id

    for config_path in (
        schematiq_session_dir / "schematiq_config.json",
        schematiq_session_dir / "config.json",
    ):
        try:
            if config_path.exists():
                with open(config_path, encoding="utf-8") as f:
                    found = _extract_retriever_mapping(json.load(f))
                if found is not None:
                    return found
        except Exception as exc:
            logger.debug("Could not load retriever config from %s: %s", config_path, exc)

    schema_candidates: list[Mapping[str, Any]] = []
    if isinstance(extracted_schema, Mapping):
        schema_candidates.append(extracted_schema)
    if session_manager is not None:
        try:
            session = session_manager.get_session(session_id)
            if session and session.metadata and isinstance(session.metadata.extracted_schema, Mapping):
                schema_candidates.append(session.metadata.extracted_schema)
        except Exception as exc:
            logger.debug("Could not load retriever config from session metadata: %s", exc)

    for candidate in schema_candidates:
        found = _extract_retriever_mapping(candidate)
        if found is not None:
            return found

    for data_config_path in (
        data_session_dir / "parsed_schema.json",
        data_session_dir / "schematiq_config.json",
    ):
        try:
            if data_config_path.exists():
                with open(data_config_path, encoding="utf-8") as f:
                    found = _extract_retriever_mapping(json.load(f))
                if found is not None:
                    return found
        except Exception as exc:
            logger.debug("Could not load retriever config from %s: %s", data_config_path, exc)

    return None


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
