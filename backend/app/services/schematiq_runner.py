"""ScheMatiQ integration service — thin facade delegating to pipeline modules."""

import json
import asyncio
import logging
import os
import random
import threading
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from datetime import datetime

from app.core.config import (
    MAX_DOCUMENTS, DEVELOPER_MODE, LLM_CALL_GLOBAL_LIMIT, LLM_CALL_LIMIT_WINDOW_DAYS,
    LLM_USAGE_SYNC_TTL_SECONDS,
)
from app.core.logging_utils import set_session_context
from app.services import schematiq_thread_pool, concurrency_limiter

from app.services.pipeline.llm_factory import build_llm_interface, enforce_release_llm_config
from app.services.pipeline.callbacks import start_heartbeat
from app.services.pipeline.config_handler import (
    resolve_docs_paths, convert_config_to_schematiq_format, validate_config as _validate_config,
)
from app.services.pipeline.schema_discovery import run_schema_discovery
from app.services.pipeline.error_reporting import describe_llm_error
from app.services.pipeline.value_extraction import run_value_extraction
from app.services.pipeline.data_query import (
    compute_statistics, get_status as _get_status, get_schema as _get_schema, get_data as _get_data,
)

logger = logging.getLogger(__name__)

from schematiq.core.schema import ObservationUnit
from schematiq.core.llm_call_tracker import LLMCallTracker, GlobalLLMUsageTracker, QuotaExceededError
from schematiq.core.cost_estimator import estimate_from_config

from app.models.schematiq import ScheMatiQConfig, ScheMatiQStatus
from app.models.session import (
    ColumnInfo, PaginatedData, SessionStatus,
    ObservationUnitInfo,
    SkippedDocumentInfo
)
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin

class ScheMatiQRunner(WebSocketBroadcasterMixin):
    """Handles ScheMatiQ execution and integration."""

    def __init__(self, work_dir: str = "./schematiq_work", websocket_manager=None, session_manager=None,
                 data_collection_service=None, pubmed_enrichment_service=None,
                 uniprot_enrichment_service=None):
        if websocket_manager is not None:
            self.websocket_manager = websocket_manager
        else:
            self.websocket_manager = WebSocketManager()

        if session_manager is not None:
            self.session_manager = session_manager
        else:
            self.session_manager = SessionManager()

        super().__init__(self.websocket_manager)

        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.running_sessions: Dict[str, asyncio.Task] = {}
        self._data_collection_service = data_collection_service
        self._pubmed_enrichment_service = pubmed_enrichment_service
        self._uniprot_enrichment_service = uniprot_enrichment_service
        self.stop_flags: Dict[str, bool] = {}
        self._state_lock = threading.Lock()
        self._global_usage = GlobalLLMUsageTracker(self.work_dir / "global_llm_usage.json")
        # Throttle Google Sheets reads made by quota checks. The local usage
        # file already reflects everything recorded by this process (pipeline
        # and chat), so the external total only guards against redeploys and
        # is allowed to be a few minutes stale.
        self._usage_sync_ttl = LLM_USAGE_SYNC_TTL_SECONDS
        self._usage_sync_lock = threading.Lock()
        self._last_sheets_sync = 0.0
        self._external_window_cache: Dict[int, Tuple[float, int]] = {}

    # ── Usage tracking ─────────────────────────────────────────────

    def _sync_usage_from_sheets(self, force: bool = False) -> None:
        """Sync the local global usage file from Google Sheets.

        Reads are throttled to once per LLM_USAGE_SYNC_TTL_SECONDS (default
        300). Pass ``force=True`` to bypass the throttle (e.g. /api/usage).
        """
        now = time.monotonic()
        with self._usage_sync_lock:
            if not force and now - self._last_sheets_sync < self._usage_sync_ttl:
                return
            self._last_sheets_sync = now
        try:
            from app.storage.google_sheets import GoogleSheetsLogger
            sheets = GoogleSheetsLogger.get_instance()
            if sheets is None:
                return
            external_total = sheets.read_total_llm_calls()
            if external_total > 0:
                self._global_usage.sync_from_external(external_total)
        except Exception as e:
            logger.debug("Could not sync usage from Google Sheets: %s", e)

    def _log_llm_usage_to_sheets(self, session_id: str, counts: dict) -> None:
        """Write this session's LLM usage to Google Sheets."""
        try:
            from app.storage.google_sheets import GoogleSheetsLogger
            sheets = GoogleSheetsLogger.get_instance()
            if sheets is None:
                return
            total = sum(counts.values())
            sheets.log_llm_usage(session_id, total, counts)
        except Exception as e:
            logger.debug("Could not log LLM usage to Google Sheets: %s", e)

    def _external_usage_total(self, window_days: int = 0, force: bool = False) -> int:
        """Read the cumulative (or windowed) LLM call total from Google Sheets.

        Results are cached for LLM_USAGE_SYNC_TTL_SECONDS per window (failures
        cache as 0, which also avoids hammering a failing Sheets API).
        """
        now = time.monotonic()
        if not force:
            with self._usage_sync_lock:
                cached = self._external_window_cache.get(window_days)
                if cached and now - cached[0] < self._usage_sync_ttl:
                    return cached[1]
        total = 0
        try:
            from app.storage.google_sheets import GoogleSheetsLogger
            sheets = GoogleSheetsLogger.get_instance()
            if sheets is not None:
                if window_days > 0:
                    total = sheets.read_recent_llm_calls(window_days)
                else:
                    total = sheets.read_total_llm_calls()
        except Exception as e:
            logger.debug("Could not read LLM usage from Google Sheets: %s", e)
            total = 0
        with self._usage_sync_lock:
            self._external_window_cache[window_days] = (now, total)
        return total

    def get_quota_usage(self, limit: int, force_sync: bool = False) -> Dict[str, Any]:
        """Return effective usage vs *limit*, honoring LLM_CALL_LIMIT_WINDOW_DAYS.

        With a rolling window configured, the effective total is the max of
        the local windowed total and the Google Sheets windowed total (the
        local file may be wiped by a redeploy; the sheet may lag behind the
        local file). Without a window, legacy behavior: sync the lifetime
        scalar from Sheets, then read the local lifetime total.
        """
        window = LLM_CALL_LIMIT_WINDOW_DAYS
        if window > 0:
            used = max(
                self._global_usage.get_windowed_total(window),
                self._external_usage_total(window, force=force_sync),
            )
        else:
            self._sync_usage_from_sheets(force=force_sync)
            used = self._global_usage.get_total()
        return {
            "used": used,
            "limit": limit,
            "window_days": window,
            "remaining": max(limit - used, 0) if limit > 0 else None,
        }

    def check_global_quota(self, limit: int) -> None:
        """Raise ``QuotaExceededError`` when the global LLM quota is exhausted.

        A *limit* of 0 (or less) disables the check. Honors the optional
        rolling window (``LLM_CALL_LIMIT_WINDOW_DAYS``).
        """
        if limit <= 0:
            return
        usage = self.get_quota_usage(limit)
        if usage["used"] >= limit:
            # Visible on the uvicorn terminal even when logging is minimal
            print(
                f"[LLM quota] blocked: used={usage['used']} quota_limit={limit} "
                f"window_days={usage['window_days']} "
                f"(raise LLM_CALL_GLOBAL_LIMIT, set a rolling window via "
                f"LLM_CALL_LIMIT_WINDOW_DAYS, or LLM_CALL_GLOBAL_LIMIT=0 to disable)",
                flush=True,
            )
            raise QuotaExceededError(used=usage["used"], limit=limit)

    def get_usage_report(self) -> Dict[str, Any]:
        """Full quota usage report for the ``/api/usage`` endpoint.

        Always bypasses the Sheets read throttle so the endpoint reports
        fresh numbers when you are actively investigating.
        """
        report = self.get_quota_usage(LLM_CALL_GLOBAL_LIMIT, force_sync=True)
        data = self._global_usage.get_usage()
        report["enforced"] = (not DEVELOPER_MODE) and LLM_CALL_GLOBAL_LIMIT > 0
        report["lifetime_total"] = data.get("total_calls", 0)
        report["per_stage"] = data.get("per_stage", {})
        report["recent_sessions"] = [
            {
                "session_id": (entry.get("session_id") or "")[:8],
                "calls": entry.get("calls", 0),
                "timestamp": entry.get("timestamp"),
            }
            for entry in data.get("sessions", [])[-10:]
        ]
        return report

    def record_external_usage(self, source_id: str, counts: Dict[str, int]) -> None:
        """Record LLM calls made outside the main pipeline (e.g. chat) toward the global quota.

        Writes to the same local usage file and Google Sheet as pipeline runs.
        """
        self._global_usage.record_session(source_id, counts)
        self._log_llm_usage_to_sheets(source_id, counts)

    # ── Stop management ────────────────────────────────────────────

    def is_stop_requested(self, session_id: str) -> bool:
        with self._state_lock:
            return self.stop_flags.get(session_id, False)

    def clear_stop_flag(self, session_id: str):
        with self._state_lock:
            self.stop_flags.pop(session_id, None)

    async def request_stop(self, session_id: str) -> Dict[str, Any]:
        """Set the stop flag for a session and return immediately."""
        with self._state_lock:
            if session_id not in self.running_sessions:
                return {"accepted": False, "message": "No running session found"}
            self.stop_flags[session_id] = True

        logger.info("Stop requested for session %s", session_id)

        # Emit an immediate optimistic 'stopped' so the monitor leaves the spinner
        # state right away. The pipeline can't cancel an in-flight LLM call mid-thread,
        # so its own stop handler (_handle_stop_after_*) may be delayed by up to one
        # long LLM call; without this the UI hangs on "Wrapping up...". The task's
        # later 'stopped' event refines these counts with the final numbers.
        try:
            session_dir = self.work_dir / session_id
            schema_saved = (session_dir / "discovered_schema.json").exists()
            rows_saved = 0
            data_file = session_dir / "extracted_data.jsonl"
            if data_file.exists():
                with open(data_file, 'r') as f:
                    rows_saved = sum(1 for _ in f)
            await self.broadcast_stopped(session_id, {
                "schema_saved": schema_saved,
                "data_rows_saved": rows_saved,
                "message": "Stop requested",
            })
        except Exception as e:
            logger.warning("Could not send optimistic stopped broadcast for %s: %s", session_id, e)

        return {"accepted": True, "message": "Stop signal sent"}

    async def stop_execution(self, session_id: str) -> Dict[str, Any]:
        """Stop ScheMatiQ execution gracefully."""
        logger.debug("stop_execution: Called for session %s", session_id)
        logger.debug("stop_execution: running_sessions keys = %s", list(self.running_sessions.keys()))

        result = {
            "stopped": False,
            "schema_saved": False,
            "data_rows_saved": 0,
            "message": ""
        }

        with self._state_lock:
            is_running = session_id in self.running_sessions
            if is_running:
                logger.debug("stop_execution: Session found in running_sessions, setting stop flag")
                self.stop_flags[session_id] = True
                task = self.running_sessions[session_id]

        if is_running:
            try:
                logger.debug("stop_execution: Waiting for task to finish gracefully...")
                await asyncio.wait_for(asyncio.shield(task), timeout=60.0)
                logger.debug("stop_execution: Task finished gracefully")
            except asyncio.TimeoutError:
                logger.warning("Graceful stop timed out after 60s, force cancelling task for %s", session_id)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                logger.debug("stop_execution: Task was cancelled")
                pass
            except Exception as e:
                logger.error("Exception during stop: %s", e)

            with self._state_lock:
                self.running_sessions.pop(session_id, None)
                self.stop_flags.pop(session_id, None)

            session_dir = self.work_dir / session_id
            schema_file = session_dir / "discovered_schema.json"
            data_file = session_dir / "extracted_data.jsonl"

            if schema_file.exists():
                result["schema_saved"] = True

            if data_file.exists():
                with open(data_file, 'r') as f:
                    result["data_rows_saved"] = sum(1 for _ in f)

            session = self.session_manager.get_session(session_id)
            if session and session.status not in (SessionStatus.STOPPED, SessionStatus.COMPLETED):
                logger.debug("stop_execution: Task didn't handle stop — force-updating to STOPPED")
                session.status = SessionStatus.STOPPED
                session.error_message = None
                self.session_manager.update_session(session)
                await self.broadcast_stopped(session_id, {
                    "schema_saved": result["schema_saved"],
                    "data_rows_saved": result["data_rows_saved"],
                    "message": "Processing force-stopped"
                })
            else:
                logger.debug("stop_execution: Task already handled stop (status=%s)", session.status if session else "None")

            result["stopped"] = True
            result["message"] = "Processing stopped successfully"
            return result

        logger.debug("stop_execution: Session NOT in running_sessions!")
        result["message"] = "No running session found"
        return result

    # ── Config ─────────────────────────────────────────────────────

    async def validate_config(self, config: ScheMatiQConfig) -> Dict[str, Any]:
        return await _validate_config(config)

    async def save_config(self, session_id: str, config: ScheMatiQConfig):
        session_dir = self.work_dir / session_id
        session_dir.mkdir(exist_ok=True)
        config_file = session_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config.model_dump(), f, indent=2)

    # ── Query ──────────────────────────────────────────────────────

    async def get_status(self, session_id: str) -> ScheMatiQStatus:
        return await _get_status(
            session_id, self.session_manager, self.running_sessions,
            self._state_lock, self.work_dir,
        )

    async def get_schema(self, session_id: str) -> Dict[str, Any]:
        return await _get_schema(session_id, self.session_manager, self.work_dir)

    async def get_data(
        self,
        session_id: str,
        page: int = 0,
        page_size: int = 50,
        filters: Optional[List[Dict]] = None,
        sort: Optional[List[Dict]] = None,
        search: Optional[str] = None,
        document_filter: Optional[List[str]] = None
    ) -> PaginatedData:
        return await _get_data(
            session_id, self.work_dir, page, page_size, filters, sort, search, document_filter,
        )

    # ── Pipeline execution ─────────────────────────────────────────

    async def run_schematiq(self, session_id: str):
        """Run ScheMatiQ discovery process."""
        set_session_context(session_id)
        config = None
        try:
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.PROCESSING
            
            # Initialize write_artifacts from config or DEVELOPER_MODE
            config_file = self.work_dir / session_id / "config.json"
            with open(config_file) as f:
                config_data = json.load(f)
            config = ScheMatiQConfig(**config_data)
            session.write_artifacts = config.write_artifacts if config.write_artifacts is not None else DEVELOPER_MODE
            
            self.session_manager.update_session(session)

            await self.broadcast_step_progress(
                session_id,
                "Starting ScheMatiQ execution...",
                step_number=1,
                total_steps=5,
                step_progress=0.0,
                message="Initializing pipeline..."
            )

            config_file = self.work_dir / session_id / "config.json"
            with open(config_file) as f:
                config_data = json.load(f)
            config = ScheMatiQConfig(**config_data)

            task = asyncio.create_task(self._execute_schematiq(session_id, config))
            with self._state_lock:
                self.running_sessions[session_id] = task

            await task

        except Exception as e:
            user_message, log_detail = describe_llm_error(e)
            logger.error("ScheMatiQ run failed for session %s: %s", session_id, log_detail)
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = user_message
            self.session_manager.update_session(session)

            is_quota_error = "quota exceeded" in str(e).lower()
            if is_quota_error:
                usage = self._global_usage.get_usage()
                total_used = usage.get("total_calls", 0)
                effective_limit = LLM_CALL_GLOBAL_LIMIT if not DEVELOPER_MODE else ((config.llm_call_limit or 0) if config else 0)
                await self.websocket_manager.broadcast_to_session(session_id, {
                    "type": "quota_exceeded",
                    "message": "The system has reached its API usage limit and cannot start new processing sessions.",
                    "data": {
                        "total_used": total_used,
                        "limit": effective_limit,
                    },
                    "timestamp": datetime.now().isoformat(),
                })
            else:
                await self.broadcast_error(session_id, user_message)

        finally:
            with self._state_lock:
                self.running_sessions.pop(session_id, None)
            self.clear_stop_flag(session_id)
            await concurrency_limiter.release(session_id)

    async def _reset_for_observation_unit_rediscovery(self, session_id: str) -> None:
        """Clear schema/data artifacts so a full rediscovery run starts clean."""
        session_dir = self.work_dir / session_id
        for fname in (
            "extracted_data.jsonl",
            "discovered_schema.json",
            "value_extraction_schema.json",
            "llm_call_stats.json",
        ):
            artifact = session_dir / fname
            if artifact.exists():
                artifact.unlink()
                logger.info("Removed %s for observation unit rediscovery", fname)

        session = self.session_manager.get_session(session_id)
        if not session:
            return

        session.columns = []
        session.statistics = None
        session.error_message = None
        session.metadata.schema_discovery_completed = False
        session.metadata.processed_documents = 0
        session.metadata.total_documents = 0
        session.metadata.processing_stats = {}
        self.session_manager.update_session(session)

    async def prepare_resume(self, session_id: str) -> None:
        """Stop any in-flight run, reset for rediscovery if needed, update config, acquire slot."""
        set_session_context(session_id)

        config_file = self.work_dir / session_id / "config.json"
        if not config_file.exists():
            raise RuntimeError(f"Config file not found for session {session_id}")

        with open(config_file) as f:
            config_data = json.load(f)

        session = self.session_manager.get_session(session_id)
        if not session:
            raise RuntimeError(f"Session {session_id} not found")

        rediscover = bool(session.metadata.pending_observation_unit_rediscovery)

        with self._state_lock:
            is_running = session_id in self.running_sessions

        slot_held = concurrency_limiter.is_session_active(session_id)
        if is_running or self.is_stop_requested(session_id):
            if rediscover:
                session.metadata.skip_stop_data_collection = True
                self.session_manager.update_session(session)
            logger.info(
                "Stopping in-flight pipeline before resume for session %s (rediscover=%s)",
                session_id,
                rediscover,
            )
            await self.stop_execution(session_id)
        elif slot_held:
            logger.warning(
                "Concurrency slot still held for session %s without running task — waiting to clear",
                session_id,
            )
            for _ in range(120):
                if not concurrency_limiter.is_session_active(session_id):
                    break
                await asyncio.sleep(1)
            if concurrency_limiter.is_session_active(session_id):
                await concurrency_limiter.release(session_id)

        if rediscover:
            await self._reset_for_observation_unit_rediscovery(session_id)
            session = self.session_manager.get_session(session_id)
            if session:
                session.metadata.observation_unit_rediscovery_run = True
                self.session_manager.update_session(session)
                logger.info("Prepared session %s for observation unit rediscovery", session_id)

        session = self.session_manager.get_session(session_id)
        if session and session.observation_unit:
            obs_unit = session.observation_unit
            config_data["initial_observation_unit"] = {
                "name": obs_unit.name,
                "definition": obs_unit.definition,
            }
            if obs_unit.example_names:
                config_data["initial_observation_unit"]["example_names"] = obs_unit.example_names

            examples_suffix = ""
            if obs_unit.example_names:
                examples_suffix = f" (examples: {', '.join(obs_unit.example_names)})"
            resume_log = (
                f'Resuming with observation unit: "{obs_unit.name}": '
                f'{obs_unit.definition}{examples_suffix}'
            )
            logger.info(resume_log)
            await self.websocket_manager.broadcast_to_session(session_id, {
                "type": "log",
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "level": "info",
                    "message": resume_log,
                },
            })

        config_data["review_observation_unit"] = False

        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)

        for attempt in range(120):
            try:
                await concurrency_limiter.acquire(session_id, "schematiq")
                return
            except RuntimeError as exc:
                if "already has an active operation" not in str(exc):
                    raise
                if attempt == 0:
                    logger.info(
                        "Waiting for session %s pipeline slot to free before resume",
                        session_id,
                    )
                await asyncio.sleep(1)

        raise RuntimeError(
            f"Timed out waiting for session {session_id} to stop before resume"
        )

    async def resume_qbsd(self, session_id: str, acquire_slot: bool = False):
        """Resume ScheMatiQ after observation unit review or post-edit rediscovery."""
        await self.prepare_resume(session_id)
        await self.run_schematiq(session_id)

    async def _execute_schematiq(self, session_id: str, config: ScheMatiQConfig):
        """Execute the ScheMatiQ pipeline — orchestrates pipeline modules."""
        schematiq_start_time = time.time()
        session_dir = self.work_dir / session_id
        session_dir.mkdir(exist_ok=True)

        progress_steps = [
            "Initializing ScheMatiQ pipeline",
            "Loading documents",
            "Setting up AI models",
            "Configuring retrieval system",
            "Schema Discovery: Analyzing documents",
            "Value Extraction: Processing documents",
            "Finalizing results"
        ]

        current_step = 0
        total_steps = len(progress_steps)

        initial_cost_estimate_usd = 0.0
        initial_calls_estimate = 0

        async def update_progress(step_name: str, step_progress: float = 0.0, details: Dict[str, Any] = None):
            nonlocal current_step
            llm_tracker = LLMCallTracker.get_instance()
            current_cost_data = llm_tracker.calculate_current_cost()
            llm_stats = {
                "total_calls": llm_tracker.get_total(),
                "current_cost_usd": current_cost_data["total_cost_usd"],
                "estimated_cost_usd": initial_cost_estimate_usd,
                "estimated_calls": initial_calls_estimate
            }
            if details is None:
                details = {}
            details["llm_stats"] = llm_stats
            await self.broadcast_step_progress(
                session_id,
                step_name,
                current_step + 1,
                total_steps,
                step_progress,
                details.get("message") if details else None,
                details
            )

        try:
            llm_tracker = LLMCallTracker.get_instance()
            llm_tracker.reset()

            # Check global LLM usage quota
            if DEVELOPER_MODE:
                effective_limit = config.llm_call_limit or 0
            else:
                effective_limit = LLM_CALL_GLOBAL_LIMIT

            if effective_limit > 0:
                try:
                    self.check_global_quota(effective_limit)
                except QuotaExceededError as exc:
                    logger.warning("Session %s blocked by global LLM quota: %s", session_id, exc)
                    raise RuntimeError(str(exc)) from exc

            # Step 1: Initialize
            logger.debug("Starting ScheMatiQ execution for session %s", session_id)
            await update_progress("Initializing", 0.0)

            logger.debug("Resolving document paths (may download from Supabase)")
            resolved_docs_paths = await resolve_docs_paths(config, session_id, self.work_dir)
            logger.debug("Resolved paths: %s", resolved_docs_paths)

            logger.debug("Converting config to ScheMatiQ format")
            schematiq_config = convert_config_to_schematiq_format(config, session_id, self.work_dir, resolved_docs_paths)

            schematiq_config_file = session_dir / "schematiq_config.json"
            with open(schematiq_config_file, 'w') as f:
                json.dump(schematiq_config, f, indent=2)
            logger.debug("Saved ScheMatiQ config with keys: %s", list(schematiq_config.keys()))

            await update_progress("Initializing", 1.0)

            # Step 2: Load documents
            current_step += 1
            await update_progress("Loading documents", 0.0)

            docs_paths = schematiq_config["docs_path"]
            if isinstance(docs_paths, str):
                docs_paths = [docs_paths]

            # Mirror the extension set the schematiq-lib reader (and the commit
            # layer in document_preprocessor) treat as loadable, so files placed
            # in documents/ — e.g. .html/.htm — are not silently dropped here.
            from app.services.document_preprocessor import LIB_READABLE_STORAGE_EXTENSIONS

            documents = []
            filenames = []
            seen_filenames = set()
            total_docs = 0

            for docs_path in docs_paths:
                doc_path = Path(docs_path)
                if not doc_path.exists():
                    continue
                doc_files = sorted(
                    f for f in doc_path.iterdir()
                    if f.is_file() and f.suffix.lower() in LIB_READABLE_STORAGE_EXTENSIONS
                )
                for doc_file in doc_files:
                    # De-duplicate across dirs by filename (pending_documents/
                    # takes precedence over documents/ via the ordering above).
                    if doc_file.name in seen_filenames:
                        continue
                    seen_filenames.add(doc_file.name)
                    total_docs += 1
                    try:
                        content = doc_file.read_text(encoding='utf-8')
                        documents.append(content)
                        filenames.append(doc_file.name)
                    except Exception as e:
                        logger.warning("Could not read %s: %s", doc_file, e)

            # Cap documents at MAX_DOCUMENTS
            bypass_limit = schematiq_config.get("bypass_limit", False)
            if not (DEVELOPER_MODE and bypass_limit) and len(documents) > MAX_DOCUMENTS:
                seed = schematiq_config.get("document_randomization_seed", 42)
                original_count = len(documents)
                combined = list(zip(documents, filenames))
                rng = random.Random(seed)
                rng.shuffle(combined)
                combined = combined[:MAX_DOCUMENTS]
                if combined:
                    documents, filenames = zip(*combined)
                    documents = list(documents)
                    filenames = list(filenames)
                else:
                    documents = []
                    filenames = []
                total_docs = MAX_DOCUMENTS
                logger.info("Document limit applied: selected %d of %d documents (seed=%d)", MAX_DOCUMENTS, original_count, seed)

                capped_dir = session_dir / "capped_documents"
                capped_dir.mkdir(exist_ok=True)
                for fname in filenames:
                    for dp in docs_paths:
                        source = Path(dp) / fname
                        if source.exists():
                            dest = capped_dir / fname
                            if not dest.exists():
                                os.symlink(source.resolve(), dest)
                            break
                schematiq_config["docs_path"] = [str(capped_dir)]
                logger.info("Value extraction redirected to capped_documents/ with %d files", len(filenames))

            await update_progress("Loading documents", 1.0, {
                "total_documents": total_docs,
                "loaded_documents": len(documents)
            })

            # Cost estimate
            try:
                cost_estimate = estimate_from_config(documents, schematiq_config)
                initial_cost_estimate_usd = cost_estimate.total_cost_usd
                initial_calls_estimate = cost_estimate.total_api_calls
                logger.info(f"Initial cost estimate: ${initial_cost_estimate_usd}, calls: {initial_calls_estimate}")
            except Exception as e:
                logger.warning(f"Failed to estimate cost during execution: {e}")

            has_query = bool(schematiq_config.get("query", "").strip())
            has_documents = bool(documents)

            if not has_query and not has_documents:
                raise RuntimeError("At least one of query or documents must be provided")

            if has_query and has_documents:
                logger.debug("STANDARD mode - query + %d documents", len(documents))
            elif has_documents:
                logger.debug("DOCUMENT_ONLY mode - %d documents, no query", len(documents))
            else:
                logger.debug("QUERY_ONLY mode - query provided, no documents")

            # Step 3: Build LLM backend
            current_step += 1
            logger.debug("Building Schema Creation LLM backend - provider: %s", schematiq_config['schema_creation_backend']['provider'])
            await update_progress("Building LLM backend", 0.0)

            schema_backend = enforce_release_llm_config(schematiq_config["schema_creation_backend"], is_schema_creation=True)
            logger.debug("Creating Schema Creation LLM interface...")
            llm = build_llm_interface(
                provider=schema_backend["provider"],
                model=schema_backend["model"],
                max_output_tokens=schema_backend.get("max_output_tokens"),
                temperature=schema_backend["temperature"],
                api_key=schema_backend.get("api_key"),
                context_window_size=schema_backend.get("context_window_size")
            )
            logger.debug("LLM interface created successfully")

            logger.debug("Updating progress to 1.0...")
            await update_progress("Building LLM backend", 1.0)
            logger.debug("Progress update completed")

            # Step 4: Setup retriever
            current_step += 1
            logger.debug("Setting up retriever...")
            await update_progress("Setting up retriever", 0.0)

            retriever = None
            if "retriever" in schematiq_config:
                from app.services import get_shared_retriever
                retriever = get_shared_retriever()
                logger.debug("Retriever ready (shared singleton)")
            else:
                logger.debug("No retriever config found, using None")

            await update_progress("Setting up retriever", 1.0)

            # Step 5: Observation unit review (optional pause)
            review_observation_unit = schematiq_config.get("review_observation_unit", False)
            if review_observation_unit and has_documents:
                paused = await self._handle_observation_unit_review(
                    session_id, documents, filenames, schematiq_config, llm, retriever, update_progress
                )
                if paused:
                    return  # Pipeline will be resumed via /schematiq/resume endpoint

            # Step 6: Schema discovery
            current_step += 1
            await update_progress("Discovering schema", 0.0)

            logger.debug("Starting schema discovery with %d documents", len(documents))
            heartbeat_task = await start_heartbeat(self.websocket_manager, session_id, interval=15.0)
            try:
                discovered_schema, schema_evolution = await run_schema_discovery(
                    session_id, documents, filenames, schematiq_config, llm, retriever,
                    update_progress, self.is_stop_requested,
                    ws_manager=self.websocket_manager,
                    work_dir=self.work_dir,
                )
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

            logger.debug("Schema discovery completed with %d columns", len(discovered_schema.columns))
            logger.debug("Schema evolution: %d snapshots tracked", len(schema_evolution.snapshots))
            logger.info("LLM call stats after schema discovery: %s", llm_tracker.get_summary())

            # Save and broadcast schema
            self._save_schema_to_session(session_id, discovered_schema, schematiq_config, schema_evolution)

            await update_progress("Schema Discovery: Complete", 1.0, {
                "columns_discovered": len(discovered_schema.columns)
            })

            # Check for stop after schema discovery
            if self.is_stop_requested(session_id):
                await self._handle_stop_after_schema(session_id)
                return

            # Step 6b: Value extraction
            skipped_documents: List[SkippedDocumentInfo] = []
            if schematiq_config.get("skip_value_extraction", False) or not has_documents:
                logger.info("Skipping value extraction (schema-only mode)")
            else:
                current_step += 1
                await update_progress("Extracting values", 0.0)

                value_backend = enforce_release_llm_config(schematiq_config["value_extraction_backend"], is_schema_creation=False)
                logger.debug("Creating Value Extraction LLM interface...")
                value_extraction_llm = build_llm_interface(
                    provider=value_backend["provider"],
                    model=value_backend["model"],
                    max_output_tokens=value_backend.get("max_output_tokens"),
                    temperature=value_backend["temperature"],
                    api_key=value_backend.get("api_key"),
                    context_window_size=value_backend.get("context_window_size")
                )
                logger.debug("Value Extraction LLM interface created successfully")

                heartbeat_task = await start_heartbeat(self.websocket_manager, session_id, interval=15.0)
                try:
                    skipped_documents = await run_value_extraction(
                        session_id, schematiq_config, discovered_schema, value_extraction_llm,
                        retriever, update_progress, self.is_stop_requested,
                        ws_mixin=self, ws_manager=self.websocket_manager,
                        session_manager=self.session_manager, work_dir=self.work_dir,
                        write_artifacts=config.write_artifacts if config.write_artifacts is not None else DEVELOPER_MODE,
                    )
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

                await update_progress("Extracting values", 1.0)
                logger.info("LLM call stats after value extraction: %s", llm_tracker.get_summary())

            # Check for stop after value extraction
            if self.is_stop_requested(session_id):
                await self._handle_stop_after_extraction(session_id)
                return

            # Step 7: Finalize
            current_step += 1
            await update_progress("Finalizing results", 0.0)

            statistics = compute_statistics(
                session_id,
                discovered_schema,
                schema_evolution,
                skipped_documents,
                self.work_dir,
            )

            # Save LLM call tracking
            llm_call_summary = llm_tracker.get_summary()
            llm_call_summary["log"] = llm_tracker.get_log()
            llm_stats_file = session_dir / "llm_call_stats.json"
            with open(llm_stats_file, 'w') as f:
                json.dump(llm_call_summary, f, indent=2)
            logger.info("LLM call stats saved to %s: %s", llm_stats_file, llm_tracker.get_counts())

            if config.count_toward_quota:
                self._global_usage.record_session(session_id, llm_tracker.get_counts())
                self._log_llm_usage_to_sheets(session_id, llm_tracker.get_counts())
            else:
                logger.info("Session %s opted out of quota tracking (count_toward_quota=False)", session_id)

            # Update session as completed
            session = self.session_manager.get_session(session_id)
            session.statistics = statistics
            session.status = SessionStatus.COMPLETED
            final_cost_data = llm_tracker.calculate_current_cost()
            session.metadata.processing_stats["llm_stats"] = {
                "total_calls": llm_tracker.get_total(),
                "current_cost_usd": final_cost_data.get("total_cost_usd", 0.0),
                "estimated_cost_usd": initial_cost_estimate_usd,
                "estimated_calls": initial_calls_estimate,
            }
            if session.metadata.observation_unit_rediscovery_run:
                session.metadata.pending_observation_unit_rediscovery = False
                session.metadata.observation_unit_rediscovery_run = False
                logger.info("Cleared pending_observation_unit_rediscovery after successful rediscovery")
            self.session_manager.update_session(session)
            self.session_manager.capture_schema_baseline(session_id)

            await update_progress("Finalizing results", 1.0)

            # Broadcast completion
            schema_only = schematiq_config.get("skip_value_extraction", False) or not has_documents
            completion_message = (
                "Schema discovery completed (value extraction skipped)"
                if schema_only
                else "ScheMatiQ execution completed successfully"
            )
            completion_details = {
                "total_documents": total_docs,
                "schema_columns": len(discovered_schema.columns),
                "schema_only": schema_only,
                "elapsed_seconds": int(time.time() - schematiq_start_time),
                "llm_stats": session.metadata.processing_stats.get("llm_stats", {}),
            }
            if DEVELOPER_MODE:
                completion_details["llm_call_stats"] = llm_tracker.get_counts()
            await self.broadcast_completion(session_id, completion_message, completion_details)

            # Fire-and-forget post-completion tasks
            if self._data_collection_service:
                await self._data_collection_service.trigger_archive(session_id, "schematiq_completion")
            if self._pubmed_enrichment_service:
                await self._pubmed_enrichment_service.enrich_session(session_id)
            if self._uniprot_enrichment_service:
                await self._uniprot_enrichment_service.enrich_session(session_id)

            # Move processed documents from pending_documents/ to documents/
            self._move_pending_documents(session_id)

        except Exception as e:
            user_message, log_detail = describe_llm_error(e)
            logger.error("ScheMatiQ execution failed: %s", log_detail, exc_info=True)
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = user_message
            self.session_manager.update_session(session)
            await self.broadcast_error(session_id, user_message)
            raise

    # ── Private orchestration helpers ──────────────────────────────

    async def _handle_observation_unit_review(
        self, session_id, documents, filenames, schematiq_config, llm, retriever, update_progress
    ) -> bool:
        """Discover observation unit and pause for review. Returns True if paused."""
        import functools
        from schematiq.core import schematiq as ScheMatiQ
        from schematiq import discover_observation_unit, ObservationUnitDiscoveryError

        await update_progress("Discovering observation unit", 0.0)

        initial_obs_unit = schematiq_config.get("initial_observation_unit")
        obs_unit_already_set = initial_obs_unit and initial_obs_unit.get("definition")

        current_step = 4  # observation unit review is after retriever setup
        if obs_unit_already_set:
            obs_unit = ObservationUnit(
                name=initial_obs_unit["name"],
                definition=initial_obs_unit["definition"]
            )
            logger.info("Using pre-configured observation unit for review: %s", obs_unit.name)
        else:
            batch_size = schematiq_config.get("documents_batch_size", 1)
            first_batch_docs = documents[:batch_size]
            first_batch_names = filenames[:batch_size]
            query = schematiq_config.get("query", "")

            loop = asyncio.get_running_loop()
            relevant_content = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(
                    ScheMatiQ.select_relevant_content,
                    docs=first_batch_docs,
                    query=query,
                    retriever=retriever,
                )
            )

            logger.info("Discovering observation unit for review...")
            logger.debug("[%s] Offloading discover_observation_unit to thread pool", session_id)
            try:
                obs_unit = await loop.run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(
                        discover_observation_unit,
                        query=query if query.strip() else None,
                        passages=relevant_content,
                        llm=llm,
                        context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                        source_document=first_batch_names[0] if first_batch_names else None,
                    )
                )
                # If name was pre-configured (name-only mode), override discovered name
                if initial_obs_unit and initial_obs_unit.get("name") and not initial_obs_unit.get("definition"):
                    logger.info("Overriding discovered name '%s' with pre-configured name '%s'", obs_unit.name, initial_obs_unit["name"])
                    obs_unit.name = initial_obs_unit["name"]
                logger.info("Observation unit discovered for review: %s - %s", obs_unit.name, obs_unit.definition)
            except ObservationUnitDiscoveryError as e:
                logger.error("Observation unit discovery failed: %s", e)
                raise RuntimeError(
                    f"Failed to discover observation unit: {e}. "
                    "Ensure documents contain extractable entities."
                ) from e
            except Exception as e:
                logger.error("Unexpected error during observation unit discovery: %s", e)
                raise RuntimeError(
                    f"Observation unit discovery failed unexpectedly: {e}"
                ) from e

        session = self.session_manager.get_session(session_id)
        session.observation_unit = ObservationUnitInfo(
            name=obs_unit.name,
            definition=obs_unit.definition,
            example_names=obs_unit.example_names,
            source_document=getattr(obs_unit, 'source_document', None),
            discovery_iteration=getattr(obs_unit, 'discovery_iteration', None)
        )
        session.status = SessionStatus.OBSERVATION_UNIT_REVIEW
        self.session_manager.update_session(session)
        logger.info("Pipeline paused for observation unit review: %s", obs_unit.name)

        obs_unit_data = {
            "name": obs_unit.name,
            "definition": obs_unit.definition,
            "example_names": obs_unit.example_names or [],
        }
        await self.broadcast_observation_unit_ready(session_id, obs_unit_data)
        await concurrency_limiter.release(session_id)
        return True

    def _save_schema_to_session(self, session_id, discovered_schema, schematiq_config, schema_evolution):
        """Save discovered schema to file and session object, then broadcast."""
        session_dir = self.work_dir / session_id
        schema_file = session_dir / "discovered_schema.json"
        frontend_schema = []
        for col in discovered_schema.columns:
            col_dict = col.to_dict()
            frontend_col = {
                "name": col_dict.get("column", col.name),
                "definition": col_dict.get("definition", ""),
                "rationale": col_dict.get("explanation", col.rationale)
            }
            if col_dict.get("allowed_values"):
                frontend_col["allowed_values"] = col_dict["allowed_values"]
            frontend_schema.append(frontend_col)

        schema_for_frontend = {
            "query": schematiq_config["query"],
            "schema": frontend_schema
        }
        if discovered_schema.observation_unit:
            schema_for_frontend["observation_unit"] = discovered_schema.observation_unit.to_dict()

        with open(schema_file, 'w') as f:
            json.dump(schema_for_frontend, f, indent=2)

        session = self.session_manager.get_session(session_id)
        session.status = SessionStatus.SCHEMA_READY
        session.metadata.schema_discovery_completed = True
        logger.debug("Updated session %s status to SCHEMA_READY with %d columns", session_id, len(discovered_schema.columns))

        # User-seeded (locked) columns were sanitized to canonical keys before
        # discovery; re-attach their original typed text as the display label.
        seeded_display_names = {
            entry["name"]: entry["display_name"]
            for entry in schematiq_config.get("initial_schema", [])
            if isinstance(entry, dict) and entry.get("display_name")
        }

        schema_columns = []
        for col in discovered_schema.columns:
            col_info = ColumnInfo(
                name=col.name,
                display_name=seeded_display_names.get(col.name),
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",
                source_document=col.source_document,
                discovery_iteration=col.discovery_iteration,
                allowed_values=[v for v in col.allowed_values if v is not None] if col.allowed_values else None,
                auto_expand_threshold=getattr(col, 'auto_expand_threshold', 2)
            )
            schema_columns.append(col_info)

        session.columns = schema_columns
        session.schema_query = schematiq_config["query"]
        if discovered_schema.observation_unit:
            session.observation_unit = ObservationUnitInfo(
                name=discovered_schema.observation_unit.name,
                definition=discovered_schema.observation_unit.definition,
                example_names=discovered_schema.observation_unit.example_names,
                source_document=discovered_schema.observation_unit.source_document,
                discovery_iteration=discovered_schema.observation_unit.discovery_iteration
            )
        self.session_manager.update_session(session)
        logger.debug("Session %s saved with %d columns, status: %s", session_id, len(schema_columns), session.status)

    async def _handle_stop_after_schema(self, session_id: str):
        """Handle stop requested after schema discovery."""
        logger.warning("Stop requested - skipping value extraction and finalization")
        session = self.session_manager.get_session(session_id)
        session.status = SessionStatus.STOPPED
        session.error_message = None
        self.session_manager.update_session(session)
        logger.debug("Updated session status to STOPPED (from schema discovery stop)")
        await self.broadcast_stopped(session_id, {
            "schema_saved": True,
            "data_rows_saved": 0,
            "message": "Processing stopped after schema discovery"
        })

    async def _handle_stop_after_extraction(self, session_id: str):
        """Handle stop requested after value extraction."""
        logger.warning("Stop requested - skipping finalization")
        session = self.session_manager.get_session(session_id)
        session.status = SessionStatus.STOPPED
        session.error_message = None
        self.session_manager.update_session(session)
        logger.debug("Updated session status to STOPPED (from value extraction stop)")
        session_dir = self.work_dir / session_id
        data_file = session_dir / "extracted_data.jsonl"
        rows_saved = 0
        if data_file.exists():
            with open(data_file, 'r') as f:
                rows_saved = sum(1 for _ in f)
        await self.broadcast_stopped(session_id, {
            "schema_saved": True,
            "data_rows_saved": rows_saved,
            "message": "Processing stopped during value extraction"
        })
        skip_archive = bool(session.metadata.skip_stop_data_collection)
        if skip_archive:
            session.metadata.skip_stop_data_collection = False
            self.session_manager.update_session(session)
        elif self._data_collection_service and rows_saved > 0:
            await self._data_collection_service.trigger_archive(session_id, "schematiq_stopped_partial")
        if not skip_archive and self._pubmed_enrichment_service and rows_saved > 0:
            await self._pubmed_enrichment_service.enrich_session(session_id)
        if not skip_archive and self._uniprot_enrichment_service and rows_saved > 0:
            await self._uniprot_enrichment_service.enrich_session(session_id)

    def _move_pending_documents(self, session_id: str):
        """Move processed documents from pending_documents/ to documents/ as plain text."""
        from app.services.document_preprocessor import commit_document_to_documents_dir

        data_session_dir = Path("./data") / session_id
        pending_dir = data_session_dir / "pending_documents"
        completed_docs_dir = data_session_dir / "documents"
        if pending_dir.exists():
            completed_docs_dir.mkdir(parents=True, exist_ok=True)
            moved_count = 0
            for file_path in sorted(pending_dir.iterdir()):
                if file_path.is_file():
                    if commit_document_to_documents_dir(file_path, completed_docs_dir):
                        moved_count += 1
            if moved_count:
                logger.info(
                    "Committed %d file(s) from pending_documents/ to documents/ for session %s",
                    moved_count,
                    session_id,
                )
