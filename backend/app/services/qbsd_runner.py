"""QBSD integration service."""

import json
import asyncio
import functools
import logging
import math
import os
import random
import threading
import time
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from app.core.config import MAX_DOCUMENTS, DEVELOPER_MODE, RELEASE_CONFIG
from app.core.logging_utils import set_session_context
from app.services import qbsd_thread_pool, concurrency_limiter

logger = logging.getLogger(__name__)

# Project root for path resolution
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# QBSD library imports
from qbsd.core import qbsd as QBSD
from qbsd.core.schema import Schema, Column, ObservationUnit
from qbsd.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
from qbsd.core.retrievers import EmbeddingRetriever
from qbsd.value_extraction.main import build_table_jsonl
from qbsd import discover_observation_unit, ObservationUnitDiscoveryError

QBSD_AVAILABLE = True

def build_llm_interface(
    provider: str,
    model: str,
    max_output_tokens: Optional[int],
    temperature: float,
    api_key: str = None,
    context_window_size: Optional[int] = None
):
    """Build LLM interface based on provider.

    Args:
        provider: LLM provider name (together, openai, gemini)
        model: Model name/identifier (empty string uses provider default)
        max_output_tokens: Maximum tokens the model can generate in its response.
            If None, auto-detected from model specs.
        temperature: Sampling temperature
        api_key: Optional user-provided API key (falls back to env var if None)
        context_window_size: Maximum context window size. If None, auto-detected from model specs.
    """
    if not QBSD_AVAILABLE:
        raise RuntimeError("QBSD components not available")

    if provider.lower() == "together":
        if not model:
            raise ValueError("Model must be specified for Together AI provider")
        return TogetherLLM(
            model=model,
            max_output_tokens=max_output_tokens,  # None = auto-detect
            temperature=temperature,
            context_window_size=context_window_size,  # None = auto-detect
            api_key=api_key
        )
    elif provider.lower() == "openai":
        if not model:
            raise ValueError("Model must be specified for OpenAI provider")
        return OpenAILLM(
            model=model,
            max_output_tokens=max_output_tokens,  # None = auto-detect
            temperature=temperature,
            context_window_size=context_window_size,  # None = auto-detect
            api_key=api_key
        )
    elif provider.lower() == "gemini":
        # Token limits are auto-detected from model specs when None
        kwargs = {
            "max_output_tokens": max_output_tokens,  # None = auto-detect
            "temperature": temperature,
            "context_window_size": context_window_size,  # None = auto-detect
            "api_key": api_key
        }
        if model:  # Only pass model if explicitly set
            kwargs["model"] = model
        llm = GeminiLLM(**kwargs)
        logger.info(f"Gemini LLM created: model={llm.model}, max_output_tokens={llm.max_output_tokens}, context_window={llm.context_window_size}")
        return llm
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


def enforce_release_llm_config(backend_config: dict, is_schema_creation: bool = False) -> dict:
    """Override LLM config with release-mode defaults if not in developer mode.

    Args:
        backend_config: The original LLM backend configuration dict
        is_schema_creation: True for schema creation LLM, False for value extraction

    Returns:
        The config dict, potentially with provider/model/temperature overridden
    """
    if DEVELOPER_MODE:
        return backend_config  # No override in developer mode

    # Force release-mode LLM settings
    return {
        **backend_config,
        "provider": RELEASE_CONFIG["llm_provider"],
        "model": RELEASE_CONFIG["schema_creation_model"] if is_schema_creation else RELEASE_CONFIG["value_extraction_model"],
        "temperature": RELEASE_CONFIG["llm_temperature"],
    }


from app.models.qbsd import QBSDConfig, QBSDStatus
from app.models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData, SessionStatus, SchemaEvolution, SchemaSnapshot, VisualizationSession, ObservationUnitInfo
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.storage import get_storage

class QBSDRunner(WebSocketBroadcasterMixin):
    """Handles QBSD execution and integration."""
    
    def __init__(self, work_dir: str = "./qbsd_work", websocket_manager=None, session_manager=None):
        # Use provided managers or create new ones
        if websocket_manager is not None:
            self.websocket_manager = websocket_manager
        else:
            self.websocket_manager = WebSocketManager()
            
        if session_manager is not None:
            self.session_manager = session_manager
        else:
            self.session_manager = SessionManager()
        
        # Initialize mixin
        super().__init__(self.websocket_manager)
        
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.running_sessions: Dict[str, asyncio.Task] = {}
        self.stop_flags: Dict[str, bool] = {}  # Track stop requests per session
        self._state_lock = threading.Lock()

    def is_stop_requested(self, session_id: str) -> bool:
        """Check if stop has been requested for a session."""
        with self._state_lock:
            return self.stop_flags.get(session_id, False)

    def clear_stop_flag(self, session_id: str):
        """Clear the stop flag for a session."""
        with self._state_lock:
            self.stop_flags.pop(session_id, None)

    def _create_value_extracted_callback(self, session_id: str, loop: asyncio.AbstractEventLoop):
        """Create a callback that streams extracted cell values via WebSocket.

        The callback bridges sync extraction code to async WebSocket broadcasting.
        """
        def on_value_extracted(row_name: str, column_name: str, value: Any):
            """Called for each cell value as it's extracted."""
            try:
                asyncio.run_coroutine_threadsafe(
                    self.broadcast_cell_extracted(session_id, {
                        "row_name": row_name,
                        "column": column_name,
                        "value": value
                    }),
                    loop
                )
            except Exception as e:
                logger.warning("Failed to broadcast cell %s for %s: %s", column_name, row_name, e)

        return on_value_extracted

    def _create_warning_callback(self, session_id: str, loop: asyncio.AbstractEventLoop):
        """Create a callback that broadcasts warnings via WebSocket.

        The callback bridges sync extraction code to async WebSocket broadcasting.
        Used to surface issues like observation unit parsing failures to the UI.
        """
        def on_warning(paper_title: str, warning_type: str, message: str):
            """Called when a warning occurs during extraction."""
            try:
                asyncio.run_coroutine_threadsafe(
                    self.websocket_manager.broadcast_log(session_id, {
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

    async def _start_heartbeat(self, session_id: str, interval: float = 15.0) -> asyncio.Task:
        """Start a background heartbeat to keep WebSocket alive during long operations.

        Args:
            session_id: The session to send heartbeats to
            interval: Seconds between heartbeat messages (default 15s)

        Returns:
            The heartbeat task (caller should cancel when done)
        """
        async def heartbeat_loop():
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.websocket_manager.broadcast_log(session_id, {
                        "level": "info",
                        "message": "Processing... (still working)"
                    })
                except Exception as e:
                    logger.warning("Heartbeat failed: %s", e)
                    break

        return asyncio.create_task(heartbeat_loop())

    async def _download_supabase_dataset(self, dataset_name: str, session_dir: Path) -> Optional[str]:
        """Download a Supabase dataset to local directory.

        Args:
            dataset_name: Name of the dataset in Supabase
            session_dir: Session work directory

        Returns:
            Local path to downloaded dataset, or None if not a Supabase dataset
        """
        storage = get_storage()

        # Check if this is a Supabase dataset
        try:
            datasets = await storage.list_datasets()
            dataset_names = [d.name for d in datasets]

            if dataset_name in dataset_names:
                # It's a Supabase dataset - download it
                local_dataset_dir = session_dir / "datasets" / dataset_name
                local_dataset_dir.mkdir(parents=True, exist_ok=True)

                logger.info("Downloading Supabase dataset '%s' to %s", dataset_name, local_dataset_dir)
                downloaded_files = await storage.download_dataset_to_local(dataset_name, str(local_dataset_dir))
                logger.info("Downloaded %d files from '%s'", len(downloaded_files), dataset_name)

                return str(local_dataset_dir)
        except Exception as e:
            logger.warning("Error checking/downloading Supabase dataset '%s': %s", dataset_name, e)

        return None

    def _convert_config_to_qbsd_format_sync(self, config: QBSDConfig, session_id: str, resolved_docs_paths: List[str]) -> Dict[str, Any]:
        """Synchronous part of config conversion after paths are resolved."""
        session_dir = self.work_dir / session_id

        # Build QBSD config with pre-resolved paths
        qbsd_config = {
            "query": config.query,
            "docs_path": resolved_docs_paths[0] if len(resolved_docs_paths) == 1 else resolved_docs_paths,
            "max_keys_schema": config.max_keys_schema,
            "documents_batch_size": config.documents_batch_size,
            "output_path": str(session_dir / "discovered_schema.json"),
            "document_randomization_seed": config.document_randomization_seed,
            "skip_value_extraction": config.skip_value_extraction,
            "schema_creation_backend": {
                "provider": config.schema_creation_backend.provider,
                "model": config.schema_creation_backend.model,
                "max_output_tokens": config.schema_creation_backend.max_output_tokens,
                "temperature": config.schema_creation_backend.temperature,
                "context_window_size": config.schema_creation_backend.context_window_size,
                "api_key": config.schema_creation_backend.api_key
            },
            "value_extraction_backend": {
                "provider": config.value_extraction_backend.provider,
                "model": config.value_extraction_backend.model,
                "max_output_tokens": config.value_extraction_backend.max_output_tokens,
                "temperature": config.value_extraction_backend.temperature,
                "context_window_size": config.value_extraction_backend.context_window_size,
                "api_key": config.value_extraction_backend.api_key
            }
        }

        # Add retriever config if provided
        if config.retriever:
            qbsd_config["retriever"] = {
                "type": "embedding",
                "model_name": config.retriever.model_name,
                "k": config.retriever.k,
                "passage_chars": config.retriever.passage_chars,
                "overlap": config.retriever.overlap,
                "enable_dynamic_k": config.retriever.enable_dynamic_k,
                "dynamic_k_threshold": config.retriever.dynamic_k_threshold,
                "dynamic_k_minimum": config.retriever.dynamic_k_minimum
            }

        # Add initial schema if provided
        if config.initial_schema:
            qbsd_config["initial_schema"] = [
                {
                    "name": col.name,
                    "definition": col.definition,
                    "rationale": col.rationale,
                    "allowed_values": col.allowed_values
                }
                for col in config.initial_schema
            ]
        elif config.initial_schema_path:
            initial_schema_path = Path(config.initial_schema_path)
            if not initial_schema_path.is_absolute():
                initial_schema_path = (PROJECT_ROOT / initial_schema_path).resolve()
            if initial_schema_path.exists():
                qbsd_config["initial_schema_path"] = str(initial_schema_path)

        # Add initial observation unit if provided
        if config.initial_observation_unit:
            qbsd_config["initial_observation_unit"] = {
                "name": config.initial_observation_unit.name,
                "definition": config.initial_observation_unit.definition
            }

        return qbsd_config

    async def _resolve_docs_paths(self, config: QBSDConfig, session_id: str) -> List[str]:
        """Resolve document paths - download from Supabase if needed, or use uploaded files."""
        session_dir = self.work_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        # Check for uploaded documents in data/{session_id}/pending_documents/ directory
        # (documents are uploaded via add-documents endpoint which uses ./data, not ./qbsd_work)
        data_dir = Path("./data") / session_id / "pending_documents"
        if data_dir.exists():
            uploaded_files = [f for f in sorted(data_dir.iterdir())
                            if f.is_file() and not f.name.startswith('.')]
            if uploaded_files:
                logger.info("Using %d uploaded documents from %s", len(uploaded_files), data_dir)
                # Return the directory containing the files, not individual files
                return [str(data_dir.absolute())]

        # No uploaded files - resolve from config.docs_path
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        # Filter out None and empty values
        docs_paths = [p for p in docs_paths if p]

        if not docs_paths:
            logger.warning("No document paths configured and no uploaded documents found")
            return []

        resolved_docs_paths = []

        for path in docs_paths:
            # First, try to download from Supabase
            supabase_path = await self._download_supabase_dataset(path, session_dir)
            if supabase_path:
                resolved_docs_paths.append(supabase_path)
                continue

            # Not a Supabase dataset - try local paths
            doc_path = Path(path)
            if not doc_path.is_absolute():
                # Try relative to project root and common paths
                candidates = [
                    PROJECT_ROOT / path,
                    PROJECT_ROOT / "research" / "data" / Path(path).name,
                    PROJECT_ROOT / "test" / "files",
                    Path.cwd() / path,
                    Path.cwd().parent / path,
                ]
                for candidate in candidates:
                    if candidate.exists():
                        resolved_docs_paths.append(str(candidate.absolute()))
                        logger.info("Resolved document path: %s -> %s", path, candidate.absolute())
                        break
                else:
                    logger.warning("Document path not found: %s", path)
                    resolved_docs_paths.append(path)
            else:
                resolved_docs_paths.append(str(doc_path))

        return resolved_docs_paths

    async def validate_config(self, config: QBSDConfig) -> Dict[str, Any]:
        """Validate QBSD configuration.

        Supports three modes:
        - Standard: Both query and documents provided
        - Document-only: Documents provided, no query (schema discovered from document content)
        - Query-only: Query provided, no documents (schema planned based on query)

        At least one of query or documents must be provided.
        """
        errors = []
        warnings = []

        # Check if we have query and/or documents
        has_query = bool(config.query and config.query.strip())
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        has_documents = bool(docs_paths and any(p for p in docs_paths if p)) or config.upload_pending

        # Validate: at least one of query or documents must be provided
        if not has_query and not has_documents:
            errors.append("At least one of query or documents must be provided")
        
        # Validate document paths (only if actual paths are provided, not just upload_pending)
        actual_paths = [p for p in docs_paths if p]
        if actual_paths:
            for path in actual_paths:
                doc_path = Path(path)
                logger.debug("Checking document path: %s -> %s", path, doc_path.absolute())

                # Try relative to current directory and various parent directories
                paths_to_try = [
                    doc_path,
                    Path("..") / path,  # Try from parent directory
                    Path("../..") / path,  # Try from grandparent directory
                    Path("../../..") / path,  # Try from great-grandparent directory
                    # Also try the known test directory
                    Path("../../..") / "test" / "files",
                    PROJECT_ROOT / "test" / "files",  # Try from project root
                    PROJECT_ROOT / "research" / "data" / "file",  # Try research data
                    Path("../test/files"),  # Direct relative to test
                ]

                path_exists = False
                actual_path = None
                for try_path in paths_to_try:
                    if try_path.exists():
                        path_exists = True
                        actual_path = try_path.absolute()
                        logger.debug("Found path at: %s", actual_path)
                        # Check if directory has files
                        try:
                            file_count = len(list(try_path.glob("*.txt"))) + len(list(try_path.glob("*.md")))
                            if file_count == 0:
                                warnings.append(f"Document path appears to be empty: {path} (no .txt or .md files)")
                            else:
                                logger.debug("Found %d document files", file_count)
                        except Exception as e:
                            warnings.append(f"Could not check document count in {path}: {e}")
                        break

                if not path_exists:
                    # For testing, just warn instead of error and suggest test directory
                    warnings.append(f"Document path does not exist: {path}. Try using 'test/files' which contains sample documents.")
                    # errors.append(f"Document path does not exist: {path}")
        
        # Validate initial schema if provided
        if config.initial_schema_path:
            schema_path = Path(config.initial_schema_path)
            # Try to resolve relative paths against PROJECT_ROOT
            if not schema_path.is_absolute():
                schema_path = (PROJECT_ROOT / schema_path).resolve()
            if not schema_path.exists():
                errors.append(f"Initial schema file does not exist: {config.initial_schema_path} (resolved: {schema_path})")
        
        # Validate schema creation backend config
        if not config.schema_creation_backend.provider:
            errors.append("Schema creation LLM provider must be specified")

        # Model is required for OpenAI and Together, optional for Gemini (has default)
        if not config.schema_creation_backend.model:
            if config.schema_creation_backend.provider.lower() not in ["gemini"]:
                errors.append("Schema creation LLM model must be specified (required for non-Gemini providers)")

        # Validate value extraction backend config
        if not config.value_extraction_backend.provider:
            errors.append("Value extraction LLM provider must be specified")

        # Model is required for OpenAI and Together, optional for Gemini (has default)
        if not config.value_extraction_backend.model:
            if config.value_extraction_backend.provider.lower() not in ["gemini"]:
                errors.append("Value extraction LLM model must be specified (required for non-Gemini providers)")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    async def save_config(self, session_id: str, config: QBSDConfig):
        """Save QBSD configuration for a session."""
        session_dir = self.work_dir / session_id
        session_dir.mkdir(exist_ok=True)
        
        config_file = session_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config.model_dump(), f, indent=2)
    
    async def run_qbsd(self, session_id: str):
        """Run QBSD discovery process."""
        set_session_context(session_id)
        try:
            # Update session status
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.PROCESSING
            self.session_manager.update_session(session)

            # Immediately broadcast that we're starting (before any async work)
            await self.broadcast_step_progress(
                session_id,
                "Starting QBSD execution...",
                step_number=1,
                total_steps=5,
                step_progress=0.0,
                message="Initializing pipeline..."
            )

            # Load config
            config_file = self.work_dir / session_id / "config.json"
            with open(config_file) as f:
                config_data = json.load(f)
            config = QBSDConfig(**config_data)
            
            # Create task for QBSD execution
            task = asyncio.create_task(self._execute_qbsd(session_id, config))
            with self._state_lock:
                self.running_sessions[session_id] = task

            # Wait for completion
            await task

        except Exception as e:
            # Update session with error
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = str(e)
            self.session_manager.update_session(session)

            await self.broadcast_error(session_id, str(e))

        finally:
            # Clean up
            with self._state_lock:
                self.running_sessions.pop(session_id, None)
            # Release concurrency slot (safe even if not acquired)
            await concurrency_limiter.release(session_id)

    async def _execute_qbsd(self, session_id: str, config: QBSDConfig):
        """Execute the real QBSD process."""
        if not QBSD_AVAILABLE:
            raise RuntimeError("QBSD components not available. Cannot execute real QBSD pipeline.")
        
        session_dir = self.work_dir / session_id
        session_dir.mkdir(exist_ok=True)
        
        # Progress tracking - with user-friendly messages
        progress_steps = [
            "Initializing QBSD pipeline",
            "Loading documents", 
            "Setting up AI models",
            "Configuring retrieval system",
            "Schema Discovery: Analyzing documents",
            "Value Extraction: Processing documents",
            "Finalizing results"
        ]
        
        current_step = 0
        total_steps = len(progress_steps)
        
        async def update_progress(step_name: str, step_progress: float = 0.0, details: Dict[str, Any] = None):
            nonlocal current_step
            await self.broadcast_step_progress(
                session_id,
                step_name,
                current_step + 1,
                total_steps,
                step_progress,
                details.get("message") if details else None,
                details  # Pass full details dict including iteration data
            )
        
        try:
            # Step 1: Initializing
            logger.debug("Starting QBSD execution for session %s", session_id)
            await update_progress("Initializing", 0.0)

            # Resolve document paths (download from Supabase if needed)
            logger.debug("Resolving document paths (may download from Supabase)")
            resolved_docs_paths = await self._resolve_docs_paths(config, session_id)
            logger.debug("Resolved paths: %s", resolved_docs_paths)

            # Convert config to QBSD format with resolved paths
            logger.debug("Converting config to QBSD format")
            qbsd_config = self._convert_config_to_qbsd_format_sync(config, session_id, resolved_docs_paths)
            
            # Save QBSD config
            qbsd_config_file = session_dir / "qbsd_config.json"
            with open(qbsd_config_file, 'w') as f:
                json.dump(qbsd_config, f, indent=2)
            logger.debug("Saved QBSD config with keys: %s", list(qbsd_config.keys()))
            
            await update_progress("Initializing", 1.0)
            
            # Step 2: Load documents
            current_step += 1
            await update_progress("Loading documents", 0.0)
            
            # Load documents using QBSD utils
            docs_paths = qbsd_config["docs_path"]
            if isinstance(docs_paths, str):
                docs_paths = [docs_paths]

            documents = []
            filenames = []
            total_docs = 0

            for docs_path in docs_paths:
                doc_path = Path(docs_path)
                if doc_path.exists():
                    doc_files = list(doc_path.glob("*.txt")) + list(doc_path.glob("*.md"))
                    total_docs += len(doc_files)

                    # Load document contents
                    for doc_file in doc_files:
                        try:
                            content = doc_file.read_text(encoding='utf-8')
                            documents.append(content)
                            filenames.append(doc_file.name)
                        except Exception as e:
                            logger.warning("Could not read %s: %s", doc_file, e)

            # Cap documents at MAX_DOCUMENTS (with seeded randomization for reproducibility)
            bypass_limit = qbsd_config.get("bypass_limit", False)
            if not (DEVELOPER_MODE and bypass_limit) and len(documents) > MAX_DOCUMENTS:
                seed = qbsd_config.get("document_randomization_seed", 42)
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

                # Create a filtered directory so value extraction only sees capped documents
                capped_dir = session_dir / "capped_documents"
                capped_dir.mkdir(exist_ok=True)
                for fname in filenames:
                    for docs_path in docs_paths:
                        source = Path(docs_path) / fname
                        if source.exists():
                            dest = capped_dir / fname
                            if not dest.exists():
                                os.symlink(source.resolve(), dest)
                            break
                qbsd_config["docs_path"] = [str(capped_dir)]
                logger.info("Value extraction redirected to capped_documents/ with %d files", len(filenames))

            await update_progress("Loading documents", 1.0, {
                "total_documents": total_docs,
                "loaded_documents": len(documents)
            })

            # Determine mode based on what's available
            has_query = bool(qbsd_config.get("query", "").strip())
            has_documents = bool(documents)

            if not has_query and not has_documents:
                raise RuntimeError("At least one of query or documents must be provided")

            # Log the mode we're operating in
            if has_query and has_documents:
                logger.debug("STANDARD mode - query + %d documents", len(documents))
            elif has_documents:
                logger.debug("DOCUMENT_ONLY mode - %d documents, no query", len(documents))
            else:
                logger.debug("QUERY_ONLY mode - query provided, no documents")
            
            # Step 3: Build LLM backend
            current_step += 1
            logger.debug("Building Schema Creation LLM backend - provider: %s", qbsd_config['schema_creation_backend']['provider'])
            await update_progress("Building LLM backend", 0.0)

            # Build Schema Creation LLM interface
            # In release mode, force the release-mode LLM settings
            schema_backend = enforce_release_llm_config(qbsd_config["schema_creation_backend"], is_schema_creation=True)
            logger.debug("Creating Schema Creation LLM interface...")
            llm = build_llm_interface(
                provider=schema_backend["provider"],
                model=schema_backend["model"],
                max_output_tokens=schema_backend.get("max_output_tokens"),  # None = auto-detect
                temperature=schema_backend["temperature"],
                api_key=schema_backend.get("api_key"),
                context_window_size=schema_backend.get("context_window_size")  # None = auto-detect
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
            if "retriever" in qbsd_config:
                logger.debug("Creating EmbeddingRetriever...")
                retriever_config = qbsd_config["retriever"]
                logger.debug("Retriever config: %s", retriever_config)
                try:
                    retriever = EmbeddingRetriever(
                        model_name=retriever_config.get("model_name", "all-MiniLM-L6-v2"),
                        max_words=retriever_config.get("passage_chars", 512),  # Convert passage_chars to max_words
                        k=retriever_config.get("k", 15),
                        enable_dynamic_k=retriever_config.get("enable_dynamic_k", True),
                        dynamic_k_threshold=retriever_config.get("dynamic_k_threshold", 0.65),
                        dynamic_k_minimum=retriever_config.get("dynamic_k_minimum", 3)
                    )
                    logger.debug("EmbeddingRetriever created successfully!")
                except Exception as e:
                    logger.error("ERROR creating EmbeddingRetriever: %s", e)
                    raise
            else:
                logger.debug("No retriever config found, using None")
            
            await update_progress("Setting up retriever", 1.0)
            
            # Step 5: Schema discovery
            current_step += 1
            await update_progress("Discovering schema", 0.0)

            # Run real schema discovery with heartbeat to keep WebSocket alive
            logger.debug("Starting schema discovery with %d documents", len(documents))
            heartbeat_task = await self._start_heartbeat(session_id, interval=15.0)
            try:
                discovered_schema, schema_evolution = await self._run_schema_discovery(
                    session_id, documents, filenames, qbsd_config, llm, retriever, update_progress
                )
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass  # Expected when cancelling
            logger.debug("Schema discovery completed with %d columns", len(discovered_schema.columns))
            logger.debug("Schema evolution: %d snapshots tracked", len(schema_evolution.snapshots))
            
            # Save discovered schema with frontend-compatible format
            schema_file = session_dir / "discovered_schema.json"
            frontend_schema = []
            for col in discovered_schema.columns:
                col_dict = col.to_dict()
                # Convert QBSD format to frontend format
                frontend_col = {
                    "name": col_dict.get("column", col.name),  # "column" -> "name"
                    "definition": col_dict.get("definition", ""),
                    "rationale": col_dict.get("explanation", col.rationale)  # "explanation" -> "rationale"
                }
                # Include allowed_values if present
                if col_dict.get("allowed_values"):
                    frontend_col["allowed_values"] = col_dict["allowed_values"]
                frontend_schema.append(frontend_col)
            
            schema_for_frontend = {
                "query": qbsd_config["query"],
                "schema": frontend_schema
            }
            # Include observation_unit if discovered
            if discovered_schema.observation_unit:
                schema_for_frontend["observation_unit"] = discovered_schema.observation_unit.to_dict()

            with open(schema_file, 'w') as f:
                json.dump(schema_for_frontend, f, indent=2)
            
            await update_progress("Schema Discovery: Complete", 1.0, {
                "columns_discovered": len(discovered_schema.columns)
            })
            
            # Update session status to SCHEMA_READY
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.SCHEMA_READY
            session.metadata.schema_discovery_completed = True
            logger.debug("Updated session %s status to SCHEMA_READY with %d columns", session_id, len(discovered_schema.columns))
            
            # Update session with discovered schema
            schema_columns = []
            for col in discovered_schema.columns:
                # QBSD Column objects have these fields directly
                col_info = ColumnInfo(
                    name=col.name,
                    definition=col.definition,
                    rationale=col.rationale,
                    data_type="object",
                    source_document=col.source_document,
                    discovery_iteration=col.discovery_iteration,
                    allowed_values=col.allowed_values,
                    auto_expand_threshold=getattr(col, 'auto_expand_threshold', 2)
                )
                schema_columns.append(col_info)
            
            session.columns = schema_columns
            session.schema_query = qbsd_config["query"]
            # Transfer observation_unit to session if discovered
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
            
            # Broadcast schema completion event
            await self.broadcast_schema_completed(session_id, {
                "query": qbsd_config["query"],
                "columns": [col.model_dump() for col in schema_columns],
                "total_columns": len(discovered_schema.columns)
            })

            # Check if stop was requested during schema discovery - skip remaining steps
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested - skipping value extraction and finalization")
                # Update status to STOPPED immediately (don't rely on stop_execution race)
                session = self.session_manager.get_session(session_id)
                session.status = SessionStatus.STOPPED
                session.error_message = None
                self.session_manager.update_session(session)
                logger.debug("Updated session status to STOPPED (from schema discovery stop)")
                # Broadcast stopped message
                await self.broadcast_stopped(session_id, {
                    "schema_saved": True,
                    "data_rows_saved": 0,
                    "message": "Processing stopped after schema discovery"
                })
                return

            # Step 6: Value extraction (skip if schema-only mode)
            skipped_documents: List[str] = []
            if qbsd_config.get("skip_value_extraction", False):
                logger.info("Skipping value extraction (schema-only mode)")
                # Skip to finalization without value extraction
            else:
                current_step += 1
                await update_progress("Extracting values", 0.0)

                # Build Value Extraction LLM interface (separate from schema creation)
                # In release mode, force the release-mode LLM settings
                value_backend = enforce_release_llm_config(qbsd_config["value_extraction_backend"], is_schema_creation=False)
                logger.debug("Creating Value Extraction LLM interface...")
                value_extraction_llm = build_llm_interface(
                    provider=value_backend["provider"],
                    model=value_backend["model"],
                    max_output_tokens=value_backend.get("max_output_tokens"),  # None = auto-detect
                    temperature=value_backend["temperature"],
                    api_key=value_backend.get("api_key"),
                    context_window_size=value_backend.get("context_window_size")  # None = auto-detect
                )
                logger.debug("Value Extraction LLM interface created successfully")

                # Run real value extraction with dedicated LLM (with heartbeat for long operations)
                heartbeat_task = await self._start_heartbeat(session_id, interval=15.0)
                try:
                    skipped_documents = await self._run_value_extraction(
                        session_id, qbsd_config, discovered_schema, value_extraction_llm, retriever, update_progress
                    )
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass  # Expected when cancelling

                await update_progress("Extracting values", 1.0)

            # Check if stop was requested during value extraction - skip finalization
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested - skipping finalization")
                # Update status to STOPPED immediately (don't rely on stop_execution race)
                session = self.session_manager.get_session(session_id)
                session.status = SessionStatus.STOPPED
                session.error_message = None
                self.session_manager.update_session(session)
                logger.debug("Updated session status to STOPPED (from value extraction stop)")
                # Count rows saved
                data_file = session_dir / "extracted_data.jsonl"
                rows_saved = 0
                if data_file.exists():
                    with open(data_file, 'r') as f:
                        rows_saved = sum(1 for _ in f)
                # Broadcast stopped message
                await self.broadcast_stopped(session_id, {
                    "schema_saved": True,
                    "data_rows_saved": rows_saved,
                    "message": "Processing stopped during value extraction"
                })
                return

            # Step 7: Finalize
            current_step += 1
            await update_progress("Finalizing results", 0.0)

            # Compute statistics from extracted data (include evolution and skipped documents)
            statistics = self._compute_statistics_from_extracted_data(
                session_id, discovered_schema, schema_evolution, skipped_documents
            )

            # Update session as completed with statistics
            session = self.session_manager.get_session(session_id)
            session.statistics = statistics
            session.status = SessionStatus.COMPLETED
            self.session_manager.update_session(session)

            # Capture schema baseline for re-extraction change detection
            self.session_manager.capture_schema_baseline(session_id)

            await update_progress("Finalizing results", 1.0)

            # Broadcast completion
            schema_only = qbsd_config.get("skip_value_extraction", False)
            completion_message = (
                "Schema discovery completed (value extraction skipped)"
                if schema_only
                else "QBSD execution completed successfully"
            )
            await self.broadcast_completion(session_id, completion_message, {
                "total_documents": total_docs,
                "schema_columns": len(discovered_schema.columns),
                "schema_only": schema_only
            })
            
        except Exception as e:
            # Update session with error
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = str(e)
            self.session_manager.update_session(session)
            
            await self.broadcast_error(session_id, str(e))
            raise
    
    async def _run_schema_discovery(
        self,
        session_id: str,
        documents: List[str],
        filenames: List[str],
        qbsd_config: Dict[str, Any],
        llm: LLMInterface,
        retriever,
        progress_callback
    ) -> tuple[Schema, SchemaEvolution]:
        """Run real schema discovery using QBSD pipeline.

        Returns:
            Tuple of (discovered_schema, schema_evolution)
        """

        # Extract config values needed for Schema creation
        query = qbsd_config["query"]
        max_keys = qbsd_config.get("max_keys_schema", 100)

        # Initialize schema (inline schema takes priority over file path)
        initial_schema = None
        if "initial_schema" in qbsd_config:
            # Inline schema provided directly
            try:
                columns = []
                for col_data in qbsd_config["initial_schema"]:
                    col = Column(
                        name=col_data["name"],
                        definition=col_data.get("definition", ""),
                        rationale=col_data.get("rationale", ""),
                        allowed_values=col_data.get("allowed_values")
                    )
                    columns.append(col)
                initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
                logger.info("Loaded inline initial schema with %d columns", len(columns))
            except Exception as e:
                logger.warning("Could not load inline initial schema: %s", e)
        elif "initial_schema_path" in qbsd_config:
            try:
                with open(qbsd_config["initial_schema_path"]) as f:
                    initial_data = json.load(f)
                    # Handle both formats: array of columns or dict with "schema" key
                    if isinstance(initial_data, list):
                        columns = []
                        for col_data in initial_data:
                            col = Column(
                                name=col_data["name"],
                                definition=col_data.get("definition", ""),
                                rationale=col_data.get("rationale", ""),
                                allowed_values=col_data.get("allowed_values")
                            )
                            columns.append(col)
                        initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
                    elif isinstance(initial_data, dict) and "schema" in initial_data:
                        columns = []
                        for col_data in initial_data["schema"]:
                            col = Column(
                                name=col_data["name"],
                                definition=col_data.get("definition", ""),
                                rationale=col_data.get("rationale", ""),
                                allowed_values=col_data.get("allowed_values")
                            )
                            columns.append(col)
                        initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
                logger.info("Loaded initial schema from file with %d columns", len(columns))
            except Exception as e:
                logger.warning("Could not load initial schema from file: %s", e)

        current_schema = initial_schema or Schema(query=query, columns=[], max_keys=max_keys)

        # Calculate iterations based on document batching
        batch_size = qbsd_config.get("documents_batch_size", 1)
        max_iterations = math.ceil(len(documents) / batch_size) if documents else 1
        convergence_threshold = 5  # Stop if schema doesn't change for 5 consecutive batches
        unchanged_count = 0

        # Create document batches
        batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
        filename_batches = [filenames[i:i+batch_size] for i in range(0, len(filenames), batch_size)]

        logger.debug("Document batching - %d docs, batch_size=%d, %d batches", len(documents), batch_size, len(batches))

        # Initialize schema evolution tracking
        evolution = SchemaEvolution(snapshots=[], column_sources={})
        cumulative_docs = 0

        # Record initial columns (if any) as iteration 0
        if len(current_schema.columns) > 0:
            initial_column_names = [col.name for col in current_schema.columns]
            evolution.snapshots.append(SchemaSnapshot(
                iteration=0,
                documents_processed=["initial_schema"],
                total_columns=len(current_schema.columns),
                new_columns=initial_column_names,
                cumulative_documents=0
            ))
            # Mark initial columns as from initial schema
            for col in current_schema.columns:
                evolution.column_sources[col.name] = "initial_schema"

        # Handle QUERY_ONLY mode: no documents, generate schema from query alone
        if not documents:
            logger.debug("QUERY_ONLY mode - generating schema from query without documents")
            await progress_callback("Schema Discovery: Planning from query", 0.5, {
                "mode": "query_only",
                "current_columns": len(current_schema.columns)
            })

            try:
                # Call generate_schema with empty passages (offloaded to thread pool)
                loop = asyncio.get_running_loop()
                logger.debug("[%s] Offloading QUERY_ONLY generate_schema to thread pool", session_id)
                schema_result = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(
                        QBSD.generate_schema,
                        passages=[],
                        query=query,
                        max_keys_schema=qbsd_config.get("max_keys_schema", 100),
                        current_schema=current_schema,
                        llm=llm,
                        context_window_size=qbsd_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                    )
                )
                new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
                logger.debug("QUERY_ONLY generated schema with %d columns", len(new_schema.columns))

                # Merge with any initial schema (offloaded to thread pool)
                if current_schema.columns:
                    logger.debug("[%s] Offloading QUERY_ONLY schema merge to thread pool", session_id)
                    merged_schema = await loop.run_in_executor(
                        qbsd_thread_pool,
                        functools.partial(current_schema.merge, new_schema),
                    )
                else:
                    merged_schema = new_schema

                # Track new columns
                new_column_names = [col.name for col in merged_schema.columns
                                   if col.name not in {c.name for c in current_schema.columns}]

                # Record column sources
                for col_name in new_column_names:
                    evolution.column_sources[col_name] = "query_only"

                # Add evolution snapshot
                evolution.snapshots.append(SchemaSnapshot(
                    iteration=1,
                    documents_processed=["query_only"],
                    total_columns=len(merged_schema.columns),
                    new_columns=new_column_names,
                    cumulative_documents=0
                ))

                logger.debug("QUERY_ONLY mode completed with %d columns: %s", len(merged_schema.columns), [c.name for c in merged_schema.columns])
                return merged_schema, evolution

            except Exception as e:
                logger.error("ERROR in QUERY_ONLY generate_schema: %s", e)
                import traceback
                traceback.print_exc()
                raise

        # Handle pre-configured observation unit (if provided)
        pending_observation_unit_name = None  # For name-only mode
        initial_obs_unit = qbsd_config.get("initial_observation_unit")
        if initial_obs_unit:
            if initial_obs_unit.get("definition"):
                # Full specification - use as-is
                current_schema.observation_unit = ObservationUnit(
                    name=initial_obs_unit["name"],
                    definition=initial_obs_unit["definition"]
                )
                logger.info("Using pre-configured observation unit: %s - %s", initial_obs_unit['name'], initial_obs_unit['definition'])
            else:
                # Name-only mode - store name for later discovery
                pending_observation_unit_name = initial_obs_unit["name"]
                logger.info("Observation unit name pre-configured: %s (definition will be discovered)", pending_observation_unit_name)

        for iteration, (batch_docs, batch_names) in enumerate(zip(batches, filename_batches)):
            # Check for stop request at the start of each iteration
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested during schema discovery - saving partial schema with %d columns", len(current_schema.columns))
                break

            logger.debug("Schema discovery batch %d/%d (%d docs: %s)", iteration + 1, len(batches), len(batch_docs), batch_names)
            await progress_callback(f"Schema Discovery: Batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs)", iteration / len(batches), {
                "iteration": iteration + 1,
                "max_iterations": len(batches),
                "batch_docs": len(batch_docs),
                "current_columns": len(current_schema.columns)
            })

            # Track column names before this iteration
            columns_before = {col.name.lower() for col in current_schema.columns}
            cumulative_docs += len(batch_docs)

            # Select relevant content from this batch's documents (offloaded to thread pool)
            loop = asyncio.get_running_loop()
            logger.debug("[%s] Offloading select_relevant_content to thread pool", session_id)
            relevant_content = await loop.run_in_executor(
                qbsd_thread_pool,
                functools.partial(
                    QBSD.select_relevant_content,
                    docs=batch_docs,
                    query=query,
                    retriever=retriever,
                )
            )
            logger.debug("Selected %d relevant passages from batch", len(relevant_content))

            # Check for stop after content retrieval
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested after content retrieval - saving partial schema")
                break

            # Discover observation unit in first iteration (if not already set)
            # Support all modes: STANDARD (query + docs), DOCUMENT_ONLY (docs only), QUERY_ONLY handled separately
            if iteration == 0 and (query or relevant_content) and not current_schema.observation_unit:
                logger.info("Discovering observation unit from first batch...")
                try:
                    logger.debug("[%s] Offloading discover_observation_unit to thread pool", session_id)
                    obs_unit = await loop.run_in_executor(
                        qbsd_thread_pool,
                        functools.partial(
                            discover_observation_unit,
                            query=query,
                            passages=relevant_content,
                            llm=llm,
                            context_window_size=qbsd_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                            source_document=batch_names[0] if batch_names else None,
                        )
                    )
                    # If name was pre-configured, override discovered name
                    if pending_observation_unit_name:
                        logger.info("Overriding discovered name '%s' with pre-configured name '%s'", obs_unit.name, pending_observation_unit_name)
                        obs_unit.name = pending_observation_unit_name
                    current_schema.observation_unit = obs_unit
                    logger.info("Observation unit set: %s - %s", obs_unit.name, obs_unit.definition)
                    if obs_unit.example_names:
                        logger.info("   Examples: %s", obs_unit.example_names)
                except ObservationUnitDiscoveryError as e:
                    # Re-raise discovery errors with clear message
                    logger.error("Observation unit discovery failed: %s", e)
                    raise RuntimeError(
                        f"Failed to discover observation unit: {e}. "
                        "Ensure documents contain extractable entities."
                    ) from e
                except Exception as e:
                    # Wrap unexpected errors
                    logger.error("Unexpected error during observation unit discovery: %s", e)
                    raise RuntimeError(
                        f"Observation unit discovery failed unexpectedly: {e}"
                    ) from e

            # Check for stop after observation unit discovery
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested after observation unit discovery - saving partial schema")
                break

            # Generate schema for this iteration (offloaded to thread pool)
            logger.debug("[%s] Offloading generate_schema to thread pool", session_id)
            try:
                schema_result = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(
                        QBSD.generate_schema,
                        passages=relevant_content,
                        query=query,
                        max_keys_schema=qbsd_config.get("max_keys_schema", 100),
                        current_schema=current_schema,
                        llm=llm,
                        context_window_size=qbsd_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                    )
                )
                # generate_schema returns a tuple (Schema, bool)
                new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
                logger.debug("Generated schema with %d columns", len(new_schema.columns))
            except Exception as e:
                logger.error("ERROR in generate_schema: %s", e)
                raise

            # Check for stop after schema generation
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested after schema generation - saving partial schema")
                break

            # Merge with existing schema (offloaded to thread pool)
            logger.debug("[%s] Offloading schema merge to thread pool", session_id)
            logger.debug("Current schema has %d columns", len(current_schema.columns))
            logger.debug("New schema has %d columns", len(new_schema.columns))
            try:
                merged_schema = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(current_schema.merge, new_schema),
                )
                logger.debug("Merged schema has %d columns", len(merged_schema.columns))
            except Exception as e:
                logger.error("ERROR in schema merge: %s", e)
                import traceback
                traceback.print_exc()
                raise

            # Check for stop after schema merge
            if self.is_stop_requested(session_id):
                logger.warning("Stop requested after schema merge - saving partial schema")
                break

            # Identify NEW columns added in this iteration
            columns_after = {col.name.lower() for col in merged_schema.columns}
            new_column_names_lower = columns_after - columns_before
            new_columns = [col.name for col in merged_schema.columns if col.name.lower() in new_column_names_lower]

            # Record column sources for new columns (use actual document names)
            batch_source = ", ".join(batch_names) if batch_names else f"batch_{iteration + 1}"
            for col_name in new_columns:
                if col_name not in evolution.column_sources:
                    evolution.column_sources[col_name] = batch_source

            # Add snapshot to evolution with actual document names
            evolution.snapshots.append(SchemaSnapshot(
                iteration=iteration + 1,
                documents_processed=batch_names,
                total_columns=len(merged_schema.columns),
                new_columns=new_columns,
                cumulative_documents=cumulative_docs
            ))

            logger.debug("Evolution - batch %d: %d new columns: %s", iteration + 1, len(new_columns), new_columns)

            # Check convergence (offloaded to thread pool)
            logger.debug("[%s] Offloading evaluate_schema_convergence to thread pool", session_id)
            converged = await loop.run_in_executor(
                qbsd_thread_pool,
                functools.partial(QBSD.evaluate_schema_convergence, current_schema, merged_schema),
            )
            if converged:
                unchanged_count += 1
                logger.debug("Schema unchanged (count: %d/%d)", unchanged_count, convergence_threshold)
                if unchanged_count >= convergence_threshold:
                    logger.debug("Schema converged after %d batches", iteration + 1)
                    break
            else:
                unchanged_count = 0
                logger.debug("Schema changed, continuing to next batch")

            current_schema = merged_schema
            logger.debug("Completed batch %d, moving to next", iteration + 1)

            # Save partial schema to disk after each batch (for stop resilience)
            session_dir = self.work_dir / session_id
            partial_schema_file = session_dir / "discovered_schema.json"
            frontend_schema = []
            for col in current_schema.columns:
                col_dict = col.to_dict()
                frontend_col = {
                    "name": col_dict.get("column", col.name),
                    "definition": col_dict.get("definition", ""),
                    "rationale": col_dict.get("explanation", col.rationale)
                }
                if col_dict.get("allowed_values"):
                    frontend_col["allowed_values"] = col_dict["allowed_values"]
                frontend_schema.append(frontend_col)
            partial_schema_data = {"query": query, "schema": frontend_schema}
            # Include observation_unit if discovered
            if current_schema.observation_unit:
                partial_schema_data["observation_unit"] = current_schema.observation_unit.to_dict()
            with open(partial_schema_file, 'w') as f:
                json.dump(partial_schema_data, f, indent=2)
            logger.debug("Saved partial schema with %d columns", len(current_schema.columns))

            # Small delay to allow other tasks
            await asyncio.sleep(0.1)

        logger.debug("Schema discovery completed with %d columns after %d batches", len(current_schema.columns), len(evolution.snapshots))
        logger.debug("Evolution tracking: %d snapshots, %d column sources", len(evolution.snapshots), len(evolution.column_sources))
        return current_schema, evolution
    
    async def _run_value_extraction(
        self,
        session_id: str,
        qbsd_config: Dict[str, Any],
        schema: Schema,
        llm: LLMInterface,
        retriever,
        progress_callback
    ) -> List[str]:
        """Run real value extraction using the value extraction pipeline.

        Returns:
            List of skipped document names (documents with no observation units found)
        """
        
        session_dir = self.work_dir / session_id

        # Save schema in value extraction format (includes observation_unit)
        schema_data = schema.to_full_dict()

        value_extraction_schema_path = session_dir / "value_extraction_schema.json"
        with open(value_extraction_schema_path, 'w') as f:
            json.dump(schema_data, f, indent=2)
        
        # Prepare documents directories
        docs_paths = qbsd_config["docs_path"]
        if isinstance(docs_paths, str):
            docs_paths = [docs_paths]
        
        docs_directories = [Path(path) for path in docs_paths]
        output_path = session_dir / "extracted_data.jsonl"
        
        # Count total documents for progress tracking
        total_documents = 0
        for docs_dir in docs_directories:
            if docs_dir.exists():
                doc_files = list(docs_dir.glob("*.txt")) + list(docs_dir.glob("*.md"))
                total_documents += len(doc_files)
        
        # Update session metadata
        session = self.session_manager.get_session(session_id)
        session.metadata.total_documents = total_documents
        session.metadata.processed_documents = 0
        self.session_manager.update_session(session)
        
        # Run value extraction in a separate thread to avoid blocking
        loop = asyncio.get_event_loop()

        # Create callback to stream cell values as they're extracted
        on_value_extracted = self._create_value_extracted_callback(session_id, loop)

        # Create callback to broadcast warnings via WebSocket
        on_warning = self._create_warning_callback(session_id, loop)

        extraction_result = {}

        # Create should_stop callback that checks for stop requests
        def should_stop():
            return self.is_stop_requested(session_id)

        def run_value_extraction():
            nonlocal extraction_result
            extraction_result = build_table_jsonl(
                schema_path=value_extraction_schema_path,
                docs_directories=docs_directories,
                output_path=output_path,
                llm=llm,
                retriever=retriever,
                resume=False,
                mode="all",  # Process all columns together
                retrieval_k=8,
                max_workers=1,  # Single worker to avoid overwhelming API
                on_value_extracted=on_value_extracted,  # Stream values as extracted
                should_stop=should_stop,  # Allow graceful stop
                on_warning=on_warning  # Broadcast warnings to UI
            )
            return extraction_result

        # Track progress by monitoring output file
        extraction_task = loop.run_in_executor(qbsd_thread_pool, run_value_extraction)
        
        # Monitor progress while extraction runs
        start_time = time.time()
        last_line_count = 0
        last_update_time = time.time()
        
        stopped_early = False
        stop_requested_at = None
        MAX_STOP_WAIT = 120  # 2 minutes max wait for graceful stop

        while not extraction_task.done():
            # Check for stop request - continue monitoring until thread actually stops
            if self.is_stop_requested(session_id):
                if stop_requested_at is None:
                    logger.warning("Stop requested during value extraction - waiting for graceful stop")
                    stop_requested_at = time.time()
                    stopped_early = True

                # Check if we've waited too long for graceful stop
                elapsed = time.time() - stop_requested_at
                if elapsed > MAX_STOP_WAIT:
                    logger.warning("Graceful stop timeout after %ds - forcing exit", MAX_STOP_WAIT)
                    break

                # Poll more frequently while waiting for stop
                await asyncio.sleep(0.5)
                continue  # Skip progress updates while stopping

            try:
                current_time = time.time()

                # Check output file for progress - count unique documents completed
                if output_path.exists():
                    completed_documents = set()
                    current_line_count = 0
                    with open(output_path, 'r') as f:
                        for line in f:
                            current_line_count += 1
                            try:
                                row_data = json.loads(line)
                                # Get document name: use base_row_name for observation units, else _row_name
                                metadata = row_data.get("_metadata", {})
                                doc_name = metadata.get("base_row_name") or row_data.get("_row_name")
                                if doc_name:
                                    completed_documents.add(doc_name)
                            except json.JSONDecodeError:
                                pass

                    completed_doc_count = len(completed_documents)

                    if current_line_count > last_line_count:
                        # Update session metadata with actual document count
                        session = self.session_manager.get_session(session_id)
                        session.metadata.processed_documents = min(completed_doc_count, total_documents)
                        self.session_manager.update_session(session)

                        # Send progress update with document count
                        progress_msg = f"Value Extraction: Document {completed_doc_count}/{total_documents} completed"
                        await progress_callback(progress_msg, 0.5 + (completed_doc_count / total_documents) * 0.5, {
                            "rows_extracted": current_line_count,
                            "documents_completed": completed_doc_count,
                            "total_documents": total_documents,
                            "elapsed_time": int(current_time - start_time)
                        })

                        # Broadcast document completion (use document count, not row count)
                        await self.broadcast_row_completed(session_id, {
                            "row_index": completed_doc_count,
                            "total_rows": total_documents,
                            "completed_at": datetime.now().isoformat()
                        })

                        last_line_count = current_line_count
                        last_update_time = current_time
                
                await asyncio.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                logger.warning("Progress monitoring error: %s", e)
                await asyncio.sleep(2)
        
        # Wait for completion
        if not stopped_early:
            try:
                extraction_result = await extraction_task
            except Exception as e:
                raise RuntimeError(f"Value extraction failed: {e}")
        else:
            # When stopped, still wait briefly for task to finish cleanly
            try:
                await asyncio.wait_for(asyncio.shield(extraction_task), timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass  # Task didn't finish cleanly, but that's OK - we're stopping

        # Final progress update
        final_line_count = 0
        if output_path.exists():
            with open(output_path, 'r') as f:
                final_line_count = sum(1 for _ in f)

        # Update final session metadata
        session = self.session_manager.get_session(session_id)
        session.metadata.processed_documents = final_line_count
        self.session_manager.update_session(session)

        # If stopped early, return without processing suggested values or sending "Complete"
        if stopped_early:
            logger.warning("Value extraction stopped early with %d rows extracted", final_line_count)
            return []

        # Extract results from the new return format
        suggested_values = extraction_result.get("suggested_values", {}) if extraction_result else {}
        skipped_documents = extraction_result.get("skipped_documents", []) if extraction_result else []

        # Process suggested values for schema evolution
        if suggested_values:
            await self._process_suggested_values(session, schema, suggested_values)

        self.session_manager.update_session(session)

        await progress_callback("Value Extraction: Complete", 1.0, {
            "rows_extracted": final_line_count,
            "total_documents": total_documents,
            "elapsed_time": int(time.time() - start_time),
            "suggested_values_count": sum(len(vals) for vals in suggested_values.values()) if suggested_values else 0,
            "skipped_documents": skipped_documents,
            "skipped_documents_count": len(skipped_documents)
        })

        return skipped_documents

    async def _process_suggested_values(
        self,
        session: VisualizationSession,
        schema: Schema,
        suggested_values: Dict[str, Dict[str, Any]]
    ):
        """
        Process suggested values from value extraction for schema evolution.

        For each column with allowed_values:
        - Auto-add values that meet the column's auto_expand_threshold
        - Store remaining values as pending_values for user review
        """
        from app.models.session import PendingValue
        from datetime import datetime

        if not suggested_values:
            return

        logger.info("Processing %d suggested values for schema evolution", sum(len(vals) for vals in suggested_values.values()))

        for col in session.columns:
            if col.name not in suggested_values:
                continue

            col_suggestions = suggested_values[col.name]
            if not col_suggestions:
                continue

            # Get threshold for this column (default to 2 if not set)
            threshold = col.auto_expand_threshold if col.auto_expand_threshold is not None else 2

            auto_added = []
            pending = []

            for value, details in col_suggestions.items():
                doc_count = details.get("count", 0)
                documents = details.get("documents", [])

                # Skip if value already in allowed_values
                if col.allowed_values and value in col.allowed_values:
                    continue

                if threshold > 0 and doc_count >= threshold:
                    # Auto-add this value
                    if col.allowed_values is None:
                        col.allowed_values = []
                    col.allowed_values.append(value)
                    auto_added.append(value)
                    logger.info("  Auto-added '%s' to %s (appeared in %d docs, threshold=%d)", value, col.name, doc_count, threshold)
                else:
                    # Add to pending values for user review
                    pending.append(PendingValue(
                        value=value,
                        document_count=doc_count,
                        first_seen=datetime.now(),
                        documents=documents[:10]  # Limit to first 10 documents
                    ))

            # Update column's pending values
            if pending:
                if col.pending_values is None:
                    col.pending_values = []
                col.pending_values.extend(pending)
                logger.info("  Added %d pending values to %s for review", len(pending), col.name)

            if auto_added:
                logger.info("  Auto-expanded %s allowed_values with %d new values", col.name, len(auto_added))

        # Broadcast schema update if there were changes
        total_auto_added = sum(1 for col in session.columns if col.allowed_values)
        total_pending = sum(len(col.pending_values or []) for col in session.columns)

        if total_auto_added > 0 or total_pending > 0:
            await self.websocket_manager.broadcast_schema_updated(session.id, {
                "operation": "schema_evolution",
                "auto_added_values": total_auto_added,
                "pending_values": total_pending,
                "columns": [col.model_dump(mode='json') for col in session.columns]
            })

    def _compute_statistics_from_extracted_data(
        self,
        session_id: str,
        schema: Schema,
        schema_evolution: Optional[SchemaEvolution] = None,
        skipped_documents: Optional[List[str]] = None
    ) -> Optional[DataStatistics]:
        """Compute statistics from extracted JSONL data.

        Args:
            session_id: The session ID
            schema: The discovered schema with column definitions
            schema_evolution: Optional schema evolution data from discovery
            skipped_documents: Optional list of documents skipped during value extraction

        Returns:
            DataStatistics object or None if no data available
        """
        session_dir = self.work_dir / session_id
        data_file = session_dir / "extracted_data.jsonl"

        if not data_file.exists():
            logger.warning("Statistics: No extracted_data.jsonl found for session %s (schema-only mode)", session_id)
            # Schema-only mode: return statistics based on schema without data
            columns = []
            for col in schema.columns:
                col_info = ColumnInfo(
                    name=col.name,
                    definition=col.definition,
                    rationale=col.rationale,
                    data_type="object",
                    non_null_count=0,
                    unique_count=0,
                    source_document=col.source_document,
                    discovery_iteration=col.discovery_iteration,
                    allowed_values=col.allowed_values
                )
                columns.append(col_info)

            return DataStatistics(
                total_rows=0,
                total_columns=len(schema.columns),
                total_documents=0,
                completeness=0.0,
                column_stats=columns,
                schema_evolution=schema_evolution,
                skipped_documents=skipped_documents or []
            )

        # Read all rows from the extracted data
        data_rows = []
        try:
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data_rows.append(json.loads(line))
        except Exception as e:
            logger.warning("Statistics: Error reading extracted data: %s", e)
            return None

        if not data_rows:
            logger.warning("Statistics: No data rows found in extracted_data.jsonl")
            return None

        # Count unique documents from _papers field
        unique_documents = set()
        for row in data_rows:
            papers = row.get('_papers', [])
            if isinstance(papers, list):
                unique_documents.update(papers)
            elif isinstance(papers, str) and papers:
                unique_documents.add(papers)
        total_documents = len(unique_documents)

        # Build column stats from schema + data
        columns = []
        for col in schema.columns:
            # Count non-null values for this column
            # For QBSD data, check if the "answer" field exists and is not "None" string or empty
            def is_valid_value(value):
                if value is None:
                    return False
                if isinstance(value, dict):
                    answer = value.get("answer")
                    if answer is None or answer == "None" or answer == "" or answer == "[]":
                        return False
                    return True
                # For non-dict values, check if it's not None or "None" string
                return value != "None" and value != "" and value != "[]"

            non_null_count = sum(
                1 for row in data_rows
                if col.name in row and is_valid_value(row[col.name])
            )

            # Count unique values (serialize to JSON for comparison)
            unique_values = set()
            for row in data_rows:
                if col.name in row:
                    try:
                        unique_values.add(json.dumps(row[col.name], sort_keys=True))
                    except (TypeError, ValueError):
                        unique_values.add(str(row[col.name]))
            unique_count = len(unique_values)

            # Include source document info from evolution if available
            source_document = None
            if schema_evolution and col.name in schema_evolution.column_sources:
                source_document = schema_evolution.column_sources[col.name]

            col_info = ColumnInfo(
                name=col.name,
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",  # QBSD data is typically complex objects
                non_null_count=non_null_count,
                unique_count=unique_count,
                source_document=source_document,
                discovery_iteration=getattr(col, 'discovery_iteration', None),
                allowed_values=getattr(col, 'allowed_values', None),
                auto_expand_threshold=getattr(col, 'auto_expand_threshold', 2)
            )
            columns.append(col_info)

        # Calculate overall completeness
        total_cells = len(data_rows) * len(columns)
        non_null_cells = sum(col.non_null_count or 0 for col in columns)
        completeness = (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0

        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness):
            completeness = 0.0

        stats = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            total_documents=total_documents,
            completeness=completeness,
            column_stats=columns,
            schema_evolution=schema_evolution,
            skipped_documents=skipped_documents or []
        )

        logger.info("Statistics computed: %d rows, %d documents, %d columns, %.1f%% complete", len(data_rows), total_documents, len(columns), completeness)
        if skipped_documents:
            logger.info("Skipped documents: %d", len(skipped_documents))
        if schema_evolution:
            logger.info("Schema evolution: %d snapshots, %d column sources", len(schema_evolution.snapshots), len(schema_evolution.column_sources))
        return stats

    async def get_status(self, session_id: str) -> QBSDStatus:
        """Get current status of QBSD execution."""
        with self._state_lock:
            is_running = session_id in self.running_sessions
        if is_running:
            status = "processing"
            progress = 0.5  # Mock progress
        else:
            # Check session status
            session = self.session_manager.get_session(session_id)
            if not session:
                raise ValueError("Session not found")
            
            if session.status == SessionStatus.COMPLETED:
                status = "completed"
                progress = 1.0
            elif session.status == SessionStatus.DOCUMENTS_UPLOADED:
                status = "documents_uploaded"
                progress = 1.0
            elif session.status == SessionStatus.PROCESSING_DOCUMENTS:
                status = "processing_documents"
                progress = 0.5
            elif session.status == SessionStatus.ERROR:
                status = "error"
                progress = 0.0
            elif session.status == SessionStatus.STOPPED:
                status = "stopped"
                progress = 1.0  # Stopped is a final state
            else:
                status = "idle"
                progress = 0.0
        
        return QBSDStatus(
            session_id=session_id,
            status=status,
            progress=progress,
            current_step="Running" if status == "processing" else status.title(),
            steps_completed=3 if status == "processing" else (7 if status == "completed" else 0),
            total_steps=7
        )
    
    async def get_schema(self, session_id: str) -> Dict[str, Any]:
        """Get discovered schema."""
        # Prefer session.columns (always up-to-date with user edits)
        session = self.session_manager.get_session(session_id)
        if session and session.columns:
            result = {
                "query": session.schema_query or "",
                "schema": [col.model_dump() for col in session.columns]
            }
            if session.observation_unit:
                result["observation_unit"] = session.observation_unit.model_dump()
            return result

        # Fall back to file (during QBSD creation, before columns are synced to session)
        schema_file = self.work_dir / session_id / "discovered_schema.json"
        if schema_file.exists():
            with open(schema_file) as f:
                return json.load(f)

        # Last resort
        if session:
            return {"query": session.schema_query or "", "schema": []}
        return {"query": "", "schema": []}
    
    async def get_data(
        self,
        session_id: str,
        page: int = 0,
        page_size: int = 50,
        filters: Optional[List[Dict]] = None,
        sort: Optional[List[Dict]] = None,
        search: Optional[str] = None
    ) -> PaginatedData:
        """Get extracted data from all possible locations with optional filtering and sorting.

        Data can be in multiple locations:
        - ./qbsd_work/{session_id}/extracted_data.jsonl - Original QBSD value extraction
        - ./qbsd_work/{session_id}/data.jsonl - Fallback location
        - ./data/{session_id}/data.jsonl - Additional document processing (upload_document_processor)

        When original QBSD data exists AND additional data exists, both are combined.
        """
        # Collect all data files that exist
        data_files = []

        # Check qbsd_work directory (original QBSD extraction)
        extracted_file = self.work_dir / session_id / "extracted_data.jsonl"
        if extracted_file.exists():
            data_files.append(extracted_file)

        # Check qbsd_work for data.jsonl (only if extracted_data.jsonl doesn't exist)
        if not data_files:
            qbsd_data_file = self.work_dir / session_id / "data.jsonl"
            if qbsd_data_file.exists():
                data_files.append(qbsd_data_file)

        # Check data directory - ALWAYS check this as it may contain additional documents
        data_dir_file = Path("./data") / session_id / "data.jsonl"
        if data_dir_file.exists():
            # Only add if it's not already in the list (avoid duplicates)
            if data_dir_file not in data_files:
                data_files.append(data_dir_file)

        if not data_files:
            return PaginatedData(rows=[], total_count=0, filtered_count=None, page=page, page_size=page_size, has_more=False)

        # Helper function to normalize row data
        def normalize_row(row_data: dict) -> dict:
            if '_row_name' in row_data:
                return {
                    'row_name': row_data.get('_row_name'),
                    'papers': row_data.get('_papers', []),
                    'data': {k: v for k, v in row_data.items() if not k.startswith('_')},
                    'unit_name': row_data.get('_unit_name'),
                    'source_document': row_data.get('_source_document'),
                    'parent_document': row_data.get('_parent_document'),
                }
            return row_data

        # Check if we need to filter/sort (requires loading all rows)
        needs_processing = bool(filters or sort or search)

        if needs_processing:
            # Load all rows from all data files
            all_rows = []
            for data_file in data_files:
                with open(data_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            try:
                                row_data = json.loads(line.strip())
                                all_rows.append(normalize_row(row_data))
                            except (json.JSONDecodeError, TypeError):
                                pass

            total_count = len(all_rows)

            # Use FileParser's filtering/sorting methods
            from app.services.file_parser import FileParser
            parser = FileParser()

            # Apply global search
            if search and search.strip():
                all_rows = parser._apply_search(all_rows, search.strip())

            # Apply column filters
            if filters:
                all_rows = parser._apply_filters(all_rows, filters)

            filtered_count = len(all_rows)

            # Apply sorting
            if sort:
                all_rows = parser._apply_sort(all_rows, sort)

            # Paginate
            start = page * page_size
            end = start + page_size
            page_rows = all_rows[start:end]

            # Convert to DataRow objects
            rows = [DataRow(**row_data) for row_data in page_rows]

            return PaginatedData(
                rows=rows,
                total_count=total_count,
                filtered_count=filtered_count,
                page=page,
                page_size=page_size,
                has_more=end < filtered_count
            )
        else:
            # Efficient pagination (no filtering/sorting)
            # Count total rows across all files
            total_count = 0
            for data_file in data_files:
                with open(data_file, 'r', encoding='utf-8') as f:
                    total_count += sum(1 for _ in f)

            rows = []
            start_line = page * page_size
            end_line = start_line + page_size
            global_line = 0

            # Read from all files, handling pagination across files
            for data_file in data_files:
                if global_line >= end_line:
                    break  # Already have enough rows

                with open(data_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if global_line >= end_line:
                            break
                        if global_line >= start_line:
                            try:
                                row_data = json.loads(line.strip())
                                normalized = normalize_row(row_data)
                                data_row = DataRow(**normalized)
                                rows.append(data_row)
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.warning("Could not parse row %d: %s", global_line, e)
                        global_line += 1

            return PaginatedData(
                rows=rows,
                total_count=total_count,
                filtered_count=None,
                page=page,
                page_size=page_size,
                has_more=end_line < total_count
            )
    
    async def stop_execution(self, session_id: str) -> Dict[str, Any]:
        """Stop QBSD execution gracefully.

        Returns:
            Dict with status info including what was saved (schema, data counts)
        """
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

            # Give the task time to stop gracefully (LLM calls can take 30+ seconds)
            try:
                logger.debug("stop_execution: Waiting for task to finish gracefully...")
                await asyncio.wait_for(asyncio.shield(task), timeout=60.0)
                logger.debug("stop_execution: Task finished gracefully")
            except asyncio.TimeoutError:
                # If it doesn't stop gracefully after 60s, force cancel it
                logger.warning("Graceful stop timed out after 60s, force cancelling task for %s", session_id)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                logger.debug("stop_execution: Task was cancelled")
                pass  # Task was cancelled, which is expected
            except Exception as e:
                logger.error("Exception during stop: %s", e)

            # Clean up
            with self._state_lock:
                self.running_sessions.pop(session_id, None)
                self.stop_flags.pop(session_id, None)

            # Check what was saved
            session_dir = self.work_dir / session_id
            schema_file = session_dir / "discovered_schema.json"
            data_file = session_dir / "extracted_data.jsonl"

            if schema_file.exists():
                result["schema_saved"] = True

            if data_file.exists():
                with open(data_file, 'r') as f:
                    result["data_rows_saved"] = sum(1 for _ in f)

            # Update session status to STOPPED (not ERROR)
            logger.debug("stop_execution: Updating session status to STOPPED")
            session = self.session_manager.get_session(session_id)
            if session:
                logger.debug("stop_execution: Current status = %s, updating to STOPPED", session.status)
                session.status = SessionStatus.STOPPED
                session.error_message = None  # Clear any error - this was intentional stop
                self.session_manager.update_session(session)
                logger.debug("stop_execution: Session updated, new status = %s", session.status)
            else:
                logger.warning("stop_execution: WARNING - session is None!")

            # Broadcast stopped message
            logger.debug("stop_execution: Broadcasting stopped message")
            await self.broadcast_stopped(session_id, {
                "schema_saved": result["schema_saved"],
                "data_rows_saved": result["data_rows_saved"],
                "message": "Processing stopped by user"
            })
            logger.debug("stop_execution: Broadcast complete")

            result["stopped"] = True
            result["message"] = "Processing stopped successfully"
            return result

        logger.debug("stop_execution: Session NOT in running_sessions!")
        result["message"] = "No running session found"
        return result