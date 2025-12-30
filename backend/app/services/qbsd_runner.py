"""QBSD integration service."""

import json
import asyncio
import math
import subprocess
import time
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

# Import QBSD components from qbsd-lib
import sys
from pathlib import Path

# Add qbsd-lib to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
QBSD_LIB_ROOT = PROJECT_ROOT / "qbsd-lib"
sys.path.insert(0, str(QBSD_LIB_ROOT))

try:
    # Import QBSD main functions from qbsd-lib
    from qbsd.core import qbsd as QBSD
    from qbsd.core.schema import Schema, Column
    from qbsd.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
    from qbsd.core.retrievers import EmbeddingRetriever
    from qbsd.core import utils
    from qbsd.value_extraction.main import build_table_jsonl
    QBSD_AVAILABLE = True
    print(f"✓ QBSD components successfully loaded from {QBSD_LIB_ROOT}")
except ImportError as e:
    print(f"✗ QBSD components not available: {e}")
    print(f"  Tried to import from: {QBSD_LIB_ROOT}")
    print(f"  Current working directory: {Path.cwd()}")
    
    # Create mock classes for fallback
    class Schema:
        def __init__(self, **kwargs):
            self.columns = []
    class Column:
        def __init__(self, **kwargs):
            pass
    class MockLLMInterface:
        def generate(self, *args, **kwargs):
            return "Mock response"
    
    QBSD_AVAILABLE = False

def build_llm_interface(
    provider: str,
    model: str,
    max_output_tokens: int,
    temperature: float,
    api_key: str = None,
    gemini_key_type: str = None
):
    """Build LLM interface based on provider.

    Args:
        provider: LLM provider name (together, openai, gemini)
        model: Model name/identifier (empty string uses provider default)
        max_output_tokens: Maximum tokens the model can generate in its response
        temperature: Sampling temperature
        api_key: Optional user-provided API key (falls back to env var if None)
        gemini_key_type: For Gemini only - 'single' or 'multi' key mode
    """
    if not QBSD_AVAILABLE:
        raise RuntimeError("QBSD components not available")

    if provider.lower() == "together":
        if not model:
            raise ValueError("Model must be specified for Together AI provider")
        return TogetherLLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            api_key=api_key
        )
    elif provider.lower() == "openai":
        if not model:
            raise ValueError("Model must be specified for OpenAI provider")
        return OpenAILLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            api_key=api_key
        )
    elif provider.lower() == "gemini":
        # GeminiLLM has default model="gemini-2.5-flash" and max_output_tokens=8192
        # Only pass model if explicitly provided (non-empty)
        kwargs = {
            "max_output_tokens": max_output_tokens,
            "temperature": temperature,
            "api_key": api_key
        }
        if model:  # Only pass model if explicitly set
            kwargs["model"] = model
        return GeminiLLM(**kwargs)
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

from app.models.qbsd import QBSDConfig, QBSDStatus, QBSDProgress
from app.models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData, SessionStatus, SchemaEvolution, SchemaSnapshot, VisualizationSession
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

    def is_stop_requested(self, session_id: str) -> bool:
        """Check if stop has been requested for a session."""
        return self.stop_flags.get(session_id, False)

    def clear_stop_flag(self, session_id: str):
        """Clear the stop flag for a session."""
        if session_id in self.stop_flags:
            del self.stop_flags[session_id]

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
                print(f"⚠️  Failed to broadcast cell {column_name} for {row_name}: {e}")

        return on_value_extracted

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
                    print(f"⚠️ Heartbeat failed: {e}")
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

                print(f"📥 Downloading Supabase dataset '{dataset_name}' to {local_dataset_dir}")
                downloaded_files = await storage.download_dataset_to_local(dataset_name, str(local_dataset_dir))
                print(f"✓ Downloaded {len(downloaded_files)} files from '{dataset_name}'")

                return str(local_dataset_dir)
        except Exception as e:
            print(f"⚠️ Error checking/downloading Supabase dataset '{dataset_name}': {e}")

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
                "api_key": config.schema_creation_backend.api_key,
                "gemini_key_type": config.schema_creation_backend.gemini_key_type
            },
            "value_extraction_backend": {
                "provider": config.value_extraction_backend.provider,
                "model": config.value_extraction_backend.model,
                "max_output_tokens": config.value_extraction_backend.max_output_tokens,
                "temperature": config.value_extraction_backend.temperature,
                "context_window_size": config.value_extraction_backend.context_window_size,
                "api_key": config.value_extraction_backend.api_key,
                "gemini_key_type": config.value_extraction_backend.gemini_key_type
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

        return qbsd_config

    async def _resolve_docs_paths(self, config: QBSDConfig, session_id: str) -> List[str]:
        """Resolve document paths - download from Supabase if needed."""
        session_dir = self.work_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
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
                        print(f"✓ Resolved document path: {path} -> {candidate.absolute()}")
                        break
                else:
                    print(f"Warning: Document path not found: {path}")
                    resolved_docs_paths.append(path)
            else:
                resolved_docs_paths.append(str(doc_path))

        return resolved_docs_paths

    def _convert_config_to_qbsd_format(self, config: QBSDConfig, session_id: str) -> Dict[str, Any]:
        """Convert visualization QBSDConfig to QBSD pipeline format.

        NOTE: This is now a sync wrapper. Use _resolve_docs_paths() first in async context.
        """
        session_dir = self.work_dir / session_id

        # Convert docs_path to absolute paths (sync version - no Supabase download)
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        resolved_docs_paths = []

        for path in docs_paths:
            doc_path = Path(path)
            if not doc_path.is_absolute():
                # Try relative to project root and common paths
                candidates = [
                    PROJECT_ROOT / path,
                    PROJECT_ROOT / "research" / "data" / Path(path).name,  # Research data
                    PROJECT_ROOT / "test" / "files",  # Test directory
                    Path.cwd() / path,
                    Path.cwd().parent / path,  # Try from parent directory
                ]
                for candidate in candidates:
                    if candidate.exists():
                        resolved_docs_paths.append(str(candidate.absolute()))
                        print(f"✓ Resolved document path: {path} -> {candidate.absolute()}")
                        break
                else:
                    print(f"Warning: Document path not found: {path}")
                    resolved_docs_paths.append(path)  # Keep original for error reporting
            else:
                resolved_docs_paths.append(str(doc_path))
        
        # Build QBSD config
        qbsd_config = {
            "query": config.query,
            "docs_path": resolved_docs_paths[0] if len(resolved_docs_paths) == 1 else resolved_docs_paths,
            "max_keys_schema": config.max_keys_schema,
            "documents_batch_size": config.documents_batch_size,
            "output_path": str(session_dir / "discovered_schema.json"),
            "document_randomization_seed": config.document_randomization_seed,
            "schema_creation_backend": {
                "provider": config.schema_creation_backend.provider,
                "model": config.schema_creation_backend.model,
                "max_output_tokens": config.schema_creation_backend.max_output_tokens,
                "temperature": config.schema_creation_backend.temperature,
                "context_window_size": config.schema_creation_backend.context_window_size,
                "api_key": config.schema_creation_backend.api_key,
                "gemini_key_type": config.schema_creation_backend.gemini_key_type
            },
            "value_extraction_backend": {
                "provider": config.value_extraction_backend.provider,
                "model": config.value_extraction_backend.model,
                "max_output_tokens": config.value_extraction_backend.max_output_tokens,
                "temperature": config.value_extraction_backend.temperature,
                "context_window_size": config.value_extraction_backend.context_window_size,
                "api_key": config.value_extraction_backend.api_key,
                "gemini_key_type": config.value_extraction_backend.gemini_key_type
            }
        }
        
        # Add retriever config if provided
        if config.retriever:
            qbsd_config["retriever"] = {
                "type": "embedding",  # Default to embedding retriever
                "model_name": config.retriever.model_name,
                "k": config.retriever.k,
                "passage_chars": config.retriever.passage_chars,
                "overlap": config.retriever.overlap,
                "enable_dynamic_k": config.retriever.enable_dynamic_k,
                "dynamic_k_threshold": config.retriever.dynamic_k_threshold,
                "dynamic_k_minimum": config.retriever.dynamic_k_minimum
            }
        
        # Add initial schema if provided (inline takes priority over file path)
        if config.initial_schema:
            # Inline schema provided - convert to the format expected by schema loader
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

        return qbsd_config
    
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
        has_documents = bool(docs_paths and any(p for p in docs_paths if p))

        # Validate: at least one of query or documents must be provided
        if not has_query and not has_documents:
            errors.append("At least one of query or documents must be provided")
        
        # Validate document paths (only if documents are provided)
        if has_documents:
            for path in docs_paths:
                doc_path = Path(path)
                print(f"DEBUG: Checking document path: {path} -> {doc_path.absolute()}")

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
                        print(f"DEBUG: Found path at: {actual_path}")
                        # Check if directory has files
                        try:
                            file_count = len(list(try_path.glob("*.txt"))) + len(list(try_path.glob("*.md")))
                            if file_count == 0:
                                warnings.append(f"Document path appears to be empty: {path} (no .txt or .md files)")
                            else:
                                print(f"DEBUG: Found {file_count} document files")
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
            if session_id in self.running_sessions:
                del self.running_sessions[session_id]
    
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
            print(f"🐛 DEBUG: Starting QBSD execution for session {session_id}")
            await update_progress("Initializing", 0.0)

            # Resolve document paths (download from Supabase if needed)
            print(f"🐛 DEBUG: Resolving document paths (may download from Supabase)")
            resolved_docs_paths = await self._resolve_docs_paths(config, session_id)
            print(f"🐛 DEBUG: Resolved paths: {resolved_docs_paths}")

            # Convert config to QBSD format with resolved paths
            print(f"🐛 DEBUG: Converting config to QBSD format")
            qbsd_config = self._convert_config_to_qbsd_format_sync(config, session_id, resolved_docs_paths)
            
            # Save QBSD config
            qbsd_config_file = session_dir / "qbsd_config.json"
            with open(qbsd_config_file, 'w') as f:
                json.dump(qbsd_config, f, indent=2)
            print(f"DEBUG: Saved QBSD config with keys: {list(qbsd_config.keys())}")
            
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
                            print(f"Warning: Could not read {doc_file}: {e}")
            
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
                print(f"🐛 DEBUG: STANDARD mode - query + {len(documents)} documents")
            elif has_documents:
                print(f"🐛 DEBUG: DOCUMENT_ONLY mode - {len(documents)} documents, no query")
            else:
                print(f"🐛 DEBUG: QUERY_ONLY mode - query provided, no documents")
            
            # Step 3: Build LLM backend
            current_step += 1
            print(f"🐛 DEBUG: Building Schema Creation LLM backend - provider: {qbsd_config['schema_creation_backend']['provider']}")
            await update_progress("Building LLM backend", 0.0)
            
            # Build Schema Creation LLM interface
            print(f"🐛 DEBUG: Creating Schema Creation LLM interface...")
            llm = build_llm_interface(
                provider=qbsd_config["schema_creation_backend"]["provider"],
                model=qbsd_config["schema_creation_backend"]["model"],
                max_output_tokens=qbsd_config["schema_creation_backend"]["max_output_tokens"],
                temperature=qbsd_config["schema_creation_backend"]["temperature"],
                api_key=qbsd_config["schema_creation_backend"].get("api_key"),
                gemini_key_type=qbsd_config["schema_creation_backend"].get("gemini_key_type")
            )
            print(f"🐛 DEBUG: LLM interface created successfully")
            
            print(f"🐛 DEBUG: Updating progress to 1.0...")
            await update_progress("Building LLM backend", 1.0)
            print(f"🐛 DEBUG: Progress update completed")
            
            # Step 4: Setup retriever
            current_step += 1
            print(f"🐛 DEBUG: Setting up retriever...")
            await update_progress("Setting up retriever", 0.0)
            
            retriever = None
            if "retriever" in qbsd_config:
                print(f"🐛 DEBUG: Creating EmbeddingRetriever...")
                retriever_config = qbsd_config["retriever"]
                print(f"🐛 DEBUG: Retriever config: {retriever_config}")
                try:
                    retriever = EmbeddingRetriever(
                        model_name=retriever_config.get("model_name", "all-MiniLM-L6-v2"),
                        max_words=retriever_config.get("passage_chars", 512),  # Convert passage_chars to max_words
                        k=retriever_config.get("k", 15),
                        enable_dynamic_k=retriever_config.get("enable_dynamic_k", True),
                        dynamic_k_threshold=retriever_config.get("dynamic_k_threshold", 0.65),
                        dynamic_k_minimum=retriever_config.get("dynamic_k_minimum", 3)
                    )
                    print(f"🐛 DEBUG: EmbeddingRetriever created successfully!")
                except Exception as e:
                    print(f"🐛 DEBUG: ERROR creating EmbeddingRetriever: {e}")
                    raise
            else:
                print(f"🐛 DEBUG: No retriever config found, using None")
            
            await update_progress("Setting up retriever", 1.0)
            
            # Step 5: Schema discovery
            current_step += 1
            await update_progress("Discovering schema", 0.0)

            # Run real schema discovery with heartbeat to keep WebSocket alive
            print(f"🐛 DEBUG: Starting schema discovery with {len(documents)} documents")
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
            print(f"🐛 DEBUG: Schema discovery completed with {len(discovered_schema.columns)} columns")
            print(f"🐛 DEBUG: Schema evolution: {len(schema_evolution.snapshots)} snapshots tracked")
            
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
            
            with open(schema_file, 'w') as f:
                json.dump({
                    "query": qbsd_config["query"],
                    "schema": frontend_schema
                }, f, indent=2)
            
            await update_progress("Schema Discovery: Complete", 1.0, {
                "columns_discovered": len(discovered_schema.columns)
            })
            
            # Update session status to SCHEMA_READY
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.SCHEMA_READY
            session.metadata.schema_discovery_completed = True
            print(f"🔄 DEBUG: Updated session {session_id} status to SCHEMA_READY with {len(discovered_schema.columns)} columns")
            
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
            self.session_manager.update_session(session)
            print(f"💾 DEBUG: Session {session_id} saved with {len(schema_columns)} columns, status: {session.status}")
            
            # Broadcast schema completion event
            await self.broadcast_schema_completed(session_id, {
                "query": qbsd_config["query"],
                "columns": [col.model_dump() for col in schema_columns],
                "total_columns": len(discovered_schema.columns)
            })

            # Check if stop was requested during schema discovery - skip remaining steps
            if self.is_stop_requested(session_id):
                print(f"🛑 Stop requested - skipping value extraction and finalization")
                return  # Exit early, stop_execution() will handle status update

            # Step 6: Value extraction (skip if schema-only mode)
            if qbsd_config.get("skip_value_extraction", False):
                print(f"⏭️ Skipping value extraction (schema-only mode)")
                # Skip to finalization without value extraction
            else:
                current_step += 1
                await update_progress("Extracting values", 0.0)

                # Build Value Extraction LLM interface (separate from schema creation)
                print(f"🐛 DEBUG: Creating Value Extraction LLM interface...")
                value_extraction_llm = build_llm_interface(
                    provider=qbsd_config["value_extraction_backend"]["provider"],
                    model=qbsd_config["value_extraction_backend"]["model"],
                    max_output_tokens=qbsd_config["value_extraction_backend"]["max_output_tokens"],
                    temperature=qbsd_config["value_extraction_backend"]["temperature"],
                    api_key=qbsd_config["value_extraction_backend"].get("api_key"),
                    gemini_key_type=qbsd_config["value_extraction_backend"].get("gemini_key_type")
                )
                print(f"🐛 DEBUG: Value Extraction LLM interface created successfully")

                # Run real value extraction with dedicated LLM (with heartbeat for long operations)
                heartbeat_task = await self._start_heartbeat(session_id, interval=15.0)
                try:
                    await self._run_value_extraction(
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
                print(f"🛑 Stop requested - skipping finalization")
                return  # Exit early, stop_execution() will handle status update

            # Step 7: Finalize
            current_step += 1
            await update_progress("Finalizing results", 0.0)

            # Compute statistics from extracted data (include evolution)
            statistics = self._compute_statistics_from_extracted_data(session_id, discovered_schema, schema_evolution)

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
                initial_schema = Schema(columns)
                print(f"Loaded inline initial schema with {len(columns)} columns")
            except Exception as e:
                print(f"Warning: Could not load inline initial schema: {e}")
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
                        initial_schema = Schema(columns)
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
                        initial_schema = Schema(columns)
                print(f"Loaded initial schema from file with {len(columns)} columns")
            except Exception as e:
                print(f"Warning: Could not load initial schema from file: {e}")

        current_schema = initial_schema or Schema([])
        query = qbsd_config["query"]

        # Calculate iterations based on document batching
        batch_size = qbsd_config.get("documents_batch_size", 4)
        max_iterations = math.ceil(len(documents) / batch_size) if documents else 1
        convergence_threshold = 5  # Stop if schema doesn't change for 5 consecutive batches
        unchanged_count = 0

        # Create document batches
        batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
        filename_batches = [filenames[i:i+batch_size] for i in range(0, len(filenames), batch_size)]

        print(f"🐛 DEBUG: Document batching - {len(documents)} docs, batch_size={batch_size}, {len(batches)} batches")

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
            print(f"🐛 DEBUG: QUERY_ONLY mode - generating schema from query without documents")
            await progress_callback("Schema Discovery: Planning from query", 0.5, {
                "mode": "query_only",
                "current_columns": len(current_schema.columns)
            })

            try:
                # Call generate_schema with empty passages
                schema_result = QBSD.generate_schema(
                    passages=[],
                    query=query,
                    max_keys_schema=qbsd_config.get("max_keys_schema", 100),
                    current_schema=current_schema,
                    llm=llm,
                    context_window_size=qbsd_config["schema_creation_backend"].get("context_window_size", 8192)
                )
                new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
                print(f"🐛 DEBUG: QUERY_ONLY generated schema with {len(new_schema.columns)} columns")

                # Merge with any initial schema
                merged_schema = current_schema.merge(new_schema) if current_schema.columns else new_schema

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

                print(f"🐛 DEBUG: QUERY_ONLY mode completed with {len(merged_schema.columns)} columns: {[c.name for c in merged_schema.columns]}")
                return merged_schema, evolution

            except Exception as e:
                print(f"🐛 DEBUG: ERROR in QUERY_ONLY generate_schema: {e}")
                import traceback
                traceback.print_exc()
                raise

        for iteration, (batch_docs, batch_names) in enumerate(zip(batches, filename_batches)):
            # Check for stop request at the start of each iteration
            if self.is_stop_requested(session_id):
                print(f"🛑 Stop requested during schema discovery - saving partial schema with {len(current_schema.columns)} columns")
                break

            print(f"🐛 DEBUG: Schema discovery batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs: {batch_names})")
            await progress_callback(f"Schema Discovery: Batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs)", iteration / len(batches), {
                "iteration": iteration + 1,
                "max_iterations": len(batches),
                "batch_docs": len(batch_docs),
                "current_columns": len(current_schema.columns)
            })

            # Track column names before this iteration
            columns_before = {col.name.lower() for col in current_schema.columns}
            cumulative_docs += len(batch_docs)

            # Select relevant content from this batch's documents
            print(f"🐛 DEBUG: Selecting relevant content with retriever")
            relevant_content = QBSD.select_relevant_content(
                docs=batch_docs,
                query=query,
                retriever=retriever
            )
            print(f"🐛 DEBUG: Selected {len(relevant_content)} relevant passages from batch")

            # Generate schema for this iteration
            print(f"🐛 DEBUG: Calling QBSD.generate_schema with LLM...")
            try:
                schema_result = QBSD.generate_schema(
                    passages=relevant_content,
                    query=query,
                    max_keys_schema=qbsd_config.get("max_keys_schema", 100),
                    current_schema=current_schema,
                    llm=llm,
                    context_window_size=qbsd_config["schema_creation_backend"].get("context_window_size", 8192)
                )
                # generate_schema returns a tuple (Schema, bool)
                new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
                print(f"🐛 DEBUG: Generated schema with {len(new_schema.columns)} columns")
            except Exception as e:
                print(f"🐛 DEBUG: ERROR in generate_schema: {e}")
                raise

            # Merge with existing schema
            print(f"🐛 DEBUG: Merging schemas...")
            print(f"🐛 DEBUG: Current schema has {len(current_schema.columns)} columns")
            print(f"🐛 DEBUG: New schema has {len(new_schema.columns)} columns")
            try:
                merged_schema = current_schema.merge(new_schema)
                print(f"🐛 DEBUG: Merged schema has {len(merged_schema.columns)} columns")
            except Exception as e:
                print(f"🐛 DEBUG: ERROR in schema merge: {e}")
                import traceback
                traceback.print_exc()
                raise

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

            print(f"🐛 DEBUG: Evolution - batch {iteration + 1}: {len(new_columns)} new columns: {new_columns}")

            # Check convergence
            print(f"🐛 DEBUG: Checking convergence...")
            if QBSD.evaluate_schema_convergence(current_schema, merged_schema):
                unchanged_count += 1
                print(f"🐛 DEBUG: Schema unchanged (count: {unchanged_count}/{convergence_threshold})")
                if unchanged_count >= convergence_threshold:
                    print(f"🐛 DEBUG: Schema converged after {iteration + 1} batches")
                    break
            else:
                unchanged_count = 0
                print(f"🐛 DEBUG: Schema changed, continuing to next batch")

            current_schema = merged_schema
            print(f"🐛 DEBUG: Completed batch {iteration + 1}, moving to next")

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
            with open(partial_schema_file, 'w') as f:
                json.dump({"query": query, "schema": frontend_schema}, f, indent=2)
            print(f"🐛 DEBUG: Saved partial schema with {len(current_schema.columns)} columns")

            # Small delay to allow other tasks
            await asyncio.sleep(0.1)

        print(f"🐛 DEBUG: Schema discovery completed with {len(current_schema.columns)} columns after {len(evolution.snapshots)} batches")
        print(f"🐛 DEBUG: Evolution tracking: {len(evolution.snapshots)} snapshots, {len(evolution.column_sources)} column sources")
        return current_schema, evolution
    
    async def _run_value_extraction(
        self, 
        session_id: str, 
        qbsd_config: Dict[str, Any], 
        schema: Schema, 
        llm: LLMInterface, 
        retriever, 
        progress_callback
    ):
        """Run real value extraction using the value extraction pipeline."""
        
        session_dir = self.work_dir / session_id
        
        # Save schema in value extraction format
        schema_data = {
            "query": qbsd_config["query"],
            "schema": [col.to_dict() for col in schema.columns]
        }
        
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

        suggested_values_result = {}

        def run_value_extraction():
            nonlocal suggested_values_result
            suggested_values_result = build_table_jsonl(
                schema_path=value_extraction_schema_path,
                docs_directories=docs_directories,
                output_path=output_path,
                llm=llm,
                retriever=retriever,
                resume=False,
                mode="all",  # Process all columns together
                retrieval_k=8,
                max_workers=1,  # Single worker to avoid overwhelming API
                on_value_extracted=on_value_extracted  # Stream values as extracted
            )
            return suggested_values_result

        # Track progress by monitoring output file
        extraction_task = loop.run_in_executor(None, run_value_extraction)
        
        # Monitor progress while extraction runs
        start_time = time.time()
        last_line_count = 0
        last_update_time = time.time()
        
        stopped_early = False
        while not extraction_task.done():
            # Check for stop request
            if self.is_stop_requested(session_id):
                print(f"🛑 Stop requested during value extraction - cancelling task")
                extraction_task.cancel()
                try:
                    await extraction_task
                except asyncio.CancelledError:
                    pass
                stopped_early = True
                break

            try:
                current_time = time.time()

                # Check output file size for progress
                if output_path.exists():
                    with open(output_path, 'r') as f:
                        current_line_count = sum(1 for _ in f)
                    
                    if current_line_count > last_line_count:
                        # Update session metadata
                        session = self.session_manager.get_session(session_id)
                        session.metadata.processed_documents = min(current_line_count, total_documents)
                        self.session_manager.update_session(session)
                        
                        # Send progress update with meaningful message
                        progress_msg = f"Value Extraction: Document {current_line_count}/{total_documents} completed"
                        await progress_callback(progress_msg, 0.5 + (current_line_count / total_documents) * 0.5, {
                            "rows_extracted": current_line_count,
                            "total_documents": total_documents,
                            "elapsed_time": int(current_time - start_time)
                        })
                        
                        # Broadcast row completion for each new row
                        if current_line_count > last_line_count:
                            for new_row_idx in range(last_line_count, current_line_count):
                                await self.broadcast_row_completed(session_id, {
                                    "row_index": new_row_idx + 1,
                                    "total_rows": total_documents,
                                    "completed_at": datetime.now().isoformat()
                                })
                        
                        last_line_count = current_line_count
                        last_update_time = current_time
                
                await asyncio.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                print(f"Progress monitoring error: {e}")
                await asyncio.sleep(2)
        
        # Wait for completion (skip if we already handled stop)
        if not stopped_early:
            try:
                suggested_values_result = await extraction_task
            except Exception as e:
                raise RuntimeError(f"Value extraction failed: {e}")

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
            print(f"🛑 Value extraction stopped early with {final_line_count} rows extracted")
            return

        # Process suggested values for schema evolution
        if suggested_values_result:
            await self._process_suggested_values(session, schema, suggested_values_result)

        self.session_manager.update_session(session)

        await progress_callback("Value Extraction: Complete", 1.0, {
            "rows_extracted": final_line_count,
            "total_documents": total_documents,
            "elapsed_time": int(time.time() - start_time),
            "suggested_values_count": sum(len(vals) for vals in suggested_values_result.values()) if suggested_values_result else 0
        })

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

        print(f"🔄 Processing {sum(len(vals) for vals in suggested_values.values())} suggested values for schema evolution")

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
                    print(f"  ✅ Auto-added '{value}' to {col.name} (appeared in {doc_count} docs, threshold={threshold})")
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
                print(f"  📋 Added {len(pending)} pending values to {col.name} for review")

            if auto_added:
                print(f"  🎉 Auto-expanded {col.name} allowed_values with {len(auto_added)} new values")

        # Broadcast schema update if there were changes
        total_auto_added = sum(1 for col in session.columns if col.allowed_values)
        total_pending = sum(len(col.pending_values or []) for col in session.columns)

        if total_auto_added > 0 or total_pending > 0:
            await self.websocket_manager.broadcast_schema_updated(session.id, {
                "operation": "schema_evolution",
                "auto_added_values": total_auto_added,
                "pending_values": total_pending,
                "columns": [col.model_dump() for col in session.columns]
            })

    def _compute_statistics_from_extracted_data(
        self,
        session_id: str,
        schema: Schema,
        schema_evolution: Optional[SchemaEvolution] = None
    ) -> Optional[DataStatistics]:
        """Compute statistics from extracted JSONL data.

        Args:
            session_id: The session ID
            schema: The discovered schema with column definitions
            schema_evolution: Optional schema evolution data from discovery

        Returns:
            DataStatistics object or None if no data available
        """
        session_dir = self.work_dir / session_id
        data_file = session_dir / "extracted_data.jsonl"

        if not data_file.exists():
            print(f"⚠️  Statistics: No extracted_data.jsonl found for session {session_id} (schema-only mode)")
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
                completeness=0.0,
                column_stats=columns,
                schema_evolution=schema_evolution
            )

        # Read all rows from the extracted data
        data_rows = []
        try:
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data_rows.append(json.loads(line))
        except Exception as e:
            print(f"⚠️  Statistics: Error reading extracted data: {e}")
            return None

        if not data_rows:
            print(f"⚠️  Statistics: No data rows found in extracted_data.jsonl")
            return None

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
            completeness=completeness,
            column_stats=columns,
            schema_evolution=schema_evolution
        )

        print(f"✓ Statistics computed: {len(data_rows)} rows, {len(columns)} columns, {completeness:.1f}% complete")
        if schema_evolution:
            print(f"✓ Schema evolution: {len(schema_evolution.snapshots)} snapshots, {len(schema_evolution.column_sources)} column sources")
        return stats

    async def get_status(self, session_id: str) -> QBSDStatus:
        """Get current status of QBSD execution."""
        if session_id in self.running_sessions:
            status = "processing"
            # You could track more detailed progress here
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
        schema_file = self.work_dir / session_id / "discovered_schema.json"
        if schema_file.exists():
            with open(schema_file) as f:
                return json.load(f)
        
        # Fall back to session schema (return query even if no columns yet)
        session = self.session_manager.get_session(session_id)
        if session:
            return {
                "query": session.schema_query or "",
                "schema": [col.model_dump() for col in session.columns] if session.columns else []
            }

        return {"query": "", "schema": []}
    
    async def get_data(self, session_id: str, page: int = 0, page_size: int = 50) -> PaginatedData:
        """Get extracted data from all possible locations.

        Data can be in multiple locations:
        - ./qbsd_work/{session_id}/extracted_data.jsonl - Original QBSD value extraction
        - ./qbsd_work/{session_id}/data.jsonl - Fallback location
        - ./data/{session_id}/data.jsonl - Additional document processing (upload_document_processor)
        """
        data_files = []

        # Check qbsd_work directory (original QBSD extraction)
        extracted_file = self.work_dir / session_id / "extracted_data.jsonl"
        if extracted_file.exists():
            data_files.append(extracted_file)

        # Check qbsd_work for data.jsonl (fallback)
        qbsd_data_file = self.work_dir / session_id / "data.jsonl"
        if qbsd_data_file.exists():
            data_files.append(qbsd_data_file)

        # Check data directory (additional document processing writes here)
        data_dir_file = Path("./data") / session_id / "data.jsonl"
        if data_dir_file.exists() and data_dir_file.resolve() not in [f.resolve() for f in data_files]:
            data_files.append(data_dir_file)

        if not data_files:
            return PaginatedData(rows=[], total_count=0, page=page, page_size=page_size, has_more=False)

        # Count total rows across all files
        total_count = 0
        for data_file in data_files:
            with open(data_file, 'r', encoding='utf-8') as f:
                total_count += sum(1 for _ in f)

        # Read requested page across all files
        rows = []
        current_line = 0
        start_line = page * page_size
        end_line = start_line + page_size

        for data_file in data_files:
            if current_line >= end_line:
                break
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if current_line >= start_line and current_line < end_line:
                        try:
                            row_data = json.loads(line.strip())

                            # Handle both old mock format and new real extraction format
                            if '_row_name' in row_data:
                                # New format from value extraction
                                data_row = DataRow(
                                    row_name=row_data.get('_row_name'),
                                    papers=row_data.get('_papers', []),
                                    data={k: v for k, v in row_data.items() if not k.startswith('_')}
                                )
                            else:
                                # Old mock format or direct DataRow format
                                data_row = DataRow(**row_data)

                            rows.append(data_row)
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"Warning: Could not parse row {current_line}: {e}")
                    current_line += 1
                    if current_line >= end_line:
                        break

        return PaginatedData(
            rows=rows,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_more=end_line < total_count
        )
    
    async def stop_execution(self, session_id: str) -> Dict[str, Any]:
        """Stop QBSD execution gracefully.

        Returns:
            Dict with status info including what was saved (schema, data counts)
        """
        result = {
            "stopped": False,
            "schema_saved": False,
            "data_rows_saved": 0,
            "message": ""
        }

        if session_id in self.running_sessions:
            # Set stop flag first - the running task will check this and exit gracefully
            self.stop_flags[session_id] = True

            task = self.running_sessions[session_id]

            # Give the task time to stop gracefully (LLM calls can take 30+ seconds)
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=60.0)
            except asyncio.TimeoutError:
                # If it doesn't stop gracefully after 60s, force cancel it
                print(f"🛑 Graceful stop timed out after 60s, force cancelling task for {session_id}")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass  # Task was cancelled, which is expected
            except Exception as e:
                print(f"🛑 Exception during stop: {e}")

            # Clean up
            if session_id in self.running_sessions:
                del self.running_sessions[session_id]
            self.clear_stop_flag(session_id)

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
            session = self.session_manager.get_session(session_id)
            if session:
                session.status = SessionStatus.STOPPED
                session.error_message = None  # Clear any error - this was intentional stop
                self.session_manager.update_session(session)

            # Broadcast stopped message
            await self.broadcast_stopped(session_id, {
                "schema_saved": result["schema_saved"],
                "data_rows_saved": result["data_rows_saved"],
                "message": "Processing stopped by user"
            })

            result["stopped"] = True
            result["message"] = "Processing stopped successfully"
            return result

        result["message"] = "No running session found"
        return result