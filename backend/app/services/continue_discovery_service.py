"""
Continue Schema Discovery service for QBSD visualization.
Handles continuing schema discovery with existing schema as starting point,
discovering new columns, and incremental value extraction.
"""

import logging
import json
import asyncio
import functools
import threading
import uuid
import math
import shutil
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

from app.models.session import (
    ColumnInfo, VisualizationSession
)
# Note: SchemaEvolution, SchemaSnapshot are imported locally where needed
# to avoid conflict with qbsd.core.schema.SchemaEvolution
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.services import qbsd_thread_pool, concurrency_limiter
from app.storage.factory import get_storage
from app.core.config import DEVELOPER_MODE, RELEASE_CONFIG, MAX_DOCUMENTS
from app.core.logging_utils import set_session_context

# QBSD library imports
from qbsd.core import qbsd as QBSD
from qbsd.core.qbsd import discover_schema
from qbsd.core.schema import Schema, Column, SchemaEvolution, SchemaSnapshot
from qbsd.core.llm_backends import GeminiLLM
from qbsd.core.retrievers import EmbeddingRetriever
from qbsd.core import utils as qbsd_utils
from qbsd.core.llm_call_tracker import LLMCallTracker
from qbsd.value_extraction.main import build_table_jsonl

QBSD_AVAILABLE = True


def _enforce_release_llm_config(llm_config: dict, is_schema_creation: bool = False) -> dict:
    """Override LLM config with release-mode defaults if not in developer mode.

    Args:
        llm_config: The original LLM configuration dict
        is_schema_creation: True for schema creation LLM, False for value extraction

    Returns:
        The config dict, potentially with provider/model/temperature overridden
    """
    if DEVELOPER_MODE:
        return llm_config  # No override in developer mode

    # Force release-mode LLM settings
    return {
        **llm_config,
        "provider": RELEASE_CONFIG["llm_provider"],
        "model": RELEASE_CONFIG["schema_creation_model"] if is_schema_creation else RELEASE_CONFIG["value_extraction_model"],
        "temperature": RELEASE_CONFIG["llm_temperature"],
    }


class ContinueDiscoveryOperation:
    """Tracks a running continue discovery operation."""

    def __init__(
        self,
        operation_id: str,
        session_id: str,
        status: str = "pending"
    ):
        self.operation_id = operation_id
        self.session_id = session_id
        self.status = status  # pending, running, completed, failed, stopped
        self.phase = "discovery"  # discovery, extraction
        self.progress = 0.0
        self.current_batch = 0
        self.total_batches = 0
        self.initial_columns: List[str] = []
        self.new_columns: List[Dict[str, Any]] = []
        self.confirmed_columns: List[str] = []
        self.extraction_rows: List[str] = []
        self.processed_documents = 0
        self.total_documents = 0
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self.document_source: str = "original"
        self.llm_config: Optional[Dict[str, Any]] = None


class ContinueDiscoveryService(WebSocketBroadcasterMixin):
    """Handles continued schema discovery operations."""

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager,
                 data_collection_service=None):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ContinueDiscoveryOperation] = {}
        self.stop_flags: Dict[str, bool] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._state_lock = threading.Lock()
        self._data_collection_service = data_collection_service

    def is_stop_requested(self, operation_id: str) -> bool:
        """Check if stop was requested for an operation."""
        with self._state_lock:
            return self.stop_flags.get(operation_id, False)

    def clear_stop_flag(self, operation_id: str) -> None:
        """Clear the stop flag for an operation."""
        with self._state_lock:
            self.stop_flags.pop(operation_id, None)

    def _cleanup_operation(self, operation_id: str) -> None:
        """Remove operation from tracking dicts to prevent memory leaks."""
        with self._state_lock:
            self.active_operations.pop(operation_id, None)
            self._tasks.pop(operation_id, None)

    def _get_data_dir(self) -> Path:
        """Get the data directory path - uses module location for reliability."""
        # In Docker: /app/backend/app/services/ -> data is at /app/backend/data
        # Locally: /backend/app/services/ -> data is at /backend/app/data (or /backend/data)
        module_dir = Path(__file__).parent  # app/services/
        app_dir = module_dir.parent  # app/
        backend_dir = app_dir.parent  # backend/

        # Check both possible locations
        # 1. Docker/Railway: ./data relative to backend/ (created by FileParser with cwd=backend/)
        docker_data_dir = backend_dir / "data"
        # 2. Local dev: ./app/data (where sessions were created locally)
        local_data_dir = app_dir / "data"

        # Prefer the one that exists and has session data
        if docker_data_dir.exists() and any(docker_data_dir.iterdir()):
            return docker_data_dir
        elif local_data_dir.exists() and any(local_data_dir.iterdir()):
            return local_data_dir
        else:
            # Default to docker location (./data relative to backend/)
            docker_data_dir.mkdir(exist_ok=True)
            return docker_data_dir

    def _get_qbsd_work_dir(self) -> Path:
        """Get the qbsd_work directory path - uses module location for reliability."""
        module_dir = Path(__file__).parent  # app/services/
        app_dir = module_dir.parent  # app/
        backend_dir = app_dir.parent  # backend/
        qbsd_work_dir = backend_dir / "qbsd_work"
        qbsd_work_dir.mkdir(exist_ok=True)
        return qbsd_work_dir

    @staticmethod
    def _is_local_path(path: str) -> bool:
        """
        Check if a path looks like a local filesystem path rather than a cloud storage path.

        Local paths typically look like:
        - /app/backend/data/{uuid}/pending_documents
        - ./data/{uuid}/pending_documents
        - /Users/.../data/...

        Cloud storage paths look like:
        - NES_documents
        - datasets/papers_CoT
        - files
        """
        if not path:
            return False

        # Common indicators of local filesystem paths
        local_indicators = [
            '/app/',           # Docker/Railway container paths
            '/data/',          # Generic data directory
            '/backend/',       # Backend directory
            'pending_documents',  # Upload staging directory
            '/Users/',         # macOS user paths
            '/home/',          # Linux home paths
            'C:\\',            # Windows paths
            'D:\\',            # Windows paths
            './',              # Relative paths
            '../',             # Relative paths
        ]

        for indicator in local_indicators:
            if indicator in path:
                return True

        # Also check if path starts with / and has multiple segments
        # (cloud paths are typically simple folder names like "NES_documents")
        if path.startswith('/') and path.count('/') > 2:
            return True

        return False

    # ==================== Statistics Computation ====================

    def _recompute_statistics(self, session_id: str, preserve_evolution: bool = True) -> None:
        """
        Recompute statistics from data.jsonl after schema changes.

        This properly computes column stats (non_null_count, unique_count, data_type)
        instead of just copying basic column info.

        Args:
            session_id: The session ID
            preserve_evolution: If True, preserve existing schema_evolution data
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            logger.debug(f"Cannot recompute statistics - session {session_id} not found")
            return

        # Debug: print all column names before filtering
        logger.debug(f"session.columns before filtering ({len(session.columns)}): {[c.name for c in session.columns]}")

        # Deduplicate session.columns by name (keep first occurrence)
        # AND filter out _excerpt columns for statistics counting
        seen_names = set()
        unique_columns = []
        non_excerpt_columns = []
        for col in session.columns:
            if col.name and col.name not in seen_names:
                seen_names.add(col.name)
                unique_columns.append(col)
                # Only count non-excerpt columns for statistics
                if not col.name.lower().endswith('_excerpt'):
                    non_excerpt_columns.append(col)
            elif col.name:
                logger.debug(f"Removing duplicate column: {col.name}")

        if len(unique_columns) != len(session.columns):
            logger.debug(f"Deduplicated columns: {len(session.columns)} -> {len(unique_columns)}")
            session.columns = unique_columns
            self.session_manager.update_session(session)

        # Use non-excerpt count for statistics (this is what users care about)
        actual_column_count = len(non_excerpt_columns)
        logger.debug(f"Non-excerpt columns for statistics: {actual_column_count} (total with excerpts: {len(unique_columns)})")

        session_dir = self._get_data_dir() / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            logger.debug(f"Cannot recompute statistics - no data.jsonl found")
            return

        # Read all rows from data file
        data_rows = []
        try:
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data_rows.append(json.loads(line))
        except Exception as e:
            logger.error(f"Error reading data for statistics: {e}")
            return

        if not data_rows:
            logger.debug(f"No data rows found for statistics computation")
            return

        # Preserve existing schema evolution and skipped_documents, but fix any corrupted data
        existing_evolution = None
        existing_skipped = []
        if session.statistics:
            existing_skipped = session.statistics.skipped_documents or []
        if preserve_evolution and session.statistics and session.statistics.schema_evolution:
            existing_evolution = session.statistics.schema_evolution
            actual_total = actual_column_count  # Use deduplicated count

            logger.debug(f"Schema evolution cleanup - actual columns: {actual_total}")
            logger.debug(f"Before cleanup - {len(existing_evolution.snapshots)} snapshots:")
            for i, snap in enumerate(existing_evolution.snapshots):
                # Handle both dict and object formats
                if isinstance(snap, dict):
                    logger.debug(f"  Snapshot {i} (dict): iteration={snap.get('iteration')}, total_columns={snap.get('total_columns')}, new_columns={snap.get('new_columns')}")
                else:
                    logger.debug(f"  Snapshot {i} (obj): iteration={snap.iteration}, total_columns={snap.total_columns}, new_columns={snap.new_columns}")

            # Helper to get/set snapshot attributes (handles both dict and object)
            def get_snap_attr(snap, attr):
                if isinstance(snap, dict):
                    return snap.get(attr)
                return getattr(snap, attr, None)

            def set_snap_attr(snap, attr, value):
                if isinstance(snap, dict):
                    snap[attr] = value
                else:
                    setattr(snap, attr, value)

            # Remove duplicate snapshots (same iteration number)
            if existing_evolution.snapshots:
                seen_iterations = set()
                unique_snapshots = []
                for snapshot in existing_evolution.snapshots:
                    iteration = get_snap_attr(snapshot, 'iteration')
                    if iteration not in seen_iterations:
                        seen_iterations.add(iteration)
                        unique_snapshots.append(snapshot)
                    else:
                        logger.debug(f"Removing duplicate snapshot for iteration {iteration}")
                if len(unique_snapshots) != len(existing_evolution.snapshots):
                    logger.debug(f"Removed {len(existing_evolution.snapshots) - len(unique_snapshots)} duplicate snapshots")
                    existing_evolution.snapshots = unique_snapshots

                # Fix all snapshots to ensure total_columns doesn't exceed actual column count
                for snapshot in existing_evolution.snapshots:
                    total_cols = get_snap_attr(snapshot, 'total_columns')
                    iteration = get_snap_attr(snapshot, 'iteration')
                    if total_cols and total_cols > actual_total:
                        logger.debug(f"Fixing snapshot {iteration} total_columns: {total_cols} -> {actual_total}")
                        set_snap_attr(snapshot, 'total_columns', actual_total)

            logger.debug(f"After cleanup - {len(existing_evolution.snapshots)} snapshots")

        # Helper function to check if a value is valid (non-null)
        def is_valid_value(value):
            if value is None:
                return False
            if isinstance(value, dict):
                answer = value.get("answer")
                if answer is None or answer == "None" or answer == "" or answer == "[]":
                    return False
                if isinstance(answer, str) and answer.strip().lower() in ["not found", "n/a", "none", "unknown"]:
                    return False
                return True
            if isinstance(value, str) and value.strip().lower() in ["not found", "n/a", "none", "unknown", ""]:
                return False
            return value != "None" and value != "" and value != "[]"

        # Compute statistics for each column (excluding _excerpt columns)
        columns = []
        for col in non_excerpt_columns:
            non_null_count = 0
            unique_values = set()

            for row in data_rows:
                # Handle both DataRow format (with 'data' key) and direct format
                row_data = row.get('data', row)

                if col.name in row_data:
                    value = row_data[col.name]
                    if is_valid_value(value):
                        non_null_count += 1
                    # Count unique values
                    try:
                        unique_values.add(json.dumps(value, sort_keys=True))
                    except (TypeError, ValueError):
                        unique_values.add(str(value))

            unique_count = len(unique_values)

            col_info = ColumnInfo(
                name=col.name,
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",
                non_null_count=non_null_count,
                unique_count=unique_count,
                source_document=col.source_document,
                discovery_iteration=col.discovery_iteration,
                allowed_values=col.allowed_values,
                auto_expand_threshold=col.auto_expand_threshold
            )
            columns.append(col_info)

        # Calculate overall completeness
        total_cells = len(data_rows) * len(columns)
        non_null_cells = sum(col.non_null_count or 0 for col in columns)
        completeness = (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0

        if math.isnan(completeness) or math.isinf(completeness):
            completeness = 0.0

        # Count unique documents from papers field
        unique_documents = set()
        for row in data_rows:
            papers = row.get('papers', row.get('_papers', []))
            if isinstance(papers, list):
                unique_documents.update(papers)
            elif isinstance(papers, str) and papers:
                unique_documents.add(papers)
        total_documents = len(unique_documents) if unique_documents else len(data_rows)

        # Import model for type checking
        from app.models.session import DataStatistics

        # Create or update statistics
        session.statistics = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            total_documents=total_documents,
            completeness=completeness,
            column_stats=columns,
            schema_evolution=existing_evolution,  # Preserve existing evolution
            skipped_documents=existing_skipped  # Preserve skipped documents
        )

        self.session_manager.update_session(session)
        logger.info(f"Statistics recomputed - {len(data_rows)} rows, {total_documents} documents, {len(columns)} columns, {completeness:.1f}% complete")

    # ==================== Document Discovery ====================

    async def get_available_documents(self, session_id: str) -> Dict[str, Any]:
        """
        Get available document sources for continued discovery.
        Works with both local storage and Supabase cloud storage.

        Returns:
            Dictionary with:
            - original_documents: Documents from original QBSD run or data.jsonl references
            - cloud_datasets: Available cloud datasets
            - can_use_original: Whether original documents are available
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            return {
                "original_documents": [],
                "cloud_datasets": [],
                "can_use_original": False,
                "error": "Session not found"
            }

        storage = get_storage()
        all_papers: Set[str] = set()
        paper_doc_dirs: Dict[str, str] = {}  # paper_name -> document_directory

        logger.debug(f"get_available_documents: session_id={session_id}")
        logger.debug(f"storage type = {type(storage).__name__}")
        logger.debug(f"cloud_dataset from session = {session.metadata.cloud_dataset if session.metadata else None}")

        # 1. Get data.jsonl content - try Supabase first, then local
        data_content = None
        try:
            # Try to download from Supabase 'data' bucket
            logger.debug(f"Attempting Supabase download: data/{session_id}/data.jsonl")
            data_bytes = await storage.download_file('data', f'{session_id}/data.jsonl')
            if data_bytes:
                data_content = data_bytes.decode('utf-8')
                logger.debug(f"Downloaded data.jsonl from Supabase, size={len(data_content)} bytes")
        except Exception as e:
            logger.debug(f"Supabase download failed: {type(e).__name__}: {e}")

        # Fallback to local file if Supabase didn't work
        if not data_content:
            data_dir = self._get_data_dir()
            logger.debug(f"Local data_dir = {data_dir}")
            session_dir = data_dir / session_id
            data_file = session_dir / "data.jsonl"
            logger.debug(f"Checking local file: {data_file}, exists={data_file.exists()}")
            if data_file.exists():
                data_content = data_file.read_text()
                logger.debug(f"Read data.jsonl from local, size={len(data_content)} bytes")
            else:
                # List what's in the data_dir
                if data_dir.exists():
                    sessions_in_dir = list(data_dir.iterdir())[:5]
                    logger.debug(f"data_dir exists, sample contents: {[s.name for s in sessions_in_dir]}")
                else:
                    logger.debug(f"data_dir does not exist: {data_dir}")

        # 2. Parse data.jsonl to collect paper references
        if data_content:
            for line in data_content.strip().split('\n'):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    papers_raw = (
                        row.get('papers') or
                        row.get('_papers') or
                        row.get('Papers') or
                        row.get('data', {}).get('Papers') or
                        row.get('data', {}).get('papers') or
                        []
                    )
                    # Handle QBSD answer format
                    if isinstance(papers_raw, dict) and 'answer' in papers_raw:
                        papers_raw = papers_raw.get('answer', [])
                    if isinstance(papers_raw, str):
                        papers_raw = [papers_raw] if papers_raw else []
                    for paper in papers_raw:
                        if paper:
                            all_papers.add(paper)

                    # Get document directory for cloud lookup
                    doc_dir = (
                        row.get('Document Directory') or
                        row.get('document_directory') or
                        row.get('data', {}).get('Document Directory') or
                        row.get('data', {}).get('document_directory')
                    )
                    if isinstance(doc_dir, dict) and 'answer' in doc_dir:
                        doc_dir = doc_dir.get('answer')
                    if doc_dir:
                        for paper in papers_raw:
                            if paper and paper not in paper_doc_dirs:
                                paper_doc_dirs[paper] = doc_dir
                except json.JSONDecodeError:
                    continue

        logger.debug(f"Found {len(all_papers)} paper references in data.jsonl")

        # 3. Check local documents
        local_docs: Set[str] = set()
        session_dir = self._get_data_dir() / session_id
        docs_dir = session_dir / "documents"
        if docs_dir.exists():
            for f in docs_dir.iterdir():
                if f.is_file() and not f.name.startswith('.'):
                    local_docs.add(f.name)

        # Also check qbsd_work
        qbsd_work_dir = self._get_qbsd_work_dir() / session_id
        if qbsd_work_dir.exists():
            for subdir in qbsd_work_dir.iterdir():
                if subdir.is_dir() and not subdir.name.startswith('.'):
                    for f in subdir.iterdir():
                        if f.is_file() and f.suffix in ['.txt', '.md']:
                            local_docs.add(f.name)

        # 4. Get cloud dataset from session metadata
        cloud_dataset = session.metadata.cloud_dataset if session.metadata else None

        # 5. Check cloud storage for papers (batch by folder like reextraction_service)
        cloud_docs: Set[str] = set()
        papers_to_check_cloud = all_papers - local_docs

        if papers_to_check_cloud:
            # Group papers by their document directory
            folders_to_check: Dict[str, List[str]] = {}
            for paper in papers_to_check_cloud:
                doc_dir = paper_doc_dirs.get(paper)

                # If doc_dir is a local path, try to use cloud_dataset as fallback
                if doc_dir and self._is_local_path(doc_dir):
                    logger.debug(f"Detected local path for paper {paper}: {doc_dir}")
                    if cloud_dataset:
                        doc_dir = f"datasets/{cloud_dataset}"
                        logger.debug(f"Using cloud_dataset fallback: {doc_dir}")
                    else:
                        logger.debug(f"No cloud_dataset fallback - skipping paper {paper}")
                        continue

                # If no doc_dir, try cloud_dataset as fallback
                if not doc_dir and cloud_dataset:
                    doc_dir = f"datasets/{cloud_dataset}"

                if doc_dir:
                    clean_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
                    if clean_dir not in folders_to_check:
                        folders_to_check[clean_dir] = []
                    folders_to_check[clean_dir].append(paper)

            # List each folder once
            for folder, papers in folders_to_check.items():
                try:
                    folder_files = await storage.list_folder_files('datasets', folder)
                    logger.debug(f"Found {len(folder_files)} files in datasets/{folder}")
                    for paper in papers:
                        if paper in folder_files or f"{paper}.txt" in folder_files:
                            cloud_docs.add(paper)
                except Exception as e:
                    logger.debug(f"Could not list folder {folder}: {e}")

        # 6. Combine results
        available_docs = local_docs | cloud_docs
        logger.debug(f"Available docs: {len(local_docs)} local + {len(cloud_docs)} cloud = {len(available_docs)} total")

        # 7. Get list of all available cloud datasets
        cloud_datasets = []
        try:
            dataset_infos = await storage.list_datasets()
            cloud_datasets = [d.name for d in dataset_infos]
            logger.debug(f"Found {len(cloud_datasets)} cloud datasets via storage.list_datasets()")
        except Exception as e:
            logger.debug(f"Could not list cloud datasets: {e}")

        return {
            "original_documents": sorted(list(available_docs)),
            "original_count": len(available_docs),
            "local_count": len(local_docs),
            "cloud_count": len(cloud_docs),
            "cloud_datasets": cloud_datasets,
            "original_cloud_dataset": cloud_dataset,
            "can_use_original": len(available_docs) > 0,
            "query": session.schema_query or ""
        }

    async def _prepare_documents(
        self,
        session_id: str,
        document_source: str,
        cloud_dataset: Optional[str] = None,
        uploaded_files: Optional[List[str]] = None,
        bypass_limit: bool = False
    ) -> tuple[Path, List[str], List[str]]:
        """
        Prepare documents for schema discovery.

        Args:
            session_id: Session identifier
            document_source: 'original', 'upload', or 'cloud'
            cloud_dataset: Cloud dataset name (if document_source is 'cloud')
            uploaded_files: List of uploaded filenames (if document_source is 'upload')
            bypass_limit: Developer mode flag to bypass document limit

        Returns:
            Tuple of (docs_directory, document_contents, filenames)
        """
        # Use storage backend's directories for correct path resolution
        session_dir = self._get_data_dir() / session_id
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)

        documents = []
        filenames = []

        if document_source == "original":
            # Use existing documents
            qbsd_work_dir = self._get_qbsd_work_dir() / session_id

            # Check data/{session_id}/documents/ first
            if docs_dir.exists():
                for f in sorted(docs_dir.iterdir()):
                    if f.is_file() and f.suffix in ['.txt', '.md'] and not f.name.startswith('.'):
                        try:
                            content = f.read_text(encoding='utf-8')
                            documents.append(content)
                            filenames.append(f.name)
                        except Exception as e:
                            logger.debug(f"Could not read {f}: {e}")

            # Also check qbsd_work directory
            if not documents and qbsd_work_dir.exists():
                for subdir in qbsd_work_dir.iterdir():
                    if subdir.is_dir() and not subdir.name.startswith('.'):
                        for f in sorted(subdir.iterdir()):
                            if f.is_file() and f.suffix in ['.txt', '.md']:
                                try:
                                    content = f.read_text(encoding='utf-8')
                                    documents.append(content)
                                    filenames.append(f.name)
                                except Exception as e:
                                    logger.debug(f"Could not read {f}: {e}")

        elif document_source == "cloud":
            # Download from cloud storage
            if not cloud_dataset:
                raise ValueError("cloud_dataset required for cloud document source")

            storage = get_storage()
            try:
                files = await storage.list_files('datasets', cloud_dataset)
                for file_info in files:
                    if not file_info.get('is_folder', False):
                        file_path = f"{cloud_dataset}/{file_info['name']}"
                        content = await storage.download_file('datasets', file_path)
                        if content:
                            # Save locally
                            local_path = docs_dir / file_info['name']
                            local_path.write_bytes(content)

                            # Add to documents list
                            try:
                                text_content = content.decode('utf-8')
                                documents.append(text_content)
                                filenames.append(file_info['name'])
                            except UnicodeDecodeError:
                                logger.debug(f"Could not decode {file_info['name']} as UTF-8")
            except Exception as e:
                logger.error(f"Error downloading cloud documents: {e}")
                raise

        elif document_source == "upload":
            # Use uploaded files from pending_documents
            pending_dir = session_dir / "pending_documents"
            if pending_dir.exists():
                for f in sorted(pending_dir.iterdir()):
                    if f.is_file() and not f.name.startswith('.'):
                        try:
                            content = f.read_text(encoding='utf-8')
                            documents.append(content)
                            filenames.append(f.name)
                            # Copy to documents dir
                            (docs_dir / f.name).write_text(content, encoding='utf-8')
                        except Exception as e:
                            logger.debug(f"Could not read {f}: {e}")

        # Enforce document limit (same as initial QBSD creation)
        # The limit can be bypassed in developer mode via config
        if not (DEVELOPER_MODE and bypass_limit) and len(documents) > MAX_DOCUMENTS:
            import random
            original_count = len(documents)
            combined = list(zip(documents, filenames))
            rng = random.Random(42)  # deterministic sampling for reproducibility
            rng.shuffle(combined)
            combined = combined[:MAX_DOCUMENTS]
            documents, filenames = zip(*combined) if combined else ([], [])
            documents, filenames = list(documents), list(filenames)
            logger.info(f"Document limit applied: {original_count} → {len(documents)} (max: {MAX_DOCUMENTS})")

        logger.info(f"Prepared {len(documents)} documents from {document_source}")
        return docs_dir, documents, filenames

    # ==================== Schema Discovery ====================

    def _convert_session_columns_to_schema(self, columns: List[ColumnInfo], query: str) -> Schema:
        """Convert session columns to QBSD Schema object."""
        qbsd_columns = []
        for col in columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                qbsd_col = Column(
                    name=col.name,
                    definition=col.definition or "",
                    rationale=col.rationale or "",
                    allowed_values=col.allowed_values
                )
                qbsd_columns.append(qbsd_col)

        return Schema(query=query, columns=qbsd_columns, max_keys=100)

    def _identify_new_columns(
        self,
        initial_columns: List[str],
        result_schema: Schema
    ) -> List[Dict[str, Any]]:
        """
        Compare result schema to initial columns and identify NEW columns.

        Returns:
            List of new column dicts with name, definition, rationale, etc.
        """
        initial_names_lower = {name.lower() for name in initial_columns}
        new_columns = []

        for col in result_schema.columns:
            if col.name.lower() not in initial_names_lower:
                new_columns.append({
                    "name": col.name,
                    "definition": col.definition or "",
                    "rationale": col.rationale or "",
                    "allowed_values": col.allowed_values,
                    "source_document": getattr(col, 'source_document', None),
                    "discovery_iteration": getattr(col, 'discovery_iteration', None)
                })

        return new_columns

    async def start_continue_discovery(
        self,
        session_id: str,
        document_source: str,
        llm_config: Dict[str, Any],
        cloud_dataset: Optional[str] = None,
        retriever_config: Optional[Dict[str, Any]] = None,
        max_keys_schema: int = 100,
        documents_batch_size: int = 1,
        bypass_limit: bool = False
    ) -> Dict[str, Any]:
        """
        Start schema discovery continuation.

        Args:
            session_id: Session identifier
            document_source: 'original', 'upload', or 'cloud'
            llm_config: LLM configuration with provider, model, api_key
            cloud_dataset: Cloud dataset name (if using cloud documents)
            retriever_config: Retriever configuration (None = use defaults)
            max_keys_schema: Maximum schema columns
            documents_batch_size: Documents per batch
            bypass_limit: Developer mode flag to bypass document limit

        Returns:
            Dictionary with operation details
        """
        if not QBSD_AVAILABLE:
            raise RuntimeError("QBSD components not available")

        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Create operation
        operation_id = str(uuid.uuid4())[:8]
        operation = ContinueDiscoveryOperation(
            operation_id=operation_id,
            session_id=session_id,
            status="starting"
        )
        operation.initial_columns = [col.name for col in session.columns if col.name]
        operation.document_source = document_source
        operation.llm_config = llm_config
        with self._state_lock:
            self.active_operations[operation_id] = operation

        # Save LLM config for later use
        session_dir = self._get_data_dir() / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        llm_config_file = session_dir / f"continue_discovery_llm_{operation_id}.json"
        with open(llm_config_file, 'w') as f:
            json.dump(llm_config, f, indent=2)

        # Store additional config for the background task
        config = {
            "document_source": document_source,
            "cloud_dataset": cloud_dataset,
            "retriever_config": retriever_config,
            "max_keys_schema": max_keys_schema,
            "documents_batch_size": documents_batch_size,
            "query": session.schema_query or "",
            "bypass_limit": bypass_limit
        }
        config_file = session_dir / f"continue_discovery_config_{operation_id}.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        # Start background task
        task = asyncio.create_task(self._run_continue_discovery(operation_id))
        with self._state_lock:
            self._tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "initial_column_count": len(operation.initial_columns),
            "document_source": document_source
        }

    async def _run_continue_discovery(self, operation_id: str):
        """Execute continued schema discovery in background."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            logger.debug(f"Operation {operation_id} not found")
            return

        # Set session context for logging
        set_session_context(operation.session_id)
        LLMCallTracker.get_instance().set_stage("continue_discovery")

        logger.info(f"_run_continue_discovery started for operation {operation_id}")

        try:
            operation.status = "running"
            operation.phase = "discovery"
            operation.started_at = datetime.now()

            session = self.session_manager.get_session(operation.session_id)
            if not session:
                raise ValueError(f"Session {operation.session_id} not found")

            session_dir = self._get_data_dir() / operation.session_id

            # Load config
            config_file = session_dir / f"continue_discovery_config_{operation_id}.json"
            with open(config_file) as f:
                config = json.load(f)

            # Load LLM config
            llm_config_file = session_dir / f"continue_discovery_llm_{operation_id}.json"
            with open(llm_config_file) as f:
                llm_config = json.load(f)

            # Broadcast start
            await self.broadcast_event(
                operation.session_id,
                "continue_discovery_started",
                {
                    "operation_id": operation_id,
                    "initial_columns": operation.initial_columns,
                    "document_source": operation.document_source
                }
            )

            # Prepare documents
            logger.info(f"Preparing documents from {config['document_source']}")
            docs_dir, documents, filenames = await self._prepare_documents(
                operation.session_id,
                config["document_source"],
                config.get("cloud_dataset"),
                bypass_limit=config.get("bypass_limit", False)
            )

            operation.total_documents = len(documents)
            logger.info(f"Prepared {len(documents)} documents")

            if not documents:
                raise ValueError("No documents available for schema discovery")

            # Build initial schema from session columns
            query = config.get("query") or session.schema_query or ""
            initial_schema = self._convert_session_columns_to_schema(session.columns, query)
            logger.debug(f"Initial schema has {len(initial_schema.columns)} columns")

            # Build LLM - enforce release mode settings if applicable
            enforced_llm_config = _enforce_release_llm_config(llm_config, is_schema_creation=True)
            llm = qbsd_utils.build_llm(enforced_llm_config)

            # Build retriever - use config if provided, otherwise use library defaults
            retriever_cfg = config.get("retriever_config")
            if retriever_cfg:
                retriever = EmbeddingRetriever(
                    model_name=retriever_cfg.get("model_name", "all-MiniLM-L6-v2"),
                    k=retriever_cfg.get("k", 15),
                    max_words=retriever_cfg.get("passage_chars", 512),
                    enable_dynamic_k=retriever_cfg.get("enable_dynamic_k", True),
                    dynamic_k_threshold=retriever_cfg.get("dynamic_k_threshold", 0.65),
                    dynamic_k_minimum=retriever_cfg.get("dynamic_k_minimum", 3)
                )
            else:
                # No config provided - no retriever
                retriever = None

            # Calculate batches
            batch_size = config.get("documents_batch_size", 1)
            operation.total_batches = math.ceil(len(documents) / batch_size)

            # Broadcast discovery starting
            await self.broadcast_event(
                operation.session_id,
                "continue_discovery_progress",
                {
                    "operation_id": operation_id,
                    "phase": "discovery",
                    "progress": 0.1,
                    "message": f"Starting schema discovery with {len(documents)} documents...",
                    "total_documents": len(documents),
                    "initial_columns": len(operation.initial_columns)
                }
            )

            # Manual iteration loop for schema discovery (allows stop between batches)
            logger.info(f"Starting manual schema discovery loop with initial_schema")

            # Create document batches
            batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
            filename_batches = [filenames[i:i+batch_size] for i in range(0, len(filenames), batch_size)]

            # Update operation with actual batch count
            operation.total_batches = len(batches)

            # Initialize tracking
            current_schema = initial_schema
            context_window_size = llm_config.get("context_window_size") or getattr(llm, 'context_window_size', 8192)
            convergence_threshold = 2
            unchanged_count = 0
            evolution = SchemaEvolution()
            cumulative_docs = 0
            stopped = False

            for iteration, (batch_docs, batch_names) in enumerate(zip(batches, filename_batches)):
                # CHECK STOP FLAG BEFORE EACH ITERATION
                if self.is_stop_requested(operation_id):
                    logger.info(f"Stop requested during schema discovery at iteration {iteration}")
                    stopped = True
                    operation.status = "stopped"
                    operation.completed_at = datetime.now()
                    await self.broadcast_event(
                        operation.session_id,
                        "continue_discovery_stopped",
                        {"operation_id": operation_id, "message": "Stopped by user during discovery"}
                    )
                    return

                # Update progress
                operation.current_batch = iteration + 1
                progress = (iteration + 1) / len(batches)

                await self.broadcast_event(
                    operation.session_id,
                    "continue_discovery_progress",
                    {
                        "operation_id": operation_id,
                        "phase": "discovery",
                        "iteration": iteration + 1,
                        "max_iterations": len(batches),
                        "progress": progress,
                        "message": f"Processing batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs)...",
                        "current_columns": len(current_schema.columns)
                    }
                )

                logger.debug(f"Schema discovery batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs: {batch_names})")

                # Track column names before this iteration
                columns_before = {col.name.lower() for col in current_schema.columns}
                cumulative_docs += len(batch_docs)

                # Select relevant content from this batch's documents (offloaded to thread pool)
                loop = asyncio.get_running_loop()
                relevant_content = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(QBSD.select_relevant_content, docs=batch_docs, query=query, retriever=retriever),
                )
                logger.debug(f"Selected {len(relevant_content)} relevant passages from batch")

                # Generate schema for this batch (offloaded to thread pool)
                try:
                    schema_result = await loop.run_in_executor(
                        qbsd_thread_pool,
                        functools.partial(
                            QBSD.generate_schema,
                            passages=relevant_content,
                            query=query,
                            max_keys_schema=config.get("max_keys_schema", 100),
                            current_schema=current_schema,
                            llm=llm,
                            context_window_size=context_window_size,
                        ),
                    )
                    # generate_schema returns a tuple (Schema, bool)
                    new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
                    logger.debug(f"Generated schema with {len(new_schema.columns)} columns")
                except Exception as e:
                    logger.error(f"ERROR in generate_schema: {e}")
                    raise

                # Merge with existing schema (offloaded to thread pool)
                merged_schema = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(current_schema.merge, new_schema),
                )
                logger.debug(f"Merged schema has {len(merged_schema.columns)} columns")

                # Identify NEW columns added in this iteration
                columns_after = {col.name.lower() for col in merged_schema.columns}
                new_column_names_lower = columns_after - columns_before
                new_columns_in_batch = [col.name for col in merged_schema.columns if col.name.lower() in new_column_names_lower]

                # Record column sources
                batch_source = ", ".join(batch_names) if batch_names else f"batch_{iteration + 1}"
                for col_name in new_columns_in_batch:
                    if col_name not in evolution.column_sources:
                        evolution.column_sources[col_name] = batch_source

                # Add snapshot to evolution
                evolution.snapshots.append(SchemaSnapshot(
                    iteration=iteration + 1,
                    documents_processed=batch_names,
                    total_columns=len(merged_schema.columns),
                    new_columns=new_columns_in_batch,
                    cumulative_documents=cumulative_docs
                ))

                # Check convergence (offloaded to thread pool)
                converged = await loop.run_in_executor(
                    qbsd_thread_pool,
                    functools.partial(QBSD.evaluate_schema_convergence, current_schema, merged_schema),
                )
                if converged:
                    unchanged_count += 1
                    logger.debug(f"Schema unchanged (count: {unchanged_count}/{convergence_threshold})")
                    if unchanged_count >= convergence_threshold:
                        logger.info(f"Schema converged after {iteration + 1} batches")
                        break
                else:
                    unchanged_count = 0

                current_schema = merged_schema

                # Small delay to allow other tasks
                await asyncio.sleep(0.1)

            result_schema = current_schema
            logger.info(f"Schema discovery completed with {len(result_schema.columns)} columns after {len(evolution.snapshots)} batches")

            # Identify new columns
            new_columns = self._identify_new_columns(operation.initial_columns, result_schema)
            operation.new_columns = new_columns
            logger.info(f"Discovered {len(new_columns)} new columns")

            # Add new columns to session immediately after discovery
            # So they appear in Schema tab even without extraction
            session = self.session_manager.get_session(operation.session_id)
            if session and new_columns:
                # Use alias to avoid conflict with qbsd.core.schema.SchemaEvolution
                from app.models.session import SchemaEvolution as SessionSchemaEvolution, SchemaSnapshot as SessionSchemaSnapshot

                # First, deduplicate existing session.columns
                seen_names = set()
                unique_cols = []
                for col in session.columns:
                    if col.name and col.name not in seen_names:
                        seen_names.add(col.name)
                        unique_cols.append(col)
                if len(unique_cols) != len(session.columns):
                    logger.debug(f"Deduplicated existing columns: {len(session.columns)} -> {len(unique_cols)}")
                    session.columns = unique_cols

                for col_data in new_columns:
                    new_col = ColumnInfo(
                        name=col_data["name"],
                        definition=col_data.get("definition", ""),
                        rationale=col_data.get("rationale", ""),
                        allowed_values=col_data.get("allowed_values"),
                        source_document=col_data.get("source_document"),
                        discovery_iteration=col_data.get("discovery_iteration")
                    )
                    # Only add if not already present
                    if not any(c.name == new_col.name for c in session.columns):
                        session.columns.append(new_col)

                # Count only non-excerpt columns for statistics (what users care about)
                non_excerpt_count = sum(1 for c in session.columns if c.name and not c.name.lower().endswith('_excerpt'))
                actual_unique_count = non_excerpt_count
                logger.debug(f"Column count after adding new columns: {actual_unique_count} (non-excerpt), {len(session.columns)} (total with excerpts)")

                # Add null values for new columns in data.jsonl
                data_file = self._get_data_dir() / operation.session_id / "data.jsonl"
                if data_file.exists():
                    rows = []
                    with open(data_file, 'r') as f:
                        for line in f:
                            if line.strip():
                                row = json.loads(line)
                                for col_data in new_columns:
                                    col_name = col_data["name"]
                                    if col_name not in row:
                                        row[col_name] = None
                                rows.append(row)
                    with open(data_file, 'w') as f:
                        for row in rows:
                            f.write(json.dumps(row) + '\n')
                    logger.info(f"Added null values for {len(new_columns)} new columns in data.jsonl")

                # Update schema_evolution for Statistics chart
                if session.statistics:
                    if not session.statistics.schema_evolution:
                        session.statistics.schema_evolution = SessionSchemaEvolution(
                            snapshots=[],
                            column_sources={}
                        )

                    stats_evolution = session.statistics.schema_evolution
                    next_iteration = len(stats_evolution.snapshots) + 1

                    # Get document names that were used for discovery
                    docs_dir = self._get_data_dir() / operation.session_id / "documents"
                    doc_names = [f.name for f in docs_dir.glob("*") if f.is_file()][:10] if docs_dir.exists() else []

                    # Filter out any excerpt columns from new_columns list
                    non_excerpt_new_cols = [col["name"] for col in new_columns if not col["name"].lower().endswith('_excerpt')]

                    new_snapshot = SessionSchemaSnapshot(
                        iteration=next_iteration,
                        documents_processed=doc_names,
                        total_columns=actual_unique_count,  # Use non-excerpt count
                        new_columns=non_excerpt_new_cols,
                        cumulative_documents=operation.total_batches
                    )
                    stats_evolution.snapshots.append(new_snapshot)
                    logger.debug(f"Created snapshot with total_columns={actual_unique_count}, new_columns={len(non_excerpt_new_cols)}")

                    # Update column sources with actual document name (not generic iteration)
                    for col_data in new_columns:
                        if col_data["name"] not in stats_evolution.column_sources:
                            # Use source_document if available, otherwise use first doc or iteration name
                            source = col_data.get("source_document")
                            if not source and doc_names:
                                source = doc_names[0]
                            if not source:
                                source = f"continue_discovery_iteration_{next_iteration}"
                            stats_evolution.column_sources[col_data["name"]] = source

                    session.statistics.total_columns = actual_unique_count  # Use deduplicated count

                self.session_manager.update_session(session)
                logger.info(f"Added {len(new_columns)} new columns to session after discovery")

            # Ensure session status is 'completed' so Data tab is enabled (always, even if no new columns)
            session = self.session_manager.get_session(operation.session_id)
            if session:
                session.status = "completed"
                self.session_manager.update_session(session)
                logger.info(f"Set session status to 'completed' after discovery")

            # Recompute statistics with proper column stats (non_null_count, unique_count, etc.)
            self._recompute_statistics(operation.session_id, preserve_evolution=True)
            logger.info(f"Statistics recomputed after discovery phase")

            # Complete discovery phase
            operation.status = "completed"
            operation.phase = "discovery"
            operation.progress = 1.0
            operation.completed_at = datetime.now()

            # Broadcast completion with new columns
            await self.broadcast_event(
                operation.session_id,
                "continue_discovery_completed",
                {
                    "operation_id": operation_id,
                    "initial_columns": operation.initial_columns,
                    "new_columns": new_columns,
                    "total_columns": len(result_schema.columns),
                    "message": f"Discovered {len(new_columns)} new columns" if new_columns else "No new columns discovered"
                }
            )

            # Archive session data for research (fire-and-forget)
            if self._data_collection_service:
                await self._data_collection_service.trigger_archive(
                    operation.session_id, "continue_discovery_completion"
                )

            # Cleanup config files
            config_file.unlink(missing_ok=True)
            llm_config_file.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Continue discovery FAILED: {e}", exc_info=True)

            operation.status = "failed"
            operation.error = str(e)
            operation.completed_at = datetime.now()

            await self.broadcast_event(
                operation.session_id,
                "continue_discovery_failed",
                {
                    "operation_id": operation_id,
                    "error": str(e)
                }
            )
        finally:
            await concurrency_limiter.release(operation.session_id)
            self._cleanup_operation(operation_id)

    # ==================== Incremental Extraction ====================

    async def confirm_and_start_extraction(
        self,
        operation_id: str,
        selected_columns: List[str],
        row_selection: str,
        selected_rows: Optional[List[str]] = None,
        llm_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Confirm new columns and start incremental value extraction.

        Args:
            operation_id: The discovery operation ID
            selected_columns: List of new column names to add and extract
            row_selection: 'all' or 'selected'
            selected_rows: List of row names if row_selection is 'selected'
            llm_config: LLM configuration (optional, uses discovery config if not provided)

        Returns:
            Dictionary with extraction status
        """
        operation = self.active_operations.get(operation_id)
        if not operation:
            raise ValueError(f"Operation {operation_id} not found")

        if operation.status != "completed" or operation.phase != "discovery":
            raise ValueError(f"Operation not ready for extraction (status={operation.status}, phase={operation.phase})")

        session = self.session_manager.get_session(operation.session_id)
        if not session:
            raise ValueError(f"Session {operation.session_id} not found")

        # Filter selected columns from discovered new columns
        new_columns_to_add = [
            col for col in operation.new_columns
            if col["name"] in selected_columns
        ]

        if not new_columns_to_add:
            return {
                "status": "no_columns",
                "message": "No columns selected for extraction"
            }

        # Add new columns to session
        for col_data in new_columns_to_add:
            new_col = ColumnInfo(
                name=col_data["name"],
                definition=col_data.get("definition", ""),
                rationale=col_data.get("rationale", ""),
                allowed_values=col_data.get("allowed_values"),
                source_document=col_data.get("source_document"),
                discovery_iteration=col_data.get("discovery_iteration")
            )
            session.columns.append(new_col)

        self.session_manager.update_session(session)
        logger.info(f"Added {len(new_columns_to_add)} new columns to session")

        # Determine rows to process
        if row_selection == "all":
            rows_to_process = None  # Will process all rows
        else:
            rows_to_process = selected_rows or []

        # Update operation for extraction phase
        operation.confirmed_columns = selected_columns
        operation.extraction_rows = rows_to_process or []
        operation.status = "running"
        operation.phase = "extraction"
        operation.progress = 0.0

        # Save extraction config
        session_dir = self._get_data_dir() / operation.session_id
        extraction_config = {
            "columns": selected_columns,
            "row_selection": row_selection,
            "selected_rows": selected_rows,
            "llm_config": llm_config or operation.llm_config
        }
        extraction_config_file = session_dir / f"extraction_config_{operation_id}.json"
        with open(extraction_config_file, 'w') as f:
            json.dump(extraction_config, f, indent=2)

        # Start extraction in background
        task = asyncio.create_task(self._run_incremental_extraction(operation_id))
        with self._state_lock:
            self._tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "columns": selected_columns,
            "row_count": len(rows_to_process) if rows_to_process else "all"
        }

    async def _run_incremental_extraction(self, operation_id: str):
        """Execute incremental value extraction for new columns."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return

        # Set session context for logging
        set_session_context(operation.session_id)

        logger.info(f"_run_incremental_extraction started for operation {operation_id}")

        try:
            session = self.session_manager.get_session(operation.session_id)
            if not session:
                raise ValueError(f"Session {operation.session_id} not found")

            session_dir = self._get_data_dir() / operation.session_id
            docs_dir = session_dir / "documents"

            # Load extraction config
            extraction_config_file = session_dir / f"extraction_config_{operation_id}.json"
            with open(extraction_config_file) as f:
                extraction_config = json.load(f)

            columns_to_extract = extraction_config["columns"]
            llm_config = extraction_config.get("llm_config") or operation.llm_config

            # Load original discovery config for retriever settings
            discovery_config_file = session_dir / f"continue_discovery_config_{operation_id}.json"
            discovery_config = {}
            if discovery_config_file.exists():
                with open(discovery_config_file) as f:
                    discovery_config = json.load(f)
            retriever_cfg = discovery_config.get("retriever_config")

            # Broadcast extraction start
            await self.broadcast_event(
                operation.session_id,
                "incremental_extraction_started",
                {
                    "operation_id": operation_id,
                    "columns": columns_to_extract
                }
            )

            # Get target columns from session
            target_columns = [
                col for col in session.columns
                if col.name in columns_to_extract
            ]

            # Build schema for extraction (only new columns)
            schema_data = {
                "query": session.schema_query or "Extract information",
                "schema": [
                    {
                        "column": col.name,
                        "definition": col.definition or f"Data field: {col.name}",
                        "explanation": col.rationale or f"Information for {col.name}",
                        "allowed_values": col.allowed_values
                    }
                    for col in target_columns
                ]
            }

            # Save schema file
            schema_file = session_dir / f"incremental_schema_{operation_id}.json"
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)

            # Setup LLM and retriever - use config if provided, otherwise use library defaults
            # Enforce release mode settings if applicable (value extraction)
            enforced_llm_config = _enforce_release_llm_config(llm_config, is_schema_creation=False)
            llm = qbsd_utils.build_llm(enforced_llm_config)
            if retriever_cfg:
                retriever = EmbeddingRetriever(
                    model_name=retriever_cfg.get("model_name", "all-MiniLM-L6-v2"),
                    k=retriever_cfg.get("k", 15),
                    max_words=retriever_cfg.get("passage_chars", 512),
                    enable_dynamic_k=retriever_cfg.get("enable_dynamic_k", True),
                    dynamic_k_threshold=retriever_cfg.get("dynamic_k_threshold", 0.65),
                    dynamic_k_minimum=retriever_cfg.get("dynamic_k_minimum", 3)
                )
            else:
                # No config provided - no retriever
                retriever = None

            output_file = session_dir / f"incremental_output_{operation_id}.jsonl"

            # Get existing row names from data.jsonl - only extract for existing rows
            existing_rows = set()
            data_file = session_dir / "data.jsonl"
            if data_file.exists():
                with open(data_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            row_data = json.loads(line)
                            row_name = row_data.get('row_name') or row_data.get('_row_name')
                            if row_name:
                                existing_rows.add(row_name)
            logger.debug(f"Existing rows to extract: {existing_rows}")

            # Create filtered docs directory with only documents for existing rows
            filtered_docs_dir = session_dir / "documents_filtered"
            if filtered_docs_dir.exists():
                shutil.rmtree(filtered_docs_dir)
            filtered_docs_dir.mkdir(exist_ok=True)

            # Copy only documents that belong to existing rows
            if docs_dir.exists():
                for doc_path in docs_dir.iterdir():
                    if doc_path.is_file() and doc_path.suffix in ['.txt', '.md']:
                        # Extract row name from filename (first part before underscore)
                        row_name = doc_path.stem.split('_')[0]
                        if row_name in existing_rows:
                            shutil.copy2(doc_path, filtered_docs_dir / doc_path.name)
                            logger.debug(f"Including document for existing row: {doc_path.name}")
                        else:
                            logger.debug(f"Skipping document for new row: {doc_path.name}")

            # Count filtered documents
            doc_count = sum(1 for f in filtered_docs_dir.iterdir() if f.is_file()) if filtered_docs_dir.exists() else 0
            logger.info(f"Filtered to {doc_count} documents for existing rows")
            operation.total_documents = doc_count

            # Track progress via callback
            processed_count = [0]
            loop = asyncio.get_running_loop()

            def on_value_extracted(row_name: str, column_name: str, value: Any):
                processed_count[0] += 1
                operation.processed_documents = processed_count[0]

                try:
                    # Broadcast cell extracted
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_cell_extracted(
                            operation.session_id,
                            {
                                "row_name": row_name,
                                "column": column_name,
                                "value": value
                            }
                        ),
                        loop
                    )

                    # Broadcast progress
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_event(
                            operation.session_id,
                            "incremental_extraction_progress",
                            {
                                "operation_id": operation_id,
                                "column": column_name,
                                "progress": processed_count[0] / max(operation.total_documents * len(columns_to_extract), 1),
                                "processed_documents": processed_count[0],
                                "current_row": row_name
                            }
                        ),
                        loop
                    )
                except Exception as e:
                    logger.warning(f"Broadcast error: {e}")

            def should_stop():
                return self.is_stop_requested(operation_id)

            # Run extraction (using filtered docs directory with only existing rows)
            if filtered_docs_dir.exists() and doc_count > 0:
                logger.info(f"Starting incremental extraction for {len(columns_to_extract)} columns on {doc_count} documents")

                def run_extraction():
                    return build_table_jsonl(
                        schema_path=schema_file,
                        docs_directories=[filtered_docs_dir],  # Use filtered directory
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="all",  # Extract all columns at once with fallback for missing
                        retrieval_k=10,
                        max_workers=1,
                        on_value_extracted=on_value_extracted,
                        should_stop=should_stop
                    )

                await asyncio.get_event_loop().run_in_executor(qbsd_thread_pool, run_extraction)
                logger.info(f"Incremental extraction completed")

            # Clean up filtered docs directory
            if filtered_docs_dir.exists():
                shutil.rmtree(filtered_docs_dir, ignore_errors=True)

            # Merge results with existing data
            await self._merge_incremental_data(
                operation.session_id,
                columns_to_extract,
                output_file
            )

            # Capture new baseline
            from app.services.reextraction_service import ReextractionService
            # Use session manager's capture baseline
            self.session_manager.capture_schema_baseline(operation.session_id)

            # Note: Schema evolution snapshot was already added in discovery phase
            # Don't add another snapshot here to avoid double-counting columns
            # Just update the session to ensure consistency
            session = self.session_manager.get_session(operation.session_id)
            if session and session.statistics:
                # Update total columns in statistics (should already be correct from discovery phase)
                session.statistics.total_columns = len(session.columns)
                self.session_manager.update_session(session)
                logger.info(f"Extraction complete, total columns: {len(session.columns)}")

            # Update session status to completed after extraction
            session = self.session_manager.get_session(operation.session_id)
            if session:
                session.status = "completed"
                self.session_manager.update_session(session)
                logger.info(f"Set session status to 'completed' after incremental extraction")

            # Recompute statistics with proper column stats (non_null_count, unique_count, etc.)
            self._recompute_statistics(operation.session_id, preserve_evolution=True)
            logger.info(f"Statistics recomputed after extraction phase")

            # Cleanup
            schema_file.unlink(missing_ok=True)
            output_file.unlink(missing_ok=True)
            extraction_config_file.unlink(missing_ok=True)

            operation.status = "completed"
            operation.phase = "extraction"
            operation.progress = 1.0
            operation.completed_at = datetime.now()

            await self.broadcast_event(
                operation.session_id,
                "incremental_extraction_completed",
                {
                    "operation_id": operation_id,
                    "columns": columns_to_extract,
                    "status": "success"
                }
            )

            # Archive session data for research (fire-and-forget)
            if self._data_collection_service:
                await self._data_collection_service.trigger_archive(
                    operation.session_id, "continue_discovery_extraction"
                )

        except Exception as e:
            logger.error(f"Incremental extraction FAILED: {e}", exc_info=True)

            operation.status = "failed"
            operation.error = str(e)
            operation.completed_at = datetime.now()

            await self.broadcast_event(
                operation.session_id,
                "incremental_extraction_failed",
                {
                    "operation_id": operation_id,
                    "error": str(e)
                }
            )
        finally:
            await concurrency_limiter.release(operation.session_id)
            self._cleanup_operation(operation_id)

    async def _merge_incremental_data(
        self,
        session_id: str,
        new_columns: List[str],
        extraction_file: Path
    ):
        """
        Merge newly extracted column values with existing data.
        Only adds NEW column values, preserves all existing columns.
        """
        if not extraction_file.exists():
            logger.debug(f"Extraction file not found: {extraction_file}")
            return

        session_dir = self._get_data_dir() / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            logger.debug(f"Data file not found: {data_file}")
            return

        # Read extracted values indexed by row_name
        extracted_by_row: Dict[str, Dict[str, Any]] = {}
        with open(extraction_file, 'r') as f:
            for line in f:
                if line.strip():
                    row_data = json.loads(line)
                    row_name = row_data.get('_row_name') or row_data.get('row_name')
                    if row_name:
                        extracted_by_row[row_name] = row_data

        logger.debug(f"Extracted data for {len(extracted_by_row)} rows")

        # Build paper stem mapping for fallback matching
        extracted_by_paper_stem: Dict[str, Dict[str, Any]] = {}
        for row_name, row_data in extracted_by_row.items():
            extracted_by_paper_stem[row_name.lower()] = row_data

        # Backup existing data
        import shutil
        backup_file = session_dir / f"data_backup_incremental_{int(datetime.now().timestamp())}.jsonl"
        shutil.copy2(data_file, backup_file)

        # Read and update existing rows
        updated_rows = []
        rows_updated = 0

        with open(data_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue

                row = json.loads(line)
                row_name = row.get('row_name') or row.get('_row_name')
                papers = row.get('papers') or []

                # Try direct row name match first
                extracted = None
                if row_name and row_name in extracted_by_row:
                    extracted = extracted_by_row[row_name]
                else:
                    # Fallback: try to match by paper name stem
                    for paper in papers:
                        paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                        if paper_stem in extracted_by_paper_stem:
                            extracted = extracted_by_paper_stem[paper_stem]
                            break

                if extracted:
                    rows_updated += 1
                    # Add ONLY new columns, preserve existing
                    for col_name in new_columns:
                        if col_name in extracted:
                            if 'data' in row:
                                row['data'][col_name] = extracted[col_name]
                            else:
                                row[col_name] = extracted[col_name]

                updated_rows.append(row)

        # Ensure all rows have the new columns (with null if not extracted)
        for row in updated_rows:
            for col_name in new_columns:
                # Check both direct key and nested 'data' structure
                if 'data' in row:
                    if col_name not in row['data']:
                        row['data'][col_name] = None
                else:
                    if col_name not in row:
                        row[col_name] = None

        # Write updated data
        with open(data_file, 'w') as f:
            for row in updated_rows:
                f.write(json.dumps(row) + '\n')

        logger.info(f"Merged incremental data for {len(new_columns)} columns, {rows_updated} rows updated")

        # Update session statistics
        session = self.session_manager.get_session(session_id)
        if session:
            # Add new columns to column_stats if not present
            if session.statistics:
                existing_stat_names = {cs.name for cs in session.statistics.column_stats}
                for col_name in new_columns:
                    if col_name not in existing_stat_names:
                        # Find column info
                        col_info = next((c for c in session.columns if c.name == col_name), None)
                        if col_info:
                            session.statistics.column_stats.append(col_info)

                session.statistics.total_columns = len(session.columns)

            self.session_manager.update_session(session)

    # ==================== Operation Management ====================

    async def request_stop(self, operation_id: str) -> Dict[str, Any]:
        """Set the stop flag and return immediately."""
        with self._state_lock:
            operation = self.active_operations.get(operation_id)
        if not operation:
            return {"accepted": False, "message": f"Operation {operation_id} not found"}
        if operation.status in ["completed", "failed", "stopped"]:
            return {"accepted": False, "message": f"Operation already {operation.status}"}

        with self._state_lock:
            self.stop_flags[operation_id] = True
        logger.info("Stop requested for operation %s", operation_id)
        return {"accepted": True, "message": "Stop signal sent"}

    async def stop_operation(self, operation_id: str) -> Dict[str, Any]:
        """Stop a running operation."""
        with self._state_lock:
            operation = self.active_operations.get(operation_id)
            if not operation:
                return {"stopped": False, "message": f"Operation {operation_id} not found"}
            if operation.status in ["completed", "failed", "stopped"]:
                return {"stopped": False, "message": f"Operation already {operation.status}"}
            self.stop_flags[operation_id] = True

        logger.info(f"Stop requested for operation {operation_id}")

        # Cancel task if running
        with self._state_lock:
            task = self._tasks.get(operation_id)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=10.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.warning(f"Continue discovery task {operation_id} did not stop within 10s")

        # Re-check — task may have completed naturally
        if operation.status in ("completed", "failed", "stopped"):
            logger.info(f"Operation {operation_id} reached {operation.status} naturally")
            self.clear_stop_flag(operation_id)
            self._cleanup_operation(operation_id)
            return {"stopped": False, "message": f"Operation already {operation.status}"}

        operation.status = "stopped"
        operation.completed_at = datetime.now()

        await self.broadcast_event(
            operation.session_id,
            "continue_discovery_stopped",
            {"operation_id": operation_id, "phase": operation.phase, "message": "Operation stopped by user"}
        )

        self.clear_stop_flag(operation_id)
        self._cleanup_operation(operation_id)
        return {"stopped": True, "phase": operation.phase, "message": "Operation stopped"}

    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an operation."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return None

        return {
            "operation_id": operation.operation_id,
            "session_id": operation.session_id,
            "status": operation.status,
            "phase": operation.phase,
            "progress": operation.progress,
            "current_batch": operation.current_batch,
            "total_batches": operation.total_batches,
            "initial_columns": operation.initial_columns,
            "new_columns": operation.new_columns,
            "confirmed_columns": operation.confirmed_columns,
            "processed_documents": operation.processed_documents,
            "total_documents": operation.total_documents,
            "started_at": operation.started_at.isoformat() if operation.started_at else None,
            "completed_at": operation.completed_at.isoformat() if operation.completed_at else None,
            "error": operation.error
        }

    async def broadcast_event(self, session_id: str, event_type: str, data: Dict[str, Any]):
        """Broadcast an event via WebSocket."""
        if self.websocket_manager:
            await self.websocket_manager.broadcast_to_session(session_id, {
                "type": event_type,
                "session_id": session_id,
                "data": data,
                "timestamp": datetime.now().isoformat()
            })
