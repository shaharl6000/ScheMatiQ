"""
Continue Schema Discovery service for ScheMatiQ.
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
    ColumnInfo
)
# Note: SchemaEvolution, SchemaSnapshot are imported locally where needed
# to avoid conflict with schematiq.core.schema.SchemaEvolution
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.services import schematiq_thread_pool, concurrency_limiter
from app.storage.factory import get_storage
from app.core.config import DEVELOPER_MODE, MAX_DOCUMENTS
from app.core.logging_utils import set_session_context
from app.services.pipeline.llm_factory import enforce_release_llm_config as _enforce_release_llm_config
from app.services.document_preprocessor import read_document_text, MATERIALIZABLE_EXTENSIONS, commit_bytes_to_documents_dir
from app.services.data_utils import extract_papers, row_name_of

# ScheMatiQ library imports
from schematiq.core import schematiq as ScheMatiQ
from schematiq.core.schema import Schema, Column, SchemaEvolution, SchemaSnapshot
from schematiq.core import utils as schematiq_utils
from schematiq.core.llm_call_tracker import LLMCallTracker
from schematiq.value_extraction.main import build_table_jsonl

SCHEMATIQ_AVAILABLE = True

# Source-document extensions recognized when document_source == "original".
# The detector (get_available_documents) and the loader (_prepare_documents)
# MUST agree on this set, otherwise the detector reports can_use_original=True
# for files the loader then skips -> "No documents available for schema discovery".
# Both the original-docs loader and the detector load via read_document_text, so
# this is exactly the set that helper can materialize (single source of truth).
ORIGINAL_DOC_EXTENSIONS = MATERIALIZABLE_EXTENSIONS

# schematiq_work/ subdirs hold plain-text document dumps only. Keep this narrow:
# never widen it to MATERIALIZABLE_EXTENSIONS, or the .json entry would pull
# llm_call_stats.json / extracted_data.jsonl in as if they were source documents.
WORK_DIR_TEXT_EXTENSIONS = ('.txt', '.md')

# Source extensions selectable for incremental extraction. Identical to the set
# read_document_text can produce: _run_incremental_extraction materializes each
# selected document to .txt (converting .pdf) before the schematiq-lib reader
# loads documents_filtered/.
INCREMENTAL_EXTRACTION_DOC_EXTENSIONS = MATERIALIZABLE_EXTENSIONS


def _extract_papers(row_data: dict) -> List[str]:
    """Backward-compatible alias for app.services.data_utils.extract_papers."""
    return extract_papers(row_data)


def _index_session_documents(docs_dir: Path) -> tuple[Dict[str, Path], Dict[str, Path]]:
    """Index on-disk session documents by lowercase filename and stem."""
    by_name: Dict[str, Path] = {}
    by_stem: Dict[str, Path] = {}
    if docs_dir.exists():
        for doc_path in docs_dir.iterdir():
            if doc_path.is_file() and not doc_path.name.startswith('.'):
                by_name[doc_path.name.lower()] = doc_path
                by_stem[doc_path.stem.lower()] = doc_path
    return by_name, by_stem


def _resolve_paper_to_document(
    paper: str,
    by_name: Dict[str, Path],
    by_stem: Dict[str, Path],
    readable_extensions: tuple[str, ...],
) -> tuple[Optional[Path], Optional[str]]:
    """Map a papers-field value to a lib-readable file under documents/."""
    paper = paper.strip()
    if not paper:
        return None, None

    paper_lower = paper.lower()
    candidates: List[Path] = []
    if paper_lower in by_name:
        candidates.append(by_name[paper_lower])
    if paper_lower in by_stem:
        candidates.append(by_stem[paper_lower])
    if f"{paper_lower}.txt" in by_name:
        candidates.append(by_name[f"{paper_lower}.txt"])
    if paper_lower.endswith('.txt'):
        stem = paper_lower[:-4]
        if stem in by_stem:
            candidates.append(by_stem[stem])

    seen: Set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.suffix.lower() in readable_extensions:
            return candidate, None
        return None, (
            f"Paper {paper!r} resolves to {candidate.name}, which cannot be read by "
            f"the extractor (supported: {', '.join(readable_extensions)})"
        )

    return None, f"Needed paper {paper!r} not found in documents/"


def _plan_incremental_extraction_documents(
    docs_dir: Path,
    row_records: List[dict],
    rows_in_scope: Optional[Set[str]] = None,
    readable_extensions: tuple[str, ...] = INCREMENTAL_EXTRACTION_DOC_EXTENSIONS,
) -> tuple[List[Path], str, List[str]]:
    """
    Choose session documents to copy for incremental extraction.

    Returns (selected_paths, mode, warnings). mode is 'papers' or 'prefix'.
    """
    scoped_rows: List[dict] = []
    for row in row_records:
        row_name = row_name_of(row)
        if not row_name:
            continue
        if rows_in_scope is not None and row_name not in rows_in_scope:
            continue
        scoped_rows.append(row)

    by_name, by_stem = _index_session_documents(docs_dir)
    needed_papers: Set[str] = set()
    for row in scoped_rows:
        papers = _extract_papers(row)
        if papers:
            needed_papers.update(papers)
        else:
            row_name = row_name_of(row)
            logger.debug(
                "Row %r has no papers field; skipping papers-based mapping for this row",
                row_name,
            )

    warnings: List[str] = []
    selected: Dict[str, Path] = {}

    if needed_papers:
        mode = 'papers'
        for paper in sorted(needed_papers):
            doc_path, warning = _resolve_paper_to_document(
                paper, by_name, by_stem, readable_extensions
            )
            if warning:
                warnings.append(warning)
            elif doc_path:
                selected[doc_path.name] = doc_path
    else:
        mode = 'prefix'
        existing_row_names = {
            row_name_of(row) for row in scoped_rows
        } - {None}
        if docs_dir.exists():
            for doc_path in docs_dir.iterdir():
                if not doc_path.is_file():
                    continue
                if doc_path.suffix.lower() not in readable_extensions:
                    continue
                row_name = doc_path.stem.split('_')[0]
                if row_name in existing_row_names:
                    selected[doc_path.name] = doc_path

    return list(selected.values()), mode, warnings


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
                 data_collection_service=None, pubmed_enrichment_service=None,
                 uniprot_enrichment_service=None):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ContinueDiscoveryOperation] = {}
        self.stop_flags: Dict[str, bool] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._state_lock = threading.Lock()
        self._data_collection_service = data_collection_service
        self._pubmed_enrichment_service = pubmed_enrichment_service
        self._uniprot_enrichment_service = uniprot_enrichment_service

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

    def _get_schematiq_work_dir(self) -> Path:
        """Get the schematiq_work directory path - uses module location for reliability."""
        module_dir = Path(__file__).parent  # app/services/
        app_dir = module_dir.parent  # app/
        backend_dir = app_dir.parent  # backend/
        schematiq_work_dir = backend_dir / "schematiq_work"
        schematiq_work_dir.mkdir(exist_ok=True)
        return schematiq_work_dir

    @staticmethod
    def _is_local_path(path: str) -> bool:
        from app.services.data_utils import is_local_path
        return is_local_path(path)

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

        from app.services.data_utils import collect_all_data_rows, normalize_row_data

        data_rows = collect_all_data_rows(session_id)

        if not data_rows:
            logger.debug("No data rows found for statistics computation")
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
                row_data = normalize_row_data(row)

                if col.name in row_data:
                    value = row_data[col.name]
                    if is_valid_value(value):
                        non_null_count += 1
                    # Count unique values by answer only (not excerpt) to avoid inflating count
                    canonical = value.get("answer") if isinstance(value, dict) else value
                    try:
                        unique_values.add(json.dumps(canonical, sort_keys=True))
                    except (TypeError, ValueError):
                        unique_values.add(str(canonical))

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
            skipped_documents=existing_skipped,  # Preserve skipped documents
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
            - original_documents: Documents from original ScheMatiQ run or data.jsonl references
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
                    papers_raw = _extract_papers(row)
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
                if (
                    f.is_file()
                    and f.suffix.lower() in ORIGINAL_DOC_EXTENSIONS
                    and not f.name.startswith('.')
                ):
                    local_docs.add(f.name)

        # Also check schematiq_work
        schematiq_work_dir = self._get_schematiq_work_dir() / session_id
        if schematiq_work_dir.exists():
            for subdir in schematiq_work_dir.iterdir():
                if subdir.is_dir() and not subdir.name.startswith('.'):
                    for f in subdir.iterdir():
                        if f.is_file() and f.suffix.lower() in WORK_DIR_TEXT_EXTENSIONS:
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
            cloud_datasets = [{"name": d.name, "file_count": d.file_count} for d in dataset_infos]
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
        bypass_limit: bool = False,
        document_randomization_seed: int = 42
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
            schematiq_work_dir = self._get_schematiq_work_dir() / session_id

            # Check data/{session_id}/documents/ first.
            # Source docs land here verbatim via _move_pending_documents, so a
            # PDF (or other non-text source) may live here and must be converted
            # to text on read. Reading only .txt/.md silently yields 0 documents.
            if docs_dir.exists():
                for f in sorted(docs_dir.iterdir()):
                    if not f.is_file() or f.name.startswith('.'):
                        continue
                    ext = f.suffix.lower()
                    if ext not in ORIGINAL_DOC_EXTENSIONS:
                        continue
                    content = read_document_text(f)
                    if content:
                        documents.append(content)
                        filenames.append(f.name)

            # Also check schematiq_work directory
            if not documents and schematiq_work_dir.exists():
                for subdir in schematiq_work_dir.iterdir():
                    if subdir.is_dir() and not subdir.name.startswith('.'):
                        for f in sorted(subdir.iterdir()):
                            if f.is_file() and f.suffix.lower() in WORK_DIR_TEXT_EXTENSIONS:
                                try:
                                    content = f.read_text(encoding='utf-8', errors='replace')
                                except Exception as e:
                                    logger.warning(f"Could not read {f.name}: {e}")
                                    continue
                                if content and content.strip():
                                    documents.append(content)
                                    filenames.append(f.name)

        elif document_source == "cloud":
            # Download from cloud storage
            if not cloud_dataset:
                raise ValueError("cloud_dataset required for cloud document source")

            storage = get_storage()
            try:
                files = await storage.list_files('datasets', cloud_dataset)
                for file_path in files:
                    filename = file_path.rsplit('/', 1)[-1]
                    content = await storage.download_file('datasets', file_path)
                    if content:
                        committed = commit_bytes_to_documents_dir(content, filename, docs_dir)
                        if committed:
                            text_content = read_document_text(committed)
                            if text_content:
                                documents.append(text_content)
                                filenames.append(committed.name)
            except Exception as e:
                logger.error(f"Error downloading cloud documents: {e}")
                raise

            # Also include any documents uploaded via MissingDocumentsSection
            # (these supplement the cloud dataset with locally-uploaded files)
            pending_dir = session_dir / "pending_documents"
            if pending_dir.exists():
                existing = set(filenames)
                # Filter to only files from the latest upload (prevents stale files
                # from initial ScheMatiQ being re-processed)
                session = self.session_manager.get_session(session_id)
                latest_uploads = set(session.metadata.uploaded_documents) if session and session.metadata.uploaded_documents else None
                for f in sorted(pending_dir.iterdir()):
                    if f.is_file() and not f.name.startswith('.') and f.name not in existing:
                        if latest_uploads and f.name not in latest_uploads:
                            continue
                        try:
                            content = f.read_text(encoding='utf-8')
                            documents.append(content)
                            filenames.append(f.name)
                            (docs_dir / f.name).write_text(content, encoding='utf-8')
                        except Exception as e:
                            logger.debug(f"Could not read pending file {f}: {e}")
                if len(filenames) > len(existing):
                    logger.info(f"Added {len(filenames) - len(existing)} documents from pending_documents")

        elif document_source == "upload":
            # Use uploaded files from pending_documents
            pending_dir = session_dir / "pending_documents"
            if pending_dir.exists():
                # Filter to only files from the latest upload using session metadata
                # (more reliable than frontend-passed filenames which may differ due to PDF conversion/dedup)
                session = self.session_manager.get_session(session_id)
                latest_uploads = set(session.metadata.uploaded_documents) if session and session.metadata.uploaded_documents else None
                target_files = latest_uploads or (set(uploaded_files) if uploaded_files else None)
                logger.info(f"Upload filter: target_files={target_files}, pending_dir contents={[f.name for f in pending_dir.iterdir() if f.is_file()]}")
                for f in sorted(pending_dir.iterdir()):
                    if f.is_file() and not f.name.startswith('.'):
                        if target_files and f.name not in target_files:
                            continue
                        try:
                            content = f.read_text(encoding='utf-8')
                            documents.append(content)
                            filenames.append(f.name)
                            # Copy to documents dir
                            (docs_dir / f.name).write_text(content, encoding='utf-8')
                        except Exception as e:
                            logger.debug(f"Could not read {f}: {e}")

        # Enforce document limit (same as initial ScheMatiQ creation)
        # The limit can be bypassed in developer mode via config
        if not (DEVELOPER_MODE and bypass_limit) and len(documents) > MAX_DOCUMENTS:
            import random
            original_count = len(documents)
            combined = list(zip(documents, filenames))
            rng = random.Random(document_randomization_seed)  # deterministic sampling for reproducibility
            rng.shuffle(combined)
            combined = combined[:MAX_DOCUMENTS]
            documents, filenames = zip(*combined) if combined else ([], [])
            documents, filenames = list(documents), list(filenames)
            logger.info(f"Document limit applied: {original_count} → {len(documents)} (max: {MAX_DOCUMENTS})")

        logger.info(f"Prepared {len(documents)} documents from {document_source}")
        return docs_dir, documents, filenames

    # ==================== Schema Discovery ====================

    def _convert_session_columns_to_schema(self, columns: List[ColumnInfo], query: str) -> Schema:
        """Convert session columns to ScheMatiQ Schema object."""
        schematiq_columns = []
        for col in columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                schematiq_col = Column(
                    name=col.name,
                    definition=col.definition or "",
                    rationale=col.rationale or "",
                    allowed_values=col.allowed_values
                )
                schematiq_columns.append(schematiq_col)

        return Schema(query=query, columns=schematiq_columns, max_keys=100)

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
        uploaded_files: Optional[List[str]] = None,
        retriever_config: Optional[Dict[str, Any]] = None,
        max_keys_schema: int = 100,
        documents_batch_size: int = 1,
        convergence_threshold: Optional[int] = None,
        document_randomization_seed: Optional[int] = None,
        skip_value_extraction: bool = False,
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
            convergence_threshold: Batches without change to stop (None = default 2)
            document_randomization_seed: Seed for document sampling (None = default 42)
            skip_value_extraction: If True, discover columns only without extraction
            bypass_limit: Developer mode flag to bypass document limit

        Returns:
            Dictionary with operation details
        """
        if not SCHEMATIQ_AVAILABLE:
            raise RuntimeError("ScheMatiQ components not available")

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
            "uploaded_files": uploaded_files,
            "retriever_config": retriever_config,
            "max_keys_schema": max_keys_schema,
            "documents_batch_size": documents_batch_size,
            "convergence_threshold": convergence_threshold,
            "document_randomization_seed": document_randomization_seed,
            "skip_value_extraction": skip_value_extraction,
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
                uploaded_files=config.get("uploaded_files"),
                bypass_limit=config.get("bypass_limit", False),
                document_randomization_seed=config.get("document_randomization_seed") or 42
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
            llm = schematiq_utils.build_llm(enforced_llm_config)

            # Build retriever - use shared singleton if config requests one
            retriever_cfg = config.get("retriever_config")
            if retriever_cfg:
                from app.services import get_shared_retriever
                retriever = get_shared_retriever()
            else:
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
            logger.info("Starting manual schema discovery loop with initial_schema")

            # Create document batches
            batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
            filename_batches = [filenames[i:i+batch_size] for i in range(0, len(filenames), batch_size)]

            logger.info("📦 Continue discovery: %d documents → %d batches (batch_size=%d)", len(documents), len(batches), batch_size)

            # Update operation with actual batch count
            operation.total_batches = len(batches)

            # Initialize tracking
            current_schema = initial_schema
            context_window_size = llm_config.get("context_window_size") or getattr(llm, 'context_window_size', 8192)
            convergence_threshold = config.get("convergence_threshold") or 2
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
                    schematiq_thread_pool,
                    functools.partial(ScheMatiQ.select_relevant_content, docs=batch_docs, query=query, retriever=retriever),
                )
                logger.debug(f"Selected {len(relevant_content)} relevant passages from batch")

                # Generate schema for this batch (offloaded to thread pool)
                try:
                    schema_result = await loop.run_in_executor(
                        schematiq_thread_pool,
                        functools.partial(
                            ScheMatiQ.generate_schema,
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
                    schematiq_thread_pool,
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
                    schematiq_thread_pool,
                    functools.partial(ScheMatiQ.evaluate_schema_convergence, current_schema, merged_schema),
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
                # Use alias to avoid conflict with schematiq.core.schema.SchemaEvolution
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
                logger.info("Set session status to 'completed' after discovery")

            # Recompute statistics with proper column stats (non_null_count, unique_count, etc.)
            self._recompute_statistics(operation.session_id, preserve_evolution=True)
            logger.info("Statistics recomputed after discovery phase")

            # Recapture schema baseline so new columns are tracked for change detection
            self.session_manager.capture_schema_baseline(operation.session_id)
            logger.info("Schema baseline recaptured after continue discovery")

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

            # Enrich source documents with PubMed/DOI links (fire-and-forget)
            if self._pubmed_enrichment_service:
                await self._pubmed_enrichment_service.enrich_session(operation.session_id)

            # Enrich protein rows with UniProt data (fire-and-forget, protein units only)
            if self._uniprot_enrichment_service:
                await self._uniprot_enrichment_service.enrich_session(operation.session_id)

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
            # Only cleanup on stopped — failed operations persist for polling,
            # successful operations persist for the confirm/extraction step.
            # TTL in get_operation_status handles abandoned operations.
            if operation.status == "stopped":
                self._cleanup_operation(operation_id)
            elif operation.status == "completed":
                operation.completed_at = datetime.now()

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
            llm = schematiq_utils.build_llm(enforced_llm_config)
            if retriever_cfg:
                from app.services import get_shared_retriever
                retriever = get_shared_retriever()
            else:
                retriever = None

            output_file = session_dir / f"incremental_output_{operation_id}.jsonl"

            # Load existing rows and determine extraction scope
            row_records: List[dict] = []
            data_file = session_dir / "data.jsonl"
            if data_file.exists():
                with open(data_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            row_records.append(json.loads(line))

            rows_in_scope: Optional[Set[str]] = None
            if extraction_config.get("row_selection") == "selected":
                selected = extraction_config.get("selected_rows") or operation.extraction_rows
                if selected:
                    rows_in_scope = set(selected)

            existing_rows = {
                row_name_of(row)
                for row in row_records
                if row_name_of(row)
            }
            if rows_in_scope is not None:
                existing_rows &= rows_in_scope
            logger.debug(f"Existing rows to extract: {existing_rows}")

            # Create filtered docs directory with documents for in-scope rows
            filtered_docs_dir = session_dir / "documents_filtered"
            if filtered_docs_dir.exists():
                shutil.rmtree(filtered_docs_dir)
            filtered_docs_dir.mkdir(exist_ok=True)

            selected_docs, mapping_mode, mapping_warnings = _plan_incremental_extraction_documents(
                docs_dir, row_records, rows_in_scope
            )
            logger.info(
                "Incremental extraction document selection mode=%s, selected=%d document(s)",
                mapping_mode,
                len(selected_docs),
            )
            for warning in mapping_warnings:
                logger.warning(warning)

            materialized = 0
            for doc_path in selected_docs:
                text = read_document_text(doc_path)
                if not text:
                    mapping_warnings.append(
                        f"Could not materialize {doc_path.name} for incremental "
                        f"extraction; skipping"
                    )
                    logger.warning("Could not materialize %s; skipping", doc_path.name)
                    continue
                # Write as .txt so the schematiq-lib reader can load it (it does
                # not read PDFs); converted PDFs land here as plain text.
                dest = filtered_docs_dir / f"{doc_path.stem}.txt"
                dest.write_text(text, encoding="utf-8")
                logger.debug(f"Materialized document for incremental extraction: {dest.name}")
                materialized += 1

            doc_count = materialized
            if existing_rows and doc_count == 0:
                logger.warning(
                    "Incremental extraction: %d in-scope row(s) but documents_filtered/ is empty "
                    "(mode=%s); extraction will be skipped",
                    len(existing_rows),
                    mapping_mode,
                )
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
                        should_stop=should_stop,
                        write_skip_rationale_artifact=session.write_artifacts,
                    )

                await asyncio.get_event_loop().run_in_executor(schematiq_thread_pool, run_extraction)
                logger.info("Incremental extraction completed")

            # Clean up filtered docs directory
            if filtered_docs_dir.exists():
                shutil.rmtree(filtered_docs_dir, ignore_errors=True)

            # Merge results with existing data
            await self._merge_incremental_data(
                operation.session_id,
                columns_to_extract,
                output_file
            )

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
                logger.info("Set session status to 'completed' after incremental extraction")

            # Recompute statistics with proper column stats (non_null_count, unique_count, etc.)
            self._recompute_statistics(operation.session_id, preserve_evolution=True)
            logger.info("Statistics recomputed after extraction phase")

            # Recapture schema baseline so new columns are tracked for change detection
            self.session_manager.capture_schema_baseline(operation.session_id)
            logger.info("Schema baseline recaptured after incremental extraction")

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

            # Enrich source documents with PubMed/DOI links (fire-and-forget)
            if self._pubmed_enrichment_service:
                await self._pubmed_enrichment_service.enrich_session(operation.session_id)

            # Enrich protein rows with UniProt data (fire-and-forget, protein units only)
            if self._uniprot_enrichment_service:
                await self._uniprot_enrichment_service.enrich_session(operation.session_id)

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

        # Read extracted values indexed by composite key (row_name, source_document)
        from app.services.data_utils import row_dedup_key, _resolve_source_document

        extracted_by_key: Dict[tuple, Dict[str, Any]] = {}
        extracted_by_row_name: Dict[str, List[Dict[str, Any]]] = {}
        with open(extraction_file, 'r') as f:
            for line in f:
                if line.strip():
                    row_data = json.loads(line)
                    key = row_dedup_key(row_data)
                    if key[0]:
                        extracted_by_key[key] = row_data
                        extracted_by_row_name.setdefault(key[0], []).append(row_data)

        logger.debug(f"Extracted data for {len(extracted_by_key)} rows (composite keys)")

        # Build paper stem mapping for fallback matching
        extracted_by_paper_stem: Dict[str, List[Dict[str, Any]]] = {}
        for key, row_data in extracted_by_key.items():
            extracted_by_paper_stem.setdefault(key[0].lower(), []).append(row_data)

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
                row_name = row_name_of(row)
                row_src = _resolve_source_document(row)
                papers = row.get('papers') or []

                # Try composite key match first (row_name + source_document)
                extracted = None
                row_key = (row_name, row_src) if row_name else None
                if row_key and row_key in extracted_by_key:
                    extracted = extracted_by_key[row_key]
                elif row_name and row_name in extracted_by_row_name:
                    candidates = extracted_by_row_name[row_name]
                    if len(candidates) == 1:
                        extracted = candidates[0]
                    else:
                        for paper in papers:
                            paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                            for cand in candidates:
                                if _resolve_source_document(cand).lower() == paper_stem:
                                    extracted = cand
                                    break
                            if extracted:
                                break
                if not extracted:
                    for paper in papers:
                        paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                        if paper_stem in extracted_by_paper_stem:
                            candidates = extracted_by_paper_stem[paper_stem]
                            if len(candidates) == 1:
                                extracted = candidates[0]
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

    # TTL for completed discovery operations awaiting confirm/extraction (30 minutes)
    OPERATION_TTL_SECONDS = 30 * 60

    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an operation. Cleans up expired operations."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return None

        # TTL cleanup: if discovery completed/failed but no further action taken
        if (operation.status in ("completed", "failed") and operation.phase == "discovery"
                and operation.completed_at):
            elapsed = (datetime.now() - operation.completed_at).total_seconds()
            if elapsed > self.OPERATION_TTL_SECONDS:
                logger.info(f"Operation {operation_id} expired after {elapsed:.0f}s (status={operation.status})")
                self._cleanup_operation(operation_id)
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
