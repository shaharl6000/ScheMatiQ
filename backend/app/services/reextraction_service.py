"""
Re-extraction service for ScheMatiQ visualization.
Handles schema change detection, paper discovery, and selective re-extraction.
"""

import json
import asyncio
import hashlib
import threading
import uuid
import logging
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
from datetime import datetime

from app.models.session import (
    ColumnInfo, ColumnBaseline, SchemaBaseline, VisualizationSession, ObservationUnitInfo
)
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.services import schematiq_thread_pool, concurrency_limiter
from app.services.data_utils import row_name_of
from app.storage.factory import get_storage
from app.core.config import DEVELOPER_MODE, RELEASE_CONFIG
from app.core.logging_utils import set_session_context

# ScheMatiQ library imports
from schematiq.value_extraction.main import build_table_jsonl
from app.services.reference_context import build_reference_context
from schematiq.core.llm_backends import GeminiLLM
from schematiq.core.model_specs import ModelNames
from schematiq.core import utils as schematiq_utils
from schematiq.core.llm_call_tracker import LLMCallTracker

SCHEMATIQ_AVAILABLE = True

logger = logging.getLogger(__name__)


class ReextractionOperation:
    """Tracks a running re-extraction operation."""
    def __init__(
        self,
        operation_id: str,
        session_id: str,
        columns: List[str],
        status: str = "pending",
        renamed_from: Optional[Dict[str, str]] = None,
    ):
        self.operation_id = operation_id
        self.session_id = session_id
        self.columns = columns
        self.status = status
        self.progress = 0.0
        self.current_column: Optional[str] = None
        self.processed_documents = 0
        self.total_documents = 0
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        # new column name -> previous column name (from schema renames)
        self.renamed_from: Dict[str, str] = renamed_from or {}
        # composite keys already merged incrementally during extraction
        self.incrementally_merged_keys: Set[tuple] = set()
        # paper discovery snapshot for this operation (avoids redundant rescans)
        self.paper_discovery: Optional[Dict[str, Any]] = None


class ReextractionService(WebSocketBroadcasterMixin):
    """Handles selective re-extraction of column values after schema changes."""

    # Retriever is now shared across all services via get_shared_retriever()

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager,
                 data_collection_service=None, pubmed_enrichment_service=None,
                 uniprot_enrichment_service=None):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ReextractionOperation] = {}
        self.stop_flags: Dict[str, bool] = {}  # operation_id -> stop requested
        self._extraction_tasks: Dict[str, asyncio.Task] = {}  # operation_id -> task
        self._state_lock = threading.Lock()
        self._data_collection_service = data_collection_service
        self._pubmed_enrichment_service = pubmed_enrichment_service
        self._uniprot_enrichment_service = uniprot_enrichment_service
        self._incremental_merge_locks: Dict[str, asyncio.Lock] = {}

    @staticmethod
    def get_cached_retriever():
        """Return the shared EmbeddingRetriever singleton."""
        from app.services import get_shared_retriever
        return get_shared_retriever()

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
            self._extraction_tasks.pop(operation_id, None)

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
        logger.info("Stop requested for reextraction operation %s", operation_id)
        return {"accepted": True, "message": "Stop signal sent"}

    async def stop_operation(self, operation_id: str) -> Dict[str, Any]:
        """
        Stop a running re-extraction operation.

        Returns:
            Dictionary with stop status and any partial results
        """
        with self._state_lock:
            operation = self.active_operations.get(operation_id)
            if not operation:
                return {"stopped": False, "message": f"Operation {operation_id} not found"}
            if operation.status in ["completed", "failed", "stopped"]:
                return {"stopped": False, "message": f"Operation already {operation.status}"}
            # Set stop flag (only if not already set by request_stop)
            self.stop_flags[operation_id] = True

        logger.info(f"Stop requested for re-extraction operation {operation_id}")

        # Cancel the extraction task if it exists
        with self._state_lock:
            task = self._extraction_tasks.get(operation_id)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.warning(f"Reextraction task {operation_id} did not stop within 5s")

        # Re-check terminal status AFTER waiting — task may have completed naturally
        if operation.status in ("completed", "failed", "stopped"):
            logger.info(f"Operation {operation_id} reached {operation.status} naturally, skipping merge")
            self.clear_stop_flag(operation_id)
            self._cleanup_operation(operation_id)
            return {"stopped": False, "message": f"Operation already {operation.status}"}

        # Merge partial results (safe — task is done and didn't complete naturally)
        try:
            session_dir = Path("./data") / operation.session_id
            output_file = session_dir / f"reextract_output_{operation_id}.jsonl"
            if output_file.exists():
                logger.info(f"Merging partial results from {output_file}")
                await self._merge_reextracted_data(
                    operation.session_id,
                    operation.columns,
                    output_file,
                    renamed_from=operation.renamed_from,
                    initial_matched_keys=operation.incrementally_merged_keys,
                )
                output_file.unlink(missing_ok=True)
                schema_file = session_dir / f"reextract_schema_{operation_id}.json"
                schema_file.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Could not merge partial results: {e}")

        # Update status
        operation.status = "stopped"
        operation.completed_at = datetime.now()

        await self.broadcast_event(
            operation.session_id,
            "reextraction_stopped",
            {
                "operation_id": operation_id,
                "columns": operation.columns,
                "processed_documents": operation.processed_documents,
                "total_documents": operation.total_documents,
                "message": "Re-extraction stopped by user"
            }
        )

        self.clear_stop_flag(operation_id)
        self._cleanup_operation(operation_id)
        return {
            "stopped": True,
            "message": "Re-extraction stopped",
            "processed_documents": operation.processed_documents,
            "total_documents": operation.total_documents
        }

    # ==================== Schema Change Detection ====================

    @staticmethod
    def _is_local_path(path: str) -> bool:
        from app.services.data_utils import is_local_path
        return is_local_path(path)

    @staticmethod
    def calculate_column_checksum(column: ColumnInfo) -> str:
        """Calculate a checksum for change detection."""
        content = f"{column.definition or ''}{column.rationale or ''}"
        if column.allowed_values:
            content += "|".join(sorted(column.allowed_values))
        return hashlib.md5(content.encode()).hexdigest()

    def capture_baseline(self, session: VisualizationSession) -> SchemaBaseline:
        """Capture the current schema state as a baseline."""
        columns_dict = {}
        for col in session.columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                columns_dict[col.name] = ColumnBaseline(
                    name=col.name,
                    definition=col.definition or "",
                    rationale=col.rationale or "",
                    allowed_values=col.allowed_values,
                    checksum=self.calculate_column_checksum(col)
                )

        return SchemaBaseline(
            columns=columns_dict,
            captured_at=datetime.now()
        )

    async def capture_and_save_baseline(self, session_id: str) -> None:
        """Capture baseline and save to session."""
        session = self.session_manager.get_session(session_id)
        if not session:
            return

        baseline = self.capture_baseline(session)
        session.schema_baseline = baseline
        self.session_manager.update_session(session)
        logger.debug(f"Captured schema baseline for session {session_id} with {len(baseline.columns)} columns")

    def detect_schema_changes(self, session: VisualizationSession) -> Dict[str, Any]:
        """
        Detect which columns have changed since the baseline.

        Returns:
            Dictionary with change details including:
            - has_changes: bool
            - changed_columns: List of column names with definition/rationale/allowed_values changes
            - new_columns: List of new column names (not in baseline)
            - column_changes: Dict of detailed changes per column
        """
        result = {
            "has_changes": False,
            "changed_columns": [],
            "new_columns": [],
            "column_changes": {},
            "can_reextract": False,
            "missing_baseline": False
        }

        # If no baseline exists, auto-create one from current state so future edits are tracked
        if not session.schema_baseline:
            baseline = self.capture_baseline(session)
            session.schema_baseline = baseline
            # Only persist if session has extracted data to avoid writes on every poll
            if any((c.non_null_count or 0) > 0 for c in session.columns):
                self.session_manager.update_session(session)
            # Return no changes — baseline just captured from current state
            return result

        baseline_columns = session.schema_baseline.columns

        # Check each current column against baseline
        for col in session.columns:
            if not col.name or col.name.lower().endswith('_excerpt'):
                continue

            if col.name not in baseline_columns:
                # New column
                result["new_columns"].append(col.name)
                result["column_changes"][col.name] = {
                    "column_name": col.name,
                    "change_type": "new",
                    "old_value": None,
                    "new_value": col.definition or "",
                    "row_count_affected": 0
                }
            else:
                # Check for changes
                baseline = baseline_columns[col.name]
                current_checksum = self.calculate_column_checksum(col)
                # Detect rename: baseline entry was moved to new key but name field still has old name
                was_renamed = col.name != baseline.name

                if current_checksum != baseline.checksum or was_renamed:
                    # Determine what changed
                    change_type = "renamed" if was_renamed else self._determine_change_type(col, baseline)
                    result["changed_columns"].append(col.name)
                    change_detail = {
                        "column_name": col.name,
                        "change_type": change_type,
                        "old_value": baseline.name if was_renamed else self._get_change_old_value(change_type, baseline),
                        "new_value": col.name if was_renamed else self._get_change_new_value(change_type, col),
                        "row_count_affected": 0,
                        "old_definition": baseline.definition,
                        "old_rationale": baseline.rationale,
                        "old_allowed_values": baseline.allowed_values,
                    }
                    if was_renamed:
                        change_detail["old_name"] = baseline.name
                    result["column_changes"][col.name] = change_detail

        # Auto-heal stale baselines: if "new" columns already have extracted data,
        # the baseline is outdated (e.g., continue discovery added columns).
        # Recapture baseline and re-detect so they show as unchanged, not "new".
        if result["new_columns"] and session.schema_baseline:
            stale_cols = [
                name for name in result["new_columns"]
                if any(c.name == name and (c.non_null_count or 0) > 0 for c in session.columns)
            ]
            if stale_cols:
                # Only add the stale columns to the existing baseline —
                # do NOT recapture the whole baseline, or we'd erase real edits.
                for col_name in stale_cols:
                    col = next(c for c in session.columns if c.name == col_name)
                    session.schema_baseline.columns[col_name] = ColumnBaseline(
                        name=col.name,
                        definition=col.definition or "",
                        rationale=col.rationale or "",
                        allowed_values=col.allowed_values,
                        checksum=self.calculate_column_checksum(col),
                    )
                self.session_manager.update_session(session)
                # Inline re-detection: remove the now-patched stale columns from
                # the result so they are no longer reported as new or changed,
                # then fall through to recalculate has_changes. This avoids the
                # infinite recursion that a recursive self.detect_schema_changes()
                # call would cause if stale_cols were somehow repopulated.
                for col_name in stale_cols:
                    result["new_columns"] = [n for n in result["new_columns"] if n != col_name]
                    result["column_changes"].pop(col_name, None)

        result["has_changes"] = bool(result["changed_columns"] or result["new_columns"])
        return result

    def _determine_change_type(self, current: ColumnInfo, baseline: ColumnBaseline) -> str:
        """Determine what type of change occurred."""
        if (current.definition or "") != (baseline.definition or ""):
            return "definition"
        if (current.rationale or "") != (baseline.rationale or ""):
            return "rationale"

        # Check allowed_values
        current_values = set(current.allowed_values or [])
        baseline_values = set(baseline.allowed_values or [])
        if current_values != baseline_values:
            return "allowed_values"

        return "unknown"

    def _get_change_old_value(self, change_type: str, baseline: ColumnBaseline) -> Optional[str]:
        if change_type == "definition":
            return baseline.definition
        elif change_type == "rationale":
            return baseline.rationale
        elif change_type == "allowed_values":
            return ", ".join(baseline.allowed_values or [])
        return None

    def _get_change_new_value(self, change_type: str, current: ColumnInfo) -> Optional[str]:
        if change_type == "definition":
            return current.definition
        elif change_type == "rationale":
            return current.rationale
        elif change_type == "allowed_values":
            return ", ".join(current.allowed_values or [])
        return None

    # ==================== Paper Discovery ====================

    def _get_session_document_dirs(self, session_id: str) -> List[Path]:
        """Directories that may hold source documents for a session."""
        data_session_dir = Path("./data") / session_id
        schematiq_session_dir = Path("./schematiq_work") / session_id
        docs_dir = data_session_dir / "documents"
        pending_dir = data_session_dir / "pending_documents"

        local_dirs_to_check: List[Path] = [docs_dir, pending_dir]

        schematiq_datasets_dir = schematiq_session_dir / "datasets"
        if schematiq_datasets_dir.exists():
            for dataset_dir in schematiq_datasets_dir.iterdir():
                if dataset_dir.is_dir():
                    local_dirs_to_check.append(dataset_dir)

        capped_dir = schematiq_session_dir / "capped_documents"
        if capped_dir.exists():
            local_dirs_to_check.append(capped_dir)

        schematiq_config_file = schematiq_session_dir / "schematiq_config.json"
        if schematiq_config_file.exists():
            try:
                with open(schematiq_config_file) as f:
                    schematiq_config = json.load(f)
                config_docs_path = schematiq_config.get("docs_path", [])
                if isinstance(config_docs_path, str):
                    config_docs_path = [config_docs_path]
                for dp in config_docs_path:
                    if dp:
                        dp_path = Path(dp)
                        if dp_path.is_dir() and dp_path not in local_dirs_to_check:
                            local_dirs_to_check.append(dp_path)
            except Exception as e:
                logger.debug("Could not read ScheMatiQ config for docs_path: %s", e)

        return [d for d in local_dirs_to_check if d.exists()]

    def _collect_session_document_filenames(self, session_id: str) -> List[str]:
        """List document filenames on disk (used when the table has no rows yet)."""
        seen: Set[str] = set()
        filenames: List[str] = []
        for local_dir in self._get_session_document_dirs(session_id):
            for f in sorted(local_dir.iterdir()):
                if f.is_file() and not f.name.startswith("."):
                    if f.name not in seen:
                        seen.add(f.name)
                        filenames.append(f.name)
        return filenames

    async def discover_papers(self, session_id: str) -> Dict[str, Any]:
        """
        Discover papers associated with table rows in storage.

        Returns:
            Dictionary with:
            - total_rows: Number of data rows
            - rows_with_papers: Number of rows that have paper references
            - available_papers: Papers found in storage (local + cloud)
            - missing_papers: Papers referenced but not found anywhere
            - paper_to_rows: Mapping of paper name to row names
            - cloud_papers: Mapping of paper name to Supabase path (NEW)
            - local_papers: Papers found in local documents/ folder (NEW)
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            return {
                "total_rows": 0,
                "rows_with_papers": 0,
                "available_papers": [],
                "missing_papers": [],
                "paper_to_rows": {},
                "cloud_papers": {},
                "local_papers": []
            }

        # Get session's cloud_dataset as fallback for papers with local paths
        session_cloud_dataset = None
        if session.metadata and session.metadata.cloud_dataset:
            session_cloud_dataset = session.metadata.cloud_dataset
            logger.debug(f"Session has cloud_dataset fallback: {session_cloud_dataset}")

        data_session_dir = Path("./data") / session_id
        schematiq_session_dir = Path("./schematiq_work") / session_id

        # Find data files from all possible locations (same logic as schematiq_runner.get_extracted_data)
        data_files = []
        extracted_file = schematiq_session_dir / "extracted_data.jsonl"
        if extracted_file.exists():
            data_files.append(extracted_file)
        if not data_files:
            schematiq_data_file = schematiq_session_dir / "data.jsonl"
            if schematiq_data_file.exists():
                data_files.append(schematiq_data_file)
        data_dir_file = data_session_dir / "data.jsonl"
        if data_dir_file.exists() and data_dir_file not in data_files:
            data_files.append(data_dir_file)

        logger.debug(f"discover_papers: data_files={[str(f) for f in data_files]}, "
                     f"data_session_dir exists={data_session_dir.exists()}, "
                     f"schematiq_session_dir exists={schematiq_session_dir.exists()}")

        # Collect paper references and document directories from all rows
        from app.services.data_utils import _resolve_source_document

        paper_refs: Set[str] = set()
        row_paper_mapping: Dict[tuple, List[str]] = {}  # (row_name, source_doc) -> [papers]
        paper_doc_dirs: Dict[str, str] = {}  # paper_name -> document_directory
        total_rows = 0

        for data_file in data_files:
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        total_rows += 1
                        try:
                            row = json.loads(line)
                            row_name = row_name_of(row) or f"row_{total_rows}"

                            # Helper to extract value from ScheMatiQ answer format or plain value
                            def extract_value(val: Any) -> str:
                                if val is None:
                                    return ''
                                if isinstance(val, dict) and 'answer' in val:
                                    return str(val['answer']) if val['answer'] else ''
                                return str(val) if val else ''

                            # Get papers from multiple possible locations
                            from app.services.data_utils import extract_papers
                            papers = extract_papers(row)

                            # Get document directory from row data (check multiple possible locations)
                            doc_dir_raw = (
                                row.get('Document Directory') or
                                row.get('document_directory') or
                                row.get('data', {}).get('Document Directory') or
                                row.get('data', {}).get('document_directory') or
                                None
                            )
                            doc_dir = extract_value(doc_dir_raw)

                            # Clean up doc_dir - extract just the datasets/... part if it's a full path
                            if doc_dir and 'datasets/' in doc_dir:
                                doc_dir = 'datasets/' + doc_dir.split('datasets/')[-1]
                            # Handle local paths (e.g., /app/backend/data/{uuid}/pending_documents)
                            # These indicate documents were uploaded locally, not from cloud storage
                            # Fall back to session's cloud_dataset if available
                            elif doc_dir and self._is_local_path(doc_dir):
                                logger.debug(f"Detected local path in document_directory: {doc_dir}")
                                if session_cloud_dataset:
                                    doc_dir = f"datasets/{session_cloud_dataset}"
                                    logger.debug(f"Using session cloud_dataset fallback: {doc_dir}")
                                else:
                                    logger.debug("No cloud_dataset fallback available - documents may not be found")
                                    # No cloud fallback - will be checked locally only
                                    doc_dir = None

                            paper_refs.update(papers)
                            row_src = _resolve_source_document(row)
                            row_paper_mapping[(row_name, row_src)] = papers

                            # Track document directory for each paper
                            for paper in papers:
                                if doc_dir and paper not in paper_doc_dirs:
                                    paper_doc_dirs[paper] = doc_dir

                        except json.JSONDecodeError:
                            continue

        # Check which papers exist in local storage
        local_files: Set[str] = set()
        local_dirs_to_check = self._get_session_document_dirs(session_id)

        for local_dir in local_dirs_to_check:
            if local_dir.exists():
                for f in local_dir.iterdir():
                    if f.is_file() and not f.name.startswith('.'):
                        local_files.add(f.name)
                        local_files.add(f.stem)  # Also match without extension

        logger.debug(f"discover_papers: total_rows={total_rows}, paper_refs={len(paper_refs)}, "
                     f"local_files={len(local_files)}, dirs_checked={[str(d) for d in local_dirs_to_check if d.exists()]}")

        # Categorize papers: local, cloud, or missing
        local_papers: List[str] = []
        cloud_papers: Dict[str, str] = {}  # paper_name -> supabase_path
        missing: List[str] = []

        # Get storage backend for cloud checks
        storage = get_storage()

        # Step 1: Check local files first, collect papers that need cloud checking
        papers_to_check_cloud: List[str] = []
        for paper in paper_refs:
            if paper in local_files or f"{paper}.txt" in local_files:
                local_papers.append(paper)
            elif paper in paper_doc_dirs:
                papers_to_check_cloud.append(paper)
            elif session_cloud_dataset:
                # Fallback: use session's cloud_dataset for papers without explicit directory
                paper_doc_dirs[paper] = f"datasets/{session_cloud_dataset}"
                papers_to_check_cloud.append(paper)
            else:
                missing.append(paper)

        # Step 2: Group papers by their cloud folder (to minimize HTTP requests)
        folders_to_check: Dict[str, List[str]] = {}  # folder -> list of papers
        for paper in papers_to_check_cloud:
            doc_dir = paper_doc_dirs[paper]
            # Strip 'datasets/' prefix since we're checking in the 'datasets' bucket
            clean_doc_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
            if clean_doc_dir not in folders_to_check:
                folders_to_check[clean_doc_dir] = []
            folders_to_check[clean_doc_dir].append(paper)

        # Step 3: List each folder ONCE (instead of N HTTP requests per paper)
        folder_contents: Dict[str, set] = {}
        for folder in folders_to_check:
            logger.debug(f"Listing Supabase folder: {folder} (checking {len(folders_to_check[folder])} papers)")
            try:
                folder_contents[folder] = await storage.list_folder_files('datasets', folder)
                logger.debug(f"Found {len(folder_contents[folder])} files in {folder}")
            except Exception as e:
                logger.debug(f"Error listing Supabase folder {folder}: {e}")
                folder_contents[folder] = set()

        # Step 4: Check membership (no HTTP requests - just set lookups)
        for paper in papers_to_check_cloud:
            doc_dir = paper_doc_dirs[paper]
            clean_doc_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
            folder_files = folder_contents.get(clean_doc_dir, set())

            # Check exact match
            if paper in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper}"
            # Check with .txt extension
            elif not paper.endswith('.txt') and f"{paper}.txt" in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper}.txt"
            # Check without .txt extension (if paper has .txt but file doesn't)
            elif paper.endswith('.txt') and paper[:-4] in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper[:-4]}"
            else:
                missing.append(paper)

        # Combine local and cloud papers for available list
        available = local_papers + list(cloud_papers.keys())

        # Schema-only / first extraction: no table rows yet, but documents were uploaded for discovery
        session_document_filenames: List[str] = []
        if not paper_refs:
            session_document_filenames = self._collect_session_document_filenames(session_id)
            if session_document_filenames:
                for doc_name in session_document_filenames:
                    if doc_name not in local_papers:
                        local_papers.append(doc_name)
                available = list(dict.fromkeys(local_papers + list(cloud_papers.keys())))
                logger.debug(
                    "discover_papers: no row references; using %d on-disk session document(s)",
                    len(session_document_filenames),
                )

        # Build paper to rows mapping (extract display name from tuple key)
        paper_to_rows: Dict[str, List[str]] = {}
        for paper in available:
            paper_to_rows[paper] = [
                rk[0] for rk, rk_papers in row_paper_mapping.items()
                if paper in rk_papers
            ]

        rows_with_papers = sum(1 for papers in row_paper_mapping.values() if papers)

        logger.debug(f"Paper discovery - local: {len(local_papers)}, cloud: {len(cloud_papers)}, missing: {len(missing)}")

        # Backfill papers field for rows with empty papers but available documents
        if local_files and any(not papers for papers in row_paper_mapping.values()):
            logger.debug(f"Some rows have empty papers, attempting to backfill from {len(local_files)} local documents")
            await self._backfill_papers_from_documents(session_id, list(local_files), total_rows)
            # Re-read row_paper_mapping after backfill to update rows_with_papers count
            if data_files:
                row_paper_mapping = {}
                for df in data_files:
                    if df.exists():
                        with open(df, 'r') as f:
                            row_idx = 0
                            for line in f:
                                if line.strip():
                                    row_idx += 1
                                    try:
                                        row = json.loads(line)
                                        row_name = row_name_of(row) or f"row_{row_idx}"
                                        row_src = _resolve_source_document(row)
                                        papers_raw = row.get('papers') or row.get('_papers') or []
                                        if isinstance(papers_raw, list):
                                            row_paper_mapping[(row_name, row_src)] = papers_raw
                                        else:
                                            row_paper_mapping[(row_name, row_src)] = [papers_raw] if papers_raw else []
                                    except json.JSONDecodeError:
                                        continue
                rows_with_papers = sum(1 for papers in row_paper_mapping.values() if papers)
                logger.debug(f"After backfill - rows_with_papers: {rows_with_papers}")

        return {
            "total_rows": total_rows,
            "rows_with_papers": rows_with_papers,
            "available_papers": available,
            "missing_papers": missing,
            "paper_to_rows": paper_to_rows,
            "cloud_papers": cloud_papers,
            "local_papers": local_papers,
            "session_document_count": len(self._collect_session_document_filenames(session_id)),
        }

    async def _backfill_papers_from_documents(
        self,
        session_id: str,
        local_files: List[str],
        total_rows: int
    ):
        """
        Backfill the papers field in data.jsonl for rows with empty papers.

        This handles legacy data where the papers field was not populated
        during initial data creation. It matches rows to documents by index order.

        Args:
            session_id: Session identifier
            local_files: List of local document filenames
            total_rows: Total number of rows in data.jsonl
        """
        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            return

        # Sort local files for consistent ordering
        sorted_docs = sorted(local_files)
        logger.debug(f"Backfill - {len(sorted_docs)} documents available for {total_rows} rows")

        # Read all rows
        rows = []
        with open(data_file, 'r') as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))

        if not rows:
            return

        # Match rows to documents by index
        updated = False
        for idx, row in enumerate(rows):
            papers = row.get('papers') or []
            # Only backfill if papers is empty
            if not papers and idx < len(sorted_docs):
                doc_name = sorted_docs[idx]
                row['papers'] = [doc_name]
                updated = True

        # Write back if any updates were made
        if updated:
            # Backup first
            backup_file = session_dir / f"data_backup_backfill_{int(datetime.now().timestamp())}.jsonl"
            import shutil
            shutil.copy2(data_file, backup_file)

            with open(data_file, 'w') as f:
                for row in rows:
                    f.write(json.dumps(row) + '\n')

    async def download_cloud_papers(
        self,
        session_id: str,
        cloud_papers: Dict[str, str]  # paper_name -> supabase_path
    ) -> List[str]:
        """
        Download papers from Supabase to local documents/ folder.

        Args:
            session_id: Session identifier
            cloud_papers: Mapping of paper names to their Supabase paths

        Returns:
            List of successfully downloaded paper names
        """
        storage = get_storage()
        session_dir = Path("./data") / session_id
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(parents=True, exist_ok=True)

        downloaded = []
        for paper_name, supabase_path in cloud_papers.items():
            try:
                content = await storage.download_file('datasets', supabase_path)
                if content:
                    from app.services.document_preprocessor import commit_bytes_to_documents_dir
                    # Ensure paper_name has the correct extension
                    local_filename = paper_name if '.' in paper_name else f"{paper_name}.txt"
                    committed = commit_bytes_to_documents_dir(content, local_filename, docs_dir)
                    if committed:
                        downloaded.append(paper_name)
                        logger.debug(f"Downloaded {paper_name} from Supabase to {committed}")
            except Exception as e:
                logger.debug(f"Error downloading {paper_name} from Supabase: {e}")

        return downloaded

    # ==================== Re-extraction ====================

    def _collect_renamed_from_history(
        self,
        session: VisualizationSession,
        columns: List[str],
    ) -> Dict[str, str]:
        """Map new column names to their previous names from edit history."""
        renamed: Dict[str, str] = {}
        column_set = set(columns)
        valid_columns = {col.name for col in session.columns if col.name}

        if session.schema_baseline:
            for col_name in columns:
                baseline = session.schema_baseline.columns.get(col_name)
                if (
                    baseline
                    and baseline.name != col_name
                    and baseline.name not in valid_columns
                ):
                    renamed[col_name] = baseline.name

        for mod in reversed(session.modification_history or []):
            if mod.action_type != "column_edited":
                continue
            details = mod.details or {}
            old_name = details.get("original_name")
            new_name = details.get("new_name")
            if (
                new_name
                and old_name
                and new_name != old_name
                and new_name in column_set
                and old_name not in valid_columns
            ):
                renamed.setdefault(new_name, old_name)
        return renamed

    async def resolve_reextraction_columns(
        self,
        session_id: str,
        columns: Optional[List[str]] = None,
        scope: str = "explicit",
    ) -> List[str]:
        """Resolve the column scope for a re-extraction or chat reprocess.

        Shared by the manual REST route and the chat ``reextract`` / ``reprocess``
        tools so both apply the same scoping rules: explicit columns are validated
        against the schema, ``scope='all'`` targets every column, and ``edited_only``
        targets only columns changed since the baseline (never silently widening to
        all). Excerpt/derived columns are always excluded.
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        valid_columns = {col.name for col in session.columns}
        if columns:
            resolved = [c for c in columns if c in valid_columns]
            if not resolved:
                raise ValueError(
                    "None of the requested columns exist in the schema. "
                    "Check the schema for exact column names."
                )
        elif scope == "all":
            resolved = [col.name for col in session.columns]
        else:  # edited_only — do NOT silently widen to all columns
            changes = self.detect_schema_changes(session)
            resolved = changes.get("changed_columns") or changes.get("new_columns") or []
            if not resolved:
                raise ValueError(
                    "No edited or new columns to re-extract. Pass specific "
                    "columns, or scope='all' to re-extract the whole table."
                )

        resolved = [c for c in resolved if not c.lower().endswith("_excerpt")]
        if not resolved:
            raise ValueError("No columns available for re-extraction")
        return resolved

    async def start_gated_reextraction(
        self,
        session_id: str,
        columns: Optional[List[str]] = None,
        scope: str = "explicit",
        capture_baseline: bool = True,
    ) -> Dict[str, Any]:
        """Single gated entry point for re-extraction.

        Both the manual workspace button (via ``POST /schema/reextract``) and the
        chat ``reextract`` tool funnel through here so the gating is identical:
        resolve the column scope, capture a fresh baseline, verify source
        documents are available, then start the operation. Raises ``ValueError``
        with a user-facing message when a gate fails; the caller owns the
        concurrency slot and releases it on error.
        """
        resolved = await self.resolve_reextraction_columns(session_id, columns, scope)

        session = self.session_manager.get_session(session_id)
        renamed_from = (
            self._collect_renamed_from_history(session, resolved) if session else {}
        )

        # Capture baseline AFTER resolving scope (edited_only reads the old baseline)
        # and AFTER collecting rename signals from the old baseline / edit history.
        if capture_baseline:
            await self.capture_and_save_baseline(session_id)

        paper_discovery = await self.discover_papers(session_id)
        availability = await self.precheck_document_availability(
            session_id,
            operation_type="reextraction",
            paper_discovery=paper_discovery,
        )
        if not availability.get("can_proceed", False):
            missing = availability.get("missing_documents") or []
            if missing:
                raise ValueError(
                    f"{len(missing)} source document(s) are unavailable. "
                    "Add them from the Documents tab, then try again."
                )
            raise ValueError(
                "No source documents available. Add the original source "
                "documents from the Documents tab, then try again."
            )

        return await self.start_reextraction(
            session_id,
            resolved,
            renamed_from=renamed_from,
            paper_discovery=paper_discovery,
        )

    async def start_reextraction(
        self,
        session_id: str,
        columns: List[str],
        renamed_from: Optional[Dict[str, str]] = None,
        paper_discovery: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Start a re-extraction operation for selected columns.

        Args:
            session_id: Session identifier
            columns: List of column names to re-extract

        Returns:
            Dictionary with operation details
        """
        if not SCHEMATIQ_AVAILABLE:
            raise RuntimeError("ScheMatiQ components not available for re-extraction")

        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Validate columns exist
        session_column_names = {col.name for col in session.columns}
        invalid_columns = [c for c in columns if c not in session_column_names]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {invalid_columns}")

        # Validate observation_unit (required by value extraction pipeline)
        # Must check before spawning background task to avoid race condition
        # where the error fires before the WebSocket connects.
        if not session.observation_unit:
            inferred_unit_name = None
            data_dir = Path("./data") / session_id
            schematiq_dir = Path("./schematiq_work") / session_id

            # Strategy 1: Check _metadata.observation_unit in raw ScheMatiQ pipeline output
            for data_file in [schematiq_dir / "extracted_data.jsonl", data_dir / "data.jsonl"]:
                if data_file.exists():
                    with open(data_file) as f:
                        for line in f:
                            if line.strip():
                                row = json.loads(line)
                                meta = row.get("_metadata", {})
                                if meta.get("observation_unit"):
                                    inferred_unit_name = meta["observation_unit"]
                                    break
                    if inferred_unit_name:
                        break

            # Strategy 2: Check _unit_name in DataRow format (loaded exports)
            if not inferred_unit_name:
                data_jsonl = data_dir / "data.jsonl"
                if data_jsonl.exists():
                    with open(data_jsonl) as f:
                        for line in f:
                            if line.strip():
                                row = json.loads(line)
                                if row.get("_unit_name"):
                                    # _unit_name has instance names, not the type name.
                                    # Use a generic type name — re-extraction quality is
                                    # unaffected because the schema columns already define
                                    # what to extract.
                                    inferred_unit_name = "entry"
                                    logger.info(
                                        "Inferred observation_unit presence from _unit_name in data rows"
                                    )
                                    break

            if inferred_unit_name:
                logger.info(f"Inferred observation_unit from existing data: {inferred_unit_name}")
                session.observation_unit = ObservationUnitInfo(
                    name=inferred_unit_name,
                    definition=f"An individual {inferred_unit_name}",
                )
            else:
                raise ValueError(
                    "Cannot re-extract: session has no observation unit configured. "
                    "Please set the observation unit before re-extracting."
                )
        # Discover papers (includes on-disk uploads when the table has no rows yet)
        if paper_discovery is None:
            paper_discovery = await self.discover_papers(session_id)
        doc_count = len(paper_discovery.get("available_papers") or [])
        if doc_count == 0:
            doc_count = paper_discovery.get("session_document_count") or 0

        # Validate LLM config before starting background task (fail fast with HTTP error)
        try:
            llm = self._get_llm_from_session(session_id)
        except Exception as e:
            raise ValueError(f"LLM configuration error: {e}")

        # Create operation
        operation_id = str(uuid.uuid4())[:8]
        if renamed_from is None:
            renamed_from = self._collect_renamed_from_history(session, columns)
        operation = ReextractionOperation(
            operation_id=operation_id,
            session_id=session_id,
            columns=columns,
            status="starting",
            renamed_from=renamed_from,
        )
        operation.total_documents = doc_count
        operation.paper_discovery = paper_discovery
        with self._state_lock:
            self.active_operations[operation_id] = operation

        # Start background task and store reference for potential cancellation
        task = asyncio.create_task(self._run_reextraction(operation_id))
        self._extraction_tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "columns": columns,
            "estimated_papers": doc_count,
            "rows_to_process": doc_count,
            "missing_papers": paper_discovery["missing_papers"]
        }

    def _build_known_units_for_reextraction(
        self,
        session_id: str,
        session: VisualizationSession,
        rediscover_observation_units: bool,
    ) -> Dict[str, List[str]]:
        """Map paper stems to known observation-unit names for re-extraction.

        Papers with rows in extracted data map to their unit names. Papers skipped
        during the original extraction (no units found) map to ``[]`` so the lib
        reuses the empty-list skip path instead of running LLM unit discovery.
        """
        if rediscover_observation_units:
            return {}

        known_units: Dict[str, List[str]] = {}
        reextract_data_files = []
        schematiq_extracted = Path("./schematiq_work") / session_id / "extracted_data.jsonl"
        if schematiq_extracted.exists():
            reextract_data_files.append(schematiq_extracted)
        if not reextract_data_files:
            schematiq_data = Path("./schematiq_work") / session_id / "data.jsonl"
            if schematiq_data.exists():
                reextract_data_files.append(schematiq_data)
        load_data = Path("./data") / session_id / "data.jsonl"
        if load_data.exists() and load_data.resolve() not in [
            f.resolve() for f in reextract_data_files
        ]:
            reextract_data_files.append(load_data)

        for df in reextract_data_files:
            try:
                with open(df, "r") as f:
                    for line in f:
                        if line.strip():
                            try:
                                row = json.loads(line)
                                row_name = row.get("_row_name") or row.get("row_name")
                                papers = row.get("_papers") or row.get("papers") or []
                                if isinstance(papers, str):
                                    papers = [papers]
                                for paper in papers:
                                    paper_stem = Path(paper).stem
                                    if paper_stem not in known_units:
                                        known_units[paper_stem] = []
                                    if row_name and row_name not in known_units[paper_stem]:
                                        known_units[paper_stem].append(row_name)
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                logger.warning(f"Error reading data file {df} for known_units: {e}")

        # Skipped docs have no rows; present them as [] (not absent/None) to avoid
        # a wasteful LLM unit-identification call on re-extraction.
        if session.statistics and session.statistics.skipped_documents:
            for skipped in session.statistics.skipped_documents:
                paper_stem = Path(skipped.document).stem
                if paper_stem not in known_units:
                    known_units[paper_stem] = []

        return known_units

    async def _run_reextraction(self, operation_id: str):
        """Execute re-extraction in background."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            logger.debug(f"Operation {operation_id} not found in active_operations")
            return

        set_session_context(operation.session_id)
        LLMCallTracker.get_instance().set_stage("reextraction")
        logger.debug(f"_run_reextraction started for operation {operation_id}")

        try:
            operation.status = "running"
            operation.started_at = datetime.now()
            logger.debug(f"Re-extraction running for session {operation.session_id}, columns: {operation.columns}")

            session = self.session_manager.get_session(operation.session_id)
            if not session:
                raise ValueError(f"Session {operation.session_id} not found")

            data_dir = Path("./data") / operation.session_id
            schematiq_dir = Path("./schematiq_work") / operation.session_id
            session_dir = data_dir  # Keep for schema/output file paths
            docs_dir = data_dir / "documents"
            pending_dir = data_dir / "pending_documents"

            await self.broadcast_event(
                operation.session_id,
                "reextraction_started",
                {
                    "operation_id": operation_id,
                    "columns": operation.columns,
                    "total_documents": operation.total_documents
                }
            )

            # Reuse discovery from the start path (same operation, no disk changes in between)
            paper_discovery = operation.paper_discovery
            if paper_discovery is None:
                paper_discovery = await self.discover_papers(operation.session_id)
            logger.debug(f"Paper discovery result - available: {len(paper_discovery.get('available_papers', []))}, cloud: {len(paper_discovery.get('cloud_papers', {}))}, missing: {len(paper_discovery.get('missing_papers', []))}")

            if paper_discovery.get("cloud_papers"):
                logger.debug(f"Downloading {len(paper_discovery['cloud_papers'])} cloud papers...")
                downloaded = await self.download_cloud_papers(
                    operation.session_id,
                    paper_discovery["cloud_papers"]
                )
                logger.debug(f"Downloaded {len(downloaded)} papers from cloud storage for re-extraction")
            else:
                logger.debug("No cloud papers to download")

            # Get target columns
            target_columns = [
                col for col in session.columns
                if col.name in operation.columns
            ]

            # Build schema for extraction
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

            # Add observation_unit (required by value extraction)
            if session.observation_unit:
                schema_data["observation_unit"] = {
                    "name": session.observation_unit.name,
                    "definition": session.observation_unit.definition,
                }
                if session.observation_unit.example_names:
                    schema_data["observation_unit"]["example_names"] = session.observation_unit.example_names

            # Save schema file
            schema_file = session_dir / f"reextract_schema_{operation_id}.json"
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)

            # Setup LLM and retriever (use cached retriever for performance)
            llm = self._get_llm_from_session(operation.session_id)
            retriever = self.get_cached_retriever()

            output_file = session_dir / f"reextract_output_{operation_id}.jsonl"

            # Track progress via callback
            processed_count = [0]
            doc_index = [0]  # counts source files, incremented by on_document_started

            # Capture event loop before entering thread pool
            loop = asyncio.get_running_loop()

            def on_document_started(paper_title: str):
                """Fired once per source document file — drives the 'X of Y docs' counter."""
                doc_index[0] += 1
                operation.processed_documents = doc_index[0]
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_event(
                            operation.session_id,
                            "document_started",
                            {
                                "document_name": paper_title,
                                "document_index": doc_index[0],
                                "total_documents": operation.total_documents,
                                "columns": operation.columns,
                            }
                        ),
                        loop
                    )
                except Exception as e:
                    logger.warning(f"Document started broadcast error: {e}")

            def on_unit_row_written(unit_row: Dict[str, Any]):
                """Persist merged row to data file before cell broadcasts."""
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        self._merge_incremental_unit_row(
                            operation.session_id,
                            operation.columns,
                            dict(unit_row),
                            operation,
                            renamed_from=operation.renamed_from,
                        ),
                        loop,
                    )
                    future.result(timeout=120)
                except Exception as e:
                    logger.warning(f"Incremental merge failed for unit row: {e}")

            def on_value_extracted(row_name: str, column_name: str, value: Any):
                processed_count[0] += 1
                # processed_documents = source files; processed_count = cells (do not mix)

                # Schedule broadcasts on main event loop from thread (fire and forget)
                try:
                    # 1. Broadcast individual cell value for live table updates
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_cell_extracted(
                            operation.session_id,
                            {
                                "row_name": row_name,
                                "column": column_name,
                                "value": value,
                            },
                        ),
                        loop,
                    )

                    # 2. Broadcast progress for UI indicators
                    docs_done = doc_index[0]
                    total_docs = max(operation.total_documents, 1)
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_event(
                            operation.session_id,
                            "reextraction_progress",
                            {
                                "operation_id": operation_id,
                                "column": column_name,
                                "progress": min(1.0, docs_done / total_docs),
                                "processed_documents": docs_done,
                                "processed_cells": processed_count[0],
                                "total_documents": operation.total_documents,
                                "current_row": row_name
                            }
                        ),
                        loop
                    )
                except Exception as e:
                    logger.warning(f"Broadcast error: {e}")

            # Same directory resolution as discover_papers / precheck
            docs_directories = self._get_session_document_dirs(operation.session_id)
            docs_had_directories = bool(docs_directories)
            logger.info(
                "Re-extraction docs_directories=%s, count=%d",
                [str(d) for d in docs_directories],
                len(docs_directories),
            )
            if operation.total_documents > 0 and not docs_directories:
                raise RuntimeError(
                    "No document directories found on disk for this session. "
                    "Re-upload documents on the Data tab and try again."
                )

            rediscover_observation_units = bool(
                session.metadata.pending_observation_unit_rediscovery
            )

            if docs_directories:
                logger.debug("Starting build_table_jsonl extraction...")

                known_units = self._build_known_units_for_reextraction(
                    operation.session_id,
                    session,
                    rediscover_observation_units,
                )
                if rediscover_observation_units:
                    logger.info(
                        "pending_observation_unit_rediscovery: skipping known_units; "
                        "LLM will re-identify observation units from documents"
                    )

                if known_units:
                    # Update total count to reflect observation units, not just documents
                    total_units = sum(len(units) for units in known_units.values())
                    if total_units > 0:
                        operation.total_documents = total_units
                        logger.info(f"Updated total_documents to {total_units} observation units (from {len(known_units)} papers)")
                    logger.info(f"Built known_units for {len(known_units)} papers: {known_units}")
                elif not rediscover_observation_units:
                    logger.debug("No known_units found from existing data")

                # Create should_stop callback that checks for stop requests
                def should_stop():
                    return self.is_stop_requested(operation_id)

                def run_extraction():
                    return build_table_jsonl(
                        schema_path=schema_file,
                        docs_directories=docs_directories,
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="one_by_one",
                        retrieval_k=10,
                        max_workers=1,
                        on_value_extracted=on_value_extracted,
                        on_unit_row_written=on_unit_row_written,
                        on_document_started=on_document_started,
                        should_stop=should_stop,  # Allow graceful stop
                        known_units=known_units if known_units else None,
                        write_skip_rationale_artifact=session.write_artifacts,
                        reference_context=build_reference_context(session),
                    )

                await asyncio.get_event_loop().run_in_executor(schematiq_thread_pool, run_extraction)
                logger.debug(f"build_table_jsonl completed, output_file exists: {output_file.exists()}")
            else:
                logger.debug("No document directories exist, skipping extraction")

            # Merge results with existing data
            logger.debug("Merging re-extracted data...")
            await self._merge_reextracted_data(
                operation.session_id,
                operation.columns,
                output_file,
                renamed_from=operation.renamed_from,
                initial_matched_keys=operation.incrementally_merged_keys,
            )

            # Update baseline after successful extraction
            await self.capture_and_save_baseline(operation.session_id)

            if rediscover_observation_units and docs_had_directories:
                sess = self.session_manager.get_session(operation.session_id)
                if sess and sess.metadata.pending_observation_unit_rediscovery:
                    sess.metadata.pending_observation_unit_rediscovery = False
                    self.session_manager.update_session(sess)
                    logger.info("Cleared pending_observation_unit_rediscovery after successful re-extraction")

            # Cleanup
            schema_file.unlink(missing_ok=True)
            output_file.unlink(missing_ok=True)

            operation.status = "completed"
            operation.progress = 1.0
            operation.completed_at = datetime.now()

            logger.info(f"Re-extraction completed successfully for operation {operation_id}")

            await self.broadcast_event(
                operation.session_id,
                "reextraction_completed",
                {
                    "operation_id": operation_id,
                    "columns": operation.columns,
                    "status": "success"
                }
            )

            # Archive session data for research (fire-and-forget)
            if self._data_collection_service:
                await self._data_collection_service.trigger_archive(
                    operation.session_id, "reextraction_completion"
                )

            # Enrich source documents with PubMed/DOI links (fire-and-forget)
            if self._pubmed_enrichment_service:
                await self._pubmed_enrichment_service.enrich_session(operation.session_id)

            # Enrich protein rows with UniProt data (fire-and-forget, protein units only)
            if self._uniprot_enrichment_service:
                await self._uniprot_enrichment_service.enrich_session(operation.session_id)

        except Exception as e:
            logger.error(f"Re-extraction FAILED for operation {operation_id}: {e}", exc_info=True)
            operation.status = "failed"
            operation.error = str(e)
            operation.completed_at = datetime.now()

            await self.broadcast_event(
                operation.session_id,
                "reextraction_failed",
                {
                    "operation_id": operation_id,
                    "error": str(e)
                }
            )
            raise
        finally:
            await concurrency_limiter.release(operation.session_id)
            self._cleanup_operation(operation_id)

    @staticmethod
    def _row_names_loosely_match(existing_name: str, extracted_name: str) -> bool:
        """Match when OU rediscovery shortens names (e.g. 'David J. Barron' vs 'Barron')."""
        if not existing_name or not extracted_name:
            return False
        if existing_name == extracted_name:
            return True
        existing_last = existing_name.strip().split()[-1].lower()
        extracted_last = extracted_name.strip().split()[-1].lower()
        return (
            len(existing_last) > 2
            and existing_last == extracted_last
        )

    def _match_extracted_row(
        self,
        row_name: str,
        row_src: str,
        papers: List[str],
        extracted_by_key: Dict[tuple, Dict[str, Any]],
        extracted_by_row_name: Dict[str, List[Dict[str, Any]]],
        extracted_by_paper_stem: Dict[str, List[Dict[str, Any]]],
        matched_extracted_keys: set,
    ) -> Optional[Dict[str, Any]]:
        """Find the re-extraction output row that corresponds to an existing table row."""
        from app.services.data_utils import _resolve_source_document

        row_key = (row_name, row_src) if row_name else None
        if row_key and row_key in extracted_by_key:
            return extracted_by_key[row_key]

        if row_name and row_name in extracted_by_row_name:
            candidates = extracted_by_row_name[row_name]
            if len(candidates) == 1:
                return candidates[0]
            for paper in papers:
                paper_stem = (
                    paper.split("_")[0].lower()
                    if "_" in paper
                    else paper.rsplit(".", 1)[0].lower()
                )
                for cand in candidates:
                    if _resolve_source_document(cand).lower() == paper_stem:
                        return cand

        # OU rediscovery may shorten unit names — match by last name + source document.
        if row_name:
            loose_matches = []
            for ext_key, ext_row in extracted_by_key.items():
                if ext_key in matched_extracted_keys:
                    continue
                ext_name, ext_src = ext_key
                if not self._row_names_loosely_match(row_name, ext_name):
                    continue
                if row_src and ext_src and row_src.lower() != ext_src.lower():
                    continue
                ext_papers = ext_row.get("_papers") or ext_row.get("papers") or []
                if papers and ext_papers and not set(papers) & set(ext_papers):
                    continue
                loose_matches.append(ext_row)
            if len(loose_matches) == 1:
                return loose_matches[0]

        for paper in papers:
            paper_stem = (
                paper.split("_")[0].lower()
                if "_" in paper
                else paper.rsplit(".", 1)[0].lower()
            )
            if paper_stem in extracted_by_paper_stem:
                candidates = extracted_by_paper_stem[paper_stem]
                if len(candidates) == 1:
                    return candidates[0]

        return None

    def _get_incremental_merge_lock(self, session_id: str) -> asyncio.Lock:
        if session_id not in self._incremental_merge_locks:
            self._incremental_merge_locks[session_id] = asyncio.Lock()
        return self._incremental_merge_locks[session_id]

    async def _merge_incremental_unit_row(
        self,
        session_id: str,
        columns: List[str],
        extracted_row: Dict[str, Any],
        operation: ReextractionOperation,
        renamed_from: Optional[Dict[str, str]] = None,
    ) -> None:
        """Persist a single re-extracted unit row and track it for the final merge."""
        from app.services.data_utils import row_dedup_key

        ext_key = row_dedup_key(extracted_row)
        if not ext_key[0]:
            return

        async with self._get_incremental_merge_lock(session_id):
            extracted_by_key = {ext_key: extracted_row}
            await self._merge_extracted_index_into_data_files(
                session_id,
                columns,
                extracted_by_key,
                renamed_from=renamed_from,
                create_backup=False,
                update_session_stats=False,
                initial_matched_keys=operation.incrementally_merged_keys,
            )
            operation.incrementally_merged_keys.add(ext_key)

    async def _merge_reextracted_data(
        self,
        session_id: str,
        columns: List[str],
        extraction_file: Path,
        renamed_from: Optional[Dict[str, str]] = None,
        initial_matched_keys: Optional[Set[tuple]] = None,
    ):
        """Merge re-extracted values with existing data across ALL data files."""
        if not extraction_file.exists():
            return

        from app.services.data_utils import row_dedup_key

        extracted_by_key: Dict[tuple, Dict[str, Any]] = {}
        with open(extraction_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row_data = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed line in extraction output: {line[:100]}")
                    continue
                key = row_dedup_key(row_data)
                if key[0]:
                    extracted_by_key[key] = row_data

        logger.debug(f"Extracted composite keys from extraction file: {list(extracted_by_key.keys())}")

        matched_keys: set = set(initial_matched_keys or ())
        await self._merge_extracted_index_into_data_files(
            session_id,
            columns,
            extracted_by_key,
            renamed_from=renamed_from,
            create_backup=True,
            update_session_stats=True,
            initial_matched_keys=matched_keys,
        )

    async def _merge_extracted_index_into_data_files(
        self,
        session_id: str,
        columns: List[str],
        extracted_by_key: Dict[tuple, Dict[str, Any]],
        renamed_from: Optional[Dict[str, str]] = None,
        *,
        create_backup: bool = True,
        update_session_stats: bool = True,
        initial_matched_keys: Optional[set] = None,
    ) -> None:
        """Apply extracted rows to on-disk session data files."""
        if not extracted_by_key:
            return

        from app.services.data_utils import (
            get_extraction_column_value,
            get_schematiq_work_dir,
            persist_session_data_file,
            remove_column_keys_in_row,
            resolve_session_data_files,
            row_dedup_key,
            session_has_stored_data,
            _resolve_source_document,
        )

        work_dir = get_schematiq_work_dir()
        data_files = await resolve_session_data_files(session_id, work_dir=work_dir)

        extracted_by_row_name: Dict[str, List[Dict[str, Any]]] = {}
        for key, row_data in extracted_by_key.items():
            extracted_by_row_name.setdefault(key[0], []).append(row_data)

        if not data_files:
            if await session_has_stored_data(session_id, work_dir=work_dir):
                raise RuntimeError(
                    f"Cannot merge re-extracted data for session {session_id}: "
                    "existing table data is in storage but could not be loaded locally. "
                    "Refusing to overwrite."
                )

            schematiq_extracted_file = work_dir / session_id / "extracted_data.jsonl"
            schematiq_extracted_file.parent.mkdir(parents=True, exist_ok=True)

            existing_rows: List[Dict[str, Any]] = []
            if schematiq_extracted_file.exists():
                existing_rows = []
                with open(schematiq_extracted_file, "r") as f:
                    for line in f:
                        if line.strip():
                            existing_rows.append(json.loads(line))

            matched_extracted_keys: set = set(initial_matched_keys or ())
            updated_rows = list(existing_rows)
            rows_updated = 0
            new_rows_added = 0

            for row in updated_rows:
                row_name = row_name_of(row)
                row_src = _resolve_source_document(row)
                papers = row.get('papers') or row.get('_papers') or []
                extracted = self._match_extracted_row(
                    row_name,
                    row_src,
                    papers,
                    extracted_by_key,
                    extracted_by_row_name,
                    {},
                    matched_extracted_keys,
                )
                if extracted:
                    rows_updated += 1
                    matched_extracted_keys.add(row_dedup_key(extracted))
                    for col_name in columns:
                        old_name = (renamed_from or {}).get(col_name)
                        if old_name:
                            remove_column_keys_in_row(row, old_name)
                        extracted_value = get_extraction_column_value(extracted, col_name)
                        if extracted_value is not None:
                            if 'data' in row:
                                row['data'][col_name] = extracted_value
                            else:
                                row[col_name] = extracted_value

            for ext_key, ext_row_data in extracted_by_key.items():
                if ext_key in matched_extracted_keys:
                    continue
                updated_rows.append(
                    self._storage_row_from_extraction(ext_row_data, columns=columns)
                )
                new_rows_added += 1
                matched_extracted_keys.add(ext_key)

            with open(schematiq_extracted_file, "w") as f:
                for row in updated_rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            logger.info(
                "Wrote %s with %d rows (%d updated, %d new)",
                schematiq_extracted_file,
                len(updated_rows),
                rows_updated,
                new_rows_added,
            )
            await persist_session_data_file(session_id, schematiq_extracted_file)
            if update_session_stats:
                self._update_session_stats_after_merge(session_id, columns, updated_rows)
            return

        extracted_by_paper_stem: Dict[str, List[Dict[str, Any]]] = {}
        for key, row_data in extracted_by_key.items():
            extracted_by_paper_stem.setdefault(key[0].lower(), []).append(row_data)

        import shutil
        total_rows_updated = 0
        total_new_rows = 0
        all_updated_rows = []
        matched_extracted_keys: set = set(initial_matched_keys or ())
        primary_data_file = data_files[0]

        for data_file in data_files:
            if create_backup:
                backup_file = data_file.parent / f"data_backup_{int(datetime.now().timestamp())}.jsonl"
                shutil.copy2(data_file, backup_file)

            updated_rows = []
            rows_updated = 0
            with open(data_file, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue

                    row = json.loads(line)
                    row_name = row_name_of(row)
                    row_src = _resolve_source_document(row)
                    papers = row.get('papers') or row.get('_papers') or []

                    extracted = self._match_extracted_row(
                        row_name,
                        row_src,
                        papers,
                        extracted_by_key,
                        extracted_by_row_name,
                        extracted_by_paper_stem,
                        matched_extracted_keys,
                    )

                    if extracted:
                        rows_updated += 1
                        matched_extracted_keys.add(row_dedup_key(extracted))

                        for col_name in columns:
                            old_name = (renamed_from or {}).get(col_name)
                            if old_name:
                                remove_column_keys_in_row(row, old_name)
                            extracted_value = get_extraction_column_value(
                                extracted, col_name
                            )
                            if extracted_value is not None:
                                if 'data' in row:
                                    row['data'][col_name] = extracted_value
                                else:
                                    row[col_name] = extracted_value
                                logger.info(
                                    "Merged column %r into row %r from %s",
                                    col_name,
                                    row_name,
                                    data_file.name,
                                )
                    else:
                        logger.debug(
                            "No extracted match for row %r (src=%s) in %s (papers: %s)",
                            row_name,
                            row_src,
                            data_file.name,
                            papers[:3],
                        )

                    updated_rows.append(row)

            new_rows_added = 0
            if data_file.resolve() == primary_data_file.resolve():
                for ext_key, ext_row_data in extracted_by_key.items():
                    if ext_key in matched_extracted_keys:
                        continue

                    new_row = self._storage_row_from_extraction(
                        ext_row_data, columns=columns
                    )
                    updated_rows.append(new_row)
                    new_rows_added += 1
                    matched_extracted_keys.add(ext_key)

                if new_rows_added > 0:
                    logger.info(f"Appended {new_rows_added} new rows to {data_file.name}")

            with open(data_file, 'w') as f:
                for row in updated_rows:
                    f.write(json.dumps(row) + '\n')

            await persist_session_data_file(session_id, data_file)

            total_rows_updated += rows_updated
            total_new_rows += new_rows_added
            all_updated_rows.extend(updated_rows)
            logger.debug(
                "Merged re-extracted data in %s: %d rows updated, %d new rows added",
                data_file,
                rows_updated,
                new_rows_added,
            )

        logger.debug(
            "Merged re-extracted data for %d columns across %d files, "
            "%d rows updated, %d new rows added",
            len(columns),
            len(data_files),
            total_rows_updated,
            total_new_rows,
        )

        if update_session_stats:
            self._update_session_stats_after_merge(session_id, columns, all_updated_rows)

    def _storage_row_from_extraction(
        self,
        ext_row_data: Dict[str, Any],
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Normalize build_table_jsonl output for extracted_data.jsonl (flat runtime format)."""
        from schematiq.value_extraction.utils.schema_builder import (
            align_extraction_keys_to_schema,
        )

        if columns:
            ext_row_data = align_extraction_keys_to_schema(ext_row_data, columns)
        if ext_row_data.get("_row_name"):
            return ext_row_data
        row_name = ext_row_data.get("row_name")
        storage: Dict[str, Any] = {}
        if row_name:
            storage["_row_name"] = row_name
        papers = ext_row_data.get("_papers") or ext_row_data.get("papers") or []
        if papers:
            storage["_papers"] = papers
        nested = ext_row_data.get("data")
        if isinstance(nested, dict):
            for k, v in nested.items():
                if not k.startswith("_"):
                    storage[k] = v
        for k, v in ext_row_data.items():
            if k.startswith("_") and k not in storage:
                storage[k] = v
            elif k not in ("row_name", "papers", "data", "document_directory"):
                storage[k] = v
        if ext_row_data.get("_unit_name"):
            storage["_unit_name"] = ext_row_data["_unit_name"]
        if ext_row_data.get("_source_document"):
            storage["_source_document"] = ext_row_data["_source_document"]
        return storage

    def _cell_value_present(self, row: Dict[str, Any], col_name: str) -> bool:
        val = row.get(col_name)
        if val is None and isinstance(row.get("data"), dict):
            val = row["data"].get(col_name)
        if val is None:
            return False
        if isinstance(val, dict) and "answer" in val:
            return val.get("answer") is not None and val.get("answer") != ""
        return val != ""

    def _update_session_stats_after_merge(
        self,
        session_id: str,
        columns: List[str],
        all_rows: List[Dict[str, Any]],
    ) -> None:
        """Update session statistics and column fill counts after a merge."""
        session = self.session_manager.get_session(session_id)
        if not session:
            return

        if session.statistics:
            session.statistics.total_rows = len(all_rows)
            for col_stat in session.statistics.column_stats:
                if col_stat.name in columns:
                    col_stat.non_null_count = sum(
                        1 for row in all_rows if self._cell_value_present(row, col_stat.name)
                    )
        for col in session.columns:
            if col.name in columns:
                col.non_null_count = sum(
                    1 for row in all_rows if self._cell_value_present(row, col.name)
                )

        session.metadata.processed_documents = session.metadata.total_documents or len(all_rows)
        self.session_manager.update_session(session)
        logger.info(
            "Updated session %s after merge: %d rows, columns=%s",
            session_id,
            len(all_rows),
            columns[:5],
        )

    def _get_llm_from_session(self, session_id: str):
        """Get LLM configuration from session, including API key."""
        session_dir = Path("./data") / session_id

        # Priority 0: Check user_llm_config.json (user-provided config from frontend)
        # This is checked FIRST even in release mode, because it contains the user's API key.
        try:
            user_config_file = session_dir / "user_llm_config.json"
            if user_config_file.exists():
                with open(user_config_file) as f:
                    user_config = json.load(f)
                if not DEVELOPER_MODE:
                    # Release mode: use locked model but with user's API key
                    api_key = user_config.get('api_key')
                    if api_key:
                        logger.info(f"Release mode - using locked LLM {RELEASE_CONFIG['value_extraction_model']} with user API key")
                        return GeminiLLM(
                            model=RELEASE_CONFIG["value_extraction_model"],
                            api_key=api_key,
                            temperature=RELEASE_CONFIG["llm_temperature"]
                        )
                else:
                    logger.debug(f"Using LLM config from user_llm_config.json: {user_config.get('provider')} {user_config.get('model')}, api_key={'present' if user_config.get('api_key') else 'MISSING'}")
                    return schematiq_utils.build_llm(user_config)
        except Exception as e:
            logger.debug(f"Could not load user LLM config: {e}")

        # In release mode without user config, use the release-mode LLM (requires GEMINI_API_KEY env var)
        if not DEVELOPER_MODE:
            logger.info(f"Release mode - using locked LLM: {RELEASE_CONFIG['value_extraction_model']} (no user API key, using env var)")
            return GeminiLLM(
                model=RELEASE_CONFIG["value_extraction_model"],
                temperature=RELEASE_CONFIG["llm_temperature"]
            )

        # Priority 1: Check session's metadata.extracted_schema for llm_configuration
        try:
            session = self.session_manager.get_session(session_id)
            if session and session.metadata.extracted_schema:
                extracted_schema = session.metadata.extracted_schema
                if "llm_configuration" in extracted_schema:
                    llm_config = extracted_schema["llm_configuration"]
                    # Use value_extraction_backend if available, fallback to schema_creation_backend
                    backend_config = llm_config.get("value_extraction_backend") or llm_config.get("schema_creation_backend")
                    if backend_config:
                        logger.debug(f"Using LLM config from session metadata: {backend_config.get('provider')} {backend_config.get('model')}")
                        return schematiq_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug(f"Could not load LLM config from session metadata: {e}")

        # Priority 2: Check parsed_schema.json (contains llm_configuration with api_key)
        try:
            parsed_schema_file = session_dir / "parsed_schema.json"
            if parsed_schema_file.exists():
                with open(parsed_schema_file) as f:
                    parsed_schema = json.load(f)
                if "llm_configuration" in parsed_schema:
                    llm_config = parsed_schema["llm_configuration"]
                    backend_config = llm_config.get("value_extraction_backend") or llm_config.get("schema_creation_backend")
                    if backend_config:
                        logger.debug(f"Using LLM config from parsed_schema.json: {backend_config.get('provider')} {backend_config.get('model')}")
                        return schematiq_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug(f"Could not load LLM config from parsed_schema.json: {e}")

        # Priority 3: Check schematiq_config.json (legacy location)
        try:
            schematiq_config_file = session_dir / "schematiq_config.json"
            if schematiq_config_file.exists():
                with open(schematiq_config_file) as f:
                    schematiq_config = json.load(f)
                backend_config = schematiq_config.get("value_extraction_backend") or schematiq_config.get("schema_creation_backend")
                if backend_config:
                    logger.debug(f"Using LLM config from schematiq_config.json: {backend_config.get('provider')} {backend_config.get('model')}")
                    return schematiq_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug(f"Could not load LLM config from schematiq_config.json: {e}")

        # Fallback: Use default GeminiLLM (will use GEMINI_API_KEY env var)
        logger.debug("Using default GeminiLLM - this will use GEMINI_API_KEY env var")
        return GeminiLLM(model=ModelNames.DEFAULT_VALUE_EXTRACTION, temperature=0)

    async def broadcast_event(self, session_id: str, event_type: str, data: Dict[str, Any]):
        """Broadcast an event via WebSocket."""
        if self.websocket_manager:
            await self.websocket_manager.broadcast_to_session(session_id, {
                "type": event_type,
                "session_id": session_id,
                "data": data,
                "timestamp": datetime.now().isoformat()
            })

    def get_operation_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a re-extraction operation."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return None

        return {
            "operation_id": operation.operation_id,
            "session_id": operation.session_id,
            "status": operation.status,
            "progress": operation.progress,
            "columns": operation.columns,
            "current_column": operation.current_column,
            "processed_documents": operation.processed_documents,
            "total_documents": operation.total_documents,
            "started_at": operation.started_at.isoformat() if operation.started_at else None,
            "completed_at": operation.completed_at.isoformat() if operation.completed_at else None,
            "error": operation.error
        }

    async def precheck_document_availability(
        self,
        session_id: str,
        operation_type: str = "reextraction",
        paper_discovery: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Pre-check document availability before extraction starts.

        Args:
            session_id: Session identifier
            operation_type: Type of operation ('reextraction' or 'continue_discovery')

        Returns:
            Dictionary with detailed document availability information:
            - total_documents: Total unique documents referenced
            - local_documents: List of documents available locally
            - cloud_documents: List of documents available in cloud storage
            - missing_documents: List of documents not found anywhere
            - can_proceed: Whether any documents are available
            - total_rows: Total number of rows in the table
            - rows_with_missing_docs: Number of rows that reference missing documents
        """
        if paper_discovery is None:
            paper_discovery = await self.discover_papers(session_id)
        discovery = paper_discovery

        # Build paper_to_rows mapping (already returned by discover_papers)
        paper_to_rows = discovery.get("paper_to_rows", {})

        # Format local documents with affected rows
        local_documents = []
        for paper in discovery.get("local_papers", []):
            affected_rows = paper_to_rows.get(paper, [])
            local_documents.append({
                "name": paper,
                "status": "local",
                "affected_rows": affected_rows
            })

        # Format cloud documents with affected rows
        cloud_documents = []
        cloud_papers_dict = discovery.get("cloud_papers", {})
        for paper, cloud_path in cloud_papers_dict.items():
            affected_rows = paper_to_rows.get(paper, [])
            cloud_documents.append({
                "name": paper,
                "status": "cloud",
                "cloud_path": cloud_path,
                "affected_rows": affected_rows
            })

        # Format missing documents with affected rows
        # For missing papers, we need to find which rows reference them
        missing_documents = []

        # Find data files from all possible locations
        precheck_data_files = []
        schematiq_extracted = Path("./schematiq_work") / session_id / "extracted_data.jsonl"
        if schematiq_extracted.exists():
            precheck_data_files.append(schematiq_extracted)
        if not precheck_data_files:
            schematiq_data = Path("./schematiq_work") / session_id / "data.jsonl"
            if schematiq_data.exists():
                precheck_data_files.append(schematiq_data)
        data_dir_file = Path("./data") / session_id / "data.jsonl"
        if data_dir_file.exists() and data_dir_file not in precheck_data_files:
            precheck_data_files.append(data_dir_file)

        # Build a mapping from paper name to rows for missing papers
        missing_paper_to_rows: Dict[str, List[str]] = {}
        for data_file in precheck_data_files:
            with open(data_file, 'r') as f:
                row_idx = 0
                for line in f:
                    if line.strip():
                        row_idx += 1
                        try:
                            row = json.loads(line)
                            row_name = row_name_of(row) or f"row_{row_idx}"
                            papers_raw = (
                                row.get('papers') or
                                row.get('_papers') or
                                row.get('Papers') or
                                row.get('data', {}).get('Papers') or
                                row.get('data', {}).get('papers') or
                                []
                            )
                            if isinstance(papers_raw, dict) and 'answer' in papers_raw:
                                papers_raw = papers_raw.get('answer', [])
                            if isinstance(papers_raw, str):
                                papers = [papers_raw] if papers_raw else []
                            elif isinstance(papers_raw, list):
                                papers = papers_raw
                            else:
                                papers = []

                            for paper in papers:
                                if paper in discovery.get("missing_papers", []):
                                    if paper not in missing_paper_to_rows:
                                        missing_paper_to_rows[paper] = []
                                    missing_paper_to_rows[paper].append(row_name)
                        except json.JSONDecodeError:
                            continue

        for paper in discovery.get("missing_papers", []):
            affected_rows = missing_paper_to_rows.get(paper, [])
            missing_documents.append({
                "name": paper,
                "status": "missing",
                "affected_rows": affected_rows
            })

        # Calculate rows with missing docs
        rows_with_missing = set()
        for doc in missing_documents:
            rows_with_missing.update(doc["affected_rows"])

        total_documents = len(local_documents) + len(cloud_documents) + len(missing_documents)
        can_proceed = len(local_documents) + len(cloud_documents) > 0

        return {
            "total_documents": total_documents,
            "local_documents": local_documents,
            "cloud_documents": cloud_documents,
            "missing_documents": missing_documents,
            "can_proceed": can_proceed,
            "total_rows": discovery.get("total_rows", 0),
            "rows_with_missing_docs": len(rows_with_missing)
        }
