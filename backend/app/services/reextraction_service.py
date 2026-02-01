"""
Re-extraction service for QBSD visualization.
Handles schema change detection, paper discovery, and selective re-extraction.
"""

import json
import asyncio
import hashlib
import logging
import uuid
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

from app.models.session import (
    ColumnInfo, ColumnBaseline, SchemaBaseline, VisualizationSession
)
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.storage.factory import get_storage

# QBSD library imports
from qbsd.value_extraction.main import build_table_jsonl
from qbsd.value_extraction.core.paper_processor import PaperProcessor
from qbsd.core.schema import Schema, Column
from qbsd.core.llm_backends import GeminiLLM
from qbsd.core.retrievers import EmbeddingRetriever
from qbsd.core import utils as qbsd_utils

QBSD_AVAILABLE = True


class ReextractionOperation:
    """Tracks a running re-extraction operation."""
    def __init__(
        self,
        operation_id: str,
        session_id: str,
        columns: List[str],
        status: str = "pending"
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


class ReextractionService(WebSocketBroadcasterMixin):
    """Handles selective re-extraction of column values after schema changes."""

    # Class-level cached retriever to avoid reloading the model for each extraction
    _cached_retriever = None
    _retriever_config = {
        "model_name": "all-MiniLM-L6-v2",
        "k": 10,
        "max_words": 768
    }

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ReextractionOperation] = {}
        self.stop_flags: Dict[str, bool] = {}  # operation_id -> stop requested
        self._extraction_tasks: Dict[str, asyncio.Task] = {}  # operation_id -> task

    @classmethod
    def get_cached_retriever(cls):
        """Get or create the cached retriever instance."""
        if cls._cached_retriever is None:
            logger.info("Creating cached EmbeddingRetriever (will be reused for all re-extractions)")
            cls._cached_retriever = EmbeddingRetriever(**cls._retriever_config)
        return cls._cached_retriever

    def is_stop_requested(self, operation_id: str) -> bool:
        """Check if stop was requested for an operation."""
        return self.stop_flags.get(operation_id, False)

    def clear_stop_flag(self, operation_id: str) -> None:
        """Clear the stop flag for an operation."""
        self.stop_flags.pop(operation_id, None)

    async def stop_operation(self, operation_id: str) -> Dict[str, Any]:
        """
        Stop a running re-extraction operation.

        Returns:
            Dictionary with stop status and any partial results
        """
        operation = self.active_operations.get(operation_id)
        if not operation:
            return {
                "stopped": False,
                "message": f"Operation {operation_id} not found"
            }

        if operation.status in ["completed", "failed", "stopped"]:
            return {
                "stopped": False,
                "message": f"Operation already {operation.status}"
            }

        # Set stop flag
        self.stop_flags[operation_id] = True
        logger.warning("Stop requested for re-extraction operation %s", operation_id)

        # Cancel the extraction task if it exists
        task = self._extraction_tasks.get(operation_id)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Merge any partial results that were written before stop
        try:
            session_dir = Path("./data") / operation.session_id
            output_file = session_dir / f"reextract_output_{operation_id}.jsonl"
            if output_file.exists():
                logger.info("Merging partial results from %s", output_file)
                await self._merge_reextracted_data(
                    operation.session_id,
                    operation.columns,
                    output_file
                )
                # Clean up temp files
                output_file.unlink(missing_ok=True)
                schema_file = session_dir / f"reextract_schema_{operation_id}.json"
                schema_file.unlink(missing_ok=True)
                logger.info("Partial results merged and temp files cleaned up")
        except Exception as e:
            logger.warning("Could not merge partial results: %s", e)

        # Update operation status
        operation.status = "stopped"
        operation.completed_at = datetime.now()

        # Broadcast stopped event
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

        # Clean up
        self.clear_stop_flag(operation_id)

        return {
            "stopped": True,
            "message": "Re-extraction stopped",
            "processed_documents": operation.processed_documents,
            "total_documents": operation.total_documents
        }

    # ==================== Schema Change Detection ====================

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
            'qbsd_work',       # QBSD working directory
        ]

        for indicator in local_indicators:
            if indicator in path:
                return True

        # Also check if path starts with / and has multiple segments
        # (cloud paths are typically simple folder names like "NES_documents")
        if path.startswith('/') and path.count('/') > 2:
            return True

        return False

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
        logger.debug("Captured schema baseline for session %s with %d columns", session_id, len(baseline.columns))

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

        # Check if baseline exists
        if not session.schema_baseline:
            result["missing_baseline"] = True
            # If no baseline, all columns are considered "new"
            for col in session.columns:
                if col.name and not col.name.lower().endswith('_excerpt'):
                    result["new_columns"].append(col.name)
                    result["column_changes"][col.name] = {
                        "column_name": col.name,
                        "change_type": "new",
                        "old_value": None,
                        "new_value": col.definition or "",
                        "row_count_affected": 0  # Will be filled by caller
                    }
            if result["new_columns"]:
                result["has_changes"] = True
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

                if current_checksum != baseline.checksum:
                    # Determine what changed
                    change_type = self._determine_change_type(col, baseline)
                    result["changed_columns"].append(col.name)
                    result["column_changes"][col.name] = {
                        "column_name": col.name,
                        "change_type": change_type,
                        "old_value": self._get_change_old_value(change_type, baseline),
                        "new_value": self._get_change_new_value(change_type, col),
                        "row_count_affected": 0
                    }

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

    def _collect_paper_references_from_data(
        self,
        data_file: Path,
        session_cloud_dataset: Optional[str]
    ) -> tuple[int, Set[str], Dict[str, List[str]], Dict[str, str]]:
        """
        Read data file and collect paper references from all rows.

        Args:
            data_file: Path to the data.jsonl file
            session_cloud_dataset: Cloud dataset fallback for local paths

        Returns:
            Tuple of (total_rows, paper_refs, row_paper_mapping, paper_doc_dirs)
        """
        paper_refs: Set[str] = set()
        row_paper_mapping: Dict[str, List[str]] = {}
        paper_doc_dirs: Dict[str, str] = {}
        total_rows = 0
        rows_with_papers = 0
        rows_using_source_doc_fallback = 0

        def extract_value(val: Any) -> str:
            """Extract value from QBSD answer format or plain value."""
            if val is None:
                return ''
            if isinstance(val, dict) and 'answer' in val:
                return str(val['answer']) if val['answer'] else ''
            return str(val) if val else ''

        with open(data_file, 'r') as f:
            for line in f:
                if line.strip():
                    total_rows += 1
                    try:
                        row = json.loads(line)
                        row_name = row.get('row_name') or row.get('_row_name') or f"row_{total_rows}"

                        # Get papers from multiple possible locations
                        papers_raw = (
                            row.get('papers') or
                            row.get('_papers') or
                            row.get('Papers') or
                            row.get('data', {}).get('Papers') or
                            row.get('data', {}).get('papers') or
                            []
                        )

                        # Handle QBSD answer format for papers
                        if isinstance(papers_raw, dict) and 'answer' in papers_raw:
                            papers_raw = papers_raw.get('answer', [])

                        if isinstance(papers_raw, str):
                            papers = [papers_raw] if papers_raw else []
                        elif isinstance(papers_raw, list):
                            papers = papers_raw
                        else:
                            papers = []

                        # Fallback to observation unit document fields if papers is empty
                        if not papers:
                            source_doc = (
                                row.get('_source_document') or
                                row.get('source_document') or
                                row.get('_parent_document') or
                                row.get('parent_document') or
                                None
                            )
                            if source_doc:
                                # Handle QBSD answer format
                                if isinstance(source_doc, dict) and 'answer' in source_doc:
                                    source_doc = source_doc.get('answer')
                                if source_doc:
                                    papers = [str(source_doc)]
                                    rows_using_source_doc_fallback += 1
                                    if rows_using_source_doc_fallback <= 3:
                                        logger.info("Row %d using _source_document fallback: %s -> papers=%s",
                                                   total_rows, source_doc, papers)

                        if papers:
                            rows_with_papers += 1

                        # Get document directory from row data
                        doc_dir_raw = (
                            row.get('Document Directory') or
                            row.get('document_directory') or
                            row.get('data', {}).get('Document Directory') or
                            row.get('data', {}).get('document_directory') or
                            None
                        )
                        doc_dir = extract_value(doc_dir_raw)

                        # Clean up doc_dir - handle different path formats
                        if doc_dir:
                            if 'qbsd_work/' in doc_dir or 'qbsd_work\\' in doc_dir:
                                # Extract datasets folder from qbsd_work path if present
                                if 'datasets/' in doc_dir:
                                    doc_dir = 'datasets/' + doc_dir.split('datasets/')[-1]
                                    logger.info("Extracted cloud path from qbsd_work: %s", doc_dir)
                                else:
                                    logger.debug("Detected qbsd_work path without datasets: %s - will use local files", doc_dir)
                                    doc_dir = None
                            elif 'datasets/' in doc_dir:
                                doc_dir = 'datasets/' + doc_dir.split('datasets/')[-1]
                            elif self._is_local_path(doc_dir):
                                logger.debug("Detected local path in document_directory: %s", doc_dir)
                                if session_cloud_dataset:
                                    doc_dir = f"datasets/{session_cloud_dataset}"
                                    logger.debug("Using session cloud_dataset fallback: %s", doc_dir)
                                else:
                                    logger.debug("No cloud_dataset fallback available")
                                    doc_dir = None

                        # If no doc_dir but session has cloud_dataset, use it as fallback
                        if not doc_dir and session_cloud_dataset:
                            doc_dir = f"datasets/{session_cloud_dataset}"
                            if total_rows <= 3:
                                logger.info("Row %d: No doc_dir, using session cloud_dataset fallback: %s",
                                           total_rows, doc_dir)

                        paper_refs.update(papers)
                        row_paper_mapping[row_name] = papers

                        for paper in papers:
                            if doc_dir and paper not in paper_doc_dirs:
                                paper_doc_dirs[paper] = doc_dir

                    except json.JSONDecodeError:
                        continue

        logger.info("Paper collection summary: total_rows=%d, rows_with_papers=%d, "
                   "rows_using_source_doc_fallback=%d, unique_papers=%d, papers_with_doc_dir=%d",
                   total_rows, rows_with_papers, rows_using_source_doc_fallback,
                   len(paper_refs), len(paper_doc_dirs))
        if paper_refs:
            sample_papers = list(paper_refs)[:5]
            logger.info("Sample paper refs: %s", sample_papers)
        if paper_doc_dirs:
            sample_dirs = list(paper_doc_dirs.items())[:3]
            logger.info("Sample paper_doc_dirs: %s", sample_dirs)

        return total_rows, paper_refs, row_paper_mapping, paper_doc_dirs

    def _find_local_documents(
        self,
        session_dir: Path,
        qbsd_work_dir: Path
    ) -> Set[str]:
        """
        Find all local documents in session and qbsd_work directories.

        Args:
            session_dir: Path to session data directory
            qbsd_work_dir: Path to qbsd_work directory

        Returns:
            Set of local document names (with and without extensions)
        """
        local_files: Set[str] = set()
        docs_dir = session_dir / "documents"
        pending_dir = session_dir / "pending_documents"

        local_dirs_to_check = [docs_dir, pending_dir]

        # Add qbsd_work datasets directories
        if qbsd_work_dir.exists():
            datasets_dir = qbsd_work_dir / "datasets"
            if datasets_dir.exists():
                local_dirs_to_check.append(datasets_dir)
                for subdir in datasets_dir.iterdir():
                    if subdir.is_dir():
                        local_dirs_to_check.append(subdir)
                        logger.debug("Added qbsd_work datasets subdirectory: %s", subdir)

        for local_dir in local_dirs_to_check:
            if local_dir.exists():
                dir_file_count = 0
                for f in local_dir.iterdir():
                    if f.is_file() and not f.name.startswith('.'):
                        local_files.add(f.name)
                        local_files.add(f.stem)
                        dir_file_count += 1
                if dir_file_count > 0:
                    logger.info("Found %d files in %s", dir_file_count, local_dir)
            else:
                logger.debug("Directory does not exist: %s", local_dir)

        logger.info("Total local files found: %d unique names/stems", len(local_files))
        if local_files:
            sample_files = list(local_files)[:5]
            logger.info("Sample local files: %s", sample_files)

        return local_files

    async def _categorize_papers_by_location(
        self,
        paper_refs: Set[str],
        local_files: Set[str],
        paper_doc_dirs: Dict[str, str]
    ) -> tuple[List[str], Dict[str, str], List[str]]:
        """
        Categorize papers into local, cloud, or missing.

        Args:
            paper_refs: Set of all paper references
            local_files: Set of local document names
            paper_doc_dirs: Mapping of paper name to document directory

        Returns:
            Tuple of (local_papers, cloud_papers, missing_papers)
        """
        local_papers: List[str] = []
        cloud_papers: Dict[str, str] = {}
        missing: List[str] = []

        storage = get_storage()

        # Step 1: Check local files first
        papers_to_check_cloud: List[str] = []
        no_doc_dir_count = 0
        for paper in paper_refs:
            if paper in local_files or f"{paper}.txt" in local_files:
                local_papers.append(paper)
            elif paper in paper_doc_dirs:
                papers_to_check_cloud.append(paper)
            else:
                missing.append(paper)
                no_doc_dir_count += 1
                if no_doc_dir_count <= 3:
                    logger.info("Paper '%s' not in local_files and no doc_dir mapping", paper)

        logger.info("Categorization step 1: local=%d, need_cloud_check=%d, missing_no_doc_dir=%d",
                   len(local_papers), len(papers_to_check_cloud), no_doc_dir_count)

        # Step 2: Group papers by their cloud folder
        folders_to_check: Dict[str, List[str]] = {}
        for paper in papers_to_check_cloud:
            doc_dir = paper_doc_dirs[paper]
            clean_doc_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
            if clean_doc_dir not in folders_to_check:
                folders_to_check[clean_doc_dir] = []
            folders_to_check[clean_doc_dir].append(paper)

        # Step 3: List each folder ONCE
        folder_contents: Dict[str, set] = {}
        for folder in folders_to_check:
            logger.debug("Listing Supabase folder: %s (checking %d papers)", folder, len(folders_to_check[folder]))
            try:
                folder_contents[folder] = await storage.list_folder_files('datasets', folder)
                logger.debug("Found %d files in %s", len(folder_contents[folder]), folder)
            except Exception as e:
                logger.debug("Error listing Supabase folder %s: %s", folder, e)
                folder_contents[folder] = set()

        # Step 4: Check membership
        for paper in papers_to_check_cloud:
            doc_dir = paper_doc_dirs[paper]
            clean_doc_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
            folder_files = folder_contents.get(clean_doc_dir, set())

            if paper in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper}"
            elif not paper.endswith('.txt') and f"{paper}.txt" in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper}.txt"
            elif paper.endswith('.txt') and paper[:-4] in folder_files:
                cloud_papers[paper] = f"{clean_doc_dir}/{paper[:-4]}"
            else:
                missing.append(paper)

        logger.info("Final categorization: local=%d, cloud=%d, missing=%d",
                   len(local_papers), len(cloud_papers), len(missing))
        if missing:
            sample_missing = missing[:5]
            logger.info("Sample missing papers: %s", sample_missing)

        return local_papers, cloud_papers, missing

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
            - cloud_papers: Mapping of paper name to Supabase path
            - local_papers: Papers found in local documents/ folder
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
            logger.info("Session has cloud_dataset: %s", session_cloud_dataset)
        else:
            logger.info("Session has NO cloud_dataset (metadata=%s)",
                       session.metadata if session.metadata else "None")

        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"
        qbsd_work_dir = Path("./qbsd_work") / session_id
        qbsd_data_file = qbsd_work_dir / "extracted_data.jsonl"

        # Use qbsd_work data file if main data file doesn't exist
        if not data_file.exists() and qbsd_data_file.exists():
            logger.debug("Using qbsd_work data file: %s", qbsd_data_file)
            data_file = qbsd_data_file

        # Collect paper references from data file
        total_rows = 0
        paper_refs: Set[str] = set()
        row_paper_mapping: Dict[str, List[str]] = {}
        paper_doc_dirs: Dict[str, str] = {}

        if data_file.exists():
            total_rows, paper_refs, row_paper_mapping, paper_doc_dirs = \
                self._collect_paper_references_from_data(data_file, session_cloud_dataset)

        # Find local documents
        local_files = self._find_local_documents(session_dir, qbsd_work_dir)

        # Categorize papers by location (local, cloud, missing)
        local_papers, cloud_papers, missing = await self._categorize_papers_by_location(
            paper_refs, local_files, paper_doc_dirs
        )

        # Combine local and cloud papers for available list
        available = local_papers + list(cloud_papers.keys())

        # Build paper to rows mapping
        paper_to_rows: Dict[str, List[str]] = {}
        for paper in available:
            paper_to_rows[paper] = [
                row_name for row_name, papers in row_paper_mapping.items()
                if paper in papers
            ]

        rows_with_papers = sum(1 for papers in row_paper_mapping.values() if papers)

        logger.debug("Paper discovery - local: %d, cloud: %d, missing: %d",
                     len(local_papers), len(cloud_papers), len(missing))

        # Backfill papers field for rows with empty papers but available documents
        if local_files and any(not papers for papers in row_paper_mapping.values()):
            logger.debug("Some rows have empty papers, attempting to backfill from %d local documents",
                         len(local_files))
            await self._backfill_papers_from_documents(session_id, list(local_files), total_rows)
            # Re-read row_paper_mapping after backfill
            row_paper_mapping = self._reread_paper_mapping(data_file)
            rows_with_papers = sum(1 for papers in row_paper_mapping.values() if papers)
            logger.debug("After backfill - rows_with_papers: %d", rows_with_papers)

        return {
            "total_rows": total_rows,
            "rows_with_papers": rows_with_papers,
            "available_papers": available,
            "missing_papers": missing,
            "paper_to_rows": paper_to_rows,
            "cloud_papers": cloud_papers,
            "local_papers": local_papers
        }

    def _reread_paper_mapping(self, data_file: Path) -> Dict[str, List[str]]:
        """Re-read paper mapping from data file after backfill."""
        row_paper_mapping: Dict[str, List[str]] = {}
        if not data_file.exists():
            return row_paper_mapping

        with open(data_file, 'r') as f:
            row_idx = 0
            for line in f:
                if line.strip():
                    row_idx += 1
                    try:
                        row = json.loads(line)
                        row_name = row.get('row_name') or row.get('_row_name') or f"row_{row_idx}"
                        papers_raw = row.get('papers') or row.get('_papers') or []
                        if isinstance(papers_raw, list):
                            row_paper_mapping[row_name] = papers_raw
                        else:
                            row_paper_mapping[row_name] = [papers_raw] if papers_raw else []
                    except json.JSONDecodeError:
                        continue
        return row_paper_mapping

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
        logger.debug("Backfill - %d documents available for %d rows", len(sorted_docs), total_rows)

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
                    # Ensure paper_name has the correct extension
                    local_filename = paper_name if '.' in paper_name else f"{paper_name}.txt"
                    local_path = docs_dir / local_filename
                    local_path.write_bytes(content)
                    downloaded.append(paper_name)
                    logger.debug("Downloaded %s from Supabase to %s", paper_name, local_path)
            except Exception as e:
                logger.debug("Error downloading %s from Supabase: %s", paper_name, e)

        return downloaded

    def _find_paper_path(self, session_dir: Path, paper_name: str) -> Optional[Path]:
        """Find the actual file path for a paper name.

        Checks both documents/ and pending_documents/ directories.
        """
        docs_dir = session_dir / "documents"
        pending_dir = session_dir / "pending_documents"

        # Check both directories
        for search_dir in [docs_dir, pending_dir]:
            if not search_dir.exists():
                continue

            # Try exact match
            exact = search_dir / paper_name
            if exact.exists():
                return exact

            # Try with .txt extension
            with_ext = search_dir / f"{paper_name}.txt"
            if with_ext.exists():
                return with_ext

            # Try matching stem
            for f in search_dir.iterdir():
                if f.is_file() and f.stem == paper_name:
                    return f

        return None

    # ==================== Re-extraction ====================

    async def start_reextraction(
        self,
        session_id: str,
        columns: List[str]
    ) -> Dict[str, Any]:
        """
        Start a re-extraction operation for selected columns.

        Args:
            session_id: Session identifier
            columns: List of column names to re-extract

        Returns:
            Dictionary with operation details
        """
        if not QBSD_AVAILABLE:
            raise RuntimeError("QBSD components not available for re-extraction")

        session = self.session_manager.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")

        # Validate columns exist
        session_column_names = {col.name for col in session.columns}
        invalid_columns = [c for c in columns if c not in session_column_names]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {invalid_columns}")

        # Discover papers
        paper_discovery = await self.discover_papers(session_id)

        # Create operation
        operation_id = str(uuid.uuid4())[:8]
        operation = ReextractionOperation(
            operation_id=operation_id,
            session_id=session_id,
            columns=columns,
            status="starting"
        )
        operation.total_documents = len(paper_discovery["available_papers"])
        self.active_operations[operation_id] = operation

        # Start background task and store reference for potential cancellation
        task = asyncio.create_task(self._run_reextraction(operation_id))
        self._extraction_tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "columns": columns,
            "estimated_papers": len(paper_discovery["available_papers"]),
            "rows_to_process": paper_discovery["total_rows"],
            "missing_papers": paper_discovery["missing_papers"]
        }

    def _build_docs_directories(self, session_id: str) -> List[Path]:
        """
        Build list of directories to search for documents.

        Args:
            session_id: Session identifier

        Returns:
            List of Path objects for document directories
        """
        session_dir = Path("./data") / session_id
        docs_dir = session_dir / "documents"
        pending_dir = session_dir / "pending_documents"

        docs_directories = [d for d in [docs_dir, pending_dir] if d.exists()]

        # Also add qbsd_work datasets directory
        qbsd_work_datasets = Path("./qbsd_work") / session_id / "datasets"
        if qbsd_work_datasets.exists():
            docs_directories.append(qbsd_work_datasets)
            for subdir in qbsd_work_datasets.iterdir():
                if subdir.is_dir():
                    docs_directories.append(subdir)

        return docs_directories

    def _create_extraction_callback(
        self,
        operation: ReextractionOperation,
        operation_id: str,
        loop: asyncio.AbstractEventLoop
    ):
        """
        Create callback for value extraction progress updates.

        Args:
            operation: The reextraction operation being tracked
            operation_id: Operation identifier
            loop: Event loop for async broadcasts

        Returns:
            Callback function for on_value_extracted
        """
        processed_count = [0]
        current_document = [None]
        document_index = [0]

        def on_value_extracted(row_name: str, column_name: str, value: Any):
            processed_count[0] += 1
            operation.processed_documents = processed_count[0]

            # Broadcast document_started when we start processing a new document
            if current_document[0] != row_name:
                current_document[0] = row_name
                document_index[0] += 1
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.broadcast_event(
                            operation.session_id,
                            "document_started",
                            {
                                "document_name": row_name,
                                "document_index": document_index[0],
                                "total_documents": operation.total_documents,
                                "columns": operation.columns
                            }
                        ),
                        loop
                    )
                except Exception as e:
                    logger.warning("Document started broadcast error: %s", e)

            # Schedule broadcasts on main event loop from thread
            try:
                asyncio.run_coroutine_threadsafe(
                    self.broadcast_event(
                        operation.session_id,
                        "cell_extracted",
                        {
                            "row_name": row_name,
                            "column": column_name,
                            "value": value
                        }
                    ),
                    loop
                )

                asyncio.run_coroutine_threadsafe(
                    self.broadcast_event(
                        operation.session_id,
                        "reextraction_progress",
                        {
                            "operation_id": operation_id,
                            "column": column_name,
                            "progress": processed_count[0] / max(operation.total_documents * len(operation.columns), 1),
                            "processed_documents": processed_count[0],
                            "total_documents": operation.total_documents,
                            "current_row": row_name
                        }
                    ),
                    loop
                )
            except Exception as e:
                logger.warning("Broadcast error: %s", e)

        return on_value_extracted

    async def _run_reextraction(self, operation_id: str):
        """Execute re-extraction in background."""
        logger.debug("_run_reextraction started for operation %s", operation_id)
        operation = self.active_operations.get(operation_id)
        if not operation:
            logger.debug("Operation %s not found in active_operations", operation_id)
            return

        try:
            operation.status = "running"
            operation.started_at = datetime.now()
            logger.debug("Re-extraction running for session %s, columns: %s",
                         operation.session_id, operation.columns)

            session = self.session_manager.get_session(operation.session_id)
            if not session:
                raise ValueError(f"Session {operation.session_id} not found")

            session_dir = Path("./data") / operation.session_id

            await self.broadcast_event(
                operation.session_id,
                "reextraction_started",
                {
                    "operation_id": operation_id,
                    "columns": operation.columns,
                    "total_documents": operation.total_documents
                }
            )

            # Download cloud papers before extraction
            await self._download_cloud_papers_if_needed(operation)

            # Get target columns
            target_columns = [
                col for col in session.columns
                if col.name in operation.columns
            ]

            # Save schema file for extraction
            schema_file = session_dir / f"reextract_schema_{operation_id}.json"
            self._save_extraction_schema(schema_file, session.schema_query, target_columns)

            # Setup LLM and retriever
            llm = self._get_llm_from_session(operation.session_id)
            retriever = self.get_cached_retriever()

            output_file = session_dir / f"reextract_output_{operation_id}.jsonl"

            # Create extraction callback
            loop = asyncio.get_running_loop()
            on_value_extracted = self._create_extraction_callback(operation, operation_id, loop)

            # Build docs directories
            docs_directories = self._build_docs_directories(operation.session_id)
            logger.debug("docs_directories=%s, count=%d", docs_directories, len(docs_directories))

            # Run extraction
            def should_stop():
                return self.is_stop_requested(operation_id)

            def run_extraction_for_existing_rows():
                self._extract_for_existing_rows(
                    session_id=operation.session_id,
                    target_columns=target_columns,
                    schema_query=session.schema_query or "Extract information",
                    docs_directories=docs_directories,
                    output_file=output_file,
                    llm=llm,
                    retriever=retriever,
                    on_value_extracted=on_value_extracted,
                    should_stop=should_stop
                )

            await asyncio.get_event_loop().run_in_executor(None, run_extraction_for_existing_rows)
            logger.debug("Extraction for existing rows completed, output_file exists: %s", output_file.exists())

            # Merge results and cleanup
            await self._finalize_reextraction(operation, output_file, schema_file)

        except Exception as e:
            logger.error("Re-extraction FAILED for operation %s: %s", operation_id, e)
            import traceback
            traceback.print_exc()
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

    async def _download_cloud_papers_if_needed(self, operation: ReextractionOperation) -> None:
        """Download cloud papers if any are available."""
        logger.debug("Discovering papers for re-extraction...")
        paper_discovery = await self.discover_papers(operation.session_id)
        logger.debug("Paper discovery result - available: %d, cloud: %d, missing: %d",
                     len(paper_discovery.get('available_papers', [])),
                     len(paper_discovery.get('cloud_papers', {})),
                     len(paper_discovery.get('missing_papers', [])))

        if paper_discovery.get("cloud_papers"):
            logger.debug("Downloading %d cloud papers...", len(paper_discovery['cloud_papers']))
            downloaded = await self.download_cloud_papers(
                operation.session_id,
                paper_discovery["cloud_papers"]
            )
            logger.debug("Downloaded %d papers from cloud storage", len(downloaded))
        else:
            logger.debug("No cloud papers to download")

    def _save_extraction_schema(
        self,
        schema_file: Path,
        schema_query: Optional[str],
        target_columns: List
    ) -> None:
        """Save schema file for extraction."""
        schema_data = {
            "query": schema_query or "Extract information",
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
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2)

    async def _finalize_reextraction(
        self,
        operation: ReextractionOperation,
        output_file: Path,
        schema_file: Path
    ) -> None:
        """Merge results, update baseline, and cleanup after extraction."""
        logger.debug("Merging re-extracted data...")
        await self._merge_reextracted_data(
            operation.session_id,
            operation.columns,
            output_file
        )

        # Update baseline after successful extraction
        await self.capture_and_save_baseline(operation.session_id)

        # Cleanup
        schema_file.unlink(missing_ok=True)
        output_file.unlink(missing_ok=True)

        operation.status = "completed"
        operation.progress = 1.0
        operation.completed_at = datetime.now()

        logger.debug("Re-extraction completed successfully for operation %s", operation.operation_id)

        await self.broadcast_event(
            operation.session_id,
            "reextraction_completed",
            {
                "operation_id": operation.operation_id,
                "columns": operation.columns,
                "status": "success"
            }
        )

    def _extract_for_existing_rows(
        self,
        session_id: str,
        target_columns: List,
        schema_query: str,
        docs_directories: List[Path],
        output_file: Path,
        llm,
        retriever,
        on_value_extracted=None,
        should_stop=None
    ) -> None:
        """
        Extract values for new columns using EXISTING rows instead of re-discovering units.

        This is the correct approach for re-extraction when rows already have observation units.
        Instead of calling build_table_jsonl (which re-discovers units), we:
        1. Read existing rows from extracted_data.jsonl
        2. For each row, extract only the new columns using that row's context
        3. Write extracted values to output file for merging
        """
        # Read existing rows
        qbsd_work_dir = Path("./qbsd_work") / session_id
        data_file = qbsd_work_dir / "extracted_data.jsonl"

        if not data_file.exists():
            data_file = Path("./data") / session_id / "data.jsonl"

        if not data_file.exists():
            logger.debug("No data file found for extraction")
            return

        existing_rows = []
        with open(data_file, 'r') as f:
            for line in f:
                if line.strip():
                    existing_rows.append(json.loads(line))

        logger.debug("Found %d existing rows to process", len(existing_rows))

        # Build document path lookup from all directories
        doc_paths: Dict[str, Path] = {}
        for docs_dir in docs_directories:
            if docs_dir.exists():
                for f in docs_dir.iterdir():
                    if f.is_file() and not f.name.startswith('.'):
                        doc_paths[f.name] = f
                        doc_paths[f.stem] = f

        logger.debug("Found %d documents in %d directories", len(doc_paths), len(docs_directories))

        # Create schema with only target columns
        columns = [
            Column(
                name=col.name,
                definition=col.definition or f"Data field: {col.name}",
                rationale=col.rationale or f"Information for {col.name}",
                allowed_values=col.allowed_values
            )
            for col in target_columns
        ]
        schema = Schema(query=schema_query, columns=columns)

        # Create paper processor
        processor = PaperProcessor(
            llm=llm,
            retriever=retriever,
            on_value_extracted=on_value_extracted,
            should_stop=should_stop
        )

        # Process each existing row
        extracted_rows = []
        for i, row in enumerate(existing_rows):
            if should_stop and should_stop():
                logger.debug("Stop requested, halting extraction")
                break

            row_name = row.get('_row_name') or row.get('row_name') or f'row_{i}'
            papers = row.get('_papers') or row.get('papers') or []

            logger.debug("Processing row %d/%d: %s", i+1, len(existing_rows), row_name)

            # Find document
            doc_path = None
            for paper in papers:
                if paper in doc_paths:
                    doc_path = doc_paths[paper]
                    break
                paper_stem = paper.rsplit('.', 1)[0] if '.' in paper else paper
                if paper_stem in doc_paths:
                    doc_path = doc_paths[paper_stem]
                    break

            if not doc_path:
                logger.debug("No document found for row %s, skipping", row_name)
                # Still add row with null values
                output_row = {"_row_name": row_name}
                for col in columns:
                    output_row[col.name] = None
                extracted_rows.append(output_row)
                continue

            try:
                doc_content = doc_path.read_text(encoding='utf-8', errors='ignore')
            except Exception as e:
                logger.debug("Error reading document %s: %s", doc_path, e)
                continue

            # Get relevant passages for this unit using retriever
            if retriever and doc_content:
                col_context = " ".join([f"{c.name}: {c.definition}" for c in columns])
                query = f"{row_name} {col_context}"
                try:
                    relevant_passages = retriever.retrieve(query, doc_content, k=10)
                    logger.debug("Retrieved %d passages for %s", len(relevant_passages), row_name)
                except Exception as e:
                    logger.debug("Retrieval failed: %s", e)
                    relevant_passages = [doc_content[:5000]]
            else:
                relevant_passages = [doc_content[:5000]]

            # Extract values for this unit
            paper_title = doc_path.stem
            try:
                extracted = processor.extract_values_for_unit(
                    unit_name=row_name,
                    relevant_passages=relevant_passages,
                    schema=schema,
                    max_new_tokens=2048,
                    paper_title=paper_title
                )

                output_row = {"_row_name": row_name}
                if extracted:
                    for col_name, value in extracted.items():
                        output_row[col_name] = value
                        if on_value_extracted:
                            on_value_extracted(row_name, col_name, value)
                    logger.debug("Extracted %d columns for %s", len(extracted), row_name)
                else:
                    logger.debug("No values extracted for %s", row_name)
                    for col in columns:
                        output_row[col.name] = None

                extracted_rows.append(output_row)

            except Exception as e:
                logger.debug("Extraction error for %s: %s", row_name, e)
                import traceback
                traceback.print_exc()

        # Write extracted rows to output file
        if extracted_rows:
            with open(output_file, 'w') as f:
                for row in extracted_rows:
                    f.write(json.dumps(row) + "\n")
            logger.debug("Wrote %d rows to %s", len(extracted_rows), output_file)
        else:
            logger.debug("No rows extracted")

    async def _merge_reextracted_data(
        self,
        session_id: str,
        columns: List[str],
        extraction_file: Path
    ):
        """Merge re-extracted values with existing data."""
        if not extraction_file.exists():
            return

        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"

        # Also check qbsd_work for data file
        qbsd_work_dir = Path("./qbsd_work") / session_id
        qbsd_data_file = qbsd_work_dir / "extracted_data.jsonl"

        if not data_file.exists() and qbsd_data_file.exists():
            logger.debug("Merge using qbsd_work data file: %s", qbsd_data_file)
            data_file = qbsd_data_file
            session_dir = qbsd_work_dir

        if not data_file.exists():
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

        logger.debug("Extracted row names from extraction file: %s", list(extracted_by_row.keys()))

        # Build a mapping from paper name stem to extracted data for fallback matching
        # This handles cases where existing data uses row_1, row_2, etc. but extraction uses paper names
        extracted_by_paper_stem: Dict[str, Dict[str, Any]] = {}
        for row_name, row_data in extracted_by_row.items():
            # The row_name from extraction is typically the paper stem (e.g., "CCTalpha")
            extracted_by_paper_stem[row_name.lower()] = row_data
            logger.debug("Paper stem mapping: '%s' -> extracted data", row_name.lower())

        # Backup existing data
        backup_file = session_dir / f"data_backup_{int(datetime.now().timestamp())}.jsonl"
        import shutil
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
                papers = row.get('papers') or row.get('_papers') or []  # Handle both papers and _papers

                # Try direct row name match first
                extracted = None
                match_type = None

                if row_name and row_name in extracted_by_row:
                    extracted = extracted_by_row[row_name]
                    match_type = "direct"
                else:
                    # Fallback: try to match by paper name stem
                    # Papers are like "CCTalpha_22621903_full.txt", extract stem "CCTalpha"
                    for paper in papers:
                        # Extract paper stem (before first underscore or file extension)
                        paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                        if paper_stem in extracted_by_paper_stem:
                            extracted = extracted_by_paper_stem[paper_stem]
                            match_type = f"paper_stem:{paper_stem}"
                            break

                if extracted:
                    rows_updated += 1

                    # Update the re-extracted columns (add even if no value found)
                    for col_name in columns:
                        # Use extracted value if available, otherwise None
                        col_value = extracted.get(col_name, None)
                        # Handle nested 'data' structure or flat structure
                        if 'data' in row:
                            row['data'][col_name] = col_value
                        else:
                            row[col_name] = col_value

                updated_rows.append(row)

        # Write updated data
        with open(data_file, 'w') as f:
            for row in updated_rows:
                f.write(json.dumps(row) + '\n')

        logger.debug("Merged re-extracted data for %d columns, %d rows updated", len(columns), rows_updated)

        # Update session statistics to reflect new data
        session = self.session_manager.get_session(session_id)
        if session and session.statistics:
            # Recalculate column stats for re-extracted columns
            for col_stat in session.statistics.column_stats:
                if col_stat.name in columns:
                    # Count non-null values for this column
                    non_null_count = sum(
                        1 for row in updated_rows
                        if row.get('data', {}).get(col_stat.name) is not None
                    )
                    old_count = col_stat.non_null_count
                    col_stat.non_null_count = non_null_count
                    logger.debug("Updated stats for column '%s': non_null_count %d -> %d", col_stat.name, old_count, non_null_count)

            # Update session
            self.session_manager.update_session(session)
            logger.debug("Updated session statistics for %d columns", len(columns))

    def _get_llm_from_session(self, session_id: str):
        """Get LLM configuration from session, including API key."""
        session_dir = Path("./data") / session_id

        # Priority 0: Check user_llm_config.json (user-provided config from frontend)
        try:
            user_config_file = session_dir / "user_llm_config.json"
            if user_config_file.exists():
                with open(user_config_file) as f:
                    user_config = json.load(f)
                logger.debug("Using LLM config from user_llm_config.json: %s %s, api_key=%s", user_config.get('provider'), user_config.get('model'), 'present' if user_config.get('api_key') else 'MISSING')
                return qbsd_utils.build_llm(user_config)
        except Exception as e:
            logger.debug("Could not load user LLM config: %s", e)

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
                        logger.debug("Using LLM config from session metadata: %s %s", backend_config.get('provider'), backend_config.get('model'))
                        return qbsd_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug("Could not load LLM config from session metadata: %s", e)

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
                        logger.debug("Using LLM config from parsed_schema.json: %s %s", backend_config.get('provider'), backend_config.get('model'))
                        return qbsd_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug("Could not load LLM config from parsed_schema.json: %s", e)

        # Priority 3: Check qbsd_config.json (legacy location)
        try:
            qbsd_config_file = session_dir / "qbsd_config.json"
            if qbsd_config_file.exists():
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                backend_config = qbsd_config.get("value_extraction_backend") or qbsd_config.get("schema_creation_backend")
                if backend_config:
                    logger.debug("Using LLM config from qbsd_config.json: %s %s", backend_config.get('provider'), backend_config.get('model'))
                    return qbsd_utils.build_llm(backend_config)
        except Exception as e:
            logger.debug("Could not load LLM config from qbsd_config.json: %s", e)

        # Fallback: Use default GeminiLLM (will use GEMINI_API_KEY env var)
        logger.debug("Using default GeminiLLM - this will use GEMINI_API_KEY env var")
        return GeminiLLM(model="gemini-2.5-flash-lite", max_output_tokens=2048, temperature=0)

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
        operation_type: str = "reextraction"
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
        # Use the existing discover_papers method which already categorizes documents
        discovery = await self.discover_papers(session_id)

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
        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"

        # Build a mapping from paper name to rows for missing papers
        missing_paper_to_rows: Dict[str, List[str]] = {}
        if data_file.exists():
            with open(data_file, 'r') as f:
                row_idx = 0
                for line in f:
                    if line.strip():
                        row_idx += 1
                        try:
                            row = json.loads(line)
                            row_name = row.get('row_name') or row.get('_row_name') or f"row_{row_idx}"
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
