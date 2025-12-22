"""
Re-extraction service for QBSD visualization.
Handles schema change detection, paper discovery, and selective re-extraction.
"""

import json
import asyncio
import hashlib
import uuid
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
from datetime import datetime

from app.models.session import (
    ColumnInfo, ColumnBaseline, SchemaBaseline, VisualizationSession
)
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.storage.factory import get_storage

# Import QBSD components from qbsd-lib for value extraction
import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
QBSD_LIB_ROOT = PROJECT_ROOT / "qbsd-lib"
sys.path.insert(0, str(QBSD_LIB_ROOT))

try:
    from qbsd.value_extraction.main import build_table_jsonl
    from qbsd.core.llm_backends import GeminiLLM
    from qbsd.core.retrievers import EmbeddingRetriever
    QBSD_AVAILABLE = True
except ImportError as e:
    print(f"QBSD components not available for reextraction service: {e}")
    QBSD_AVAILABLE = False


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

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ReextractionOperation] = {}

    # ==================== Schema Change Detection ====================

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
        print(f"DEBUG: Captured schema baseline for session {session_id} with {len(baseline.columns)} columns")

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

        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"
        docs_dir = session_dir / "documents"

        # Collect paper references and document directories from all rows
        paper_refs: Set[str] = set()
        row_paper_mapping: Dict[str, List[str]] = {}  # row_name -> [papers]
        paper_doc_dirs: Dict[str, str] = {}  # paper_name -> document_directory
        total_rows = 0

        if data_file.exists():
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        total_rows += 1
                        try:
                            row = json.loads(line)
                            row_name = row.get('row_name') or row.get('_row_name') or f"row_{total_rows}"

                            # Helper to extract value from QBSD answer format or plain value
                            def extract_value(val: Any) -> str:
                                if val is None:
                                    return ''
                                if isinstance(val, dict) and 'answer' in val:
                                    return str(val['answer']) if val['answer'] else ''
                                return str(val) if val else ''

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

                            paper_refs.update(papers)
                            row_paper_mapping[row_name] = papers

                            # Track document directory for each paper
                            for paper in papers:
                                if doc_dir and paper not in paper_doc_dirs:
                                    paper_doc_dirs[paper] = doc_dir

                            # Debug logging
                            print(f"DEBUG: Row {row_name} - papers: {papers}, doc_dir: {doc_dir}")

                        except json.JSONDecodeError:
                            continue

        # Check which papers exist in local storage
        local_files: Set[str] = set()
        if docs_dir.exists():
            for f in docs_dir.iterdir():
                if f.is_file() and not f.name.startswith('.'):
                    local_files.add(f.name)
                    local_files.add(f.stem)  # Also match without extension

        # Categorize papers: local, cloud, or missing
        local_papers: List[str] = []
        cloud_papers: Dict[str, str] = {}  # paper_name -> supabase_path
        missing: List[str] = []

        # Get storage backend for cloud checks
        storage = get_storage()

        for paper in paper_refs:
            # Check local first
            if paper in local_files or f"{paper}.txt" in local_files:
                local_papers.append(paper)
            # Then check Supabase if we have a document directory
            elif paper in paper_doc_dirs:
                doc_dir = paper_doc_dirs[paper]
                # Strip 'datasets/' prefix since we're already checking in the 'datasets' bucket
                clean_doc_dir = doc_dir.replace('datasets/', '', 1) if doc_dir.startswith('datasets/') else doc_dir
                supabase_path = f"{clean_doc_dir}/{paper}"

                print(f"DEBUG: Checking Supabase path: {supabase_path} in 'datasets' bucket")

                # Try to check if file exists in Supabase
                try:
                    exists = await storage.file_exists('datasets', supabase_path)
                    if exists:
                        cloud_papers[paper] = supabase_path
                    else:
                        # Try with .txt extension
                        supabase_path_txt = f"{clean_doc_dir}/{paper}.txt" if not paper.endswith('.txt') else supabase_path
                        exists_txt = await storage.file_exists('datasets', supabase_path_txt)
                        if exists_txt:
                            cloud_papers[paper] = supabase_path_txt
                        else:
                            missing.append(paper)
                except Exception as e:
                    print(f"DEBUG: Error checking Supabase for {paper}: {e}")
                    missing.append(paper)
            else:
                missing.append(paper)

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

        print(f"DEBUG: Paper discovery - local: {len(local_papers)}, cloud: {len(cloud_papers)}, missing: {len(missing)}")

        return {
            "total_rows": total_rows,
            "rows_with_papers": rows_with_papers,
            "available_papers": available,
            "missing_papers": missing,
            "paper_to_rows": paper_to_rows,
            "cloud_papers": cloud_papers,
            "local_papers": local_papers
        }

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
                    print(f"DEBUG: Downloaded {paper_name} from Supabase to {local_path}")
            except Exception as e:
                print(f"DEBUG: Error downloading {paper_name} from Supabase: {e}")

        return downloaded

    def _find_paper_path(self, session_dir: Path, paper_name: str) -> Optional[Path]:
        """Find the actual file path for a paper name."""
        docs_dir = session_dir / "documents"
        if not docs_dir.exists():
            return None

        # Try exact match
        exact = docs_dir / paper_name
        if exact.exists():
            return exact

        # Try with .txt extension
        with_ext = docs_dir / f"{paper_name}.txt"
        if with_ext.exists():
            return with_ext

        # Try matching stem
        for f in docs_dir.iterdir():
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

        # Start background task
        asyncio.create_task(self._run_reextraction(operation_id))

        return {
            "status": "started",
            "operation_id": operation_id,
            "columns": columns,
            "estimated_papers": len(paper_discovery["available_papers"]),
            "rows_to_process": paper_discovery["total_rows"],
            "missing_papers": paper_discovery["missing_papers"]
        }

    async def _run_reextraction(self, operation_id: str):
        """Execute re-extraction in background."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return

        try:
            operation.status = "running"
            operation.started_at = datetime.now()

            session = self.session_manager.get_session(operation.session_id)
            if not session:
                raise ValueError(f"Session {operation.session_id} not found")

            session_dir = Path("./data") / operation.session_id
            docs_dir = session_dir / "documents"

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
            paper_discovery = await self.discover_papers(operation.session_id)
            if paper_discovery.get("cloud_papers"):
                downloaded = await self.download_cloud_papers(
                    operation.session_id,
                    paper_discovery["cloud_papers"]
                )
                print(f"DEBUG: Downloaded {len(downloaded)} papers from cloud storage for re-extraction")

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

            # Save schema file
            schema_file = session_dir / f"reextract_schema_{operation_id}.json"
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)

            # Setup LLM and retriever
            llm = self._get_llm_from_session(operation.session_id)
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2",
                k=10,
                max_words=768
            )

            output_file = session_dir / f"reextract_output_{operation_id}.jsonl"

            # Track progress via callback
            processed_count = [0]

            def on_value_extracted(row_name: str, column_name: str, value: Any):
                processed_count[0] += 1
                operation.processed_documents = processed_count[0]
                # Fire progress event asynchronously
                asyncio.create_task(self.broadcast_event(
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
                ))

            # Run extraction
            if docs_dir.exists():
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: build_table_jsonl(
                        schema_path=schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="one_by_one",
                        retrieval_k=10,
                        max_workers=1,
                        on_value_extracted=on_value_extracted
                    )
                )

            # Merge results with existing data
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

            await self.broadcast_event(
                operation.session_id,
                "reextraction_completed",
                {
                    "operation_id": operation_id,
                    "columns": operation.columns,
                    "status": "success"
                }
            )

        except Exception as e:
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

        # Backup existing data
        backup_file = session_dir / f"data_backup_{int(datetime.now().timestamp())}.jsonl"
        import shutil
        shutil.copy2(data_file, backup_file)

        # Read and update existing rows
        updated_rows = []
        with open(data_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue

                row = json.loads(line)
                row_name = row.get('row_name') or row.get('_row_name')

                if row_name and row_name in extracted_by_row:
                    extracted = extracted_by_row[row_name]

                    # Update only the re-extracted columns
                    for col_name in columns:
                        if col_name in extracted:
                            # Handle nested 'data' structure or flat structure
                            if 'data' in row:
                                row['data'][col_name] = extracted[col_name]
                            else:
                                row[col_name] = extracted[col_name]

                updated_rows.append(row)

        # Write updated data
        with open(data_file, 'w') as f:
            for row in updated_rows:
                f.write(json.dumps(row) + '\n')

        print(f"DEBUG: Merged re-extracted data for {len(columns)} columns across {len(extracted_by_row)} rows")

    def _get_llm_from_session(self, session_id: str):
        """Get LLM configuration from session."""
        try:
            session_dir = Path("./data") / session_id
            qbsd_config_file = session_dir / "qbsd_config.json"

            if qbsd_config_file.exists():
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)

                if "value_extraction_backend" in qbsd_config:
                    backend_config = qbsd_config["value_extraction_backend"]

                    if backend_config["provider"] == "gemini":
                        return GeminiLLM(
                            model=backend_config["model"],
                            max_output_tokens=backend_config.get("max_output_tokens", 2048),
                            temperature=backend_config["temperature"]
                        )
        except Exception as e:
            print(f"DEBUG: Could not load LLM config: {e}")

        return GeminiLLM(model="gemini-2.5-flash-lite", max_output_tokens=2048, temperature=0.1)

    async def broadcast_event(self, session_id: str, event_type: str, data: Dict[str, Any]):
        """Broadcast an event via WebSocket."""
        if self.websocket_manager:
            await self.websocket_manager.broadcast(session_id, {
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
