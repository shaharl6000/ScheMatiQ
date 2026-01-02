"""
Continue Schema Discovery service for QBSD visualization.
Handles continuing schema discovery with existing schema as starting point,
discovering new columns, and incremental value extraction.
"""

import json
import asyncio
import uuid
import math
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
from datetime import datetime

from app.models.session import (
    ColumnInfo, VisualizationSession, SchemaEvolution, SchemaSnapshot
)
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.storage.factory import get_storage

# Import QBSD components from qbsd-lib
import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
QBSD_LIB_ROOT = PROJECT_ROOT / "qbsd-lib"
sys.path.insert(0, str(QBSD_LIB_ROOT))

try:
    from qbsd.core.qbsd import discover_schema
    from qbsd.core.schema import Schema, Column
    from qbsd.core.llm_backends import GeminiLLM
    from qbsd.core.retrievers import EmbeddingRetriever
    from qbsd.core import utils as qbsd_utils
    from qbsd.value_extraction.main import build_table_jsonl
    QBSD_AVAILABLE = True
except ImportError as e:
    print(f"QBSD components not available for continue discovery service: {e}")
    QBSD_AVAILABLE = False


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

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.active_operations: Dict[str, ContinueDiscoveryOperation] = {}
        self.stop_flags: Dict[str, bool] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    def is_stop_requested(self, operation_id: str) -> bool:
        """Check if stop was requested for an operation."""
        return self.stop_flags.get(operation_id, False)

    def clear_stop_flag(self, operation_id: str) -> None:
        """Clear the stop flag for an operation."""
        self.stop_flags.pop(operation_id, None)

    def _get_data_dir(self) -> Path:
        """Get the data directory path from storage backend."""
        storage = get_storage()
        if hasattr(storage, 'data_dir'):
            return storage.data_dir
        return Path("./data")

    def _get_qbsd_work_dir(self) -> Path:
        """Get the qbsd_work directory path from storage backend."""
        storage = get_storage()
        if hasattr(storage, 'qbsd_work_dir'):
            return storage.qbsd_work_dir
        return Path("./qbsd_work")

    # ==================== Document Discovery ====================

    async def get_available_documents(self, session_id: str) -> Dict[str, Any]:
        """
        Get available document sources for continued discovery.

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

        # Use storage backend's data directory for correct path resolution
        session_dir = self._get_data_dir() / session_id
        data_file = session_dir / "data.jsonl"
        docs_dir = session_dir / "documents"

        # 1. Collect document references from data.jsonl (like reextraction_service)
        all_papers: Set[str] = set()
        if data_file.exists():
            with open(data_file, 'r') as f:
                for line in f:
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
                        if isinstance(papers_raw, str):
                            papers_raw = [papers_raw]
                        for paper in papers_raw:
                            if paper:
                                all_papers.add(paper)
                    except json.JSONDecodeError:
                        continue

        # 2. Check which documents exist locally
        local_docs: Set[str] = set()
        if docs_dir.exists():
            for f in docs_dir.iterdir():
                if f.is_file() and not f.name.startswith('.'):
                    local_docs.add(f.name)

        # 3. Also check qbsd_work/{session_id}/ for original QBSD documents
        qbsd_work_dir = self._get_qbsd_work_dir() / session_id
        if qbsd_work_dir.exists():
            for subdir in qbsd_work_dir.iterdir():
                if subdir.is_dir() and not subdir.name.startswith('.'):
                    for f in subdir.iterdir():
                        if f.is_file() and f.suffix in ['.txt', '.md']:
                            local_docs.add(f.name)

        # 4. Get cloud dataset from session metadata
        cloud_dataset = session.metadata.cloud_dataset if session.metadata else None

        # 5. Check cloud storage for documents referenced in data.jsonl
        cloud_docs: Set[str] = set()
        storage = get_storage()
        papers_to_check_cloud = all_papers - local_docs
        if papers_to_check_cloud and cloud_dataset:
            try:
                cloud_files = await storage.list_folder_files('datasets', cloud_dataset)
                cloud_docs = set(cloud_files) & papers_to_check_cloud
            except Exception as e:
                print(f"DEBUG: Could not check cloud storage: {e}")

        # 6. Combine results - documents that exist either locally or in cloud
        available_docs = local_docs | cloud_docs

        # 7. Get list of all available cloud datasets
        cloud_datasets = []
        try:
            folders = await storage.list_files('datasets', '')
            cloud_datasets = [f['name'] for f in folders if f.get('is_folder', False)]
        except Exception as e:
            print(f"DEBUG: Could not list cloud datasets: {e}")

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
        uploaded_files: Optional[List[str]] = None
    ) -> tuple[Path, List[str], List[str]]:
        """
        Prepare documents for schema discovery.

        Args:
            session_id: Session identifier
            document_source: 'original', 'upload', or 'cloud'
            cloud_dataset: Cloud dataset name (if document_source is 'cloud')
            uploaded_files: List of uploaded filenames (if document_source is 'upload')

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
                            print(f"DEBUG: Could not read {f}: {e}")

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
                                    print(f"DEBUG: Could not read {f}: {e}")

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
                                print(f"DEBUG: Could not decode {file_info['name']} as UTF-8")
            except Exception as e:
                print(f"DEBUG: Error downloading cloud documents: {e}")
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
                            print(f"DEBUG: Could not read {f}: {e}")

        print(f"DEBUG: Prepared {len(documents)} documents from {document_source}")
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
        max_keys_schema: int = 100,
        documents_batch_size: int = 1
    ) -> Dict[str, Any]:
        """
        Start schema discovery continuation.

        Args:
            session_id: Session identifier
            document_source: 'original', 'upload', or 'cloud'
            llm_config: LLM configuration with provider, model, api_key
            cloud_dataset: Cloud dataset name (if using cloud documents)
            max_keys_schema: Maximum schema columns
            documents_batch_size: Documents per batch

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
            "max_keys_schema": max_keys_schema,
            "documents_batch_size": documents_batch_size,
            "query": session.schema_query or ""
        }
        config_file = session_dir / f"continue_discovery_config_{operation_id}.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        # Start background task
        task = asyncio.create_task(self._run_continue_discovery(operation_id))
        self._tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "initial_column_count": len(operation.initial_columns),
            "document_source": document_source
        }

    async def _run_continue_discovery(self, operation_id: str):
        """Execute continued schema discovery in background."""
        print(f"DEBUG: _run_continue_discovery started for operation {operation_id}")
        operation = self.active_operations.get(operation_id)
        if not operation:
            print(f"DEBUG: Operation {operation_id} not found")
            return

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
            print(f"DEBUG: Preparing documents from {config['document_source']}")
            docs_dir, documents, filenames = await self._prepare_documents(
                operation.session_id,
                config["document_source"],
                config.get("cloud_dataset")
            )

            operation.total_documents = len(documents)
            print(f"DEBUG: Prepared {len(documents)} documents")

            if not documents:
                raise ValueError("No documents available for schema discovery")

            # Build initial schema from session columns
            query = config.get("query") or session.schema_query or ""
            initial_schema = self._convert_session_columns_to_schema(session.columns, query)
            print(f"DEBUG: Initial schema has {len(initial_schema.columns)} columns")

            # Build LLM
            llm = qbsd_utils.build_llm(llm_config)

            # Build retriever
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2",
                k=10,
                max_words=768
            )

            # Calculate batches
            batch_size = config.get("documents_batch_size", 1)
            operation.total_batches = math.ceil(len(documents) / batch_size)

            # Run schema discovery with initial schema
            print(f"DEBUG: Starting discover_schema with initial_schema")
            result_schema, contributing_files, non_contributing_files, evolution = discover_schema(
                query=query,
                documents=documents,
                filenames=filenames,
                max_keys_schema=config.get("max_keys_schema", 100),
                llm=llm,
                retriever=retriever,
                documents_batch_size=batch_size,
                context_window_size=llm_config.get("context_window_size", 8192),
                initial_schema=initial_schema,
                max_iters=6
            )

            print(f"DEBUG: discover_schema completed with {len(result_schema.columns)} columns")

            # Check for stop
            if self.is_stop_requested(operation_id):
                operation.status = "stopped"
                operation.completed_at = datetime.now()
                await self.broadcast_event(
                    operation.session_id,
                    "continue_discovery_stopped",
                    {"operation_id": operation_id, "message": "Stopped by user"}
                )
                return

            # Identify new columns
            new_columns = self._identify_new_columns(operation.initial_columns, result_schema)
            operation.new_columns = new_columns
            print(f"DEBUG: Discovered {len(new_columns)} new columns")

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

            # Cleanup config files
            config_file.unlink(missing_ok=True)
            llm_config_file.unlink(missing_ok=True)

        except Exception as e:
            print(f"DEBUG: Continue discovery FAILED: {e}")
            import traceback
            traceback.print_exc()

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
        print(f"DEBUG: Added {len(new_columns_to_add)} new columns to session")

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
        self._tasks[operation_id] = task

        return {
            "status": "started",
            "operation_id": operation_id,
            "columns": selected_columns,
            "row_count": len(rows_to_process) if rows_to_process else "all"
        }

    async def _run_incremental_extraction(self, operation_id: str):
        """Execute incremental value extraction for new columns."""
        print(f"DEBUG: _run_incremental_extraction started for operation {operation_id}")
        operation = self.active_operations.get(operation_id)
        if not operation:
            return

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

            # Setup LLM and retriever
            llm = qbsd_utils.build_llm(llm_config)
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2",
                k=10,
                max_words=768
            )

            output_file = session_dir / f"incremental_output_{operation_id}.jsonl"

            # Count documents
            doc_count = sum(1 for f in docs_dir.iterdir() if f.is_file() and f.suffix in ['.txt', '.md']) if docs_dir.exists() else 0
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
                    print(f"DEBUG: Broadcast error: {e}")

            def should_stop():
                return self.is_stop_requested(operation_id)

            # Run extraction
            if docs_dir.exists() and doc_count > 0:
                print(f"DEBUG: Starting incremental extraction for {len(columns_to_extract)} columns")

                def run_extraction():
                    return build_table_jsonl(
                        schema_path=schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="one_by_one",
                        retrieval_k=10,
                        max_workers=1,
                        on_value_extracted=on_value_extracted,
                        should_stop=should_stop
                    )

                await asyncio.get_event_loop().run_in_executor(None, run_extraction)
                print(f"DEBUG: Incremental extraction completed")

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

        except Exception as e:
            print(f"DEBUG: Incremental extraction FAILED: {e}")
            import traceback
            traceback.print_exc()

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
            print(f"DEBUG: Extraction file not found: {extraction_file}")
            return

        session_dir = self._get_data_dir() / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            print(f"DEBUG: Data file not found: {data_file}")
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

        print(f"DEBUG: Extracted data for {len(extracted_by_row)} rows")

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

        # Write updated data
        with open(data_file, 'w') as f:
            for row in updated_rows:
                f.write(json.dumps(row) + '\n')

        print(f"DEBUG: Merged incremental data for {len(new_columns)} columns, {rows_updated} rows updated")

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

    async def stop_operation(self, operation_id: str) -> Dict[str, Any]:
        """Stop a running operation."""
        operation = self.active_operations.get(operation_id)
        if not operation:
            return {"stopped": False, "message": f"Operation {operation_id} not found"}

        if operation.status in ["completed", "failed", "stopped"]:
            return {"stopped": False, "message": f"Operation already {operation.status}"}

        # Set stop flag
        self.stop_flags[operation_id] = True
        print(f"DEBUG: Stop requested for operation {operation_id}")

        # Cancel task if running
        task = self._tasks.get(operation_id)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        operation.status = "stopped"
        operation.completed_at = datetime.now()

        await self.broadcast_event(
            operation.session_id,
            "continue_discovery_stopped",
            {
                "operation_id": operation_id,
                "phase": operation.phase,
                "message": "Operation stopped by user"
            }
        )

        self.clear_stop_flag(operation_id)

        return {
            "stopped": True,
            "phase": operation.phase,
            "message": "Operation stopped"
        }

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
