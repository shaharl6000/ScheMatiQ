"""QBSD integration service."""

import json
import asyncio
import subprocess
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime

# Import QBSD components
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from schema import Schema, Column
    from llm_backends import LLMInterface
    import utils
    from value_extraction.main import build_table_jsonl
    QBSD_AVAILABLE = True
except ImportError as e:
    print(f"Note: QBSD components not available in simulation mode: {e}")
    # Create mock classes for simulation
    class Schema:
        def __init__(self, **kwargs):
            pass
    class Column:
        def __init__(self, **kwargs):
            pass
    QBSD_AVAILABLE = False

from models.qbsd import QBSDConfig, QBSDStatus, QBSDProgress
from models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData, SessionStatus
from services.websocket_manager import WebSocketManager
from services.session_manager import SessionManager

class QBSDRunner:
    """Handles QBSD execution and integration."""
    
    def __init__(self, work_dir: str = "./qbsd_work"):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.running_sessions: Dict[str, asyncio.Task] = {}
        self.websocket_manager = WebSocketManager()
        self.session_manager = SessionManager()
    
    async def validate_config(self, config: QBSDConfig) -> Dict[str, Any]:
        """Validate QBSD configuration."""
        errors = []
        warnings = []
        
        # Validate query
        if not config.query.strip():
            errors.append("Query cannot be empty")
        
        # Validate document paths
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        for path in docs_paths:
            doc_path = Path(path)
            print(f"DEBUG: Checking document path: {path} -> {doc_path.absolute()}")
            
            # Try relative to current directory and parent directories
            paths_to_try = [
                doc_path,
                Path("..") / path,  # Try from parent directory
                Path("../..") / path,  # Try from grandparent directory
            ]
            
            path_exists = False
            for try_path in paths_to_try:
                if try_path.exists():
                    path_exists = True
                    print(f"DEBUG: Found path at: {try_path.absolute()}")
                    if not any(try_path.iterdir()):
                        warnings.append(f"Document path appears to be empty: {path}")
                    break
            
            if not path_exists:
                # For testing, just warn instead of error
                warnings.append(f"Document path does not exist: {path} (tried: {[str(p.absolute()) for p in paths_to_try]})")
                # errors.append(f"Document path does not exist: {path}")
        
        # Validate initial schema if provided
        if config.initial_schema_path:
            schema_path = Path(config.initial_schema_path)
            if not schema_path.exists():
                errors.append(f"Initial schema file does not exist: {config.initial_schema_path}")
        
        # Validate backend config
        if not config.backend.provider:
            errors.append("LLM provider must be specified")
        
        if not config.backend.model:
            errors.append("LLM model must be specified")
        
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
            
            await self.websocket_manager.broadcast_error(session_id, str(e))
        
        finally:
            # Clean up
            if session_id in self.running_sessions:
                del self.running_sessions[session_id]
    
    async def _execute_qbsd(self, session_id: str, config: QBSDConfig):
        """Execute the QBSD process."""
        session_dir = self.work_dir / session_id
        
        # Progress tracking
        progress_steps = [
            "Initializing",
            "Loading documents", 
            "Building LLM backend",
            "Setting up retriever",
            "Discovering schema",
            "Extracting values",
            "Finalizing results"
        ]
        
        current_step = 0
        total_steps = len(progress_steps)
        
        async def update_progress(step_name: str, step_progress: float = 0.0, details: Dict[str, Any] = None):
            nonlocal current_step
            await self.websocket_manager.broadcast_progress(session_id, {
                "session_id": session_id,
                "status": "processing",
                "progress": (current_step + step_progress) / total_steps,
                "current_step": step_name,
                "steps_completed": current_step,
                "total_steps": total_steps,
                "details": details or {}
            })
        
        try:
            # Step 1: Initializing
            await update_progress("Initializing", 0.0)
            await asyncio.sleep(0.5)  # Small delay for UI feedback
            
            # Step 2: Load documents
            current_step += 1
            await update_progress("Loading documents", 0.0)
            
            # Count documents for progress
            docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
            total_docs = 0
            for path in docs_paths:
                doc_path = Path(path)
                if doc_path.exists():
                    total_docs += len(list(doc_path.glob("*")))
            
            await update_progress("Loading documents", 1.0, {"total_documents": total_docs})
            
            # Step 3: Build LLM backend
            current_step += 1
            await update_progress("Building LLM backend", 0.0)
            
            # Here we would actually build the LLM backend
            # For now, we'll simulate the process
            await asyncio.sleep(1.0)
            await update_progress("Building LLM backend", 1.0)
            
            # Step 4: Setup retriever
            current_step += 1
            await update_progress("Setting up retriever", 0.0)
            await asyncio.sleep(1.0)
            await update_progress("Setting up retriever", 1.0)
            
            # Step 5: Schema discovery
            current_step += 1
            await update_progress("Discovering schema", 0.0)
            
            # Create QBSD config file
            qbsd_config_file = session_dir / "qbsd_config.json"
            with open(qbsd_config_file, 'w') as f:
                json.dump(config.model_dump(), f, indent=2)
            
            # Run schema discovery (simulation)
            await self._simulate_schema_discovery(session_id, config, update_progress)
            
            # Step 6: Value extraction
            current_step += 1
            await update_progress("Extracting values", 0.0)
            
            # Run value extraction (simulation)
            await self._simulate_value_extraction(session_id, config, update_progress)
            
            # Step 7: Finalize
            current_step += 1
            await update_progress("Finalizing results", 0.0)
            
            # Update session as completed
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.COMPLETED
            self.session_manager.update_session(session)
            
            await update_progress("Finalizing results", 1.0)
            
            # Broadcast completion
            await self.websocket_manager.broadcast_completion(session_id, {
                "message": "QBSD execution completed successfully",
                "total_documents": total_docs
            })
            
        except Exception as e:
            await self.websocket_manager.broadcast_error(session_id, str(e))
            raise
    
    async def _simulate_schema_discovery(self, session_id: str, config: QBSDConfig, progress_callback):
        """Simulate schema discovery process."""
        # This is a simulation - in reality would call actual QBSD functions
        
        for i in range(5):  # Simulate 5 iterations
            await progress_callback("Discovering schema", i / 5, 
                                  {"iteration": i + 1, "max_iterations": 5})
            await asyncio.sleep(0.5)
        
        # Create a mock discovered schema
        mock_columns = [
            ColumnInfo(name="protein_name", definition="The name of the protein", rationale="Essential for identification"),
            ColumnInfo(name="has_nuclear_export_signal", definition="Whether the protein has NES", rationale="Main query target"),
            ColumnInfo(name="nes_sequence", definition="The NES amino acid sequence", rationale="Specific sequence information"),
            ColumnInfo(name="nes_strength", definition="Strength of the NES", rationale="Quantitative measure")
        ]
        
        # Save discovered schema
        schema_file = self.work_dir / session_id / "discovered_schema.json"
        schema_data = {
            "query": config.query,
            "schema": [col.model_dump() for col in mock_columns]
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2)
        
        # Update session with discovered schema
        session = self.session_manager.get_session(session_id)
        session.columns = mock_columns
        self.session_manager.update_session(session)
        
        await progress_callback("Discovering schema", 1.0, {"columns_discovered": len(mock_columns)})
    
    async def _simulate_value_extraction(self, session_id: str, config: QBSDConfig, progress_callback):
        """Simulate value extraction process."""
        # This would call the actual value extraction in real implementation
        
        # Count documents for progress tracking
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        total_docs = 0
        for path in docs_paths:
            doc_path = Path(path)
            if doc_path.exists():
                total_docs += len(list(doc_path.glob("*")))
        
        # Simulate processing documents
        for i in range(total_docs):
            await progress_callback("Extracting values", i / total_docs, 
                                  {"documents_processed": i + 1, "total_documents": total_docs})
            await asyncio.sleep(0.1)
        
        # Create mock extracted data
        mock_data = []
        for i in range(min(10, total_docs)):  # Mock up to 10 rows
            row = DataRow(
                row_name=f"protein_{i}",
                papers=[f"paper_{i}_1", f"paper_{i}_2"],
                data={
                    "protein_name": f"Protein {i}",
                    "has_nuclear_export_signal": "Yes" if i % 2 == 0 else "No",
                    "nes_sequence": f"SEQUENCE{i}" if i % 2 == 0 else None,
                    "nes_strength": f"Strong" if i % 3 == 0 else "Weak"
                }
            )
            mock_data.append(row)
        
        # Save extracted data
        data_file = self.work_dir / session_id / "extracted_data.jsonl"
        with open(data_file, 'w') as f:
            for row in mock_data:
                f.write(json.dumps(row.model_dump()) + '\n')
        
        await progress_callback("Extracting values", 1.0, {"rows_extracted": len(mock_data)})
    
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
        
        # Fall back to session schema
        session = self.session_manager.get_session(session_id)
        if session and session.columns:
            return {
                "query": session.schema_query,
                "schema": [col.model_dump() for col in session.columns]
            }
        
        return {"query": "", "schema": []}
    
    async def get_data(self, session_id: str, page: int = 0, page_size: int = 50) -> PaginatedData:
        """Get extracted data."""
        data_file = self.work_dir / session_id / "extracted_data.jsonl"
        if not data_file.exists():
            return PaginatedData(rows=[], total_count=0, page=page, page_size=page_size, has_more=False)
        
        # Count total rows
        with open(data_file) as f:
            total_count = sum(1 for _ in f)
        
        # Read requested page
        rows = []
        start_line = page * page_size
        end_line = start_line + page_size
        
        with open(data_file) as f:
            for i, line in enumerate(f):
                if i >= start_line and i < end_line:
                    row_data = json.loads(line)
                    rows.append(DataRow(**row_data))
                elif i >= end_line:
                    break
        
        return PaginatedData(
            rows=rows,
            total_count=total_count,
            page=page,
            page_size=page_size,
            has_more=end_line < total_count
        )
    
    async def stop_execution(self, session_id: str) -> bool:
        """Stop QBSD execution."""
        if session_id in self.running_sessions:
            task = self.running_sessions[session_id]
            task.cancel()
            del self.running_sessions[session_id]
            
            # Update session status
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = "Execution stopped by user"
            self.session_manager.update_session(session)
            
            await self.websocket_manager.broadcast_error(session_id, "Execution stopped by user")
            return True
        
        return False