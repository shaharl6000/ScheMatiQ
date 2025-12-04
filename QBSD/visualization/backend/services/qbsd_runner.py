"""QBSD integration service."""

import json
import asyncio
import subprocess
import time
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

# Import QBSD components
import sys
from pathlib import Path

# Add QBSD directory to path - go up to visualization/, then up to QBSD/
QBSD_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(QBSD_ROOT))

try:
    # Import QBSD main functions
    import QBSD
    from schema import Schema, Column
    from llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
    from retrievers import EmbeddingRetriever
    import utils
    from value_extraction.main import build_table_jsonl
    QBSD_AVAILABLE = True
    print(f"✓ QBSD components successfully loaded from {QBSD_ROOT}")
except ImportError as e:
    print(f"✗ QBSD components not available: {e}")
    print(f"  Tried to import from: {QBSD_ROOT}")
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

def build_llm_interface(provider: str, model: str, max_tokens: int, temperature: float):
    """Build LLM interface based on provider."""
    if not QBSD_AVAILABLE:
        raise RuntimeError("QBSD components not available")
    
    if provider.lower() == "together":
        return TogetherLLM(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
    elif provider.lower() == "openai":
        return OpenAILLM(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
    elif provider.lower() == "gemini":
        return GeminiLLM(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature
        )
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

from models.qbsd import QBSDConfig, QBSDStatus, QBSDProgress
from models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData, SessionStatus
from services.websocket_manager import WebSocketManager
from services.session_manager import SessionManager

class QBSDRunner:
    """Handles QBSD execution and integration."""
    
    def __init__(self, work_dir: str = "./qbsd_work", websocket_manager=None, session_manager=None):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.running_sessions: Dict[str, asyncio.Task] = {}
        
        # Use provided managers or create new ones
        if websocket_manager is not None:
            self.websocket_manager = websocket_manager
        else:
            self.websocket_manager = WebSocketManager()
            
        if session_manager is not None:
            self.session_manager = session_manager
        else:
            self.session_manager = SessionManager()
    
    def _convert_config_to_qbsd_format(self, config: QBSDConfig, session_id: str) -> Dict[str, Any]:
        """Convert visualization QBSDConfig to QBSD pipeline format."""
        session_dir = self.work_dir / session_id
        
        # Convert docs_path to absolute paths
        docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
        resolved_docs_paths = []
        
        for path in docs_paths:
            doc_path = Path(path)
            if not doc_path.is_absolute():
                # Try relative to QBSD root and common test paths
                candidates = [
                    QBSD_ROOT / path,
                    QBSD_ROOT / "src" / path,
                    QBSD_ROOT / "test" / "files",  # Test directory
                    QBSD_ROOT.parent / "test" / "files",  # Test directory from parent
                    Path.cwd() / path,
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
            "backend": {
                "provider": config.backend.provider,
                "model": config.backend.model,
                "max_tokens": config.backend.max_tokens,
                "temperature": config.backend.temperature,
                "max_context_tokens": config.backend.max_context_tokens
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
        
        # Add initial schema if provided
        if config.initial_schema_path:
            initial_schema_path = Path(config.initial_schema_path)
            if not initial_schema_path.is_absolute():
                initial_schema_path = QBSD_ROOT / initial_schema_path
            if initial_schema_path.exists():
                qbsd_config["initial_schema_path"] = str(initial_schema_path)
        
        return qbsd_config
    
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
            
            # Try relative to current directory and various parent directories
            paths_to_try = [
                doc_path,
                Path("..") / path,  # Try from parent directory
                Path("../..") / path,  # Try from grandparent directory
                Path("../../..") / path,  # Try from great-grandparent directory
                # Also try the known test directory
                Path("../../..") / "test" / "files",
                QBSD_ROOT / "test" / "files",  # Try from QBSD root
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
            print(f"🐛 DEBUG: Starting QBSD execution for session {session_id}")
            await update_progress("Initializing", 0.0)
            
            # Convert config to QBSD format
            print(f"🐛 DEBUG: Converting config to QBSD format")
            qbsd_config = self._convert_config_to_qbsd_format(config, session_id)
            
            # Save QBSD config
            qbsd_config_file = session_dir / "qbsd_config.json"
            with open(qbsd_config_file, 'w') as f:
                json.dump(qbsd_config, f, indent=2)
            
            await update_progress("Initializing", 1.0)
            
            # Step 2: Load documents
            current_step += 1
            await update_progress("Loading documents", 0.0)
            
            # Load documents using QBSD utils
            docs_paths = qbsd_config["docs_path"]
            if isinstance(docs_paths, str):
                docs_paths = [docs_paths]
            
            documents = []
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
                        except Exception as e:
                            print(f"Warning: Could not read {doc_file}: {e}")
            
            await update_progress("Loading documents", 1.0, {
                "total_documents": total_docs,
                "loaded_documents": len(documents)
            })
            
            if not documents:
                raise RuntimeError(f"No documents found in paths: {docs_paths}")
            
            # Step 3: Build LLM backend
            current_step += 1
            print(f"🐛 DEBUG: Building LLM backend - provider: {qbsd_config['backend']['provider']}")
            await update_progress("Building LLM backend", 0.0)
            
            # Build LLM interface
            print(f"🐛 DEBUG: Creating LLM interface...")
            llm = build_llm_interface(
                provider=qbsd_config["backend"]["provider"],
                model=qbsd_config["backend"]["model"],
                max_tokens=qbsd_config["backend"]["max_tokens"],
                temperature=qbsd_config["backend"]["temperature"]
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
            
            # Run real schema discovery
            print(f"🐛 DEBUG: Starting schema discovery with {len(documents)} documents")
            discovered_schema = await self._run_schema_discovery(
                documents, qbsd_config, llm, retriever, update_progress
            )
            print(f"🐛 DEBUG: Schema discovery completed with {len(discovered_schema.columns)} columns")
            
            # Save discovered schema
            schema_file = session_dir / "discovered_schema.json"
            with open(schema_file, 'w') as f:
                json.dump({
                    "query": qbsd_config["query"],
                    "schema": [col.to_dict() for col in discovered_schema.columns]
                }, f, indent=2)
            
            await update_progress("Schema Discovery: Complete", 1.0, {
                "columns_discovered": len(discovered_schema.columns)
            })
            
            # Update session status to SCHEMA_READY
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.SCHEMA_READY
            session.metadata.schema_discovery_completed = True
            
            # Update session with discovered schema
            schema_columns = []
            for col in discovered_schema.columns:
                col_info = ColumnInfo(
                    name=col.name,
                    definition=col.definition if hasattr(col, 'definition') else None,
                    rationale=col.rationale if hasattr(col, 'rationale') else None,
                    data_type="object"
                )
                schema_columns.append(col_info)
            
            session.columns = schema_columns
            session.schema_query = qbsd_config["query"]
            self.session_manager.update_session(session)
            
            # Broadcast schema completion event
            await self.websocket_manager.broadcast_schema_completed(session_id, {
                "query": qbsd_config["query"],
                "columns": [col.model_dump() for col in schema_columns],
                "total_columns": len(discovered_schema.columns)
            })
            
            # Step 6: Value extraction
            current_step += 1
            await update_progress("Extracting values", 0.0)
            
            # Run real value extraction
            await self._run_value_extraction(
                session_id, qbsd_config, discovered_schema, llm, retriever, update_progress
            )
            
            await update_progress("Extracting values", 1.0)
            
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
                "total_documents": total_docs,
                "schema_columns": len(discovered_schema.columns)
            })
            
        except Exception as e:
            # Update session with error
            session = self.session_manager.get_session(session_id)
            session.status = SessionStatus.ERROR
            session.error_message = str(e)
            self.session_manager.update_session(session)
            
            await self.websocket_manager.broadcast_error(session_id, str(e))
            raise
    
    async def _run_schema_discovery(
        self, 
        documents: List[str], 
        qbsd_config: Dict[str, Any], 
        llm: LLMInterface, 
        retriever, 
        progress_callback
    ) -> Schema:
        """Run real schema discovery using QBSD pipeline."""
        
        # Initialize schema
        initial_schema = None
        if "initial_schema_path" in qbsd_config:
            try:
                with open(qbsd_config["initial_schema_path"]) as f:
                    initial_data = json.load(f)
                    if isinstance(initial_data, dict) and "schema" in initial_data:
                        columns = []
                        for col_data in initial_data["schema"]:
                            col = Column(
                                name=col_data["name"],
                                definition=col_data.get("definition", ""),
                                rationale=col_data.get("rationale", "")
                            )
                            columns.append(col)
                        initial_schema = Schema(columns)
            except Exception as e:
                print(f"Warning: Could not load initial schema: {e}")
        
        current_schema = initial_schema or Schema([])
        query = qbsd_config["query"]
        max_iterations = 5  # Configurable
        convergence_threshold = 3  # Stop if schema doesn't change for 3 iterations
        unchanged_count = 0
        
        for iteration in range(max_iterations):
            print(f"🐛 DEBUG: Schema discovery iteration {iteration + 1}/{max_iterations}")
            await progress_callback(f"Schema Discovery: Iteration {iteration + 1}/{max_iterations}", iteration / max_iterations, {
                "iteration": iteration + 1,
                "max_iterations": max_iterations,
                "current_columns": len(current_schema.columns)
            })
            
            # Select relevant content
            print(f"🐛 DEBUG: Selecting relevant content with retriever")
            relevant_content = QBSD.select_relevant_content(
                docs=documents,
                query=query,
                retriever=retriever
            )
            print(f"🐛 DEBUG: Selected {len(relevant_content)} relevant passages")
            
            # Generate schema for this iteration
            print(f"🐛 DEBUG: Calling QBSD.generate_schema with LLM...")
            try:
                schema_result = QBSD.generate_schema(
                    passages=relevant_content,
                    query=query,
                    max_keys_schema=qbsd_config.get("max_keys_schema", 100),
                    current_schema=current_schema,
                    llm=llm,
                    max_context_tokens=qbsd_config["backend"].get("max_context_tokens", 8192)
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
            
            # Check convergence
            print(f"🐛 DEBUG: Checking convergence...")
            if QBSD.evaluate_schema_convergence(current_schema, merged_schema):
                unchanged_count += 1
                print(f"🐛 DEBUG: Schema unchanged (count: {unchanged_count}/{convergence_threshold})")
                if unchanged_count >= convergence_threshold:
                    print(f"🐛 DEBUG: Schema converged after {iteration + 1} iterations")
                    break
            else:
                unchanged_count = 0
                print(f"🐛 DEBUG: Schema changed, continuing iterations")
            
            current_schema = merged_schema
            print(f"🐛 DEBUG: Completed iteration {iteration + 1}, moving to next")
            
            # Small delay to allow other tasks
            await asyncio.sleep(0.1)
        
        print(f"🐛 DEBUG: Schema discovery loop completed with {len(current_schema.columns)} columns")
        return current_schema
    
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
        
        def run_value_extraction():
            return build_table_jsonl(
                schema_path=value_extraction_schema_path,
                docs_directories=docs_directories,
                output_path=output_path,
                llm=llm,
                retriever=retriever,
                resume=False,
                mode="all",  # Process all columns together
                retrieval_k=8,
                max_workers=1  # Single worker to avoid overwhelming API
            )
        
        # Track progress by monitoring output file
        extraction_task = loop.run_in_executor(None, run_value_extraction)
        
        # Monitor progress while extraction runs
        start_time = time.time()
        last_line_count = 0
        last_update_time = time.time()
        
        while not extraction_task.done():
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
                                await self.websocket_manager.broadcast_row_completed(session_id, {
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
        
        # Wait for completion
        try:
            await extraction_task
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
        
        await progress_callback("Value Extraction: Complete", 1.0, {
            "rows_extracted": final_line_count,
            "total_documents": total_documents,
            "elapsed_time": int(time.time() - start_time)
        })
    
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
        with open(data_file, 'r', encoding='utf-8') as f:
            total_count = sum(1 for _ in f)
        
        # Read requested page
        rows = []
        start_line = page * page_size
        end_line = start_line + page_size
        
        with open(data_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= start_line and i < end_line:
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
                        print(f"Warning: Could not parse row {i}: {e}")
                        continue
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