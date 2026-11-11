"""
Schema management service for ScheMatiQ visualization.
Handles schema editing operations and document reprocessing.
"""

import json
import asyncio
import functools
import logging
import threading
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime

from app.services.atomic_jsonl import write_jsonl_atomic
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.services import schematiq_thread_pool, concurrency_limiter, find_session_data_file
from app.core.config import DEFAULT_DATA_DIR
from app.services.pipeline.llm_factory import resolve_llm_for_session

logger = logging.getLogger(__name__)

# ScheMatiQ library imports
from schematiq.value_extraction.main import build_table_jsonl
from app.services.session_documents import committed_docs_dir


class SchemaManager(WebSocketBroadcasterMixin):
    """Manages schema editing operations and document reprocessing."""
    
    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.reprocessing_status: Dict[str, Dict[str, Any]] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self._stop_flags: Dict[str, bool] = {}  # session_id -> stop requested
        self._state_lock = threading.Lock()

    def is_stop_requested(self, session_id: str) -> bool:
        """Check if stop was requested for an operation on this session."""
        with self._state_lock:
            return self._stop_flags.get(session_id, False)

    def request_stop(self, session_id: str) -> None:
        """Request stop for an operation on this session."""
        with self._state_lock:
            self._stop_flags[session_id] = True

    def clear_stop_flag(self, session_id: str) -> None:
        """Clear the stop flag for a session."""
        with self._state_lock:
            self._stop_flags.pop(session_id, None)
        
    def _get_value_extraction_llm_from_session(self, session_id: str):
        """Get value extraction LLM configuration from session, including API key."""
        return resolve_llm_for_session(session_id, session_manager=self.session_manager)

    async def reprocess_column(self, session_id: str, column_name: str):
        """Reprocess documents for a specific column after editing."""
        # Track this task for stop support
        with self._state_lock:
            self._stop_flags.pop(session_id, None)

        try:
            session = self.session_manager.get_session(session_id)
            if not session:
                return

            await self.broadcast_progress(
                session_id,
                f"Starting reprocessing for column '{column_name}'",
                0.0,
                "reprocessing_column"
            )

            # Update reprocessing status
            self.reprocessing_status[session_id] = {
                "is_running": True,
                "progress": 0.0,
                "current_column": column_name,
                "columns_processed": 0,
                "total_columns": 1,
                "start_time": datetime.now().isoformat()
            }

            # Find the column definition
            column = next((col for col in session.columns if col.name == column_name), None)
            if not column:
                raise ValueError(f"Column '{column_name}' not found")

            # Create schema for value extraction
            schema_data = {
                "query": session.schema_query or "Extract information",
                "schema": [{
                    "column": column.name,
                    "definition": column.definition or "",
                    "explanation": column.rationale or ""
                }]
            }

            # Add observation_unit (required by value extraction)
            if session.observation_unit:
                schema_data["observation_unit"] = {
                    "name": session.observation_unit.name,
                    "definition": session.observation_unit.definition,
                }
                if session.observation_unit.example_names:
                    schema_data["observation_unit"]["example_names"] = session.observation_unit.example_names

            # Save temporary schema file
            session_dir = Path(DEFAULT_DATA_DIR) / session_id
            schema_file = session_dir / f"temp_schema_{column_name}.json"
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)

            # Setup LLM and retriever for extraction
            llm = self._get_value_extraction_llm_from_session(session_id)
            from app.services import get_shared_retriever
            retriever = get_shared_retriever()

            # Find documents directory
            docs_dir = committed_docs_dir(session_dir)
            if not docs_dir.exists():
                # Look for uploaded documents in session
                if session.metadata.uploaded_documents:
                    # Create docs directory and copy files if needed
                    docs_dir.mkdir(exist_ok=True)

            if docs_dir.exists():
                # Extract values for the column
                output_file = session_dir / f"reprocessed_{column_name}.jsonl"

                # Create should_stop callback for graceful cancellation
                def should_stop():
                    return self.is_stop_requested(session_id)

                await asyncio.get_event_loop().run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(
                        build_table_jsonl,
                        schema_path=schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="all",
                        retrieval_k=8,
                        max_workers=1,
                        should_stop=should_stop,
                        write_skip_rationale_artifact=session.write_artifacts,
                    )
                )

                # Update existing data with new values
                await self._update_column_data(session_id, column_name, output_file)

            # Update progress
            self.reprocessing_status[session_id].update({
                "is_running": False,
                "progress": 1.0,
                "columns_processed": 1
            })

            await self.broadcast_completion(
                session_id,
                f"Reprocessing completed for column '{column_name}'"
            )

            # Capture schema baseline after reprocessing completes
            self.session_manager.capture_schema_baseline(session_id)

            # Cleanup temporary files
            schema_file.unlink(missing_ok=True)
            if 'output_file' in locals():
                output_file.unlink(missing_ok=True)

        except Exception as e:
            self.reprocessing_status[session_id] = {
                "is_running": False,
                "progress": 0.0,
                "error": str(e)
            }
            await self.broadcast_error(session_id, f"Reprocessing failed: {str(e)}")
            raise
        finally:
            self.clear_stop_flag(session_id)
            await concurrency_limiter.release(session_id)
    
    async def remove_column_data(self, session_id: str, column_name: str):
        """Remove a column's data from all existing records, including excerpt columns."""
        try:
            data_file = await find_session_data_file(session_id)

            if not data_file:
                await self.broadcast_progress(
                    session_id,
                    f"No data file found for column '{column_name}' deletion",
                    1.0,
                    "column_deleted"
                )
                return
            
            # Determine all column variants to remove
            columns_to_remove = [column_name]
            excerpt_column = f"{column_name}_excerpt"
            
            # Read existing data and determine what columns actually exist
            columns_found = set()
            existing_rows = []
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        row_data = json.loads(line)
                        existing_rows.append(row_data)
                        if 'data' in row_data:
                            columns_found.update(row_data['data'].keys())
            
            # Add excerpt column if it exists in the data
            if excerpt_column in columns_found:
                columns_to_remove.append(excerpt_column)
            
            logger.debug(f"Removing columns {columns_to_remove} from {len(existing_rows)} rows")
            
            # Remove the columns from all rows
            updated_rows = []
            columns_removed_count = 0
            for row_data in existing_rows:
                row_updated = False
                if 'data' in row_data:
                    for col_to_remove in columns_to_remove:
                        if col_to_remove in row_data['data']:
                            del row_data['data'][col_to_remove]
                            row_updated = True
                            logger.debug(f"Removed column '{col_to_remove}' from row: {row_data.get('row_name', 'unknown')}")
                
                if row_updated:
                    columns_removed_count += 1
                
                updated_rows.append(row_data)
            
            # Write back updated data
            write_jsonl_atomic(data_file, updated_rows)
            
            logger.info(f"Successfully removed columns {columns_to_remove} from {columns_removed_count} rows")
            
            await self.broadcast_progress(
                session_id,
                f"Column '{column_name}' and related data removed from {columns_removed_count} rows",
                1.0,
                "column_deleted"
            )
            
            # Also check for and update data.json if it exists (backup format)
            session_dir = data_file.parent
            json_data_file = session_dir / "data.json"
            if json_data_file.exists():
                try:
                    with open(json_data_file, 'r') as f:
                        json_data = json.load(f)
                    
                    if 'rows' in json_data and isinstance(json_data['rows'], list):
                        for row in json_data['rows']:
                            for col_to_remove in columns_to_remove:
                                if col_to_remove in row:
                                    del row[col_to_remove]
                    
                    with open(json_data_file, 'w') as f:
                        json.dump(json_data, f, indent=2)
                    
                    logger.debug("Also updated data.json file")
                except Exception as json_error:
                    logger.warning(f"Could not update data.json: {json_error}")
            
        except Exception as e:
            logger.error(f"Failed to remove column data: {e}", exc_info=True)
            await self.broadcast_error(session_id, f"Failed to remove column data: {str(e)}")
            raise
    
    async def merge_column_data(self, session_id: str, source_columns: List[str], target_column: str, strategy: str, separator: str = " | "):
        """Merge data from multiple columns into a new column."""
        try:
            await self.broadcast_progress(
                session_id,
                f"Merging columns {source_columns} into '{target_column}'",
                0.0,
                "merging_columns"
            )
            
            data_file = await find_session_data_file(session_id)

            if not data_file:
                return

            # Read and update data
            updated_rows = []
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        row_data = json.loads(line)

                        # Extract source values
                        source_values = []
                        for col_name in source_columns:
                            if 'data' in row_data and col_name in row_data['data']:
                                value = row_data['data'][col_name]
                                if value and str(value).strip():
                                    source_values.append(str(value))
                        
                        # Merge values based on strategy
                        merged_value = self._merge_values(source_values, strategy, separator)
                        
                        # Add merged column and remove source columns
                        if 'data' not in row_data:
                            row_data['data'] = {}
                        
                        row_data['data'][target_column] = merged_value
                        
                        # Remove source columns
                        for col_name in source_columns:
                            if col_name in row_data['data']:
                                del row_data['data'][col_name]
                        
                        updated_rows.append(row_data)
            
            # Write back updated data
            write_jsonl_atomic(data_file, updated_rows)

            await self.broadcast_completion(
                session_id,
                f"Successfully merged {len(source_columns)} columns into '{target_column}'"
            )
            
        except Exception as e:
            await self.broadcast_error(session_id, f"Failed to merge columns: {str(e)}")
            raise
    
    def _merge_values(self, values: List[str], strategy: str, separator: str) -> str:
        """Merge values according to the specified strategy."""
        if not values:
            return ""

        # Normalize strategy: map frontend names and uppercase for matching
        strategy_map = {
            "FIRST_NON_EMPTY": "TAKE_FIRST",
            "SMART_MERGE": "COMBINE_UNIQUE",
        }
        normalized = strategy.upper()
        normalized = strategy_map.get(normalized, normalized)

        if normalized == "CONCATENATE":
            return separator.join(values)
        elif normalized == "COMBINE_UNIQUE":
            unique_values = list(dict.fromkeys(values))  # Preserve order
            return separator.join(unique_values)
        elif normalized == "TAKE_FIRST":
            return values[0]
        elif normalized == "TAKE_LONGEST":
            return max(values, key=len)
        else:
            # Default to concatenate
            return separator.join(values)
    
    async def _update_column_data(self, session_id: str, column_name: str, extraction_file: Path):
        """Update existing data with newly extracted values for a column.

        Values are matched to existing rows by a composite key (row name +
        source document), with row-name and paper-stem fallbacks. Matching by
        key rather than by file position keeps each value aligned to the
        correct observation unit even when the extraction file and the main
        data file differ in row order or count (e.g. after dedup, sorting, or
        sampling).
        """
        if not extraction_file.exists():
            return

        from app.services.data_utils import row_dedup_key, _resolve_source_document

        # Index extracted values by key, with fallbacks.
        extracted_by_key: Dict[tuple, Any] = {}
        extracted_by_row_name: Dict[str, List[tuple]] = {}
        extracted_by_paper_stem: Dict[str, List[tuple]] = {}
        with open(extraction_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                row_data = json.loads(line)
                if column_name not in row_data:
                    continue
                key = row_dedup_key(row_data)
                value = row_data.get(column_name)
                if key[0] and value is not None:
                    extracted_by_key[key] = value
                    extracted_by_row_name.setdefault(key[0], []).append((key, value))
                    extracted_by_paper_stem.setdefault(key[0].lower(), []).append((key, value))

        # Update main data file
        data_file = await find_session_data_file(session_id)

        if not data_file:
            return

        updated_rows = []
        with open(data_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                row_data = json.loads(line)
                row_name = row_data.get('row_name') or row_data.get('_row_name')
                row_src = _resolve_source_document(row_data)
                papers = row_data.get('papers') or []

                # Match by composite key first, then row-name, then paper-stem.
                value = None
                row_key = (row_name, row_src) if row_name else None
                if row_key and row_key in extracted_by_key:
                    value = extracted_by_key[row_key]
                elif row_name and row_name in extracted_by_row_name:
                    candidates = extracted_by_row_name[row_name]
                    if len(candidates) == 1:
                        value = candidates[0][1]
                    else:
                        for paper in papers:
                            paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                            for cand_key, cand_val in candidates:
                                if cand_key[1].lower() == paper_stem:
                                    value = cand_val
                                    break
                            if value is not None:
                                break
                if value is None:
                    for paper in papers:
                        paper_stem = paper.split('_')[0].lower() if '_' in paper else paper.rsplit('.', 1)[0].lower()
                        if paper_stem in extracted_by_paper_stem:
                            candidates = extracted_by_paper_stem[paper_stem]
                            if len(candidates) == 1:
                                value = candidates[0][1]
                            break

                if value is not None:
                    if 'data' not in row_data:
                        row_data['data'] = {}
                    row_data['data'][column_name] = value

                updated_rows.append(row_data)

        # Write back updated data
        write_jsonl_atomic(data_file, updated_rows)

    async def reprocess_documents(self, session_id: str, columns: List[str], incremental: bool = True, force: bool = False):
        """Reprocess documents for multiple columns using comprehensive schema context."""
        try:
            session = self.session_manager.get_session(session_id)
            if not session:
                return
            
            self.reprocessing_status[session_id] = {
                "is_running": True,
                "progress": 0.0,
                "current_column": None,
                "columns_processed": 0,
                "total_columns": len(columns),
                "start_time": datetime.now().isoformat()
            }
            
            await self.broadcast_progress(
                session_id,
                f"Starting schema-aware reprocessing of {len(columns)} columns",
                0.0,
                "reprocessing_documents"
            )
            
            # Create comprehensive schema context for better LLM understanding
            session_dir = Path(DEFAULT_DATA_DIR) / session_id
            comprehensive_schema = {
                "query": session.schema_query or "Extract structured information",
                "context": f"Processing session: {session.metadata.source}",
                "extraction_instructions": self._generate_extraction_instructions(session),
                "schema": []
            }

            # Add observation_unit (required by value extraction)
            if session.observation_unit:
                comprehensive_schema["observation_unit"] = {
                    "name": session.observation_unit.name,
                    "definition": session.observation_unit.definition,
                }
                if session.observation_unit.example_names:
                    comprehensive_schema["observation_unit"]["example_names"] = session.observation_unit.example_names

            # Add all column definitions for context, even if only processing some
            for col in session.columns:
                if col.name and not col.name.lower().endswith('_excerpt'):
                    comprehensive_schema["schema"].append({
                        "column": col.name,
                        "definition": col.definition or f"Data field: {col.name}",
                        "explanation": col.rationale or f"Information related to {col.name}",
                        "processing_status": "target" if col.name in columns else "reference"
                    })
            
            # Save comprehensive schema for context
            comprehensive_schema_file = session_dir / "comprehensive_schema_context.json"
            with open(comprehensive_schema_file, 'w') as f:
                json.dump(comprehensive_schema, f, indent=2)
            
            # Process each column individually with full schema context
            for i, column_name in enumerate(columns):
                self.reprocessing_status[session_id]["current_column"] = column_name
                self.reprocessing_status[session_id]["progress"] = i / len(columns)
                
                await self.reprocess_column_with_context(session_id, column_name, comprehensive_schema)
                
                self.reprocessing_status[session_id]["columns_processed"] = i + 1
                
                await self.broadcast_progress(
                    session_id,
                    f"Completed {i + 1}/{len(columns)} columns with schema context",
                    (i + 1) / len(columns),
                    "reprocessing_documents"
                )
            
            self.reprocessing_status[session_id]["is_running"] = False
            self.reprocessing_status[session_id]["progress"] = 1.0
            
            await self.broadcast_completion(
                session_id,
                f"Schema-aware reprocessing completed for all {len(columns)} columns"
            )

            # Capture schema baseline after reprocessing completes
            self.session_manager.capture_schema_baseline(session_id)

            # Cleanup temporary files
            comprehensive_schema_file.unlink(missing_ok=True)
            
        except Exception as e:
            self.reprocessing_status[session_id] = {
                "is_running": False,
                "progress": 0.0,
                "error": str(e)
            }
            await self.broadcast_error(session_id, f"Schema-aware document reprocessing failed: {str(e)}")
            raise
        finally:
            await concurrency_limiter.release(session_id)

    def _generate_extraction_instructions(self, session) -> str:
        """Generate enhanced extraction instructions based on schema context."""
        instructions = [
            "Extract information according to the column definitions provided.",
            "Use the column definitions and rationales to understand what type of information to extract.",
            "Ensure extracted values match the semantic meaning described in each column's definition.",
            "If a column has a specific rationale, use that context to guide extraction accuracy."
        ]
        
        # Add session-specific context
        if session.schema_query:
            instructions.insert(0, f"Research Context: {session.schema_query}")
        
        if session.metadata.source:
            instructions.append(f"Data Source Context: Processing documents from {session.metadata.source}")
        
        return " ".join(instructions)
    
    async def reprocess_column_with_context(self, session_id: str, column_name: str, comprehensive_schema: dict):
        """Reprocess a column using full schema context for improved accuracy."""
        try:
            session = self.session_manager.get_session(session_id)
            if not session:
                return
            
            await self.broadcast_progress(
                session_id, 
                f"Processing column '{column_name}' with schema context", 
                0.0, 
                "reprocessing_column"
            )
            
            # Find the target column
            column = next((col for col in session.columns if col.name == column_name), None)
            if not column:
                raise ValueError(f"Column '{column_name}' not found")
            
            # Validate schema metadata
            if not column.definition and not column.rationale:
                await self.broadcast_progress(
                    session_id,
                    f"Warning: Column '{column_name}' has no definition or rationale - using basic extraction",
                    0.0,
                    "schema_warning"
                )
            
            # Create schema for targeted extraction — must use "schema" key (list)
            # so that Schema.from_dict() can parse it correctly
            column_entry = {
                "column": column.name,
                "definition": column.definition or f"Data field: {column.name}",
                "explanation": column.rationale or f"Information related to {column.name}",
            }
            if column.allowed_values:
                column_entry["allowed_values"] = column.allowed_values

            enhanced_schema = {
                "query": comprehensive_schema["query"],
                "schema": [column_entry],
            }

            # Add observation_unit (required by value extraction)
            if session.observation_unit:
                enhanced_schema["observation_unit"] = {
                    "name": session.observation_unit.name,
                    "definition": session.observation_unit.definition,
                }
                if session.observation_unit.example_names:
                    enhanced_schema["observation_unit"]["example_names"] = session.observation_unit.example_names

            # Save enhanced schema file
            session_dir = Path(DEFAULT_DATA_DIR) / session_id
            enhanced_schema_file = session_dir / f"enhanced_schema_{column_name}.json"
            with open(enhanced_schema_file, 'w') as f:
                json.dump(enhanced_schema, f, indent=2)
            
            # Setup LLM with enhanced parameters for schema-aware extraction
            llm = self._get_value_extraction_llm_from_session(session_id)
            from app.services import get_shared_retriever
            retriever = get_shared_retriever()

            # Find documents directory
            docs_dir = committed_docs_dir(session_dir)
            if docs_dir.exists():
                # Extract values with enhanced schema context
                output_file = session_dir / f"enhanced_reprocessed_{column_name}.jsonl"

                # Create should_stop callback for graceful cancellation
                def should_stop():
                    return self.is_stop_requested(session_id)

                await asyncio.get_event_loop().run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(
                        build_table_jsonl,
                        schema_path=enhanced_schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="one_by_one",  # More focused extraction
                        retrieval_k=10,
                        max_workers=1,
                        should_stop=should_stop,
                        write_skip_rationale_artifact=session.write_artifacts,
                    )
                )
                
                # Update existing data with enhanced extraction results
                await self._update_column_data(session_id, column_name, output_file)
                
                # Cleanup
                output_file.unlink(missing_ok=True)
            
            enhanced_schema_file.unlink(missing_ok=True)
            
            await self.broadcast_completion(
                session_id,
                f"Enhanced reprocessing completed for column '{column_name}'"
            )
                
        except Exception as e:
            await self.broadcast_error(session_id, f"Enhanced reprocessing failed for '{column_name}': {str(e)}")
            raise
    
    async def get_reprocessing_status(self, session_id: str) -> Dict[str, Any]:
        """Get the current reprocessing status for a session."""
        return self.reprocessing_status.get(session_id, {
            "is_running": False,
            "progress": 0.0,
            "current_column": None,
            "columns_processed": 0,
            "total_columns": 0,
            "estimated_time_remaining": None,
            "start_time": None
        })