"""
Schema management service for QBSD visualization.
Handles schema editing operations and document reprocessing.
"""

import json
import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import time

from models.session import ColumnInfo, SessionStatus
from services.websocket_manager import WebSocketManager
from services.session_manager import SessionManager
from services.websocket_mixin import WebSocketBroadcasterMixin

# Import QBSD components for value extraction
import sys
QBSD_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(QBSD_ROOT))

try:
    from value_extraction.main import build_table_jsonl
    from llm_backends import GeminiLLM
    from retrievers import EmbeddingRetriever
    import utils
    QBSD_AVAILABLE = True
except ImportError as e:
    print(f"QBSD components not available for schema manager: {e}")
    QBSD_AVAILABLE = False


class SchemaManager(WebSocketBroadcasterMixin):
    """Manages schema editing operations and document reprocessing."""
    
    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.reprocessing_status: Dict[str, Dict[str, Any]] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        
    def _get_value_extraction_llm_from_session(self, session_id: str):
        """Get value extraction LLM configuration from session if available, otherwise use default."""
        try:
            # Try to load the session's QBSD config
            session_dir = Path("./data") / session_id
            qbsd_config_file = session_dir / "qbsd_config.json"
            
            if qbsd_config_file.exists():
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    
                # Check if we have the new dual LLM config format
                if "value_extraction_backend" in qbsd_config:
                    backend_config = qbsd_config["value_extraction_backend"]
                    
                    if backend_config["provider"] == "gemini":
                        return GeminiLLM(
                            model=backend_config["model"],
                            max_tokens=backend_config["max_tokens"],
                            temperature=backend_config["temperature"]
                        )
                    # Could add other providers here when needed
                        
        except Exception as e:
            print(f"DEBUG: Could not load LLM config from session {session_id}: {e}")
            
        # Fallback to default configuration
        print(f"DEBUG: Using default LLM configuration for session {session_id}")
        return GeminiLLM(model="gemini-2.5-flash-lite", max_tokens=2048, temperature=0.1)
    
    async def reprocess_column(self, session_id: str, column_name: str):
        """Reprocess documents for a specific column after editing."""
        try:
            session = self.session_manager.get_session(session_id)
            if not session or not QBSD_AVAILABLE:
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
            
            # Save temporary schema file
            session_dir = Path("./data") / session_id
            schema_file = session_dir / f"temp_schema_{column_name}.json"
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)
            
            # Setup LLM and retriever for extraction
            llm = self._get_value_extraction_llm_from_session(session_id)
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2",
                k=8,
                max_words=512
            )
            
            # Find documents directory
            docs_dir = session_dir / "documents"
            if not docs_dir.exists():
                # Look for uploaded documents in session
                if session.metadata.uploaded_documents:
                    # Create docs directory and copy files if needed
                    docs_dir.mkdir(exist_ok=True)
            
            if docs_dir.exists():
                # Extract values for the column
                output_file = session_dir / f"reprocessed_{column_name}.jsonl"
                
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: build_table_jsonl(
                        schema_path=schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="all",
                        retrieval_k=8,
                        max_workers=1
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
    
    async def remove_column_data(self, session_id: str, column_name: str):
        """Remove a column's data from all existing records, including excerpt columns."""
        try:
            session_dir = Path("./data") / session_id
            data_file = session_dir / "data.jsonl"
            
            if not data_file.exists():
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
            
            print(f"DEBUG: Removing columns {columns_to_remove} from {len(existing_rows)} rows")
            
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
                            print(f"DEBUG: Removed column '{col_to_remove}' from row with row_name: {row_data.get('row_name', 'unknown')}")
                
                if row_updated:
                    columns_removed_count += 1
                
                updated_rows.append(row_data)
            
            # Write back updated data
            with open(data_file, 'w') as f:
                for row in updated_rows:
                    f.write(json.dumps(row) + '\n')
            
            print(f"DEBUG: Successfully removed columns {columns_to_remove} from {columns_removed_count} rows")
            
            await self.broadcast_progress(
                session_id,
                f"Column '{column_name}' and related data removed from {columns_removed_count} rows",
                1.0,
                "column_deleted"
            )
            
            # Also check for and update data.json if it exists (backup format)
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
                    
                    print(f"DEBUG: Also updated data.json file")
                except Exception as json_error:
                    print(f"DEBUG: Could not update data.json: {json_error}")
            
        except Exception as e:
            print(f"DEBUG: Failed to remove column data: {str(e)}")
            import traceback
            traceback.print_exc()
            await self.broadcast_error(session_id, f"Failed to remove column data: {str(e)}")
            raise
    
    async def extract_values_for_new_column(self, session_id: str, column: ColumnInfo, documents_path: Optional[str] = None):
        """Extract values for a newly added column using comprehensive schema context."""
        try:
            if not QBSD_AVAILABLE:
                await self.broadcast_error(session_id, "QBSD components not available")
                return
            
            await self.broadcast_progress(
                session_id,
                f"Extracting values for new column '{column.name}' with schema context",
                0.0,
                "extracting_new_column"
            )
            
            session = self.session_manager.get_session(session_id)
            if not session:
                return
            
            # Validate column metadata
            if not column.definition and not column.rationale:
                await self.broadcast_progress(
                    session_id,
                    f"Warning: New column '{column.name}' lacks definition and rationale - extraction may be less accurate",
                    0.0,
                    "schema_warning"
                )
            
            # Create comprehensive schema context including existing columns
            session_dir = Path("./data") / session_id
            comprehensive_schema = {
                "query": session.schema_query or "Extract structured information",
                "context": f"Adding new column to existing schema: {session.metadata.source}",
                "extraction_instructions": self._generate_extraction_instructions(session),
                "target_column": {
                    "column": column.name,
                    "definition": column.definition or f"New data field: {column.name}",
                    "explanation": column.rationale or f"Additional information for {column.name}",
                    "data_type": column.data_type or "text",
                    "is_new": True
                },
                "existing_schema_context": []
            }
            
            # Add existing column context to help with coherent extraction
            for existing_col in session.columns:
                if (existing_col.name != column.name and 
                    existing_col.name and 
                    not existing_col.name.lower().endswith('_excerpt')):
                    comprehensive_schema["existing_schema_context"].append({
                        "column": existing_col.name,
                        "definition": existing_col.definition or "",
                        "explanation": existing_col.rationale or ""
                    })
            
            schema_file = session_dir / f"new_column_enhanced_schema_{column.name}.json"
            with open(schema_file, 'w') as f:
                json.dump(comprehensive_schema, f, indent=2)
            
            # Setup enhanced extraction components
            llm = self._get_value_extraction_llm_from_session(session_id)
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2", 
                k=10,              # More retrieval for better context
                max_words=768      # More text for understanding
            )
            
            # Determine documents directory
            docs_dir = Path(documents_path) if documents_path else session_dir / "documents"
            
            if not docs_dir.exists():
                await self.broadcast_error(session_id, f"Documents directory not found: {docs_dir}")
                return
            
            # Extract values with enhanced schema awareness
            output_file = session_dir / f"new_column_enhanced_values_{column.name}.jsonl"
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: build_table_jsonl(
                    schema_path=schema_file,
                    docs_directories=[docs_dir],
                    output_path=output_file,
                    llm=llm,
                    retriever=retriever,
                    resume=False,
                    mode="one_by_one",  # More focused extraction for new columns
                    retrieval_k=10,
                    max_workers=1
                )
            )
            
            # Add new column data to existing records
            await self._add_new_column_data(session_id, column.name, output_file)
            
            await self.broadcast_completion(
                session_id,
                f"Enhanced extraction completed for new column '{column.name}'"
            )
            
            # Cleanup
            schema_file.unlink(missing_ok=True)
            output_file.unlink(missing_ok=True)
            
        except Exception as e:
            await self.broadcast_error(session_id, f"Enhanced extraction failed for new column: {str(e)}")
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
            
            session_dir = Path("./data") / session_id
            data_file = session_dir / "data.jsonl"
            
            if not data_file.exists():
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
            with open(data_file, 'w') as f:
                for row in updated_rows:
                    f.write(json.dumps(row) + '\n')
            
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
        
        if strategy == "CONCATENATE":
            return separator.join(values)
        elif strategy == "COMBINE_UNIQUE":
            unique_values = list(dict.fromkeys(values))  # Preserve order
            return separator.join(unique_values)
        elif strategy == "TAKE_FIRST":
            return values[0]
        elif strategy == "TAKE_LONGEST":
            return max(values, key=len)
        else:
            # Default to concatenate
            return separator.join(values)
    
    async def _update_column_data(self, session_id: str, column_name: str, extraction_file: Path):
        """Update existing data with newly extracted values for a column."""
        if not extraction_file.exists():
            return
        
        # Read extracted values
        extracted_data = {}
        with open(extraction_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.strip():
                    row_data = json.loads(line)
                    if column_name in row_data:
                        extracted_data[line_num] = row_data[column_name]
        
        # Update main data file
        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"
        
        if not data_file.exists():
            return
        
        updated_rows = []
        with open(data_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.strip():
                    row_data = json.loads(line)
                    
                    # Update with extracted value if available
                    if line_num in extracted_data:
                        if 'data' not in row_data:
                            row_data['data'] = {}
                        row_data['data'][column_name] = extracted_data[line_num]
                    
                    updated_rows.append(row_data)
        
        # Write back updated data
        with open(data_file, 'w') as f:
            for row in updated_rows:
                f.write(json.dumps(row) + '\n')
    
    async def _add_new_column_data(self, session_id: str, column_name: str, extraction_file: Path):
        """Add data for a new column to existing records."""
        if not extraction_file.exists():
            return
        
        # Read extracted values
        extracted_data = {}
        with open(extraction_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.strip():
                    row_data = json.loads(line)
                    if column_name in row_data:
                        extracted_data[line_num] = row_data[column_name]
        
        # Update main data file
        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"
        
        if not data_file.exists():
            # Create new data file with extracted values
            with open(data_file, 'w') as f:
                for line_num, value in extracted_data.items():
                    row_data = {
                        "row_name": f"Row_{line_num + 1}",
                        "data": {column_name: value},
                        "papers": []
                    }
                    f.write(json.dumps(row_data) + '\n')
        else:
            # Add to existing data
            updated_rows = []
            with open(data_file, 'r') as f:
                for line_num, line in enumerate(f):
                    if line.strip():
                        row_data = json.loads(line)
                        
                        # Add new column value
                        if line_num in extracted_data:
                            if 'data' not in row_data:
                                row_data['data'] = {}
                            row_data['data'][column_name] = extracted_data[line_num]
                        
                        updated_rows.append(row_data)
            
            # Write back updated data
            with open(data_file, 'w') as f:
                for row in updated_rows:
                    f.write(json.dumps(row) + '\n')
    
    async def reprocess_documents(self, session_id: str, columns: List[str], incremental: bool = True, force: bool = False):
        """Reprocess documents for multiple columns using comprehensive schema context."""
        try:
            if not QBSD_AVAILABLE:
                await self.broadcast_error(session_id, "QBSD components not available")
                return
            
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
            session_dir = Path("./data") / session_id
            comprehensive_schema = {
                "query": session.schema_query or "Extract structured information",
                "context": f"Processing session: {session.metadata.source}",
                "extraction_instructions": self._generate_extraction_instructions(session),
                "schema": []
            }
            
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
            if not session or not QBSD_AVAILABLE:
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
            
            # Create enhanced schema for targeted extraction
            enhanced_schema = {
                "query": comprehensive_schema["query"],
                "context": comprehensive_schema["context"],
                "extraction_instructions": comprehensive_schema["extraction_instructions"],
                "target_column": {
                    "column": column.name,
                    "definition": column.definition or f"Data field: {column.name}",
                    "explanation": column.rationale or f"Information related to {column.name}",
                    "data_type": column.data_type or "text"
                },
                "schema_context": [
                    col for col in comprehensive_schema["schema"] 
                    if col["processing_status"] == "reference"
                ][:5]  # Limit to top 5 for context
            }
            
            # Save enhanced schema file
            session_dir = Path("./data") / session_id
            enhanced_schema_file = session_dir / f"enhanced_schema_{column_name}.json"
            with open(enhanced_schema_file, 'w') as f:
                json.dump(enhanced_schema, f, indent=2)
            
            # Setup LLM with enhanced parameters for schema-aware extraction
            llm = self._get_value_extraction_llm_from_session(session_id)
            retriever = EmbeddingRetriever(
                model_name="all-MiniLM-L6-v2",
                k=10,  # Increased retrieval for better context
                max_words=768  # More text for better understanding
            )
            
            # Find documents directory
            docs_dir = session_dir / "documents"
            if docs_dir.exists():
                # Extract values with enhanced schema context
                output_file = session_dir / f"enhanced_reprocessed_{column_name}.jsonl"
                
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: build_table_jsonl(
                        schema_path=enhanced_schema_file,
                        docs_directories=[docs_dir],
                        output_path=output_file,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="one_by_one",  # More focused extraction
                        retrieval_k=10,
                        max_workers=1
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