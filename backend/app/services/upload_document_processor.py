"""Upload document processing service integrating with QBSD pipeline."""

import json
import asyncio
import time
import math
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

# Import QBSD components from qbsd-lib
import sys

# Add qbsd-lib to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
QBSD_LIB_ROOT = PROJECT_ROOT / "qbsd-lib"
sys.path.insert(0, str(QBSD_LIB_ROOT))

try:
    # Import QBSD value extraction components from qbsd-lib
    from qbsd.value_extraction.main import build_table_jsonl
    from qbsd.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
    from qbsd.core.retrievers import EmbeddingRetriever
    from qbsd.core import utils
    QBSD_AVAILABLE = True
    print(f"✓ QBSD components successfully loaded for upload document processing from {QBSD_LIB_ROOT}")
except ImportError as e:
    print(f"✗ QBSD components not available for upload processing: {e}")
    QBSD_AVAILABLE = False

from app.models.session import SessionStatus, DataRow, DataStatistics, ColumnInfo, VisualizationSession
from app.services.websocket_manager import WebSocketManager
from app.services.session_manager import SessionManager
from app.services.websocket_mixin import WebSocketBroadcasterMixin
from app.core.config import DEFAULT_MAX_OUTPUT_TOKENS, DEFAULT_TEMPERATURE, DEFAULT_RETRIEVAL_K, PROGRESS_CHECK_INTERVAL


class UploadDocumentProcessor(WebSocketBroadcasterMixin):
    """Handles document processing for upload sessions using QBSD pipeline."""

    # Metadata columns that should not be sent to LLM for extraction
    METADATA_COLUMNS = {'papers', 'document_directory', 'row_name', '_row_name', '_papers', '_metadata'}

    def __init__(self, websocket_manager: WebSocketManager, session_manager: SessionManager):
        super().__init__(websocket_manager)
        self.session_manager = session_manager
        self.running_sessions: Dict[str, bool] = {}

    def _create_value_extracted_callback(self, session_id: str, loop: asyncio.AbstractEventLoop):
        """Create a callback that streams extracted cell values via WebSocket.

        The callback bridges sync extraction code to async WebSocket broadcasting.
        """
        def on_value_extracted(row_name: str, column_name: str, value: Any):
            """Called for each cell value as it's extracted."""
            print(f"📤 CELL EXTRACTED: {row_name} / {column_name} = {str(value)[:50]}...")
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self.broadcast_cell_extracted(session_id, {
                        "row_name": row_name,
                        "column": column_name,
                        "value": value
                    }),
                    loop
                )
                # Check if future completed (with short timeout to not block extraction)
                try:
                    future.result(timeout=0.1)
                    print(f"✅ CELL BROADCAST SUCCESS: {row_name}/{column_name}")
                except TimeoutError:
                    pass  # Still running, that's fine - async broadcast in progress
                except Exception as e:
                    print(f"❌ CELL BROADCAST FAILED: {row_name}/{column_name}: {e}")
            except Exception as e:
                print(f"⚠️  Failed to schedule broadcast for {column_name}: {e}")

        return on_value_extracted
    
    async def process_documents(self, session_id: str):
        """Process uploaded documents using QBSD pipeline."""
        if not QBSD_AVAILABLE:
            raise RuntimeError("QBSD components not available for document processing")
        
        self.running_sessions[session_id] = True
        
        try:
            session = self.session_manager.get_session(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")
            
            print(f"🔄 DEBUG: Starting document processing for session {session_id}")
            
            # Progress tracking
            await self.broadcast_progress(session_id, "Initializing document processing", 0.0, "processing_documents")
            
            # Get paths and configuration
            session_dir = Path("./data") / session_id
            pending_dir = session_dir / "pending_documents"
            docs_dir = session_dir / "documents"

            if not pending_dir.exists() or not any(pending_dir.iterdir()):
                raise FileNotFoundError(f"No pending documents to process in: {pending_dir}")

            # Clean up any system files (like .DS_Store) that might have been created
            self._clean_system_files(pending_dir)
            
            # Build schema from session.columns (includes any user edits from SCHEMA tab)
            # This ensures that edits like allowed_values affect new document processing
            current_schema = self._build_schema_from_session(session)

            print(f"🔄 DEBUG: Processing with schema: {len(current_schema['schema'])} columns")

            # Create enhanced QBSD schema file for value extraction
            await self.broadcast_progress(session_id, "Preparing schema for processing", 0.1, "processing_documents")

            # Enhance schema with extraction instructions based on column metadata
            enhanced_schema = self._enhance_schema_for_extraction(current_schema)
            
            schema_path = session_dir / "processing_schema.json"
            with open(schema_path, 'w') as f:
                json.dump(enhanced_schema, f, indent=2)
            
            # Build LLM and retriever
            await self.broadcast_progress(session_id, "Setting up AI models", 0.2, "processing_documents")
            
            # Try to use preserved LLM configuration, fallback to defaults
            backend_config = self._get_llm_config_for_extraction(current_schema, session_id)
            
            llm = utils.build_llm(backend_config)
            print(f"🔄 DEBUG: LLM interface created successfully")
            
            # Create retriever
            retriever_config = {
                "type": "embedding",
                "model_name": "all-MiniLM-L6-v2",
                "k": DEFAULT_RETRIEVAL_K,
                "max_words": 512,
                "enable_dynamic_k": True,
                "dynamic_k_threshold": 0.65,
                "dynamic_k_minimum": 3
            }
            retriever = utils.build_retriever(retriever_config)
            print(f"🔄 DEBUG: Retriever created successfully")
            
            # Run value extraction
            await self.broadcast_progress(session_id, "Processing documents with AI", 0.3, "processing_documents")

            output_path = session_dir / "additional_data.jsonl"

            # Run extraction in executor to avoid blocking
            # Use get_running_loop() to ensure we have the active event loop
            loop = asyncio.get_running_loop()

            # Create callback to stream cell values as they're extracted
            on_value_extracted = self._create_value_extracted_callback(session_id, loop)

            def run_extraction():
                print(f"🚀 EXTRACTION STARTING for session {session_id}")
                print(f"   Schema: {schema_path}")
                print(f"   Docs: {pending_dir}")
                print(f"   Output: {output_path}")
                try:
                    result = build_table_jsonl(
                        schema_path=schema_path,
                        docs_directories=[pending_dir],  # Only process new (pending) documents
                        output_path=output_path,
                        llm=llm,
                        retriever=retriever,
                        resume=False,
                        mode="all",  # Process all columns together
                        retrieval_k=DEFAULT_RETRIEVAL_K,
                        max_workers=1,  # Single worker to avoid overwhelming API
                        on_value_extracted=on_value_extracted  # Stream values as extracted
                    )
                    print(f"✅ EXTRACTION COMPLETED for session {session_id}")
                    return result
                except Exception as e:
                    print(f"❌ EXTRACTION FAILED for session {session_id}: {type(e).__name__}: {e}")
                    import traceback
                    traceback.print_exc()
                    raise
            
            # Monitor progress while extraction runs
            # NOTE: We only track progress here - actual data writing happens in _merge_extracted_data()
            # to avoid duplicate writes
            extraction_task = loop.run_in_executor(None, run_extraction)

            start_time = time.time()
            total_docs = len(session.metadata.uploaded_documents)
            last_processed_line = 0

            while not extraction_task.done():
                # Check output file for progress tracking only
                if output_path.exists():
                    try:
                        current_new_rows = []
                        current_line_count = 0

                        # Read new rows since last check (for progress tracking and broadcasting)
                        with open(output_path, 'r') as f:
                            for line_num, line in enumerate(f):
                                current_line_count += 1
                                if line_num >= last_processed_line and line.strip():
                                    row_data = json.loads(line)
                                    current_new_rows.append(row_data)

                        # If we have new rows, update progress (but DON'T write to data.jsonl - that happens in _merge_extracted_data)
                        if current_new_rows:
                            # Update tracking
                            last_processed_line = current_line_count

                            # Update session metadata
                            session.metadata.processed_documents = min(current_line_count, total_docs)
                            session.metadata.additional_rows_added = current_line_count
                            self.session_manager.update_session(session)

                            # Broadcast progress
                            progress = 0.3 + (current_line_count / total_docs) * 0.6  # 30% to 90%
                            await self.broadcast_progress(
                                session_id,
                                f"Processed {current_line_count} documents",
                                progress,
                                "processing_documents"
                            )

                            # Broadcast enhanced row completion for each new row with schema context
                            original_row_count = session.metadata.original_row_count or 0
                            for i, new_row_data in enumerate(current_new_rows):
                                new_row_index = original_row_count + last_processed_line - len(current_new_rows) + i + 1

                                # Analyze extraction quality for this row
                                extraction_summary = self._analyze_extraction_quality(new_row_data, current_schema)

                                await self.broadcast_row_completed(session_id, {
                                    "row_index": new_row_index,
                                    "total_rows": total_docs,
                                    "completed_at": datetime.now().isoformat(),
                                    "additional_rows": current_line_count,
                                    "extraction_quality": extraction_summary,
                                    "row_preview": self._create_row_preview(new_row_data, current_schema)
                                })

                    except Exception as e:
                        print(f"Progress monitoring error: {e}")

                await asyncio.sleep(PROGRESS_CHECK_INTERVAL)  # Check every few seconds for faster updates
            
            # Wait for completion
            await extraction_task
            
            # Merge additional extracted data into main data file
            await self.broadcast_progress(session_id, "Merging extracted data", 0.95, "processing_documents")
            current_session = self.session_manager.get_session(session_id)
            rows_added = await self._merge_extracted_data(session_id, current_session)

            # Update session as completed
            await self.broadcast_progress(session_id, "Processing complete", 1.0, "processing_documents")

            session = self.session_manager.get_session(session_id)
            print(f"🔍 DEBUG: Session before completion update: status={session.status}")

            session.status = SessionStatus.COMPLETED
            session.metadata.last_modified = datetime.now()

            # Update final statistics using the count returned from merge
            session.metadata.additional_rows_added = rows_added
            session.metadata.processed_documents = len(session.metadata.uploaded_documents)

            print(f"🔍 DEBUG: Session after completion update: status={session.status}, additional_rows={session.metadata.additional_rows_added}")

            self.session_manager.update_session(session)

            # Recompute statistics from merged data
            try:
                statistics = self._compute_statistics_from_data(session_id, session)
                if statistics:
                    session.statistics = statistics
                    self.session_manager.update_session(session)
                    print(f"✓ Session statistics updated after document processing")
                else:
                    print(f"⚠️  No statistics generated for session {session_id}")
            except Exception as e:
                print(f"⚠️  Failed to compute statistics for session {session_id}: {e}")
                # Don't fail the entire operation - statistics are supplementary

            # Capture schema baseline for re-extraction change detection
            self.session_manager.capture_schema_baseline(session_id)

            # Verify the update was successful
            updated_session = self.session_manager.get_session(session_id)
            print(f"🔍 DEBUG: Session after manager update: status={updated_session.status}, additional_rows={updated_session.metadata.additional_rows_added}")

            # Small delay to ensure session update is committed before broadcasting completion
            await asyncio.sleep(0.5)

            # Broadcast completion
            completion_data = {
                "additional_rows": rows_added,
                "total_documents": len(session.metadata.uploaded_documents)
            }
            
            print(f"🎯 DEBUG: Broadcasting completion for session {session_id} with data: {completion_data}")
            await self.broadcast_completion(session_id, 
                "Document processing completed successfully", completion_data)
            
            print(f"✅ DEBUG: Document processing completed and broadcast sent for session {session_id}")
            
        except Exception as e:
            # Update session with error
            session = self.session_manager.get_session(session_id)
            if session:
                session.status = SessionStatus.ERROR
                session.error_message = f"Document processing failed: {str(e)}"
                self.session_manager.update_session(session)
            
            await self.broadcast_error(session_id, str(e))
            print(f"❌ DEBUG: Document processing failed for session {session_id}: {e}")
            raise
        
        finally:
            # Clean up
            if session_id in self.running_sessions:
                del self.running_sessions[session_id]
    
    
    async def _merge_extracted_data(self, session_id: str, session: VisualizationSession = None) -> int:
        """Merge newly extracted data with existing session data. Returns number of rows added.

        Args:
            session_id: The session ID
            session: The session object (optional, used to get cloud_dataset for document_directory)
        """
        session_dir = Path("./data") / session_id
        original_data_file = session_dir / "data.jsonl"
        additional_data_file = session_dir / "additional_data.jsonl"

        if not additional_data_file.exists():
            print(f"No additional data to merge for session {session_id}")
            return 0

        # Get cloud dataset name for document_directory if available
        cloud_dataset = None
        if session and session.metadata and session.metadata.cloud_dataset:
            cloud_dataset = session.metadata.cloud_dataset
            print(f"DEBUG: Using cloud_dataset '{cloud_dataset}' for document_directory")

        # Read original data to get the base row count and detect row name column pattern
        original_rows = []
        row_name_column_in_data = None  # Track if original data has a "Row Name" type column inside data

        if original_data_file.exists():
            with open(original_data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        row = json.loads(line)
                        original_rows.append(row)

                        # Check if original data stores row name inside 'data' field
                        # Look for common row name column patterns (case-insensitive)
                        if row_name_column_in_data is None and 'data' in row:
                            row_data_dict = row.get('data', {})
                            for key in row_data_dict.keys():
                                key_lower = key.lower().replace('_', ' ').replace('-', ' ')
                                if key_lower in ['row name', 'rowname', 'row_name', 'name', 'id', 'identifier']:
                                    row_name_column_in_data = key
                                    print(f"DEBUG: Detected row name column in data: '{key}'")
                                    break

        # Read new extracted data and append to original file
        new_rows_added = 0
        with open(additional_data_file, 'r') as additional_f:
            with open(original_data_file, 'a') as original_f:  # Append mode
                for line in additional_f:
                    if line.strip():
                        row_data = json.loads(line)
                        # Ensure the row has proper structure for DataRow
                        if 'data' not in row_data:
                            # Convert QBSD format to DataRow format with proper data cleaning
                            row_name = row_data.get("_row_name", f"Row_{len(original_rows) + new_rows_added + 1}")
                            papers_list = row_data.get("_papers", [])

                            # Clean the data by removing metadata fields and ensuring proper types
                            clean_data = {}
                            for key, value in row_data.items():
                                # Skip most metadata fields (_row_name, _papers, row_name, papers)
                                if key.startswith('_') or key.lower() in {'papers', 'row_name', '_row_name', '_papers', '_metadata'}:
                                    continue

                                # Special handling for document_directory - use cloud_dataset if available
                                if key.lower() == 'document_directory':
                                    if cloud_dataset:
                                        clean_data['document_directory'] = cloud_dataset
                                    else:
                                        clean_data['document_directory'] = value
                                    continue

                                # Try to parse string values that look like JSON/Python objects
                                parsed_value = self._try_parse_string_value(value)

                                # Handle QBSD answer format vs direct values
                                if isinstance(parsed_value, dict):
                                    # Normalize to QBSD format with 'answer' and 'excerpts' keys
                                    normalized = self._normalize_to_qbsd_format(parsed_value)
                                    clean_data[key] = normalized
                                elif isinstance(parsed_value, list) and len(parsed_value) > 0:
                                    # Handle list format (e.g., [{'value': '...', 'excerpt': '...'}])
                                    if isinstance(parsed_value[0], dict):
                                        # Take first item and normalize it
                                        normalized = self._normalize_to_qbsd_format(parsed_value[0])
                                        # If there are multiple items, collect all excerpts
                                        if len(parsed_value) > 1:
                                            all_excerpts = normalized.get('excerpts', [])
                                            for item in parsed_value[1:]:
                                                if isinstance(item, dict):
                                                    item_normalized = self._normalize_to_qbsd_format(item)
                                                    all_excerpts.extend(item_normalized.get('excerpts', []))
                                            normalized['excerpts'] = all_excerpts
                                        clean_data[key] = normalized
                                    else:
                                        # List of non-dict values, keep as is
                                        clean_data[key] = parsed_value
                                else:
                                    clean_data[key] = parsed_value

                            # If original data has a row name column inside 'data', put the row name there
                            # to maintain consistency and avoid column duplication
                            if row_name_column_in_data:
                                clean_data[row_name_column_in_data] = row_name
                                converted_row = {
                                    "data": clean_data,
                                    "row_name": None,  # Don't duplicate at DataRow level
                                    "papers": papers_list if isinstance(papers_list, list) else [str(papers_list)] if papers_list else []
                                }
                            else:
                                converted_row = {
                                    "data": clean_data,
                                    "row_name": row_name,
                                    "papers": papers_list if isinstance(papers_list, list) else [str(papers_list)] if papers_list else []
                                }
                            original_f.write(json.dumps(converted_row) + '\n')
                        else:
                            # Already in DataRow format, but validate papers field
                            row_copy = row_data.copy()
                            papers = row_copy.get("papers", [])
                            if not isinstance(papers, list):
                                row_copy["papers"] = [str(papers)] if papers else []

                            # Also check if we need to move row_name into data for consistency
                            if row_name_column_in_data and row_copy.get("row_name"):
                                if 'data' not in row_copy:
                                    row_copy['data'] = {}
                                row_copy['data'][row_name_column_in_data] = row_copy["row_name"]
                                row_copy["row_name"] = None

                            original_f.write(json.dumps(row_copy) + '\n')
                        new_rows_added += 1
        
        print(f"Successfully appended {new_rows_added} new rows to session {session_id}. Total rows: {len(original_rows) + new_rows_added}")

        # Clean up the additional data file
        additional_data_file.unlink(missing_ok=True)

        # Move processed documents from pending_documents/ to documents/
        pending_dir = session_dir / "pending_documents"
        docs_dir = session_dir / "documents"
        docs_dir.mkdir(exist_ok=True)

        if pending_dir.exists():
            import shutil
            for file_path in pending_dir.iterdir():
                if file_path.is_file():
                    dest_path = docs_dir / file_path.name
                    # Handle duplicate filenames
                    if dest_path.exists():
                        base_name = file_path.stem
                        extension = file_path.suffix
                        counter = 1
                        while dest_path.exists():
                            dest_path = docs_dir / f"{base_name}_{counter}{extension}"
                            counter += 1
                    shutil.move(str(file_path), str(dest_path))
                    print(f"Moved {file_path.name} to documents/")

        return new_rows_added

    def _try_parse_string_value(self, value: Any) -> Any:
        """Try to parse a string value that might be a JSON/Python object representation.

        Returns the parsed object if successful, otherwise returns the original value.
        This handles cases where LLM outputs are stored as string representations.
        """
        if not isinstance(value, str):
            return value

        # Quick check: does it look like a JSON object/array?
        stripped = value.strip()
        if not stripped:
            return value

        if stripped.startswith('{') or stripped.startswith('['):
            # Try JSON parsing first
            try:
                import json
                parsed = json.loads(stripped)
                return parsed
            except json.JSONDecodeError:
                pass

            # Try Python literal parsing (handles single quotes)
            try:
                import ast
                parsed = ast.literal_eval(stripped)
                return parsed
            except (ValueError, SyntaxError):
                pass

        return value

    def _normalize_to_qbsd_format(self, value: dict) -> dict:
        """Normalize a dict value to QBSD format with 'answer' and 'excerpts' keys.

        Handles various possible key names from LLM outputs:
        - 'answer' / 'excerpts' (standard QBSD format)
        - 'value' / 'excerpt' (alternative LLM output)
        - 'response' / 'evidence' (another alternative)
        """
        # Already in correct format
        if 'answer' in value:
            answer_val = value['answer']
            excerpts_val = value.get('excerpts', [])

            # Check if 'answer' is a string that looks like a JSON/Python object
            # e.g., "[{'value': 'reduction', 'excerpt': '...'}]"
            if isinstance(answer_val, str):
                parsed_answer = self._try_parse_string_value(answer_val)
                if parsed_answer != answer_val:
                    # Successfully parsed - extract the actual answer
                    if isinstance(parsed_answer, list) and len(parsed_answer) > 0:
                        # It's a list of dicts like [{'value': '...', 'excerpt': '...'}]
                        first_item = parsed_answer[0]
                        if isinstance(first_item, dict):
                            # Extract answer from first item
                            answer_val = first_item.get('value', first_item.get('answer', str(first_item)))
                            # Collect all excerpts
                            all_excerpts = []
                            for item in parsed_answer:
                                if isinstance(item, dict):
                                    exc = item.get('excerpt', item.get('excerpts'))
                                    if exc:
                                        if isinstance(exc, list):
                                            all_excerpts.extend(exc)
                                        else:
                                            all_excerpts.append(exc)
                            if all_excerpts:
                                excerpts_val = all_excerpts
                        else:
                            answer_val = str(first_item)
                    elif isinstance(parsed_answer, dict):
                        # Recursively normalize the parsed dict
                        return self._normalize_to_qbsd_format(parsed_answer)

            result = {
                'answer': answer_val,
                'excerpts': excerpts_val
            }
            # Preserve any additional metadata fields
            for key in ['normalized_to_allowed', 'unmatched_value', 'suggested_for_allowed_values']:
                if key in value:
                    result[key] = value[key]
            return result

        # Alternative key mappings
        answer_keys = ['value', 'response', 'result', 'text', 'content']
        excerpt_keys = ['excerpt', 'excerpts', 'evidence', 'sources', 'quotes', 'supporting_text']

        answer = None
        excerpts = []

        # Find the answer value
        for key in answer_keys:
            if key in value:
                answer = value[key]
                break

        # Find the excerpts value
        for key in excerpt_keys:
            if key in value:
                exc_val = value[key]
                if isinstance(exc_val, list):
                    excerpts = exc_val
                elif isinstance(exc_val, str):
                    excerpts = [exc_val] if exc_val else []
                break

        # If we found a recognized format, return normalized QBSD format
        if answer is not None:
            return {
                'answer': str(answer),
                'excerpts': excerpts
            }

        # If it's a dict with unknown structure, try to extract something meaningful
        # Look for any string value that could be the answer
        for key, val in value.items():
            if isinstance(val, str) and val.strip():
                return {
                    'answer': val,
                    'excerpts': []
                }

        # Fallback: convert to string representation
        return {
            'answer': str(value),
            'excerpts': []
        }

    def stop_processing(self, session_id: str) -> bool:
        """Stop document processing for a session."""
        if session_id in self.running_sessions:
            self.running_sessions[session_id] = False
            return True
        return False
    
    def _build_schema_from_session(self, session) -> Dict[str, Any]:
        """Build schema dict from session.columns (includes user edits from SCHEMA tab).

        This ensures that any schema edits (like adding allowed_values) are used
        when processing new documents.
        """
        # Start with extracted_schema as base (for query, llm_configuration, etc.)
        base_schema = session.metadata.extracted_schema or {}

        # Build schema columns from session.columns (the editable version)
        # Filter out metadata columns that should not be sent to LLM for extraction
        schema_columns = []
        for col in session.columns:
            # Skip columns with no name or metadata columns
            if not col.name:
                continue
            # Skip metadata columns - these are not for LLM extraction
            if col.name.lower() in self.METADATA_COLUMNS or col.name.startswith('_'):
                continue

            col_dict = {
                "name": col.name,
                "column": col.name,  # Some code expects 'column' key
                "definition": col.definition or "",
                "rationale": col.rationale or "",
                "explanation": col.rationale or "",  # Alias for compatibility
            }
            # Include allowed_values if set (important for value constraints)
            if col.allowed_values:
                col_dict["allowed_values"] = col.allowed_values
            # Include auto_expand_threshold if set
            if col.auto_expand_threshold is not None:
                col_dict["auto_expand_threshold"] = col.auto_expand_threshold

            schema_columns.append(col_dict)

        # Build the final schema dict
        return {
            "query": session.schema_query or base_schema.get("query", ""),
            "schema": schema_columns,
            "llm_configuration": base_schema.get("llm_configuration", {}),
        }

    def _enhance_schema_for_extraction(self, extracted_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance schema with detailed extraction instructions based on column metadata."""
        enhanced_schema = extracted_schema.copy()
        
        # Add extraction instructions to each column based on definition and rationale
        for column in enhanced_schema.get("schema", []):
            column_name = column.get("name", "unknown")
            definition = column.get("definition", "")
            rationale = column.get("rationale", "")
            
            # Create detailed extraction prompt that leverages metadata
            extraction_prompt_parts = []
            
            # Start with clear column identification
            extraction_prompt_parts.append(f"For column '{column_name}':")
            
            # Add definition context if available
            if definition:
                extraction_prompt_parts.append(f"Definition: {definition}")
                
            # Add rationale context if available  
            if rationale:
                extraction_prompt_parts.append(f"Purpose: {rationale}")
            
            # Add specific extraction instructions based on metadata
            if definition and rationale:
                # Rich metadata available - create targeted prompt
                extraction_prompt_parts.append(
                    f"Given that this column represents '{definition}' and is important because '{rationale}', "
                    f"carefully extract the most relevant information from the document that matches this purpose. "
                    f"Focus on information that directly relates to {column_name.replace('_', ' ').lower()} as described."
                )
            elif definition:
                # Definition available - use it for context
                extraction_prompt_parts.append(
                    f"Extract information that matches the definition: '{definition}'. "
                    f"Look for data specifically related to {column_name.replace('_', ' ').lower()}."
                )
            else:
                # Fallback for minimal metadata
                extraction_prompt_parts.append(
                    f"Extract relevant information for {column_name.replace('_', ' ').lower()}."
                )
            
            # Add quality guidelines
            extraction_prompt_parts.append(
                "Provide specific, factual information from the document. "
                "If the exact information is not available, return 'Not found' rather than guessing."
            )
            
            # Combine into comprehensive extraction guidance
            column["extraction_guidance"] = " ".join(extraction_prompt_parts)
            
            # Add metadata-aware extraction hints for the QBSD value extractor
            column["extraction_strategy"] = self._generate_extraction_strategy(column_name, definition, rationale)
            
            # Ensure column has basic structure if metadata is missing
            if "definition" not in column or not column["definition"]:
                column["definition"] = f"Information about {column_name.replace('_', ' ').lower()}"
            if "rationale" not in column or not column["rationale"]:
                column["rationale"] = f"Relevant data for {column_name.replace('_', ' ').lower()} analysis"
        
        return enhanced_schema
    
    def _generate_extraction_strategy(self, column_name: str, definition: str, rationale: str) -> Dict[str, Any]:
        """Generate extraction strategy based on column metadata."""
        strategy = {
            "search_keywords": [],
            "fallback_strategy": "heuristic",
            "confidence_threshold": 0.7,
            "extraction_type": "general"
        }
        
        # Analyze definition and rationale to determine extraction approach
        combined_text = f"{definition} {rationale}".lower()
        
        # Generate search keywords from metadata
        import re
        # Extract meaningful terms from definition and rationale
        keywords = set()
        keywords.add(column_name.lower())
        
        # Add words from definition (excluding common words)
        if definition:
            definition_words = re.findall(r'\b\w{3,}\b', definition.lower())
            keywords.update(word for word in definition_words 
                          if word not in {'the', 'and', 'for', 'with', 'this', 'that', 'from', 'about'})
        
        # Determine extraction type based on content
        if any(term in combined_text for term in ['unique', 'identifier', 'name', 'id']):
            strategy["extraction_type"] = "identifier"
            strategy["confidence_threshold"] = 0.8
        elif any(term in combined_text for term in ['numeric', 'number', 'count', 'score', 'value']):
            strategy["extraction_type"] = "numeric"
            strategy["confidence_threshold"] = 0.75
        elif any(term in combined_text for term in ['categorical', 'classification', 'type', 'category']):
            strategy["extraction_type"] = "categorical"
            strategy["confidence_threshold"] = 0.65
        elif any(term in combined_text for term in ['detailed', 'description', 'content', 'text']):
            strategy["extraction_type"] = "text"
            strategy["fallback_strategy"] = "expanded_retrieval"
            strategy["confidence_threshold"] = 0.6
        
        strategy["search_keywords"] = list(keywords)
        return strategy
    
    def _analyze_extraction_quality(self, row_data: Dict[str, Any], extracted_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the quality of extraction for a single row."""
        schema_columns = extracted_schema.get("schema", [])
        total_columns = len(schema_columns)
        
        if total_columns == 0:
            return {
                "success_rate": 0.0,
                "extracted_columns": 0,
                "failed_columns": 0,
                "quality_score": 0.0,
                "column_status": {}
            }
        
        extracted_count = 0
        failed_count = 0
        column_status = {}
        
        # Get actual data (handle both DataRow format and direct format)
        actual_data = row_data.get('data', row_data)
        
        for column in schema_columns:
            column_name = column.get('name')
            if not column_name:
                continue
                
            if column_name in actual_data:
                value = actual_data[column_name]
                
                # Check if extraction was successful based on content
                if self._is_successful_extraction(value, column):
                    extracted_count += 1
                    column_status[column_name] = {
                        "status": "success",
                        "value_type": type(value).__name__,
                        "has_excerpts": isinstance(value, dict) and "excerpts" in value,
                        "definition": column.get("definition", "")
                    }
                else:
                    failed_count += 1
                    column_status[column_name] = {
                        "status": "failed",
                        "reason": "No relevant information found",
                        "definition": column.get("definition", "")
                    }
            else:
                failed_count += 1
                column_status[column_name] = {
                    "status": "missing",
                    "reason": "Column not found in extracted data",
                    "definition": column.get("definition", "")
                }
        
        success_rate = extracted_count / total_columns if total_columns > 0 else 0.0
        quality_score = min(1.0, success_rate * 1.2)  # Bonus for high success rate
        
        return {
            "success_rate": success_rate,
            "extracted_columns": extracted_count,
            "failed_columns": failed_count,
            "total_columns": total_columns,
            "quality_score": quality_score,
            "column_status": column_status
        }
    
    def _is_successful_extraction(self, value: Any, column: Dict[str, Any]) -> bool:
        """Determine if an extraction was successful based on value content."""
        if value is None:
            return False
            
        # Handle QBSD format
        if isinstance(value, dict):
            if "answer" in value:
                answer = value["answer"]
                if answer and str(answer).strip().lower() not in ["not found", "n/a", "none", "unknown", ""]:
                    return True
            return False
        
        # Handle direct values
        if isinstance(value, str):
            cleaned_value = value.strip().lower()
            if cleaned_value in ["not found", "n/a", "none", "unknown", ""] or len(cleaned_value) < 2:
                return False
            return True
        
        # Handle other types
        return True
    
    def _create_row_preview(self, row_data: Dict[str, Any], extracted_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create a preview of the row with schema context."""
        schema_columns = extracted_schema.get("schema", [])
        actual_data = row_data.get('data', row_data)
        
        preview = {
            "row_name": row_data.get("row_name", actual_data.get("_row_name", "Unknown")),
            "papers": row_data.get("papers", actual_data.get("_papers", [])),
            "columns": {}
        }
        
        # Create column previews with schema context
        for column in schema_columns[:3]:  # Limit to first 3 columns for preview
            column_name = column.get('name')
            if not column_name or column_name not in actual_data:
                continue
                
            value = actual_data[column_name]
            
            # Create preview with truncation
            if isinstance(value, dict) and "answer" in value:
                preview_value = str(value["answer"])[:100]
                has_excerpts = bool(value.get("excerpts"))
            else:
                preview_value = str(value)[:100]
                has_excerpts = False
            
            if len(preview_value) > 97:
                preview_value = preview_value[:97] + "..."
                
            preview["columns"][column_name] = {
                "value": preview_value,
                "definition": column.get("definition", ""),
                "has_excerpts": has_excerpts
            }
        
        return preview
    
    def _get_llm_config_for_extraction(self, extracted_schema: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Get LLM configuration for value extraction, prioritizing user selection."""
        session_dir = Path("./data") / session_id
        
        # First priority: User-provided LLM configuration
        user_config_file = session_dir / "user_llm_config.json"
        if user_config_file.exists():
            try:
                with open(user_config_file) as f:
                    user_config = json.load(f)
                    print(f"DEBUG: Using user-selected LLM configuration for session {session_id}")
                    return user_config
            except Exception as e:
                print(f"DEBUG: Could not load user LLM config: {e}")
        
        # Second priority: Preserved schema metadata LLM configuration
        if "llm_configuration" in extracted_schema:
            llm_config = extracted_schema["llm_configuration"]
            
            # Use value_extraction_backend if available, fallback to schema_creation_backend
            if llm_config.get("value_extraction_backend"):
                print(f"DEBUG: Using preserved value_extraction_backend for session {session_id}")
                return llm_config["value_extraction_backend"]
            elif llm_config.get("schema_creation_backend"):
                print(f"DEBUG: Using preserved schema_creation_backend for extraction in session {session_id}")
                return llm_config["schema_creation_backend"]
        
        # Third priority: Session-level QBSD config file
        qbsd_config_file = session_dir / "qbsd_config.json"
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    
                # Use value_extraction_backend if available
                if "value_extraction_backend" in qbsd_config:
                    print(f"DEBUG: Using session QBSD value_extraction_backend for session {session_id}")
                    return qbsd_config["value_extraction_backend"]
                elif "schema_creation_backend" in qbsd_config:
                    print(f"DEBUG: Using session QBSD schema_creation_backend for extraction in session {session_id}")
                    return qbsd_config["schema_creation_backend"]
                elif "backend" in qbsd_config:  # Legacy support
                    print(f"DEBUG: Using legacy backend config for session {session_id}")
                    return qbsd_config["backend"]
            except Exception as e:
                print(f"DEBUG: Could not load session QBSD config: {e}")
        
        # Fallback to default configuration
        print(f"DEBUG: Using default LLM configuration for extraction in session {session_id}")
        return {
            "provider": "gemini",
            "model": "gemini-2.5-flash-lite",  # Use lite model for extraction by default
            "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
            "temperature": DEFAULT_TEMPERATURE
        }
    
    def _clean_system_files(self, directory: Path) -> None:
        """Remove system files like .DS_Store that shouldn't be processed as documents."""
        system_files = ['.DS_Store', '._.DS_Store', 'Thumbs.db', '.gitkeep']
        
        for system_file in system_files:
            file_path = directory / system_file
            if file_path.exists():
                print(f"🧹 Removing system file: {file_path}")
                file_path.unlink()
                
        # Also remove any hidden files starting with ._
        for file_path in directory.glob('._*'):
            print(f"🧹 Removing hidden system file: {file_path}")
            file_path.unlink()

    def _compute_statistics_from_data(self, session_id: str, session: VisualizationSession) -> Optional[DataStatistics]:
        """Compute statistics from the merged data.jsonl file.

        NOTE: Similar to qbsd_runner._compute_statistics_from_extracted_data() but:
        - Reads from data.jsonl (not extracted_data.jsonl)
        - Handles DataRow format with 'data' wrapper
        - Does not include schema_evolution (upload sessions don't track evolution)

        Args:
            session_id: The session ID
            session: The session object with columns

        Returns:
            DataStatistics object or None if no data available
        """
        session_dir = Path("./data") / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            print(f"⚠️  Statistics: No data.jsonl found for session {session_id}")
            return None

        # Read all rows from the data file
        data_rows = []
        try:
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data_rows.append(json.loads(line))
        except Exception as e:
            print(f"⚠️  Statistics: Error reading data: {e}")
            return None

        if not data_rows:
            print(f"⚠️  Statistics: No data rows found in data.jsonl")
            return None

        if not session.columns:
            print(f"⚠️  Statistics: No columns defined in session {session_id}")
            return None

        # Build column stats from session.columns + data
        columns = []
        for col in session.columns:
            # Count non-null values for this column
            def is_valid_value(value):
                if value is None:
                    return False
                if isinstance(value, dict):
                    answer = value.get("answer")
                    if answer is None or answer == "None" or answer == "" or answer == "[]":
                        return False
                    # Also check for "not found" type values
                    if isinstance(answer, str) and answer.strip().lower() in ["not found", "n/a", "none", "unknown"]:
                        return False
                    return True
                # For non-dict values, check if it's not None or "None" string
                if isinstance(value, str) and value.strip().lower() in ["not found", "n/a", "none", "unknown", ""]:
                    return False
                return value != "None" and value != "" and value != "[]"

            non_null_count = 0
            unique_values = set()

            for row in data_rows:
                # Handle both DataRow format (with 'data' key) and direct format
                row_data = row.get('data', row)

                if col.name in row_data:
                    value = row_data[col.name]
                    if is_valid_value(value):
                        non_null_count += 1
                    # Count unique values (serialize to JSON for comparison)
                    try:
                        unique_values.add(json.dumps(value, sort_keys=True))
                    except (TypeError, ValueError):
                        unique_values.add(str(value))

            unique_count = len(unique_values)

            col_info = ColumnInfo(
                name=col.name,
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",  # Upload data is typically complex objects
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

        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness):
            completeness = 0.0

        stats = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            completeness=completeness,
            column_stats=columns,
            schema_evolution=None  # Upload sessions don't have schema evolution
        )

        print(f"✓ Statistics computed: {len(data_rows)} rows, {len(columns)} columns, {completeness:.1f}% complete")
        return stats