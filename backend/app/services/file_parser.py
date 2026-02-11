"""File parsing and data processing service."""

import csv
import json
import logging
import math
import io
import re
import aiofiles
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
from fastapi import UploadFile

logger = logging.getLogger(__name__)

from app.models.upload import (
    FileValidationResult, ColumnMappingRequest, SchemaValidationResult,
    QBSDSchemaFormat, SchemaColumn, CompatibilityCheck, DualFileUploadResult
)
from app.models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData, SchemaEvolution, SchemaSnapshot
from app.core.config import DEFAULT_DATA_DIR, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

_COLUMN_DISPLAY_NAMES = {
    '_row_name': 'Doc Name',
    'row_name': 'Doc Name',
    '_unit_name': 'Observation Unit',
    '_source_document': 'Source Document',
}


def format_column_header(name: str) -> str:
    """Convert internal column name to display header (mirrors frontend formatColumnName).

    Examples:
        'confidence_assessment_methods' -> 'Confidence Assessment Methods'
        '_row_name' -> 'Doc Name'
        '_unit_name' -> 'Observation Unit'
    """
    if name in _COLUMN_DISPLAY_NAMES:
        return _COLUMN_DISPLAY_NAMES[name]
    clean = name.lstrip('_')
    return ' '.join(word.capitalize() for word in clean.split('_'))


class FileParser:
    """Handles file parsing and data processing."""

    # Metadata columns that should not be included as schema columns for LLM extraction
    # Includes both internal names and their display-header equivalents (for reimported CSVs)
    METADATA_COLUMNS = {
        'papers', 'document_directory', 'row_name', '_row_name', '_papers', '_metadata',
        '_unit_name', 'unit_name', '_source_document', 'source_document',
        '_parent_document', '_observation_unit', '_unit_confidence',
        'doc name', 'observation unit', 'source document',
    }

    @staticmethod
    def _extract_and_pop_field(data: dict, field_variations: List[str]) -> Optional[str]:
        """Extract field value from data dict, trying multiple field names.

        Removes the field from the dict and returns the cleaned value.

        Args:
            data: Dictionary to extract from (will be modified)
            field_variations: List of possible field names to try

        Returns:
            Stripped string value if found and non-empty, else None
        """
        for field_name in field_variations:
            if field_name in data:
                raw_val = data.pop(field_name)
                if raw_val is not None and str(raw_val).strip():
                    return str(raw_val).strip()
        return None

    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
    
    async def validate_file(self, file: UploadFile) -> FileValidationResult:
        """Validate uploaded file."""
        errors = []
        warnings = []
        detected_format = None
        sample_data = None
        estimated_rows = None
        estimated_columns = None
        
        # Check file size
        if file.size > 100 * 1024 * 1024:  # 100MB limit
            errors.append("File size exceeds 100MB limit")
        
        # Detect format from extension and content type
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            detected_format = "csv"
        elif filename.endswith('.json') or filename.endswith('.jsonl'):
            detected_format = "json"
        else:
            errors.append("Unsupported file format. Please upload CSV or JSON files.")
        
        # Try to read and validate content
        try:
            if detected_format == "csv":
                # Use progressive reading for CSV to handle large metadata sections
                # (QBSD exports can have extensive comment headers that exceed 8KB)
                csv_result = await self._validate_csv_with_metadata(file)
                errors.extend(csv_result.get("errors", []))
                warnings.extend(csv_result.get("warnings", []))
                estimated_rows = csv_result.get("estimated_rows")
                estimated_columns = csv_result.get("estimated_columns")
                sample_data = csv_result.get("sample_data")

            elif detected_format == "json":
                # Validate JSON structure
                try:
                    if filename.endswith('.jsonl'):
                        content = await file.read(8192)  # Read first 8KB for JSONL
                        await file.seek(0)
                        # JSONL format - each line is a JSON object
                        lines = content.decode('utf-8').strip().split('\n')
                        sample_obj = json.loads(lines[0])
                        estimated_rows = len(lines)
                        estimated_columns = len(sample_obj.keys())
                        # Filter out None keys for Pydantic validation
                        sample_obj = {k: v for k, v in sample_obj.items() if k is not None}
                        sample_data = [sample_obj]
                    else:
                        # Regular JSON - read full file to avoid truncation errors
                        # (complete QBSD exports can be large with embedded data)
                        full_content = await file.read()
                        await file.seek(0)
                        data = json.loads(full_content.decode('utf-8'))
                        if isinstance(data, list):
                            estimated_rows = len(data)
                            if data:
                                estimated_columns = len(data[0].keys()) if isinstance(data[0], dict) else 1
                                # Filter out None keys from each sample item for Pydantic validation
                                sample_data = [
                                    {k: v for k, v in item.items() if k is not None}
                                    if isinstance(item, dict) else item
                                    for item in data[:3]
                                ]
                        elif isinstance(data, dict):
                            if 'data' in data and isinstance(data.get('data'), list):
                                # Complete export format with "data" array
                                warnings.append("Detected QBSD complete export format")
                                estimated_rows = len(data['data'])
                                if 'schema' in data and isinstance(data['schema'], dict):
                                    estimated_columns = len(data['schema'].get('columns', []))
                                else:
                                    estimated_columns = len(data['data'][0].get('data', {}).keys()) if data['data'] else 0
                            elif 'schema' in data:
                                # QBSD schema format
                                warnings.append("Detected QBSD schema format")
                                estimated_columns = len(data.get('schema', []))
                            else:
                                estimated_rows = 1
                                estimated_columns = len(data.keys())
                                # Filter out None keys for Pydantic validation
                                sample_data = [{k: v for k, v in data.items() if k is not None}]
                except json.JSONDecodeError as e:
                    errors.append(f"Invalid JSON format: {str(e)}")
                    
        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
        
        return FileValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            detected_format=detected_format,
            estimated_rows=estimated_rows,
            estimated_columns=estimated_columns,
            sample_data=sample_data
        )

    async def _validate_csv_with_metadata(self, file: UploadFile) -> dict:
        """
        Validate CSV file with progressive reading to handle large metadata sections.

        QBSD-exported CSVs can have extensive metadata comments (session ID, query,
        LLM config, column definitions with rationales) that may exceed 8KB. This
        method reads in chunks until finding header + data row, up to 256KB max.
        """
        INITIAL_CHUNK = 8192      # 8KB initial read
        MAX_CHUNK = 65536         # 64KB max single chunk
        MAX_TOTAL = 262144        # 256KB max total read for validation

        result = {
            "errors": [],
            "warnings": [],
            "estimated_rows": None,
            "estimated_columns": None,
            "sample_data": None
        }

        accumulated = b""
        total_read = 0
        chunk_size = INITIAL_CHUNK

        while total_read < MAX_TOTAL:
            chunk = await file.read(chunk_size)
            if not chunk:
                break  # End of file

            accumulated += chunk
            total_read += len(chunk)

            try:
                lines = accumulated.decode('utf-8').split('\n')
                data_lines = [l for l in lines if l.strip() and not l.strip().startswith('#')]
                comment_count = len([l for l in lines if l.strip().startswith('#')])

                if len(data_lines) >= 1:  # Found at least header row (data rows optional for schema-only exports)
                    await file.seek(0)  # Reset file position for subsequent processing

                    if comment_count > 0:
                        result["warnings"].append(
                            f"Detected {comment_count} metadata comment lines - will be preserved during import"
                        )

                    # Check for schema-only export (header but no data rows)
                    if len(data_lines) == 1:
                        result["warnings"].append(
                            "Schema-only CSV detected (no data rows) - will import schema structure only"
                        )

                    # Parse CSV structure from data lines
                    try:
                        dialect = csv.Sniffer().sniff(data_lines[0])
                        # Force doublequote=True - Sniffer often gets this wrong when
                        # sniffing only the header row (which has no quotes to escape)
                        dialect.doublequote = True
                        reader = csv.DictReader(data_lines, dialect=dialect)
                        result["estimated_rows"] = len(data_lines) - 1  # Exclude header

                        # Try to get sample data (may be empty for schema-only CSVs)
                        try:
                            sample = next(reader)
                            # Filter out None keys - can occur if CSV has more fields
                            # than headers (shouldn't happen with proper parsing but
                            # provides defensive safety for Pydantic validation)
                            sample = {k: v for k, v in sample.items() if k is not None}
                            result["estimated_columns"] = len(sample)
                            result["sample_data"] = [sample]
                        except StopIteration:
                            # Schema-only CSV: header row but no data rows
                            # Parse header manually to get column count
                            header_cols = list(csv.reader([data_lines[0]], dialect=dialect))[0]
                            result["estimated_columns"] = len(header_cols)
                            result["sample_data"] = []
                    except Exception as e:
                        result["errors"].append(f"Error parsing CSV structure: {str(e)}")

                    return result

            except UnicodeDecodeError:
                pass  # Partial UTF-8 at chunk boundary, continue reading

            # Double chunk size for next iteration (exponential backoff)
            chunk_size = min(chunk_size * 2, MAX_CHUNK)

        # Reset file position
        await file.seek(0)

        # Validation failed - generate appropriate error message
        if total_read >= MAX_TOTAL:
            result["errors"].append(
                f"CSV metadata exceeds {MAX_TOTAL // 1024}KB - could not find data rows within validation limit"
            )
        else:
            result["errors"].append("CSV file must have at least a header row")

        return result

    async def save_uploaded_file(self, session_id: str, file: UploadFile):
        """Save uploaded file to data directory."""
        session_dir = self.data_dir / session_id
        session_dir.mkdir(exist_ok=True)
        
        file_path = session_dir / file.filename
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        return file_path
    
    async def parse_file(self, session_id: str, mapping: Optional[ColumnMappingRequest] = None) -> Dict[str, Any]:
        """Parse file and extract data."""
        session_dir = self.data_dir / session_id
        
        # Find the uploaded file
        file_path = None
        for f in session_dir.glob("*"):
            if f.is_file() and not f.name.startswith('.'):
                file_path = f
                break
        
        if not file_path:
            raise FileNotFoundError("No uploaded file found")
        
        # Parse based on file type
        if file_path.suffix.lower() == '.csv':
            result = await self._parse_csv(file_path, mapping)
        elif file_path.suffix.lower() in ['.json', '.jsonl']:
            result = await self._parse_json(file_path)
        else:
            raise ValueError("Unsupported file format")

        # Save documents_batch_size to qbsd_config.json if present (for loaded exports)
        if result.get("documents_batch_size") is not None:
            qbsd_config_file = session_dir / "qbsd_config.json"
            try:
                # Load existing config or create new one
                if qbsd_config_file.exists():
                    with open(qbsd_config_file) as f:
                        config = json.load(f)
                else:
                    config = {}
                config["documents_batch_size"] = result["documents_batch_size"]
                with open(qbsd_config_file, 'w') as f:
                    json.dump(config, f, indent=2)
                logger.debug("Saved documents_batch_size=%s to qbsd_config.json", result['documents_batch_size'])
            except Exception as e:
                logger.debug("Could not save documents_batch_size to config: %s", e)

        return result
    
    async def _parse_csv(self, file_path: Path, mapping: Optional[ColumnMappingRequest] = None) -> Dict[str, Any]:
        """Parse CSV file, handling metadata comments."""
        
        # First, extract metadata from comment lines and clean the CSV
        metadata_info = self._extract_csv_metadata(file_path)
        
        # Read CSV with leading comment lines filtered out
        if metadata_info['has_comments']:
            # Create clean CSV content - skip leading comments only, filter empty lines throughout
            clean_lines = []
            past_comments = False
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped:
                        continue  # Skip empty lines
                    if not past_comments and stripped.startswith('#'):
                        continue  # Skip comment lines at start only
                    past_comments = True
                    clean_lines.append(line)

            # Create temporary clean CSV content
            clean_csv_content = ''.join(clean_lines)
            df = pd.read_csv(io.StringIO(clean_csv_content))
        else:
            # Standard CSV without comments
            df = pd.read_csv(file_path)
        
        # Apply column mapping if provided
        if mapping:
            df = df.rename(columns=mapping.column_mappings)
        
        # Extract columns info, using metadata if available
        columns = []
        for col in df.columns:
            # Skip metadata columns - these are not schema columns for LLM extraction
            if col.lower() in self.METADATA_COLUMNS or col.startswith('_'):
                continue

            non_null_count = int(df[col].notna().sum())
            unique_count = int(df[col].nunique())

            # Ensure values are valid
            if non_null_count < 0:
                non_null_count = 0
            if unique_count < 0:
                unique_count = 0

            # Use metadata if available, otherwise generate basic info
            allowed_values = None
            if col in metadata_info['column_definitions']:
                definition = metadata_info['column_definitions'][col]['definition']
                rationale = metadata_info['column_definitions'][col]['rationale']
                allowed_values = metadata_info['column_definitions'][col].get('allowed_values')
            else:
                definition = f"Column containing {col.replace('_', ' ').lower()} data"
                rationale = "Imported from CSV data"

            col_info = ColumnInfo(
                name=col,
                data_type=str(df[col].dtype),
                non_null_count=non_null_count,
                unique_count=unique_count,
                definition=definition,
                rationale=rationale,
                allowed_values=allowed_values
            )
            columns.append(col_info)
        
        # Calculate statistics (handle NaN values)
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.notna().sum().sum()
        completeness = float(non_null_cells / total_cells * 100) if total_cells > 0 else 0.0
        
        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness) or not (0 <= completeness <= 100):
            completeness = 0.0
            
        # Check if CSV has _excerpts or _excerpt columns that should be merged
        has_excerpt_columns = any(
            col.endswith('_excerpts') or col.endswith('_excerpt')
            for col in df.columns
        )
        source_filename = file_path.name if has_excerpt_columns else None

        # Filter out excerpt columns from the schema (they'll be merged with parent columns)
        if has_excerpt_columns:
            columns = [col for col in columns if not (col.name.endswith('_excerpts') or col.name.endswith('_excerpt'))]

        # Convert schema_evolution dict to SchemaEvolution model if present (backward compatible)
        schema_evolution = None
        if metadata_info.get('schema_evolution'):
            try:
                evolution_data = metadata_info['schema_evolution']
                snapshots = [
                    SchemaSnapshot(
                        iteration=s["iteration"],
                        documents_processed=s.get("documents_processed", []),
                        total_columns=s["total_columns"],
                        new_columns=s.get("new_columns", []),
                        cumulative_documents=s.get("cumulative_documents", 0)
                    )
                    for s in evolution_data.get("snapshots", [])
                ]
                schema_evolution = SchemaEvolution(
                    snapshots=snapshots,
                    column_sources=evolution_data.get("column_sources", {})
                )
                logger.debug("Imported schema evolution from CSV with %d snapshots", len(snapshots))
            except Exception as e:
                logger.debug("Could not parse CSV schema_evolution: %s", e)

        statistics = DataStatistics(
            total_rows=len(df),
            total_columns=len(columns),  # Use filtered column count
            total_documents=metadata_info.get('total_documents', 0) or len(df),  # Fallback to row count
            completeness=completeness,
            column_stats=columns,
            schema_evolution=schema_evolution,  # Include if parsed (backward compatible - None if not present)
            skipped_documents=metadata_info.get('skipped_documents', [])
        )

        # Save processed data as JSONL
        data_file = file_path.parent / "data.jsonl"
        with open(data_file, 'w') as f:
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                # Merge _excerpts columns into QBSD format if they exist
                if has_excerpt_columns:
                    merged_data = self._merge_excerpt_columns(row_dict, source_filename)
                else:
                    merged_data = self._sanitize_data_dict(row_dict)

                # Extract metadata fields from data using helper
                # Note: _row_name is the export format, row_name is the legacy format
                row_name_value = self._extract_and_pop_field(
                    merged_data, ['_row_name', 'row_name', 'Row Name', 'Row_Name', 'RowName', 'Doc Name']
                )
                unit_name_value = self._extract_and_pop_field(
                    merged_data, ['_unit_name', 'unit_name', 'Unit Name', 'Observation Unit']
                )
                source_doc_value = self._extract_and_pop_field(
                    merged_data, ['_source_document', 'source_document', 'Source Document']
                )

                # Remove document_directory (local path, not useful in exports)
                merged_data.pop('document_directory', None)

                # Extract papers field from data if present
                papers_col_names = ['Papers', 'papers', 'Paper', 'paper', 'Documents', 'documents']
                papers_value = []
                for col_name in papers_col_names:
                    if col_name in merged_data:
                        raw_val = merged_data.pop(col_name)  # Remove from data dict
                        # Handle QBSD answer format
                        if isinstance(raw_val, dict) and 'answer' in raw_val:
                            raw_val = raw_val.get('answer')
                        # Convert to list
                        if isinstance(raw_val, str):
                            papers_value = [raw_val] if raw_val else []
                        elif isinstance(raw_val, list):
                            papers_value = raw_val
                        break

                row_data = DataRow(
                    data=merged_data,
                    papers=papers_value,
                    row_name=row_name_value,
                    unit_name=unit_name_value,        # Uses field name, Pydantic handles alias
                    source_document=source_doc_value  # Uses field name, Pydantic handles alias
                )
                f.write(json.dumps(row_data.model_dump(by_alias=True)) + '\n')
        
        # Include extracted metadata in the result
        result = {"columns": columns, "statistics": statistics}
        
        # Add metadata information if available
        if metadata_info['has_comments']:
            result["extracted_metadata"] = {
                "query": metadata_info['query'],
                "llm_config": metadata_info['llm_config'],
                "original_session_id": metadata_info['session_id'],
                "generated_timestamp": metadata_info['generated_timestamp'],
                "column_count_with_metadata": len(metadata_info['column_definitions']),
                "observation_unit": metadata_info.get('observation_unit')  # Include if parsed
            }

        return result
    
    async def _parse_json(self, file_path: Path) -> Dict[str, Any]:
        """Parse JSON/JSONL file."""
        schema_evolution = None  # Will be extracted if present (backward compatible)
        documents_batch_size = None  # Will be extracted if present (backward compatible)
        observation_unit = None  # Will be extracted if present (backward compatible)
        metadata_info = {}  # Will hold query, llm_config, etc. from complete export

        schema_metadata_from_export = {}  # Populated from complete export's schema.columns

        if file_path.suffix.lower() == '.jsonl':
            # JSONL format
            data_rows = []
            with open(file_path) as f:
                for line in f:
                    if line.strip():
                        obj = json.loads(line)
                        data_rows.append(obj)
        else:
            # Regular JSON - could be complete export or raw data
            with open(file_path) as f:
                data = json.load(f)

                # Check if this is a complete QBSD export with schema_evolution
                if isinstance(data, dict):
                    # Extract schema_evolution if present (backward compatible)
                    if "schema_evolution" in data:
                        try:
                            evolution_data = data["schema_evolution"]
                            snapshots = [
                                SchemaSnapshot(
                                    iteration=s["iteration"],
                                    documents_processed=s.get("documents_processed", []),
                                    total_columns=s["total_columns"],
                                    new_columns=s.get("new_columns", []),
                                    cumulative_documents=s.get("cumulative_documents", 0)
                                )
                                for s in evolution_data.get("snapshots", [])
                            ]
                            schema_evolution = SchemaEvolution(
                                snapshots=snapshots,
                                column_sources=evolution_data.get("column_sources", {})
                            )
                            logger.debug("Imported schema evolution with %d snapshots", len(snapshots))
                        except Exception as e:
                            logger.debug("Could not parse schema_evolution: %s", e)

                    # Extract observation_unit if present (backward compatible)
                    if "observation_unit" in data:
                        try:
                            observation_unit = data["observation_unit"]
                            logger.debug("Imported observation unit: %s", observation_unit.get('name'))
                        except Exception as e:
                            logger.debug("Could not parse observation_unit: %s", e)

                    # Extract documents_batch_size from metadata if present (backward compatible)
                    if "metadata" in data and isinstance(data["metadata"], dict):
                        documents_batch_size = data["metadata"].get("documents_batch_size")
                        if documents_batch_size is not None:
                            logger.debug("Imported documents_batch_size: %s", documents_batch_size)

                    # Extract metadata from complete QBSD export (.qbsd.json format)
                    if "query" in data and "schema" in data and isinstance(data["schema"], dict) and "columns" in data["schema"]:
                        metadata_info["query"] = data["query"]
                        if data.get("llm_configuration"):
                            metadata_info["llm_config"] = data["llm_configuration"]
                        export_meta = data.get("metadata", {})
                        for field in ("total_documents", "skipped_documents", "session_id", "generated_timestamp"):
                            if export_meta.get(field):
                                metadata_info[field] = export_meta[field]
                        # Pre-populate schema column definitions from the export
                        for col_def in data["schema"]["columns"]:
                            if isinstance(col_def, dict) and "name" in col_def:
                                schema_metadata_from_export[col_def["name"]] = {
                                    "definition": col_def.get("definition", ""),
                                    "rationale": col_def.get("rationale", ""),
                                    "allowed_values": col_def.get("allowed_values"),
                                    "data_type": col_def.get("data_type"),
                                    "source_document": col_def.get("source_document"),
                                    "discovery_iteration": col_def.get("discovery_iteration"),
                                }
                        logger.debug("Extracted QBSD export metadata: query=%s, %d column definitions",
                                     data["query"][:50] if data["query"] else "", len(schema_metadata_from_export))

                    # Check if this is a complete export format with "data" array
                    if "data" in data and isinstance(data["data"], list):
                        data_rows = data["data"]
                    elif "schema" in data:
                        # QBSD schema format without data array
                        data_rows = [data]
                    else:
                        data_rows = [data]
                elif isinstance(data, list):
                    data_rows = data
                else:
                    data_rows = [data]

        if not data_rows:
            raise ValueError("No data found in file")

        # Extract schema metadata from row-level "schema" field (legacy formats)
        # Only needed when we don't already have column definitions from a complete export
        schema_metadata = {}
        sample_row = data_rows[0]
        if not schema_metadata_from_export:
            if isinstance(sample_row, dict) and "schema" in sample_row and isinstance(sample_row["schema"], list):
                for schema_col in sample_row["schema"]:
                    if isinstance(schema_col, dict) and "name" in schema_col:
                        schema_metadata[schema_col["name"]] = {
                            "definition": schema_col.get("definition", ""),
                            "rationale": schema_col.get("rationale", ""),
                            "allowed_values": schema_col.get("allowed_values")
                        }

        columns = []

        # When we have schema definitions from a complete export, use those as the
        # definitive column list (individual rows may be sparse / missing columns)
        if schema_metadata_from_export:
            for key, meta in schema_metadata_from_export.items():
                # Count non-null across all rows, handling both DataRow and flat formats
                non_null = 0
                values_set = set()
                for row in data_rows:
                    if 'data' in row and isinstance(row.get('data'), dict):
                        val = row['data'].get(key)
                    else:
                        val = row.get(key)
                    if val is not None:
                        non_null += 1
                    values_set.add(json.dumps(val, sort_keys=True) if val is not None else 'null')

                col_info = ColumnInfo(
                    name=key,
                    data_type=meta.get("data_type") or "object",
                    non_null_count=non_null,
                    unique_count=len(values_set),
                    definition=meta.get("definition", ""),
                    rationale=meta.get("rationale", ""),
                    allowed_values=meta.get("allowed_values"),
                    source_document=meta.get("source_document"),
                    discovery_iteration=meta.get("discovery_iteration"),
                )
                columns.append(col_info)
        elif '_row_name' in sample_row and '_papers' in sample_row:
            # QBSD extracted data format
            for key, value in sample_row.items():
                # Skip metadata columns - these are not schema columns for LLM extraction
                if key.startswith('_') or key.lower() in self.METADATA_COLUMNS:
                    continue

                # Get metadata from schema if available
                meta = schema_metadata.get(key, {})
                col_info = ColumnInfo(
                    name=key,
                    data_type="object",
                    non_null_count=sum(1 for row in data_rows if key in row and row[key] is not None),
                    unique_count=len(set(json.dumps(row.get(key, None), sort_keys=True) for row in data_rows)),
                    definition=meta.get("definition", ""),
                    rationale=meta.get("rationale", ""),
                    allowed_values=meta.get("allowed_values")
                )
                columns.append(col_info)
        elif 'data' in sample_row and isinstance(sample_row.get('data'), dict):
            # DataRow format (from complete export)
            sample_data = sample_row['data']
            for key in sample_data.keys():
                # Skip metadata columns - these are not schema columns for LLM extraction
                if key.startswith('_') or key.lower() in self.METADATA_COLUMNS:
                    continue

                # Get metadata from schema if available
                meta = schema_metadata.get(key, {})
                col_info = ColumnInfo(
                    name=key,
                    data_type=type(sample_data[key]).__name__,
                    non_null_count=sum(1 for row in data_rows if 'data' in row and key in row['data'] and row['data'][key] is not None),
                    unique_count=len(set(json.dumps(row.get('data', {}).get(key, None), sort_keys=True) for row in data_rows)),
                    definition=meta.get("definition", ""),
                    rationale=meta.get("rationale", ""),
                    allowed_values=meta.get("allowed_values")
                )
                columns.append(col_info)
        else:
            # Regular JSON format
            for key in sample_row.keys():
                # Skip metadata columns - these are not schema columns for LLM extraction
                if key.startswith('_') or key.lower() in self.METADATA_COLUMNS:
                    continue

                # Get metadata from schema if available
                meta = schema_metadata.get(key, {})
                col_info = ColumnInfo(
                    name=key,
                    data_type=type(sample_row[key]).__name__,
                    non_null_count=sum(1 for row in data_rows if key in row and row[key] is not None),
                    unique_count=len(set(json.dumps(row.get(key, None), sort_keys=True) for row in data_rows)),
                    definition=meta.get("definition", ""),
                    rationale=meta.get("rationale", ""),
                    allowed_values=meta.get("allowed_values")
                )
                columns.append(col_info)

        # Calculate statistics
        total_cells = len(data_rows) * len(columns) if columns else 0
        non_null_cells = sum(col.non_null_count for col in columns)

        # Calculate completeness safely
        completeness = float(non_null_cells / total_cells * 100) if total_cells > 0 else 0.0

        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness) or not (0 <= completeness <= 100):
            completeness = 0.0

        statistics = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            total_documents=metadata_info.get('total_documents', 0) or len(data_rows),  # Fallback to row count
            completeness=completeness,
            column_stats=columns,
            schema_evolution=schema_evolution,  # Include if parsed (backward compatible - None if not present)
            skipped_documents=metadata_info.get('skipped_documents', [])
        )

        # Save processed data as JSONL
        data_file = file_path.parent / "data.jsonl"
        with open(data_file, 'w') as f:
            for row_data in data_rows:
                if '_row_name' in row_data:
                    # QBSD format - preserve _unit_name from extraction (set by observation unit logic)
                    data_row = DataRow(
                        row_name=row_data.get('_row_name'),
                        papers=row_data.get('_papers', []),
                        data={k: v for k, v in row_data.items() if not k.startswith('_')},
                        unit_name=row_data.get('_unit_name')  # Preserve actual unit_name from extraction
                    )
                elif 'data' in row_data and isinstance(row_data.get('data'), dict):
                    # Already in DataRow format - preserve unit metadata if present
                    data_row = DataRow(
                        row_name=row_data.get('row_name'),
                        papers=row_data.get('papers', []),
                        data=row_data['data'],
                        unit_name=row_data.get('unit_name') or row_data.get('_unit_name'),
                        source_document=row_data.get('source_document') or row_data.get('_source_document'),
                        parent_document=row_data.get('parent_document') or row_data.get('_parent_document'),
                    )
                else:
                    # Regular format - extract papers field if present
                    papers_col_names = ['Papers', 'papers', 'Paper', 'paper', 'Documents', 'documents']
                    papers_value = []
                    clean_data = dict(row_data)  # Copy to avoid modifying original
                    for col_name in papers_col_names:
                        if col_name in clean_data:
                            raw_val = clean_data.pop(col_name)
                            # Handle QBSD answer format
                            if isinstance(raw_val, dict) and 'answer' in raw_val:
                                raw_val = raw_val.get('answer')
                            # Convert to list
                            if isinstance(raw_val, str):
                                papers_value = [raw_val] if raw_val else []
                            elif isinstance(raw_val, list):
                                papers_value = raw_val
                            break
                    data_row = DataRow(data=clean_data, papers=papers_value)

                f.write(json.dumps(data_row.model_dump(by_alias=True)) + '\n')

        result = {"columns": columns, "statistics": statistics}
        if documents_batch_size is not None:
            result["documents_batch_size"] = documents_batch_size
        if observation_unit is not None:
            result["observation_unit"] = observation_unit

        # Include extracted_metadata for complete export format (same structure as CSV path)
        if metadata_info.get("query") or metadata_info.get("llm_config"):
            result["extracted_metadata"] = {
                "query": metadata_info.get("query"),
                "llm_config": metadata_info.get("llm_config"),
                "original_session_id": metadata_info.get("session_id"),
                "generated_timestamp": metadata_info.get("generated_timestamp"),
                "column_count_with_metadata": len(schema_metadata_from_export),
                "observation_unit": observation_unit,
            }

        return result
    
    def _sanitize_value(self, value):
        """Sanitize a value to ensure it's JSON serializable."""
        if value is None:
            return None
        
        # Handle pandas NaN, infinity values
        if isinstance(value, float):
            if math.isnan(value):
                return None
            elif math.isinf(value):
                return "Infinity" if value > 0 else "-Infinity"
        
        # Handle string representations of NaN/inf
        if isinstance(value, str):
            if value.lower() in ['nan', 'null', '']:
                return None
            elif value.lower() in ['inf', 'infinity']:
                return "Infinity"
            elif value.lower() in ['-inf', '-infinity']:
                return "-Infinity"
        
        return value
    
    def _sanitize_data_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize all values in a data dictionary."""
        sanitized = {}
        for key, value in data_dict.items():
            if isinstance(value, dict):
                sanitized[key] = self._sanitize_data_dict(value)
            elif isinstance(value, list):
                sanitized[key] = [self._sanitize_value(v) for v in value]
            else:
                sanitized[key] = self._sanitize_value(value)
        return sanitized

    def _merge_excerpt_columns(self, data_dict: Dict[str, Any], source_filename: str = None) -> Dict[str, Any]:
        """Merge _excerpts columns with their parent columns into QBSD format.

        Converts CSV format with separate columns:
            {'Column': 'value', 'Column_excerpts': 'excerpt text'}
        Into QBSD format:
            {'Column': {'answer': 'value', 'excerpts': [{'text': 'excerpt text', 'source': 'filename'}]}}

        Maintains backward compatibility - if no _excerpts columns exist, returns data as-is.
        """
        import ast

        merged = {}
        processed_excerpt_keys = set()

        # First pass: identify all excerpt columns (both _excerpts and _excerpt suffixes)
        excerpt_columns = {k for k in data_dict.keys() if k.endswith('_excerpts') or k.endswith('_excerpt')}

        for key, value in data_dict.items():
            # Skip excerpt columns - they'll be merged with parent
            if key in excerpt_columns:
                processed_excerpt_keys.add(key)
                continue

            # Check if this column has a corresponding excerpt column (try both suffixes)
            excerpt_key = f"{key}_excerpts"
            if excerpt_key not in data_dict:
                excerpt_key = f"{key}_excerpt"
            if excerpt_key in data_dict:
                # Merge into QBSD format
                excerpt_value = data_dict[excerpt_key]
                excerpts = []

                if excerpt_value and excerpt_value != '' and not (isinstance(excerpt_value, float) and math.isnan(excerpt_value)):
                    # Parse excerpt value - could be string, list, or already structured
                    if isinstance(excerpt_value, str):
                        # Check for QBSD export format: "{'text': '...', 'source': '...'} | {'text': '...', 'source': '...'}"
                        if " | " in excerpt_value and "{'text':" in excerpt_value:
                            # Split by ' | ' and parse each part as Python literal
                            parts = excerpt_value.split(" | ")
                            for part in parts:
                                part = part.strip()
                                if part:
                                    try:
                                        parsed_item = ast.literal_eval(part)
                                        if isinstance(parsed_item, dict) and 'text' in parsed_item:
                                            excerpts.append(parsed_item)
                                        else:
                                            excerpts.append({
                                                "text": str(parsed_item),
                                                "source": source_filename or "Unknown"
                                            })
                                    except (ValueError, SyntaxError) as parse_err:
                                        logger.debug("ast.literal_eval failed for excerpt part: %s — %s", part[:80], parse_err)
                                        # Try regex extraction for cases where ast.literal_eval fails
                                        # (e.g., text contains apostrophes like "it's")
                                        text_match = re.search(r"'text'\s*:\s*'(.*?)'(?:\s*,\s*'source'|\s*})", part, re.DOTALL)
                                        source_match = re.search(r"'source'\s*:\s*'(.*?)'", part)
                                        if text_match:
                                            excerpts.append({
                                                "text": text_match.group(1),
                                                "source": source_match.group(1) if source_match else (source_filename or "Unknown")
                                            })
                                        else:
                                            excerpts.append({
                                                "text": part,
                                                "source": source_filename or "Unknown"
                                            })
                        # Also check for [source] text format from our CSV export
                        elif excerpt_value.startswith("[") and "] " in excerpt_value:
                            # Format: "[source1] text1 | [source2] text2"
                            parts = excerpt_value.split(" | ")
                            for part in parts:
                                part = part.strip()
                                if part.startswith("[") and "] " in part:
                                    bracket_end = part.index("] ")
                                    source = part[1:bracket_end]
                                    text = part[bracket_end + 2:]
                                    excerpts.append({
                                        "text": text,
                                        "source": source
                                    })
                                elif part:
                                    excerpts.append({
                                        "text": part,
                                        "source": source_filename or "Unknown"
                                    })
                        else:
                            # Try to parse as JSON list first
                            try:
                                parsed = json.loads(excerpt_value)
                                if isinstance(parsed, list):
                                    for item in parsed:
                                        if isinstance(item, dict) and 'text' in item:
                                            # Already has structure
                                            excerpts.append(item)
                                        elif isinstance(item, str):
                                            excerpts.append({
                                                "text": item,
                                                "source": source_filename or "Unknown"
                                            })
                                else:
                                    excerpts.append({
                                        "text": str(parsed),
                                        "source": source_filename or "Unknown"
                                    })
                            except (json.JSONDecodeError, TypeError):
                                # Plain string excerpt
                                excerpts.append({
                                    "text": excerpt_value,
                                    "source": source_filename or "Unknown"
                                })
                    elif isinstance(excerpt_value, list):
                        for item in excerpt_value:
                            if isinstance(item, dict) and 'text' in item:
                                excerpts.append(item)
                            else:
                                excerpts.append({
                                    "text": str(item),
                                    "source": source_filename or "Unknown"
                                })

                # Create QBSD format
                merged[key] = {
                    "answer": self._sanitize_value(value),
                    "excerpts": excerpts
                }
            else:
                # No excerpt column - keep as plain value
                merged[key] = self._sanitize_value(value)

        return merged

    async def get_paginated_data(
        self,
        session_id: str,
        page: int = 0,
        page_size: int = DEFAULT_PAGE_SIZE,
        filters: Optional[List[Dict]] = None,
        sort: Optional[List[Dict]] = None,
        search: Optional[str] = None
    ) -> PaginatedData:
        """Get paginated data for a session with optional filtering and sorting."""
        session_dir = self.data_dir / session_id
        data_file = session_dir / "data.jsonl"

        if not data_file.exists():
            raise FileNotFoundError("No processed data found")

        # Check if we need to filter/sort (requires loading all rows)
        needs_processing = bool(filters or sort or search)

        if needs_processing:
            # Load all rows for filtering/sorting
            all_rows = self._load_all_rows(data_file)
            total_count = len(all_rows)

            # Apply global search
            if search and search.strip():
                all_rows = self._apply_search(all_rows, search.strip())

            # Apply column filters
            if filters:
                all_rows = self._apply_filters(all_rows, filters)

            filtered_count = len(all_rows)

            # Apply sorting
            if sort:
                all_rows = self._apply_sort(all_rows, sort)

            # Paginate
            start = page * page_size
            end = start + page_size
            page_rows = all_rows[start:end]

            # Convert to DataRow objects
            rows = []
            for row_data in page_rows:
                if 'data' in row_data:
                    row_data['data'] = self._sanitize_data_dict(row_data['data'])
                rows.append(DataRow(**row_data))

            return PaginatedData(
                rows=rows,
                total_count=total_count,
                filtered_count=filtered_count,
                page=page,
                page_size=page_size,
                has_more=end < filtered_count
            )
        else:
            # Original efficient pagination (no filtering/sorting)
            with open(data_file) as f:
                total_count = sum(1 for _ in f)

            rows = []
            start_line = page * page_size
            end_line = start_line + page_size

            with open(data_file) as f:
                for i, line in enumerate(f):
                    if i >= start_line and i < end_line:
                        row_data = json.loads(line)
                        if 'data' in row_data:
                            row_data['data'] = self._sanitize_data_dict(row_data['data'])
                        rows.append(DataRow(**row_data))
                    elif i >= end_line:
                        break

            return PaginatedData(
                rows=rows,
                total_count=total_count,
                filtered_count=None,  # No filtering applied
                page=page,
                page_size=page_size,
                has_more=end_line < total_count
            )

    def _load_all_rows(self, data_file: Path) -> List[Dict]:
        """Load all rows from JSONL file."""
        rows = []
        with open(data_file) as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return rows

    def _apply_search(self, rows: List[Dict], search: str) -> List[Dict]:
        """Apply global search term to all columns."""
        search_lower = search.lower()
        return [row for row in rows if self._row_matches_search(row, search_lower)]

    def _row_matches_search(self, row: Dict, search_lower: str) -> bool:
        """Check if any field in row matches search term."""
        # Check row_name
        if row.get('row_name') and search_lower in str(row['row_name']).lower():
            return True
        # Check data fields
        data = row.get('data', row)
        for value in data.values():
            if self._value_contains_search(value, search_lower):
                return True
        return False

    def _value_contains_search(self, value: Any, search_lower: str) -> bool:
        """Check if a value contains the search term."""
        if value is None:
            return False
        # Handle QBSD answer format
        if isinstance(value, dict) and 'answer' in value:
            value = value['answer']
        return search_lower in str(value).lower()

    def _apply_filters(self, rows: List[Dict], filters: List[Dict]) -> List[Dict]:
        """Apply filter rules to rows (AND logic)."""
        for filter_rule in filters:
            rows = [row for row in rows if self._row_matches_filter(row, filter_rule)]
        return rows

    def _row_matches_filter(self, row: Dict, filter_rule: Dict) -> bool:
        """Evaluate a single filter rule against a row."""
        column = filter_rule.get('column', '')
        operator = filter_rule.get('operator', '')
        filter_value = filter_rule.get('value')
        case_sensitive = filter_rule.get('caseSensitive', False)

        # Get cell value
        if column == '_row_name':
            cell_value = row.get('row_name')
        elif column == '_papers':
            cell_value = row.get('papers', [])
        else:
            data = row.get('data', row)
            cell_value = data.get(column)

        return self._evaluate_filter(cell_value, operator, filter_value, case_sensitive)

    def _evaluate_filter(self, cell_value: Any, operator: str, filter_value: Any, case_sensitive: bool) -> bool:
        """Evaluate filter operator against cell value."""
        # Handle QBSD answer format
        if isinstance(cell_value, dict) and 'answer' in cell_value:
            cell_value = cell_value['answer']

        # Null checks
        is_empty = cell_value is None or cell_value == '' or cell_value == [] or cell_value == {}
        if isinstance(cell_value, str) and cell_value.lower() in ['none', 'n/a', 'null']:
            is_empty = True

        if operator == 'isNull':
            return is_empty
        if operator == 'isNotNull':
            return not is_empty

        if is_empty:
            return False

        # Boolean operators
        if operator == 'isTrue':
            return cell_value in [True, 'true', 'True', '1', 1]
        if operator == 'isFalse':
            return cell_value in [False, 'false', 'False', '0', 0]

        # Text operators
        str_value = str(cell_value)
        filter_str = str(filter_value or '')
        if not case_sensitive:
            str_value = str_value.lower()
            filter_str = filter_str.lower()

        if operator == 'contains':
            return filter_str in str_value
        if operator == 'equals':
            return str_value == filter_str
        if operator == 'startsWith':
            return str_value.startswith(filter_str)
        if operator == 'endsWith':
            return str_value.endswith(filter_str)
        if operator == 'regex':
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                return bool(re.search(str(filter_value), str(cell_value), flags))
            except:
                return False

        # Numeric operators
        if operator in ['eq', 'gt', 'lt', 'gte', 'lte', 'between']:
            try:
                num_value = float(str(cell_value))
                if operator == 'eq':
                    return num_value == float(filter_value)
                if operator == 'gt':
                    return num_value > float(filter_value)
                if operator == 'lt':
                    return num_value < float(filter_value)
                if operator == 'gte':
                    return num_value >= float(filter_value)
                if operator == 'lte':
                    return num_value <= float(filter_value)
                if operator == 'between' and isinstance(filter_value, list) and len(filter_value) >= 2:
                    return float(filter_value[0]) <= num_value <= float(filter_value[1])
            except:
                return False

        # Categorical operators
        if operator == 'in':
            allowed = filter_value if isinstance(filter_value, list) else [filter_value]
            str_val_lower = str(cell_value).lower()
            return any(str(v).lower() == str_val_lower for v in allowed)
        if operator == 'notIn':
            allowed = filter_value if isinstance(filter_value, list) else [filter_value]
            str_val_lower = str(cell_value).lower()
            return not any(str(v).lower() == str_val_lower for v in allowed)

        return True

    def _apply_sort(self, rows: List[Dict], sort_columns: List[Dict]) -> List[Dict]:
        """Apply multi-column sorting."""
        if not sort_columns:
            return rows

        # Sort by priority (lower = more important)
        sorted_cols = sorted(sort_columns, key=lambda x: x.get('priority', 1))

        def compare_rows(a: Dict, b: Dict) -> int:
            for sort_col in sorted_cols:
                column = sort_col.get('column', '')
                direction = sort_col.get('direction', 'asc')

                a_val = self._get_sortable_value(a, column)
                b_val = self._get_sortable_value(b, column)

                # Handle nulls - always last
                a_null = a_val is None or a_val == ''
                b_null = b_val is None or b_val == ''

                if a_null and b_null:
                    continue
                if a_null:
                    return 1  # a after b
                if b_null:
                    return -1  # b after a

                # Compare values
                if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                    cmp = (a_val > b_val) - (a_val < b_val)
                else:
                    cmp = (str(a_val).lower() > str(b_val).lower()) - \
                          (str(a_val).lower() < str(b_val).lower())

                if cmp != 0:
                    return cmp if direction == 'asc' else -cmp

            return 0

        from functools import cmp_to_key
        return sorted(rows, key=cmp_to_key(compare_rows))

    def _get_sortable_value(self, row: Dict, column: str) -> Any:
        """Extract a sortable value from a row."""
        if column == '_row_name':
            return row.get('row_name')
        if column == '_papers':
            papers = row.get('papers', [])
            return papers[0] if papers else None

        data = row.get('data', row)
        value = data.get(column)

        # Handle QBSD format
        if isinstance(value, dict) and 'answer' in value:
            value = value['answer']

        # Try to parse as number
        if isinstance(value, str):
            try:
                return float(value)
            except:
                pass

        return value
    
    async def validate_schema_file(self, file: UploadFile) -> SchemaValidationResult:
        """Validate QBSD schema file."""
        errors = []
        warnings = []
        detected_columns = []
        query = None
        schema = None
        
        # Check file format
        filename = file.filename.lower()
        if not filename.endswith('.json'):
            errors.append("Schema file must be a JSON file (.json)")
            return SchemaValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
                detected_columns=detected_columns,
                query=query,
                schema=schema
            )
        
        try:
            # Read and parse JSON content
            content = await file.read()
            await file.seek(0)  # Reset file position
            
            schema_data = json.loads(content.decode('utf-8'))
            
            # Validate QBSD schema format
            if not isinstance(schema_data, dict):
                errors.append("Schema file must contain a JSON object")
                return SchemaValidationResult(
                    is_valid=False,
                    errors=errors,
                    warnings=warnings,
                    detected_columns=detected_columns,
                    query=query,
                    schema=schema
                )
            
            # Check for required 'schema' field
            if 'schema' not in schema_data:
                errors.append("Schema file must contain a 'schema' field")
                return SchemaValidationResult(
                    is_valid=False,
                    errors=errors,
                    warnings=warnings,
                    detected_columns=detected_columns,
                    query=query,
                    schema=schema
                )
            
            # Validate schema structure
            if not isinstance(schema_data['schema'], list):
                errors.append("Schema field must be a list of column definitions")
                return SchemaValidationResult(
                    is_valid=False,
                    errors=errors,
                    warnings=warnings,
                    detected_columns=detected_columns,
                    query=query,
                    schema=schema
                )
            
            # Parse schema columns
            schema_columns = []
            for i, col_def in enumerate(schema_data['schema']):
                if not isinstance(col_def, dict):
                    errors.append(f"Column definition {i} must be an object")
                    continue
                
                if 'name' not in col_def:
                    errors.append(f"Column definition {i} must have a 'name' field")
                    continue
                
                # Create schema column
                try:
                    col = SchemaColumn(
                        name=col_def['name'],
                        definition=col_def.get('definition'),
                        rationale=col_def.get('rationale')
                    )
                    schema_columns.append(col)
                    detected_columns.append(col.name)
                except Exception as e:
                    errors.append(f"Invalid column definition {i}: {str(e)}")
            
            # Extract query if present
            query = schema_data.get('query')
            
            # Check for warnings
            if not query:
                warnings.append("No query field found in schema")
            
            if len(schema_columns) == 0:
                errors.append("No valid column definitions found in schema")
            
            # Parse as QBSD schema format for validation
            try:
                qbsd_schema = QBSDSchemaFormat(**schema_data)
                schema = schema_columns
            except Exception as e:
                warnings.append(f"Schema format validation warning: {str(e)}")
                schema = schema_columns
            
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON format: {str(e)}")
        except Exception as e:
            errors.append(f"Error parsing schema file: {str(e)}")
        
        return SchemaValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            detected_columns=detected_columns,
            query=query,
            schema=schema
        )
    
    def _normalize_column_name(self, name: str) -> str:
        """Normalize column name for comparison."""
        return name.lower().replace('_', '').replace(' ', '').replace('-', '')
    
    def check_schema_data_compatibility(
        self, 
        schema_validation: SchemaValidationResult,
        data_validation: FileValidationResult,
        data_columns: List[str]
    ) -> CompatibilityCheck:
        """Check compatibility between schema and data columns."""
        
        if not schema_validation.is_valid or not data_validation.is_valid:
            return CompatibilityCheck(
                is_compatible=False,
                detailed_errors=["Cannot check compatibility: schema or data validation failed"],
                schema_count=len(schema_validation.detected_columns) if schema_validation.schema else 0,
                data_count=len(data_columns)
            )
        
        schema_columns = schema_validation.detected_columns
        
        # Normalize column names for comparison
        normalized_schema = {self._normalize_column_name(col): col for col in schema_columns}
        normalized_data = {self._normalize_column_name(col): col for col in data_columns}
        
        # Find matches
        matching_columns = []
        for norm_schema_col, orig_schema_col in normalized_schema.items():
            if norm_schema_col in normalized_data:
                matching_columns.append(orig_schema_col)
        
        # Find missing in data (in schema but not in data)
        missing_in_data = []
        for norm_schema_col, orig_schema_col in normalized_schema.items():
            if norm_schema_col not in normalized_data:
                missing_in_data.append(orig_schema_col)
        
        # Find extra in data (in data but not in schema)
        extra_in_data = []
        for norm_data_col, orig_data_col in normalized_data.items():
            if norm_data_col not in normalized_schema:
                extra_in_data.append(orig_data_col)
        
        # Calculate compatibility score
        schema_count = len(schema_columns)
        data_count = len(data_columns)
        matching_count = len(matching_columns)
        
        if schema_count == 0:
            compatibility_score = 0.0
        else:
            compatibility_score = (matching_count / schema_count) * 100.0
        
        # Determine if compatible
        is_compatible = len(missing_in_data) == 0  # All schema columns must be present
        
        # Generate detailed error messages
        detailed_errors = []
        suggestions = []
        
        if missing_in_data:
            detailed_errors.append(
                f"Missing columns in data file: {', '.join(missing_in_data)}"
            )
            suggestions.append(
                "Ensure your data file contains all columns defined in the schema."
            )
        
        if extra_in_data:
            if len(extra_in_data) <= 3:
                detailed_errors.append(
                    f"Extra columns in data file (not in schema): {', '.join(extra_in_data)}"
                )
            else:
                detailed_errors.append(
                    f"Extra columns in data file: {', '.join(extra_in_data[:3])} and {len(extra_in_data) - 3} more"
                )
            suggestions.append(
                "Extra columns will be ignored during visualization, or update your schema to include them."
            )
        
        if matching_count == 0:
            detailed_errors.append(
                "No columns match between schema and data file. Please check column names."
            )
            suggestions.append(
                "Verify that column names in your data file match those in your schema (case-insensitive)."
            )
        
        if is_compatible and extra_in_data:
            suggestions.append(
                f"Schema compatibility: {matching_count}/{schema_count} columns matched perfectly."
            )
        
        return CompatibilityCheck(
            is_compatible=is_compatible,
            matching_columns=matching_columns,
            missing_in_data=missing_in_data,
            extra_in_data=extra_in_data,
            schema_count=schema_count,
            data_count=data_count,
            compatibility_score=compatibility_score,
            detailed_errors=detailed_errors,
            suggestions=suggestions
        )
    
    async def save_schema_file(self, session_id: str, file: UploadFile, validation: SchemaValidationResult):
        """Save schema file to session directory."""
        session_dir = self.data_dir / session_id
        session_dir.mkdir(exist_ok=True)
        
        # Save original schema file
        schema_path = session_dir / "schema.json"
        async with aiofiles.open(schema_path, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        # Save parsed schema for easy access
        if validation.schema:
            parsed_schema = {
                "query": validation.query,
                "schema": [col.model_dump() for col in validation.schema]
            }
            
            # Include LLM configuration if available
            try:
                await file.seek(0)  # Reset file position
                content = await file.read()
                schema_data = json.loads(content.decode('utf-8'))
                if "llm_configuration" in schema_data:
                    parsed_schema["llm_configuration"] = schema_data["llm_configuration"]
                    logger.debug("Preserved LLM configuration in parsed schema")
            except Exception as e:
                logger.debug("Could not extract LLM configuration: %s", e)
            
            parsed_path = session_dir / "parsed_schema.json"
            with open(parsed_path, 'w') as f:
                json.dump(parsed_schema, f, indent=2)
    
    async def extract_schema_from_data(self, session_id: str, query: Optional[str] = None) -> Dict[str, Any]:
        """Extract schema information from uploaded data and convert to QBSD format."""
        session_dir = self.data_dir / session_id
        data_file = session_dir / "data.jsonl"
        
        if not data_file.exists():
            raise FileNotFoundError("No processed data found. Please parse the file first.")
        
        # Read sample rows to analyze schema
        sample_rows = []
        with open(data_file) as f:
            for i, line in enumerate(f):
                if i >= 10:  # Read first 10 rows for analysis
                    break
                row_data = json.loads(line)
                
                # Skip system file rows (like .DS_Store)
                if self._is_system_row(row_data):
                    logger.debug("Skipping system row during schema extraction: %s", self._get_row_identifier(row_data))
                    continue
                    
                sample_rows.append(row_data)
        
        if not sample_rows:
            raise ValueError("No data found to extract schema from")
        
        # Extract column information from data
        extracted_columns = []
        
        # Get all unique column names from the data
        all_columns = set()
        for row in sample_rows:
            if 'data' in row:
                all_columns.update(row['data'].keys())
            else:
                all_columns.update(row.keys())
        
        # Analyze each column
        for col_name in sorted(all_columns):
            if col_name.startswith('_'):  # Skip metadata columns
                continue
            
            # Analyze column data types and patterns
            column_info = self._analyze_column_data(col_name, sample_rows)
            
            # Create QBSD-compatible column definition
            extracted_column = {
                "name": col_name,
                "definition": column_info["definition"],
                "rationale": column_info["rationale"]
            }
            extracted_columns.append(extracted_column)
        
        # Create QBSD schema format
        extracted_schema = {
            "query": query or f"Analysis of uploaded data with {len(extracted_columns)} columns",
            "schema": extracted_columns,
            "extracted_from_upload": True,
            "extraction_metadata": {
                "total_rows_analyzed": len(sample_rows),
                "extraction_timestamp": json.loads(json.dumps({"timestamp": str(pd.Timestamp.now())}, default=str))["timestamp"]
            }
        }
        
        # Save extracted schema
        schema_file = session_dir / "extracted_schema.json"
        with open(schema_file, 'w') as f:
            json.dump(extracted_schema, f, indent=2)
        
        return extracted_schema
    
    def _analyze_column_data(self, col_name: str, sample_rows: List[Dict[str, Any]]) -> Dict[str, str]:
        """Analyze column data to generate definition and rationale."""
        values = []
        
        # Extract values for this column
        for row in sample_rows:
            if 'data' in row and col_name in row['data']:
                values.append(row['data'][col_name])
            elif col_name in row:
                values.append(row[col_name])
        
        # Remove null values for analysis
        non_null_values = [v for v in values if v is not None and v != ""]
        
        if not non_null_values:
            return {
                "definition": f"Column containing data about {self._format_column_name_for_definition(col_name)}",
                "rationale": "Column contains mostly null or empty values in the sample data"
            }
        
        # Analyze data patterns
        data_analysis = self._perform_data_analysis(col_name, non_null_values)
        
        # Generate definition based on analysis
        definition = self._generate_column_definition(col_name, data_analysis)
        
        # Generate rationale
        rationale = self._generate_column_rationale(col_name, data_analysis)
        
        return {
            "definition": definition,
            "rationale": rationale
        }
    
    def _format_column_name_for_definition(self, col_name: str) -> str:
        """Convert column name to human-readable format for definitions."""
        # Replace underscores and hyphens with spaces
        formatted = col_name.replace('_', ' ').replace('-', ' ')
        
        # Title case
        formatted = formatted.title()
        
        # Handle common abbreviations
        replacements = {
            'Id': 'ID',
            'Url': 'URL',
            'Api': 'API',
            'Xml': 'XML',
            'Json': 'JSON',
            'Pdf': 'PDF',
            'Csv': 'CSV'
        }
        
        for old, new in replacements.items():
            formatted = formatted.replace(old, new)
        
        return formatted.lower()
    
    def _perform_data_analysis(self, col_name: str, values: List[Any]) -> Dict[str, Any]:
        """Perform detailed analysis of column values."""
        analysis = {
            "total_values": len(values),
            "unique_values": len(set(str(v) for v in values)),
            "data_types": {},
            "patterns": {},
            "sample_values": values[:5],  # First 5 values as examples
        }
        
        # Analyze data types
        type_counts = {}
        for value in values:
            value_type = type(value).__name__
            if isinstance(value, str):
                # Analyze string patterns
                if value.lower() in ['true', 'false']:
                    value_type = 'boolean_string'
                elif value.replace('.', '').replace('-', '').isdigit():
                    value_type = 'numeric_string'
                elif '@' in value and '.' in value:
                    value_type = 'email_string'
                elif value.startswith('http'):
                    value_type = 'url_string'
            elif isinstance(value, dict):
                # Check if it looks like QBSD format
                if 'answer' in value or 'excerpts' in value:
                    value_type = 'qbsd_format'
            
            type_counts[value_type] = type_counts.get(value_type, 0) + 1
        
        analysis["data_types"] = type_counts
        
        # Determine primary data type
        primary_type = max(type_counts.keys(), key=lambda k: type_counts[k]) if type_counts else "unknown"
        analysis["primary_type"] = primary_type
        
        # Analyze value patterns
        if isinstance(values[0], str):
            # String pattern analysis
            avg_length = sum(len(str(v)) for v in values) / len(values)
            max_length = max(len(str(v)) for v in values)
            min_length = min(len(str(v)) for v in values)
            
            analysis["patterns"].update({
                "avg_length": avg_length,
                "max_length": max_length,
                "min_length": min_length,
                "has_long_text": max_length > 100
            })
        
        return analysis
    
    def _generate_column_definition(self, col_name: str, analysis: Dict[str, Any]) -> str:
        """Generate a human-readable definition for the column."""
        formatted_name = self._format_column_name_for_definition(col_name)
        primary_type = analysis.get("primary_type", "unknown")
        unique_ratio = analysis["unique_values"] / analysis["total_values"]
        
        # Base definition
        if unique_ratio > 0.9:
            # Mostly unique values - likely identifiers or specific data
            if "id" in col_name.lower() or "name" in col_name.lower():
                definition = f"Unique identifier or name for {formatted_name}"
            else:
                definition = f"Specific {formatted_name} values, mostly unique across records"
        elif unique_ratio < 0.1:
            # Low uniqueness - likely categorical
            definition = f"Categorical {formatted_name} data with limited distinct values"
        else:
            # Mixed uniqueness
            definition = f"Data about {formatted_name}"
        
        # Add type-specific information
        if primary_type == "qbsd_format":
            definition += " (extracted from documents with supporting evidence)"
        elif primary_type == "url_string":
            definition += " (URL/link format)"
        elif primary_type == "email_string":
            definition += " (email address format)"
        elif primary_type in ["int", "float", "numeric_string"]:
            definition += " (numeric values)"
        elif analysis.get("patterns", {}).get("has_long_text", False):
            definition += " (detailed text content)"
        
        return definition
    
    def _generate_column_rationale(self, col_name: str, analysis: Dict[str, Any]) -> str:
        """Generate rationale explaining why this column is useful."""
        sample_values = analysis.get("sample_values", [])
        primary_type = analysis.get("primary_type", "unknown")
        unique_ratio = analysis["unique_values"] / analysis["total_values"]
        
        rationale_parts = []
        
        # Value distribution rationale
        if unique_ratio > 0.9:
            rationale_parts.append("High uniqueness suggests this field contains specific identifying information")
        elif unique_ratio < 0.1:
            rationale_parts.append("Low uniqueness indicates categorical classification useful for grouping and analysis")
        else:
            rationale_parts.append("Moderate uniqueness suggests semi-structured data suitable for detailed analysis")
        
        # Type-specific rationale
        if primary_type == "qbsd_format":
            rationale_parts.append("Contains extracted answers with supporting document excerpts for verification")
        elif "id" in col_name.lower():
            rationale_parts.append("Serves as a key identifier for linking and referencing records")
        elif "name" in col_name.lower():
            rationale_parts.append("Provides human-readable labels for identification and display")
        elif primary_type in ["int", "float", "numeric_string"]:
            rationale_parts.append("Numeric values enable quantitative analysis and comparisons")
        elif analysis.get("patterns", {}).get("has_long_text", False):
            rationale_parts.append("Long text content provides detailed information for comprehensive analysis")
        
        # Sample-based insights
        if sample_values and len(str(sample_values[0])) > 50:
            rationale_parts.append("Contains detailed content that may require summarization or excerpt viewing")
        
        return ". ".join(rationale_parts) + "."
    
    def convert_to_qbsd_format(self, extracted_schema: Dict[str, Any], docs_path: str = "documents/") -> Dict[str, Any]:
        """Convert extracted schema to full QBSD configuration format."""
        qbsd_config = {
            "query": extracted_schema["query"],
            "docs_path": docs_path,
            "max_keys_schema": len(extracted_schema["schema"]),
            "documents_batch_size": 1,
            "document_randomization_seed": 42,
            "backend": {
                "provider": "gemini",
                "model": "gemini-2.5-flash-lite",
                "max_output_tokens": 1024,
                "temperature": 0
            },
            "retriever": {
                "type": "embedding",
                "model_name": "all-MiniLM-L6-v2",
                "k": 8,
                "max_words": 512,
                "enable_dynamic_k": True,
                "dynamic_k_threshold": 0.65,
                "dynamic_k_minimum": 3
            },
            "schema": extracted_schema["schema"]
        }
        
        return qbsd_config
    
    def _extract_csv_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from CSV comment lines."""
        metadata_info = {
            'has_comments': False,
            'session_id': None,
            'query': None,
            'llm_config': {},
            'column_definitions': {},
            'generated_timestamp': None,
            'schema_evolution': None,  # Will hold parsed SchemaEvolution if present
            'observation_unit': None,  # Will hold parsed observation unit if present
            'total_documents': 0,  # Will hold parsed total documents if present
            'skipped_documents': []  # Will hold parsed skipped documents if present
        }

        # State tracking for multi-line sections
        in_column_sources_section = False

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()

                    # Handle quoted comment lines (CSV may quote lines with special chars)
                    # e.g., "# Query: ..." or "#   Rationale: ..."
                    if line.startswith('"#') and line.endswith('"'):
                        line = line[1:-1]  # Remove surrounding quotes

                    if line.startswith('#'):
                        metadata_info['has_comments'] = True

                        # Remove the # and preserve leading whitespace to detect indentation
                        raw_content = line[1:]
                        content = raw_content.strip()
                        is_indented = raw_content.startswith('  ') or raw_content.startswith('\t')

                        # Check for Rationale FIRST (before general column parsing)
                        # Rationale lines start with "Rationale:" after stripping
                        if content.startswith('Rationale:'):
                            # This is a rationale for the previous column
                            rationale = content.split(':', 1)[1].strip()
                            # Find the last column definition to add rationale to
                            if metadata_info['column_definitions']:
                                last_col = list(metadata_info['column_definitions'].keys())[-1]
                                metadata_info['column_definitions'][last_col]['rationale'] = rationale
                        elif content.startswith('Allowed Values:'):
                            # This is allowed_values for the previous column
                            values_str = content.split(':', 1)[1].strip()
                            # Parse comma-separated values
                            allowed_values = [v.strip() for v in values_str.split(',') if v.strip()]
                            # Find the last column definition to add allowed_values to
                            if metadata_info['column_definitions'] and allowed_values:
                                last_col = list(metadata_info['column_definitions'].keys())[-1]
                                metadata_info['column_definitions'][last_col]['allowed_values'] = allowed_values
                        elif content.startswith('Session ID:'):
                            metadata_info['session_id'] = content.split(':', 1)[1].strip()
                        elif content.startswith('Query:'):
                            metadata_info['query'] = content.split(':', 1)[1].strip()
                        elif content.startswith('Observation Unit:'):
                            try:
                                obs_unit_json = content.split(':', 1)[1].strip()
                                metadata_info['observation_unit'] = json.loads(obs_unit_json)
                                logger.debug("Imported observation unit from CSV: %s", metadata_info['observation_unit'].get('name'))
                            except Exception as e:
                                logger.debug("Could not parse observation unit from CSV: %s", e)
                        elif content.startswith('Generated:'):
                            metadata_info['generated_timestamp'] = content.split(':', 1)[1].strip()
                        elif content.startswith('Schema Creation:'):
                            config_str = content.split(':', 1)[1].strip()
                            parts = config_str.split()
                            if len(parts) >= 2:
                                metadata_info['llm_config']['schema_creation_backend'] = {
                                    'provider': parts[0],
                                    'model': ' '.join(parts[1:])
                                }
                        elif content.startswith('Value Extraction:'):
                            config_str = content.split(':', 1)[1].strip()
                            parts = config_str.split()
                            if len(parts) >= 2:
                                metadata_info['llm_config']['value_extraction_backend'] = {
                                    'provider': parts[0],
                                    'model': ' '.join(parts[1:])
                                }
                        elif content.startswith('AI Model:'):
                            config_str = content.split(':', 1)[1].strip()
                            parts = config_str.split()
                            if len(parts) >= 2:
                                metadata_info['llm_config']['backend'] = {
                                    'provider': parts[0],
                                    'model': ' '.join(parts[1:])
                                }
                        elif content.startswith('Schema Evolution:'):
                            # Initialize evolution tracking
                            if metadata_info['schema_evolution'] is None:
                                metadata_info['schema_evolution'] = {
                                    'snapshots': [],
                                    'column_sources': {}
                                }
                        elif content.startswith('Iteration ') and ':' in content:
                            # Parse iteration lines:
                            # New format: "Iteration 1: +3 columns [col1, col2, col3] from [doc1.txt] (total: 10)"
                            # Old format: "Iteration 1: +3 columns [col1, col2, col3]"
                            try:
                                if metadata_info['schema_evolution'] is None:
                                    metadata_info['schema_evolution'] = {'snapshots': [], 'column_sources': {}}

                                # Extract iteration number and column info
                                iter_part, cols_part = content.split(':', 1)
                                iteration = int(iter_part.replace('Iteration', '').strip())

                                # Parse new columns from brackets BEFORE "from" if present
                                new_columns = []
                                from_pos = cols_part.find(' from ')
                                cols_section = cols_part[:from_pos] if from_pos != -1 else cols_part
                                first_bracket_start = cols_section.find('[')
                                first_bracket_end = cols_section.find(']')
                                if first_bracket_start != -1 and first_bracket_end != -1:
                                    bracket_content = cols_section[first_bracket_start + 1:first_bracket_end]
                                    # Handle truncated columns (e.g., "col1, col2... (+3 more)")
                                    bracket_content = bracket_content.split('...')[0]
                                    new_columns = [c.strip() for c in bracket_content.split(',') if c.strip()]

                                # Parse document names from "from [doc1, doc2...]" if present
                                documents_processed = [f'iteration_{iteration}']  # Default fallback
                                from_match = re.search(r'from \[([^\]]+)\]', cols_part)
                                if from_match:
                                    docs_content = from_match.group(1).split('...')[0]  # Handle truncation
                                    documents_processed = [d.strip() for d in docs_content.split(',') if d.strip()]

                                # Extract total column count from "(total: X)" if present
                                total_match = re.search(r'\(total:\s*(\d+)\)', cols_part)
                                if total_match:
                                    total_columns = int(total_match.group(1))
                                else:
                                    # Fallback: calculate from previous snapshots
                                    prev_total = sum(len(s.get('new_columns', [])) for s in metadata_info['schema_evolution']['snapshots'])
                                    count_match = re.search(r'\+(\d+)\s+columns?', cols_part)
                                    new_count = int(count_match.group(1)) if count_match else len(new_columns)
                                    total_columns = prev_total + new_count

                                snapshot = {
                                    'iteration': iteration,
                                    'documents_processed': documents_processed,
                                    'total_columns': total_columns,
                                    'new_columns': new_columns,
                                    'cumulative_documents': iteration
                                }
                                metadata_info['schema_evolution']['snapshots'].append(snapshot)

                                # Add column sources
                                source_name = documents_processed[0] if documents_processed else f'iteration_{iteration}'
                                for col in new_columns:
                                    metadata_info['schema_evolution']['column_sources'][col] = source_name
                            except Exception as e:
                                logger.debug("Could not parse iteration line '%s': %s", content, e)
                        elif content.startswith('Total:') and 'columns from' in content and 'iterations' in content:
                            # "Total: 5 columns from 2 iterations" - just informational, skip
                            pass
                        elif content.startswith('Total Documents:'):
                            # Parse total documents count
                            try:
                                metadata_info['total_documents'] = int(content[len('Total Documents:'):].strip())
                                logger.debug("Imported total documents from CSV: %d", metadata_info['total_documents'])
                            except ValueError as e:
                                logger.debug("Could not parse total_documents: %s", e)
                        elif content.startswith('Skipped Documents:'):
                            # Parse skipped documents JSON
                            try:
                                skipped_json = content[len('Skipped Documents:'):].strip()
                                metadata_info['skipped_documents'] = json.loads(skipped_json)
                                logger.debug("Imported skipped documents from CSV: %d documents", len(metadata_info['skipped_documents']))
                            except (json.JSONDecodeError, Exception) as e:
                                logger.debug("Could not parse skipped_documents: %s", e)
                        elif content.startswith('Column Sources:'):
                            # Start of column sources section
                            in_column_sources_section = True
                        elif in_column_sources_section and is_indented and ':' in content:
                            # Parse column source line: "  column_name: source_document"
                            if metadata_info['schema_evolution'] is None:
                                metadata_info['schema_evolution'] = {'snapshots': [], 'column_sources': {}}
                            col_name, source = content.split(':', 1)
                            col_name = col_name.strip()
                            source = source.strip()
                            if col_name and source:
                                metadata_info['schema_evolution']['column_sources'][col_name] = source
                        elif ':' in content and not content.startswith(('Column Definitions', 'Metadata-Rich', 'Upload Data Export', 'QBSD Export', 'Schema Evolution')):
                            # Reset column sources section flag when we hit a non-indented line
                            in_column_sources_section = False
                            # Parse column definitions
                            if content.count(':') >= 1:
                                col_name, definition = content.split(':', 1)
                                col_name = col_name.strip()
                                definition = definition.strip()

                                # Skip standard columns and special format indicators
                                if col_name not in ['row_name', 'papers', '{column}_excerpt', 'Format']:
                                    if col_name not in metadata_info['column_definitions']:
                                        metadata_info['column_definitions'][col_name] = {
                                            'definition': definition,
                                            'rationale': ''
                                        }
                    else:
                        # Stop processing when we reach actual CSV data
                        break
                        
        except Exception as e:
            logger.debug("Error extracting CSV metadata: %s", e)
        
        return metadata_info
    
    def _is_system_row(self, row_data: Dict[str, Any]) -> bool:
        """Check if a data row represents a system file that should be ignored."""
        # Check row_name field
        row_name = row_data.get('row_name', '')
        if isinstance(row_name, str) and self._is_system_filename(row_name):
            return True
            
        # Check nested data.row_name
        if 'data' in row_data:
            nested_row_name = row_data['data'].get('row_name', '')
            if isinstance(nested_row_name, str) and self._is_system_filename(nested_row_name):
                return True
        
        # Check papers field for system files
        papers = row_data.get('papers', '')
        if isinstance(papers, str) and self._is_system_filename(papers):
            return True
            
        # Check nested data.papers
        if 'data' in row_data:
            nested_papers = row_data['data'].get('papers', '')
            if isinstance(nested_papers, str) and self._is_system_filename(nested_papers):
                return True
                
        return False
    
    def _is_system_filename(self, filename: str) -> bool:
        """Check if a filename is a system file."""
        if not filename:
            return False
            
        system_files = {'.DS_Store', '._.DS_Store', 'Thumbs.db', '.gitkeep', '.gitignore', '.DS'}
        system_prefixes = ('._', '.tmp', '~$')
        
        # Check exact matches
        if filename in system_files:
            return True
            
        # Check prefixes
        for prefix in system_prefixes:
            if filename.startswith(prefix):
                return True
                
        return False
    
    def _get_row_identifier(self, row_data: Dict[str, Any]) -> str:
        """Get a string identifier for a row for debugging purposes."""
        # Try different fields to identify the row
        for field in ['row_name', 'papers']:
            value = row_data.get(field, '')
            if value:
                return str(value)
                
        # Try nested data
        if 'data' in row_data:
            for field in ['row_name', 'papers']:
                value = row_data['data'].get(field, '')
                if value:
                    return str(value)
                    
        return "Unknown row"