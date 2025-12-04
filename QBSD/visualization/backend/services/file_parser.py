"""File parsing and data processing service."""

import csv
import json
import math
import aiofiles
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
from fastapi import UploadFile

from models.upload import (
    FileValidationResult, ColumnMappingRequest, SchemaValidationResult,
    QBSDSchemaFormat, SchemaColumn, CompatibilityCheck, DualFileUploadResult
)
from models.session import ColumnInfo, DataStatistics, DataRow, PaginatedData

class FileParser:
    """Handles file parsing and data processing."""
    
    def __init__(self, data_dir: str = "./data"):
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
        
        # Try to read first few lines for validation
        try:
            content = await file.read(8192)  # Read first 8KB
            await file.seek(0)  # Reset file position
            
            if detected_format == "csv":
                # Validate CSV structure
                lines = content.decode('utf-8').split('\n')
                if len(lines) < 2:
                    errors.append("CSV file must have at least a header row and one data row")
                else:
                    # Try to parse header
                    dialect = csv.Sniffer().sniff(lines[0])
                    reader = csv.DictReader([lines[0], lines[1]], dialect=dialect)
                    sample_row = next(reader)
                    estimated_columns = len(sample_row)
                    estimated_rows = len(lines) - 1  # Rough estimate
                    sample_data = [sample_row]
                    
            elif detected_format == "json":
                # Validate JSON structure
                try:
                    if filename.endswith('.jsonl'):
                        # JSONL format - each line is a JSON object
                        lines = content.decode('utf-8').strip().split('\n')
                        sample_obj = json.loads(lines[0])
                        estimated_rows = len(lines)
                        estimated_columns = len(sample_obj.keys())
                        sample_data = [sample_obj]
                    else:
                        # Regular JSON - could be array or single object
                        data = json.loads(content.decode('utf-8'))
                        if isinstance(data, list):
                            estimated_rows = len(data)
                            if data:
                                estimated_columns = len(data[0].keys()) if isinstance(data[0], dict) else 1
                                sample_data = data[:3]  # First 3 items
                        elif isinstance(data, dict):
                            if 'schema' in data:
                                # QBSD schema format
                                warnings.append("Detected QBSD schema format")
                                estimated_columns = len(data.get('schema', []))
                            else:
                                estimated_rows = 1
                                estimated_columns = len(data.keys())
                                sample_data = [data]
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
            return await self._parse_csv(file_path, mapping)
        elif file_path.suffix.lower() in ['.json', '.jsonl']:
            return await self._parse_json(file_path)
        else:
            raise ValueError("Unsupported file format")
    
    async def _parse_csv(self, file_path: Path, mapping: Optional[ColumnMappingRequest] = None) -> Dict[str, Any]:
        """Parse CSV file."""
        df = pd.read_csv(file_path)
        
        # Apply column mapping if provided
        if mapping:
            df = df.rename(columns=mapping.column_mappings)
        
        # Extract columns info
        columns = []
        for col in df.columns:
            non_null_count = int(df[col].notna().sum())
            unique_count = int(df[col].nunique())
            
            # Ensure values are valid
            if non_null_count < 0:
                non_null_count = 0
            if unique_count < 0:
                unique_count = 0
                
            col_info = ColumnInfo(
                name=col,
                data_type=str(df[col].dtype),
                non_null_count=non_null_count,
                unique_count=unique_count
            )
            columns.append(col_info)
        
        # Calculate statistics (handle NaN values)
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.notna().sum().sum()
        completeness = float(non_null_cells / total_cells * 100) if total_cells > 0 else 0.0
        
        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness) or not (0 <= completeness <= 100):
            completeness = 0.0
            
        statistics = DataStatistics(
            total_rows=len(df),
            total_columns=len(df.columns),
            completeness=completeness,
            column_stats=columns
        )
        
        # Save processed data as JSONL
        data_file = file_path.parent / "data.jsonl"
        with open(data_file, 'w') as f:
            for _, row in df.iterrows():
                # Sanitize row data before saving
                sanitized_data = self._sanitize_data_dict(row.to_dict())
                row_data = DataRow(data=sanitized_data)
                f.write(json.dumps(row_data.model_dump()) + '\n')
        
        return {"columns": columns, "statistics": statistics}
    
    async def _parse_json(self, file_path: Path) -> Dict[str, Any]:
        """Parse JSON/JSONL file."""
        if file_path.suffix.lower() == '.jsonl':
            # JSONL format
            data_rows = []
            with open(file_path) as f:
                for line in f:
                    if line.strip():
                        obj = json.loads(line)
                        data_rows.append(obj)
        else:
            # Regular JSON
            with open(file_path) as f:
                data = json.load(f)
                if isinstance(data, list):
                    data_rows = data
                else:
                    data_rows = [data]
        
        if not data_rows:
            raise ValueError("No data found in file")
        
        # Extract schema from first row
        sample_row = data_rows[0]
        columns = []
        
        # Handle QBSD format
        if '_row_name' in sample_row and '_papers' in sample_row:
            # QBSD extracted data format
            for key, value in sample_row.items():
                if key.startswith('_'):
                    continue  # Skip metadata fields
                
                col_info = ColumnInfo(
                    name=key,
                    data_type="object",
                    non_null_count=sum(1 for row in data_rows if key in row and row[key] is not None),
                    unique_count=len(set(json.dumps(row.get(key, None), sort_keys=True) for row in data_rows))
                )
                columns.append(col_info)
        else:
            # Regular JSON format
            for key in sample_row.keys():
                col_info = ColumnInfo(
                    name=key,
                    data_type=type(sample_row[key]).__name__,
                    non_null_count=sum(1 for row in data_rows if key in row and row[key] is not None),
                    unique_count=len(set(json.dumps(row.get(key, None), sort_keys=True) for row in data_rows))
                )
                columns.append(col_info)
        
        # Calculate statistics
        total_cells = len(data_rows) * len(columns)
        non_null_cells = sum(col.non_null_count for col in columns)
        
        # Calculate completeness safely
        completeness = float(non_null_cells / total_cells * 100) if total_cells > 0 else 0.0
        
        # Ensure completeness is a valid number
        if math.isnan(completeness) or math.isinf(completeness) or not (0 <= completeness <= 100):
            completeness = 0.0
            
        statistics = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            completeness=completeness,
            column_stats=columns
        )
        
        # Save processed data as JSONL
        data_file = file_path.parent / "data.jsonl"
        with open(data_file, 'w') as f:
            for row_data in data_rows:
                if '_row_name' in row_data:
                    # QBSD format
                    data_row = DataRow(
                        row_name=row_data.get('_row_name'),
                        papers=row_data.get('_papers', []),
                        data={k: v for k, v in row_data.items() if not k.startswith('_')}
                    )
                else:
                    # Regular format
                    data_row = DataRow(data=row_data)
                
                f.write(json.dumps(data_row.model_dump()) + '\n')
        
        return {"columns": columns, "statistics": statistics}
    
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

    async def get_paginated_data(self, session_id: str, page: int = 0, page_size: int = 50) -> PaginatedData:
        """Get paginated data for a session."""
        session_dir = self.data_dir / session_id
        data_file = session_dir / "data.jsonl"
        
        if not data_file.exists():
            raise FileNotFoundError("No processed data found")
        
        # Count total lines
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
                    
                    # Sanitize the data before creating DataRow
                    if 'data' in row_data:
                        row_data['data'] = self._sanitize_data_dict(row_data['data'])
                    
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
            parsed_path = session_dir / "parsed_schema.json"
            with open(parsed_path, 'w') as f:
                json.dump(parsed_schema, f, indent=2)