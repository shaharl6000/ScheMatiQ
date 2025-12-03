"""File parsing and data processing service."""

import csv
import json
import aiofiles
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
from fastapi import UploadFile

from models.upload import FileValidationResult, ColumnMappingRequest
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
            col_info = ColumnInfo(
                name=col,
                data_type=str(df[col].dtype),
                non_null_count=int(df[col].notna().sum()),
                unique_count=int(df[col].nunique())
            )
            columns.append(col_info)
        
        # Calculate statistics
        statistics = DataStatistics(
            total_rows=len(df),
            total_columns=len(df.columns),
            completeness=float(df.notna().sum().sum() / (len(df) * len(df.columns)) * 100),
            column_stats=columns
        )
        
        # Save processed data as JSONL
        data_file = file_path.parent / "data.jsonl"
        with open(data_file, 'w') as f:
            for _, row in df.iterrows():
                row_data = DataRow(data=row.to_dict())
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
        
        statistics = DataStatistics(
            total_rows=len(data_rows),
            total_columns=len(columns),
            completeness=float(non_null_cells / total_cells * 100) if total_cells > 0 else 0,
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