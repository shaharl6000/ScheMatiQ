"""Upload-specific models."""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class FileUploadRequest(BaseModel):
    """Request model for file upload."""
    filename: str
    content_type: str
    size: int

class FileValidationResult(BaseModel):
    """Result of file validation."""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    detected_format: Optional[str] = None
    estimated_rows: Optional[int] = None
    estimated_columns: Optional[int] = None
    sample_data: Optional[List[Dict[str, Any]]] = None

class ColumnMappingRequest(BaseModel):
    """Request for mapping CSV columns to schema."""
    session_id: str
    column_mappings: Dict[str, str]  # original_name -> new_name
    column_types: Dict[str, str]     # column_name -> data_type
    
class DataPreviewRequest(BaseModel):
    """Request for data preview."""
    session_id: str
    page: int = Field(default=0, ge=0)
    page_size: int = Field(default=50, ge=1, le=1000)
    filters: Optional[Dict[str, Any]] = None