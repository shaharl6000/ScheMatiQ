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

class SchemaColumn(BaseModel):
    """Schema column definition from ScheMatiQ schema."""
    name: str
    definition: Optional[str] = None
    rationale: Optional[str] = None

class LLMBackendConfig(BaseModel):
    """LLM backend configuration."""
    provider: str
    model: str
    max_output_tokens: int
    temperature: float
    context_window_size: Optional[int] = None

class ScheMatiQSchemaFormat(BaseModel):
    """ScheMatiQ schema file format."""
    query: Optional[str] = None
    docs_path: Optional[str] = None
    backend: Optional[Dict[str, Any]] = None  # Legacy single backend support
    retriever: Optional[Dict[str, Any]] = None
    schema: List[SchemaColumn]
    llm_configuration: Optional[Dict[str, Any]] = None  # New dual LLM config

class SchemaValidationResult(BaseModel):
    """Result of schema file validation."""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    detected_columns: List[str] = []
    query: Optional[str] = None
    schema: Optional[List[SchemaColumn]] = None

class CompatibilityCheck(BaseModel):
    """Result of schema-data compatibility check."""
    is_compatible: bool
    matching_columns: List[str] = []
    missing_in_data: List[str] = []  # Columns in schema but not in data
    extra_in_data: List[str] = []    # Columns in data but not in schema
    schema_count: int = 0
    data_count: int = 0
    compatibility_score: float = 0.0  # Percentage of matching columns
    detailed_errors: List[str] = []
    suggestions: List[str] = []

class DualFileUploadResult(BaseModel):
    """Result of dual file upload."""
    session_id: str
    schema_validation: SchemaValidationResult
    data_validation: FileValidationResult
    compatibility: CompatibilityCheck
    requires_column_mapping: bool = False