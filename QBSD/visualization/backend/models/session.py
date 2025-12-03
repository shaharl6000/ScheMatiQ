"""Session and data models for visualization."""

from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, Field
from enum import Enum

class SessionType(str, Enum):
    """Types of visualization sessions."""
    UPLOAD = "upload"
    QBSD = "qbsd"

class SessionStatus(str, Enum):
    """Status of a visualization session."""
    CREATED = "created"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    ERROR = "error"

class ColumnInfo(BaseModel):
    """Information about a data column."""
    name: str
    definition: str = ""
    rationale: str = ""
    data_type: Optional[str] = None
    non_null_count: Optional[int] = None
    unique_count: Optional[int] = None

class SessionMetadata(BaseModel):
    """Metadata for a visualization session."""
    source: str
    created: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    file_size: Optional[int] = None
    row_count: Optional[int] = None

class DataStatistics(BaseModel):
    """Statistics about the dataset."""
    total_rows: int
    total_columns: int
    completeness: float  # Percentage of non-null values
    column_stats: List[ColumnInfo]

class VisualizationSession(BaseModel):
    """Main session model for visualization."""
    id: str
    type: SessionType
    status: SessionStatus = SessionStatus.CREATED
    metadata: SessionMetadata
    schema_query: Optional[str] = None
    columns: List[ColumnInfo] = []
    statistics: Optional[DataStatistics] = None
    error_message: Optional[str] = None

class DataRow(BaseModel):
    """A single row of data."""
    row_name: Optional[str] = None
    papers: Optional[List[str]] = None
    data: Dict[str, Any] = {}

class PaginatedData(BaseModel):
    """Paginated data response."""
    rows: List[DataRow]
    total_count: int
    page: int
    page_size: int
    has_more: bool