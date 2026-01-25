"""Session and data models for visualization."""

from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, Field
from enum import Enum

from app.models.modification import CreationMetadata, ModificationAction

class SessionType(str, Enum):
    """Types of visualization sessions."""
    UPLOAD = "upload"
    QBSD = "qbsd"

class SessionStatus(str, Enum):
    """Status of a visualization session."""
    CREATED = "created"
    PROCESSING = "processing"
    SCHEMA_READY = "schema_ready"  # Schema discovery complete, value extraction in progress
    COMPLETED = "completed"
    ERROR = "error"
    STOPPED = "stopped"  # Gracefully stopped by user, may have partial results
    # Enhanced upload workflow states
    SCHEMA_EXTRACTED = "schema_extracted"  # Schema extracted from uploaded data
    DOCUMENTS_UPLOADED = "documents_uploaded"  # Documents uploaded for processing
    PROCESSING_DOCUMENTS = "processing_documents"  # Processing documents with QBSD pipeline

class ObservationUnitInfo(BaseModel):
    """
    Defines what constitutes a single row (observation unit) in the extracted table.

    For example, if analyzing benchmark papers:
    - name: "Model-Benchmark Evaluation"
    - definition: "A single evaluation of one model on one benchmark dataset"
    - example_names: ["GPT-4 on MMLU", "Claude on HumanEval", "LLaMA on GSM8K"]
    """
    name: str                                    # e.g., "Model-Benchmark Evaluation"
    definition: str                              # What constitutes one row
    example_names: Optional[List[str]] = None    # ["GPT-4 on MMLU", "Claude on HumanEval"]
    source_document: Optional[str] = None        # Document that helped define this unit
    discovery_iteration: Optional[int] = None    # Iteration when this unit was discovered

    @classmethod
    def default(cls) -> "ObservationUnitInfo":
        """Return the default observation unit (one row per document)."""
        return cls(
            name="Document",
            definition="Each document is treated as one observation unit",
            example_names=None,
        )

    def is_default(self) -> bool:
        """Check if this is the default document-level observation unit."""
        return self.name == "Document" and "one observation unit" in self.definition.lower()


class PendingValue(BaseModel):
    """A suggested value pending approval for addition to allowed_values."""
    value: str
    document_count: int
    first_seen: datetime = Field(default_factory=datetime.now)
    documents: List[str] = Field(default_factory=list)  # Document names where value appeared


class SchemaSuggestion(BaseModel):
    """Suggested schema updates from value extraction."""
    column_name: str
    suggested_values: List[str]
    value_details: Dict[str, PendingValue] = Field(default_factory=dict)  # value -> details
    auto_approved: bool = False
    created_at: datetime = Field(default_factory=datetime.now)


class ColumnInfo(BaseModel):
    """Information about a data column."""
    name: str
    definition: str = ""
    rationale: str = ""
    data_type: Optional[str] = None
    non_null_count: Optional[int] = None
    unique_count: Optional[int] = None
    source_document: Optional[str] = None  # Document that first added this column
    discovery_iteration: Optional[int] = None  # Iteration when this column was discovered
    allowed_values: Optional[List[str]] = None  # Closed set of valid values for categorical columns
    auto_expand_threshold: Optional[int] = 2  # Auto-add new value if seen in N+ docs (None/0 = disabled)
    pending_values: Optional[List[PendingValue]] = None  # Values pending approval


class ColumnBaseline(BaseModel):
    """Baseline state for a column used in change detection."""
    name: str
    definition: str = ""
    rationale: str = ""
    allowed_values: Optional[List[str]] = None
    checksum: str = ""  # MD5 hash of definition + rationale + allowed_values


class SchemaBaseline(BaseModel):
    """Snapshot of schema state after last extraction for change detection."""
    columns: Dict[str, ColumnBaseline] = Field(default_factory=dict)
    captured_at: datetime = Field(default_factory=datetime.now)


class SchemaSnapshot(BaseModel):
    """Snapshot of schema state at a point during discovery."""
    iteration: int
    documents_processed: List[str]
    total_columns: int
    new_columns: List[str]  # Names of columns added in this iteration
    cumulative_documents: int = 0  # Total documents processed so far


class SchemaEvolution(BaseModel):
    """Tracks how the schema evolved during discovery."""
    snapshots: List[SchemaSnapshot] = Field(default_factory=list)
    column_sources: Dict[str, str] = Field(default_factory=dict)  # column_name -> source_document


class SessionMetadata(BaseModel):
    """Metadata for a visualization session."""
    source: str
    created: datetime = Field(default_factory=datetime.now)
    last_modified: datetime = Field(default_factory=datetime.now)
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    schema_discovery_completed: bool = False
    total_documents: Optional[int] = None
    processed_documents: int = 0
    # Enhanced upload workflow metadata
    extracted_schema: Optional[Dict[str, Any]] = None  # Extracted schema information
    uploaded_documents: List[str] = Field(default_factory=list)  # List of uploaded document filenames
    processing_stats: Dict[str, Any] = Field(default_factory=dict)  # Document processing statistics
    original_row_count: Optional[int] = None  # Original uploaded data row count
    additional_rows_added: int = 0  # Rows added through document processing
    cloud_dataset: Optional[str] = None  # Original cloud dataset name (e.g., "nes_full_text")

class DataStatistics(BaseModel):
    """Statistics about the dataset."""
    total_rows: int
    total_columns: int
    completeness: float  # Percentage of non-null values
    column_stats: List[ColumnInfo]
    schema_evolution: Optional[SchemaEvolution] = None  # How schema evolved during discovery

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
    schema_suggestions: Optional[List[SchemaSuggestion]] = None  # Pending schema evolution suggestions
    schema_baseline: Optional[SchemaBaseline] = None  # Baseline for schema change detection
    # Creation and modification tracking
    creation_metadata: Optional[CreationMetadata] = None  # Immutable creation info
    modification_history: List[ModificationAction] = Field(default_factory=list)  # Schema modification log
    # Observation unit tracking
    observation_unit: Optional[ObservationUnitInfo] = None  # What constitutes a single row

class DataRow(BaseModel):
    """A single row of data."""
    # populate_by_name: accept both field name and alias for input
    # serialize_by_alias: output with alias (_unit_name) for frontend compatibility
    model_config = {"populate_by_name": True, "serialize_by_alias": True}

    row_name: Optional[str] = None
    papers: List[str] = Field(default_factory=list)  # Always a list, never None
    data: Dict[str, Any] = {}
    # Observation unit metadata (for multi-row extraction from same document)
    # Uses alias for both input (from JSONL) and output (to frontend)
    unit_name: Optional[str] = Field(default=None, alias="_unit_name")
    # Source document information (preserved from extraction)
    source_document: Optional[str] = Field(default=None, alias="_source_document")
    parent_document: Optional[str] = Field(default=None, alias="_parent_document")

class PaginatedData(BaseModel):
    """Paginated data response."""
    rows: List[DataRow]
    total_count: int
    filtered_count: Optional[int] = None  # Total rows after filtering (None = no filter applied)
    page: int
    page_size: int
    has_more: bool


# Filter/Sort request models for server-side filtering
class FilterRuleRequest(BaseModel):
    """A single filter rule from the frontend."""
    column: str
    operator: str  # 'contains', 'equals', 'gt', 'lt', 'isNull', etc.
    value: Any = None
    caseSensitive: Optional[bool] = False


class SortColumnRequest(BaseModel):
    """A single sort column from the frontend."""
    column: str
    direction: Literal['asc', 'desc']
    priority: Optional[int] = 1


class FilterSortRequest(BaseModel):
    """Request body for server-side filtering and sorting."""
    filters: Optional[List[FilterRuleRequest]] = None
    sort: Optional[List[SortColumnRequest]] = None
    search: Optional[str] = None  # Global search term

# Schema editing operation models
class SchemaOperation(BaseModel):
    """Base class for schema operations."""
    operation_id: str
    session_id: str
    operation_type: str  # "edit", "delete", "add", "merge", "reprocess"
    status: str  # "pending", "in_progress", "completed", "failed"
    created_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

class ColumnEdit(BaseModel):
    """Details of a column edit operation."""
    original_name: str
    new_name: Optional[str] = None
    original_definition: Optional[str] = None
    new_definition: Optional[str] = None
    original_rationale: Optional[str] = None
    new_rationale: Optional[str] = None
    requires_reprocessing: bool = False

class ColumnMerge(BaseModel):
    """Details of a column merge operation."""
    source_columns: List[str]
    target_column: str
    merge_strategy: str  # "concatenate", "smart_merge", "first_non_empty"
    separator: str = " | "
    definition: Optional[str] = None
    rationale: Optional[str] = None

class ReprocessingStatus(BaseModel):
    """Status of document reprocessing."""
    session_id: str
    operation_id: str
    status: str  # "pending", "processing", "completed", "failed"
    progress: float = Field(ge=0.0, le=1.0, default=0.0)
    current_step: str = ""
    affected_columns: List[str] = []
    processed_documents: int = 0
    total_documents: int = 0
    estimated_completion: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class SchemaBackup(BaseModel):
    """Schema backup information."""
    backup_id: str
    session_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    backup_path: str
    includes_data: bool = False
    column_count: int
    description: Optional[str] = None
    
class SchemaValidation(BaseModel):
    """Schema validation result."""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    suggestions: List[str] = []
    column_consistency: Dict[str, bool] = {}
    data_integrity: Dict[str, Any] = {}