"""QBSD-specific models."""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field

class LLMConfig(BaseModel):
    """LLM backend configuration.

    Token limits (max_output_tokens, context_window_size) are auto-detected
    from model specs when not provided. Explicit values override auto-detection.
    """
    provider: str  # "openai", "together", "gemini"
    model: str = ""  # Empty string = use provider default (e.g., gemini-2.5-flash-lite for Gemini)
    max_output_tokens: Optional[int] = None  # Auto-detected from model specs if None
    temperature: float = 0
    context_window_size: Optional[int] = None  # Auto-detected from model specs if None
    api_key: Optional[str] = None  # User-provided API key (falls back to env var)

class RetrieverConfig(BaseModel):
    """Retriever configuration."""
    model_config = {"protected_namespaces": ()}

    type: str = "embedding"
    model_name: str = "all-MiniLM-L6-v2"
    passage_chars: int = 512
    overlap: int = 64
    k: int = 15
    enable_dynamic_k: bool = True
    dynamic_k_threshold: float = 0.65
    dynamic_k_minimum: int = 3

class InitialSchemaColumn(BaseModel):
    """Initial schema column definition."""
    name: str
    definition: str
    rationale: str
    allowed_values: Optional[List[str]] = None

class InitialObservationUnit(BaseModel):
    """Initial observation unit configuration."""
    name: str
    definition: Optional[str] = None  # If None, will be auto-discovered

class QBSDConfig(BaseModel):
    """QBSD configuration matching the existing config format.

    Supports three modes:
    - Standard: Both query and docs_path provided
    - Document-only: docs_path provided, query empty/None (schema discovered from content)
    - Query-only: query provided, docs_path empty/None (schema planned based on query)

    At least one of query or docs_path must be provided.
    """
    query: str = ""  # Optional - can be empty for document-only mode
    docs_path: Union[str, List[str], None] = None  # Optional - can be None for query-only mode
    upload_pending: bool = False  # True when documents will be uploaded after session creation
    max_keys_schema: int = 100
    documents_batch_size: int = 4
    initial_schema_path: Optional[str] = None  # Path to schema file
    initial_schema: Optional[List[InitialSchemaColumn]] = None  # Inline schema definition
    initial_observation_unit: Optional[InitialObservationUnit] = None  # Pre-configured observation unit
    schema_creation_backend: LLMConfig
    value_extraction_backend: LLMConfig
    retriever: Optional[RetrieverConfig] = None
    output_path: str
    document_randomization_seed: int = 42
    skip_value_extraction: bool = False  # Schema discovery only mode
    previous_session_id: Optional[str] = None  # Session ID to copy uploaded files from
    count_toward_quota: bool = True  # If False, this session's LLM calls won't count toward the global quota
    llm_call_limit: Optional[int] = None  # Developer-only: override the global quota limit for this session
    opt_out_data_collection: bool = False  # User opted out of research data archival

class QBSDStatus(BaseModel):
    """Status of QBSD execution."""
    session_id: str
    status: str  # "configuring", "processing", "completed", "error"
    progress: float = Field(ge=0.0, le=1.0)
    current_step: str
    steps_completed: int = 0
    total_steps: int = 0
    error_message: Optional[str] = None
    estimated_time_remaining: Optional[int] = None  # seconds
    # Phase tracking (for UI recovery on remount)
    schema_completed: bool = False
    columns_discovered: int = 0
    total_documents: int = 0
    processed_documents: int = 0

class QBSDProgress(BaseModel):
    """Detailed progress information."""
    step_name: str
    step_progress: float = Field(ge=0.0, le=1.0)
    details: Dict[str, Any] = {}
    timestamp: str


class PhaseEstimate(BaseModel):
    """Cost estimate for a single phase (schema discovery or value extraction)."""
    input_tokens: int = 0
    output_tokens: int = 0
    api_calls: int = 0
    cost_usd: float = 0.0


class DocumentStats(BaseModel):
    """Statistics about documents being processed."""
    num_documents: int = 0
    total_tokens: int = 0
    avg_tokens_per_document: int = 0
    max_tokens_in_document: int = 0


class CostEstimate(BaseModel):
    """Complete cost estimate for QBSD execution."""
    schema_discovery: PhaseEstimate = Field(default_factory=PhaseEstimate)
    value_extraction: PhaseEstimate = Field(default_factory=PhaseEstimate)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_api_calls: int = 0
    total_cost_usd: float = 0.0
    warnings: List[str] = Field(default_factory=list)
    document_stats: DocumentStats = Field(default_factory=DocumentStats)


class UploadedFileInfo(BaseModel):
    """Basic info about an uploaded file (for cost estimation without sending content)."""
    name: str
    size: int  # Size in bytes


class CostEstimateRequest(BaseModel):
    """Request body for cost estimation endpoint."""
    config: QBSDConfig
    uploaded_files: Optional[List[UploadedFileInfo]] = None  # For uploaded files - just metadata