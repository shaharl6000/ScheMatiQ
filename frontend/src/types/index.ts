// Type definitions for the visualization app

// Creation and modification tracking types
export interface ModificationAction {
  timestamp: string;
  action_type: 'column_added' | 'column_edited' | 'column_deleted';
  column_name: string;
  details: Record<string, any>;
}

export interface CreationMetadata {
  created_at: string;
  creation_query: string;
  llm_model: string;
  llm_provider: string;
  iterations_count: number;
  final_schema_size: number;
  convergence_achieved: boolean;
}

// Observation unit definition (what constitutes a single row)
export interface ObservationUnitInfo {
  /** e.g., "Model-Benchmark Evaluation" */
  name: string;
  /** What constitutes one row */
  definition: string;
  /** e.g., ["GPT-4 on MMLU", "Claude on HumanEval"] */
  example_names?: string[];
  /** Document that helped define this unit */
  source_document?: string;
  /** Iteration when this unit was discovered */
  discovery_iteration?: number;
}

// Pending value for schema evolution
export interface PendingValue {
  value: string;
  document_count: number;
  first_seen: string;
  documents: string[];  // Document names where value appeared
}

// Schema suggestion for allowed_values evolution
export interface SchemaSuggestion {
  column_name: string;
  suggested_values: string[];
  value_details: Record<string, PendingValue>;
  auto_approved: boolean;
  created_at: string;
}

export interface ColumnInfo {
  name: string;
  definition?: string;
  rationale?: string;
  data_type?: string;
  non_null_count?: number;
  unique_count?: number;
  source_document?: string;  // Document that first added this column
  discovery_iteration?: number;  // Iteration when this column was discovered
  allowed_values?: string[];  // Closed set of valid values for categorical columns
  auto_expand_threshold?: number;  // Auto-add new value if seen in N+ docs (-1 = disabled)
  pending_values?: PendingValue[];  // Values pending approval
}

// Schema evolution tracking types
export interface SchemaSnapshot {
  iteration: number;
  documents_processed: string[];
  total_columns: number;
  new_columns: string[];
  cumulative_documents: number;
}

export interface SchemaEvolution {
  snapshots: SchemaSnapshot[];
  column_sources: Record<string, string>;  // column_name -> source_document
}

export interface SessionMetadata {
  source: string;
  created: string;
  last_modified: string;
  file_size?: number;
  row_count?: number;
  // Enhanced upload workflow metadata
  extracted_schema?: SchemaData;
  uploaded_documents?: string[];
  processed_documents?: number;
  processing_stats?: ProcessingStats;
  original_row_count?: number;
  additional_rows_added?: number;
  cloud_dataset?: string;
}

export interface DataStatistics {
  total_rows: number;
  total_columns: number;
  total_documents?: number;  // Actual document count (rows may have multiple observation units per document)
  completeness: number;
  column_stats: ColumnInfo[];
  schema_evolution?: SchemaEvolution;  // How schema evolved during discovery
  skipped_documents?: string[];  // Documents skipped during value extraction (no observation units found)
}

export interface VisualizationSession {
  id: string;
  type: 'load' | 'qbsd';
  status: 'created' | 'processing' | 'schema_ready' | 'completed' | 'error' | 'stopped' |
          'schema_extracted' | 'documents_uploaded' | 'processing_documents';
  metadata: SessionMetadata;
  schema_query?: string;
  columns: ColumnInfo[];
  statistics?: DataStatistics;
  error_message?: string;
  schema_suggestions?: SchemaSuggestion[];  // Pending schema evolution suggestions
  // Creation and modification tracking
  creation_metadata?: CreationMetadata;  // Immutable creation info
  modification_history?: ModificationAction[];  // Schema modification log
  // Observation unit tracking
  observation_unit?: ObservationUnitInfo;  // What constitutes a single row
}

export interface DataRow {
  row_name?: string;
  papers?: string[];
  data: Record<string, CellValue>;
  // Observation unit metadata (for multi-row extraction)
  _unit_name?: string;
  _source_document?: string;
  _parent_document?: string;
  _observation_unit?: string;
  _unit_confidence?: string;
}

export interface PaginatedData {
  rows: DataRow[];
  total_count: number;
  filtered_count?: number;  // Total rows after filtering (undefined = no filter applied)
  page: number;
  page_size: number;
  has_more: boolean;
}

export interface FileValidationResult {
  is_valid: boolean;
  errors: string[];
  warnings: string[];
  detected_format?: string;
  estimated_rows?: number;
  estimated_columns?: number;
  sample_data?: Record<string, CellValue>[];
}

export interface LLMConfig {
  provider: string;
  model: string;
  max_output_tokens?: number;  // Auto-detected from model specs if not provided
  temperature: number;
  context_window_size?: number;  // Auto-detected from model specs if not provided
  api_key?: string;
}

export interface RetrieverConfig {
  type: string;
  model_name: string;
  passage_chars: number;
  overlap: number;
  k: number;
  enable_dynamic_k: boolean;
  dynamic_k_threshold: number;
  dynamic_k_minimum: number;
}

// Initial schema column for inline schema definition
export interface InitialSchemaColumn {
  name: string;
  definition: string;
  rationale: string;
  allowed_values?: string[];
}

// Initial observation unit configuration
export interface InitialObservationUnit {
  name: string;
  definition?: string;  // Optional - if not provided, will be auto-discovered
}

export interface QBSDConfig {
  /** Research query (optional for document-only mode) */
  query: string;
  /** Document paths (optional for query-only mode - can be empty array or null) */
  docs_path: string | string[] | null;
  /** True when documents will be uploaded after session creation */
  upload_pending?: boolean;
  max_keys_schema: number;
  documents_batch_size: number;
  initial_schema_path?: string;  // Path to schema file
  initial_schema?: InitialSchemaColumn[];  // Inline schema definition
  initial_observation_unit?: InitialObservationUnit;  // Pre-configured observation unit
  schema_creation_backend: LLMConfig;
  value_extraction_backend: LLMConfig;
  retriever?: RetrieverConfig;
  output_path: string;
  document_randomization_seed: number;
  skip_value_extraction?: boolean;  // Schema discovery only mode
  previous_session_id?: string;  // Session ID to copy uploaded files from
  opt_out_data_collection?: boolean;  // User opted out of research data archival
}

export interface QBSDStatus {
  session_id: string;
  status: string;
  progress: number;
  current_step: string;
  steps_completed: number;
  total_steps: number;
  error_message?: string;
  estimated_time_remaining?: number;
  // Phase tracking (for UI recovery on remount)
  schema_completed?: boolean;
  columns_discovered?: number;
}

// WebSocket data payload types
export interface ProgressData {
  session_id: string;
  status: string;
  progress: number;
  current_step?: string;
  steps_completed?: number;
  total_steps?: number;
  details?: Record<string, unknown>;
}

export interface LogData {
  level: 'info' | 'warning' | 'error';
  message: string;
  details?: Record<string, unknown>;
}

export interface ErrorData {
  session_id: string;
  error_message: string;
  error_details?: Record<string, unknown>;
}

export interface CompletionData {
  session_id: string;
  message: string;
  total_documents?: number;
  schema_columns?: number;
  additional_rows?: number;
}

export interface SchemaCompletionData {
  query: string;
  columns: ColumnInfo[];
  total_columns: number;
}

export interface RowCompletionData {
  row_index: number;
  total_rows: number;
  completed_at: string;
  additional_rows?: number;
  document_names?: string[];
  elapsed_seconds?: number;
}

export interface CellExtractedData {
  row_name: string;
  column: string;
  value: CellValue;
  row_index?: number;
}

export interface StoppedData {
  schema_saved: boolean;
  data_rows_saved: number;
  message: string;
}

export interface WebSocketMessage {
  type: 'progress' | 'log' | 'error' | 'completed' | 'connected' | 'disconnected' | 'reconnecting' | 'pong' | 'heartbeat' | 'schema_completed' | 'schema_progress' | 'row_completed' | 'schema_updated' | 'reprocessing_progress' | 'reprocessing_completed' | 'cell_extracted' | 'stopped' | 'continue_discovery_progress' | 'continue_discovery_completed' | 'continue_discovery_stopped' | 'incremental_extraction_progress' | 'quota_exceeded';
  timestamp?: string;
  session_id?: string;
  message?: string;
  data?: ProgressData | LogData | ErrorData | CompletionData | SchemaCompletionData | RowCompletionData | SchemaUpdatedData | ReprocessingProgressData | ReprocessingCompletedData | CellExtractedData | StoppedData;
}

// Schema types
export interface SchemaColumn {
  name: string;
  definition?: string;
  rationale?: string;
}

// Enhanced upload workflow types
export interface SchemaExtractionResult {
  status: string;
  message: string;
  schema: {
    query: string;
    schema: SchemaColumn[];
    extracted_from_upload: boolean;
    extraction_metadata: ProcessingStats;
  };
  total_columns: number;
  extracted_metadata?: {
    llm_config?: {
      value_extraction_backend?: LLMConfig;
    };
  };
}

export interface DocumentUploadResult {
  status: string;
  message: string;
  uploaded_files: string[];
  warnings: string[];
  documents_directory: string;
}

export interface DocumentProcessingResult {
  status: string;
  message: string;
  session_id: string;
  total_documents: number;
  schema_columns: number;
}

export interface ProcessingStatus {
  session_id: string;
  status: string;
  total_documents: number;
  processed_documents: number;
  original_row_count: number;
  additional_rows_added: number;
  processing_stats: ProcessingStats;
  last_modified: string;
  progress: number;
}

// Content and display types

// Excerpt with source information (new format)
export interface ExcerptWithSource {
  text: string;
  source: string;  // Source filename
}

// Union type for backwards compatibility
export type Excerpt = string | ExcerptWithSource;

export interface QBSDAnswerWithExcerpts {
  answer: string;
  excerpts: Excerpt[];  // Supports both old (string) and new (object) formats
}

export type CellValue = string | number | boolean | null | undefined | QBSDAnswerWithExcerpts | unknown[] | Record<string, unknown>;

export interface ModalContent {
  title: string;
  content: CellValue;
}

export interface ApiError {
  response?: {
    data?: {
      detail?: string;
    };
  };
  message?: string;
}

export type GenericErrorHandler = (error: ApiError) => void;

export interface SchemaData {
  query: string;
  schema: SchemaColumn[];
  llm_configuration?: {
    schema_creation_backend?: LLMConfig;
    value_extraction_backend?: LLMConfig;
  };
  metadata?: {
    imported_from_csv?: boolean;
    original_session_id?: string;
    generated_timestamp?: string;  // Original QBSD creation timestamp
    import_timestamp?: string;     // When it was imported/loaded
  };
  observation_unit?: ObservationUnitInfo;
}

export interface ProcessingStats {
  start_time?: string;
  end_time?: string;
  duration?: number;
  errors?: string[];
  warnings?: string[];
  documents_processed?: number;
  rows_generated?: number;
  [key: string]: unknown; // Allow for additional dynamic properties
}

// Schema editing operation types
export interface EditColumnRequest {
  old_name: string;  // Current name of the column to edit
  definition?: string;
  rationale?: string;
  new_name?: string; // For renaming
  allowed_values?: string[]; // Closed set of valid values
  reprocess?: boolean; // Whether to reprocess documents (default: true on backend, set false for metadata-only edits)
}

export interface AddColumnRequest {
  name: string;
  definition: string;
  rationale?: string;
  document_paths?: string[]; // Specific documents to process
  allowed_values?: string[]; // Closed set of valid values
  llm_config?: {
    provider: string;
    model: string;
    api_key?: string;
    max_output_tokens?: number;
    temperature?: number;
  };
}

export interface MergeColumnsRequest {
  source_columns: string[]; // Columns to merge
  target_column: string; // New merged column name
  merge_strategy?: 'concatenate' | 'smart_merge' | 'first_non_empty';
  definition?: string;
  rationale?: string;
  separator?: string; // For concatenation
}

export interface ReprocessRequest {
  column_names?: string[]; // Specific columns, null = all
  document_paths?: string[]; // Specific documents, null = all
  incremental?: boolean; // Only process changed columns
}

export interface SchemaEditResponse {
  status: string;
  message: string;
  columns?: ColumnInfo[];  // Updated columns returned from backend
  reprocessing?: boolean;
  reprocessing_required?: boolean;
}

export interface ReprocessingStatus {
  session_id: string;
  status: string;
  progress: number;
  current_step: string;
  affected_columns: string[];
  processed_documents: number;
  total_documents: number;
  estimated_completion?: string;
}

export interface SchemaOperation {
  operation_id: string;
  session_id: string;
  operation_type: 'edit' | 'delete' | 'add' | 'merge' | 'reprocess';
  status: 'pending' | 'in_progress' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error_message?: string;
}

export interface ColumnEdit {
  original_name: string;
  new_name?: string;
  original_definition?: string;
  new_definition?: string;
  original_rationale?: string;
  new_rationale?: string;
  requires_reprocessing?: boolean;
}

export interface ColumnMerge {
  source_columns: string[];
  target_column: string;
  merge_strategy: 'concatenate' | 'smart_merge' | 'first_non_empty';
  separator?: string;
  definition?: string;
  rationale?: string;
}

export interface SchemaBackup {
  backup_id: string;
  session_id: string;
  created_at: string;
  backup_path: string;
  includes_data?: boolean;
  column_count: number;
  description?: string;
}

export interface SchemaValidationResult {
  session_id: string;
  is_valid: boolean;
  errors: string[];
  warnings: string[];
  suggestions: string[];
  column_count: number;
  has_data: boolean;
  data_consistency: Record<string, unknown>;
}

// Enhanced WebSocket message types for schema editing
export interface SchemaUpdatedData {
  operation: 'edit_column' | 'delete_column' | 'add_column' | 'merge_columns' | 'restore_schema';
  updated_column?: ColumnInfo;
  deleted_column?: string;
  new_column?: ColumnInfo;
  source_columns?: string[];
  target_column?: string;
  total_columns?: number;
  backup_id?: string;
}

export interface ReprocessingProgressData {
  operation_id: string;
  step: string;
  progress: number;
  processed_documents: number;
  total_documents: number;
  affected_columns: string[];
}

export interface ReprocessingCompletedData {
  operation_id: string;
  affected_columns: string[];
  processed_documents: number;
  completed_at: string;
}

// Update WebSocketMessage to include new types
export interface WebSocketMessageExtended extends WebSocketMessage {
  type: 'progress' | 'log' | 'error' | 'completed' | 'connected' | 'pong' |
        'schema_completed' | 'row_completed' | 'schema_updated' |
        'reprocessing_progress' | 'reprocessing_completed' | 'cell_extracted';
  data?: ProgressData | LogData | ErrorData | CompletionData |
         SchemaCompletionData | RowCompletionData | SchemaUpdatedData |
         ReprocessingProgressData | ReprocessingCompletedData | CellExtractedData;
}

// Dialog and UI state types
export interface ColumnDialogState {
  open: boolean;
  mode: 'add' | 'edit';
  column?: ColumnInfo;
}

export interface SchemaEditingState {
  isEditing: boolean;
  editingColumn?: string;
  pendingOperations: SchemaOperation[];
  activeReprocessing?: ReprocessingStatus;
  validationResult?: SchemaValidationResult;
  backups: SchemaBackup[];
}

// Re-extraction types
export interface ColumnChangeDetail {
  column_name: string;
  change_type: 'definition' | 'rationale' | 'allowed_values' | 'new';
  old_value?: string;
  new_value?: string;
  row_count_affected: number;
}

export interface SchemaChangeStatus {
  has_changes: boolean;
  changed_columns: string[];
  new_columns: string[];
  column_changes: Record<string, ColumnChangeDetail>;
  can_reextract: boolean;
  missing_baseline: boolean;
}

export interface PaperDiscoveryResult {
  total_rows: number;
  rows_with_papers: number;
  available_papers: string[];
  missing_papers: string[];
  paper_to_rows: Record<string, string[]>;
  cloud_papers?: Record<string, string>;  // paper_name -> supabase_path
  local_papers?: string[];                 // papers already in local documents/
}

export interface ReextractionRequest {
  columns: string[];
  llm_config?: {
    provider: string;
    model: string;
    api_key?: string;
    max_output_tokens?: number;
    temperature?: number;
  };
}

export interface ReextractionResponse {
  status: string;
  operation_id: string;
  columns: string[];
  estimated_papers: number;
  rows_to_process: number;
  missing_papers: string[];
}

export interface ReextractionOperationStatus {
  operation_id: string;
  session_id: string;
  status: 'pending' | 'starting' | 'running' | 'completed' | 'failed' | 'stopped';
  progress: number;
  columns: string[];
  current_column?: string;
  processed_documents: number;
  total_documents: number;
  started_at?: string;
  completed_at?: string;
  error?: string;
}

// Re-extraction WebSocket event types
export interface ReextractionStartedData {
  operation_id: string;
  columns: string[];
  total_documents: number;
}

export interface ReextractionProgressData {
  operation_id: string;
  column: string;
  progress: number;
  processed_documents: number;
  total_documents: number;
  current_row?: string;
}

export interface ReextractionCompletedData {
  operation_id: string;
  columns: string[];
  status: 'success' | 'failed';
}

export interface ReextractionFailedData {
  operation_id: string;
  error: string;
}

// ==================== Continue Schema Discovery Types ====================

export interface ContinueDiscoveryDocuments {
  original_documents: string[];
  original_count: number;
  local_count?: number;
  cloud_count?: number;
  cloud_datasets: { name: string; file_count: number }[];
  original_cloud_dataset?: string;
  can_use_original: boolean;
  query: string;
}

export interface ContinueDiscoveryRequest {
  document_source: 'original' | 'upload' | 'cloud';
  cloud_dataset?: string;
  llm_config: {
    provider: string;
    model: string;
    api_key?: string;
    max_output_tokens?: number;
    temperature?: number;
    context_window_size?: number;
  };
  retriever_config?: {
    model_name?: string;
    passage_chars?: number;
    k?: number;
    enable_dynamic_k?: boolean;
    dynamic_k_threshold?: number;
    dynamic_k_minimum?: number;
  };
  max_keys_schema?: number;
  documents_batch_size?: number;
  bypass_limit?: boolean;
}

export interface ContinueDiscoveryResponse {
  status: string;
  operation_id: string;
  initial_column_count: number;
  document_source: string;
}

export interface NewColumnInfo {
  name: string;
  definition: string;
  rationale: string;
  allowed_values?: string[];
  source_document?: string;
  discovery_iteration?: number;
}

export interface ContinueDiscoveryStatus {
  operation_id: string;
  session_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'stopped';
  phase: 'discovery' | 'extraction';
  progress: number;
  current_batch: number;
  total_batches: number;
  initial_columns: string[];
  new_columns: NewColumnInfo[];
  confirmed_columns: string[];
  processed_documents: number;
  total_documents: number;
  started_at?: string;
  completed_at?: string;
  error?: string;
}

export interface ConfirmColumnsRequest {
  selected_columns: string[];
  row_selection: 'all' | 'selected';
  selected_rows?: string[];
  llm_config?: {
    provider: string;
    model: string;
    api_key?: string;
    max_output_tokens?: number;
    temperature?: number;
  };
}

export interface ConfirmColumnsResponse {
  status: string;
  operation_id: string;
  columns: string[];
  row_count: number | 'all';
}

// Continue Discovery WebSocket event types
export interface ContinueDiscoveryStartedData {
  operation_id: string;
  initial_columns: string[];
  document_source: string;
}

export interface ContinueDiscoveryProgressData {
  operation_id: string;
  progress: number;
  current_batch: number;
  total_batches: number;
}

export interface ContinueDiscoveryCompletedData {
  operation_id: string;
  initial_columns: string[];
  new_columns: NewColumnInfo[];
  total_columns: number;
  message: string;
}

export interface IncrementalExtractionStartedData {
  operation_id: string;
  columns: string[];
}

export interface IncrementalExtractionProgressData {
  operation_id: string;
  column: string;
  progress: number;
  processed_documents: number;
  current_row?: string;
}

export interface IncrementalExtractionCompletedData {
  operation_id: string;
  columns: string[];
  status: 'success' | 'failed';
}

export interface ContinueDiscoveryStoppedData {
  operation_id: string;
  phase: string;
  message: string;
}

export interface ContinueDiscoveryFailedData {
  operation_id: string;
  error: string;
}

// ==================== Document Availability Pre-check Types ====================

export interface DocumentInfo {
  name: string;
  status: 'local' | 'cloud' | 'missing';
  cloud_path?: string;
  affected_rows: string[];
}

export interface DocumentAvailabilityRequest {
  operation_type: 'reextraction' | 'continue_discovery';
  columns?: string[];
}

export interface DocumentAvailabilityResponse {
  total_documents: number;
  local_documents: DocumentInfo[];
  cloud_documents: DocumentInfo[];
  missing_documents: DocumentInfo[];
  can_proceed: boolean;
  total_rows: number;
  rows_with_missing_docs: number;
}

// ==================== Column Clustering Types ====================

/**
 * Represents a cluster of related schema columns.
 * Used for grouping columns by semantic similarity or user-defined categories.
 */
export interface ColumnCluster {
  /** Unique identifier for the cluster */
  id: string;
  /** Human-readable name for the cluster */
  name: string;
  /** Optional description of what this cluster represents */
  description?: string;
  /** Color for visual distinction (hex or CSS color) */
  color?: string;
  /** Whether the cluster is collapsed in the UI */
  collapsed?: boolean;
  /** Ordered list of column names in this cluster */
  column_names: string[];
}

/**
 * Configuration for column clustering feature.
 */
export interface ClusteringConfig {
  /** Whether clustering is enabled */
  enabled: boolean;
  /** List of defined clusters */
  clusters: ColumnCluster[];
  /** How to handle columns not in any cluster */
  unclustered_behavior: 'hide' | 'show_at_end' | 'show_at_start';
}

/**
 * Cost estimate for a single phase (schema discovery or value extraction).
 */
export interface PhaseEstimate {
  /** Number of input tokens estimated */
  input_tokens: number;
  /** Number of output tokens estimated */
  output_tokens: number;
  /** Number of API calls expected */
  api_calls: number;
  /** Estimated cost in USD */
  cost_usd: number;
}

/**
 * Statistics about documents being processed.
 */
export interface DocumentStats {
  /** Number of documents */
  num_documents: number;
  /** Total tokens across all documents */
  total_tokens: number;
  /** Average tokens per document */
  avg_tokens_per_document: number;
  /** Maximum tokens in a single document */
  max_tokens_in_document: number;
}

/**
 * Complete cost estimate for QBSD execution.
 */
export interface CostEstimate {
  /** Estimate for schema discovery phase */
  schema_discovery: PhaseEstimate;
  /** Estimate for value extraction phase */
  value_extraction: PhaseEstimate;
  /** Total input tokens across both phases */
  total_input_tokens: number;
  /** Total output tokens across both phases */
  total_output_tokens: number;
  /** Total API calls across both phases */
  total_api_calls: number;
  /** Total estimated cost in USD */
  total_cost_usd: number;
  /** Warning messages about the estimate */
  warnings: string[];
  /** Statistics about the documents */
  document_stats: DocumentStats;
}