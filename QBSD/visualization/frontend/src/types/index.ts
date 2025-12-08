// Type definitions for the visualization app

export interface ColumnInfo {
  name: string;
  definition?: string;
  rationale?: string;
  data_type?: string;
  non_null_count?: number;
  unique_count?: number;
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
}

export interface DataStatistics {
  total_rows: number;
  total_columns: number;
  completeness: number;
  column_stats: ColumnInfo[];
}

export interface VisualizationSession {
  id: string;
  type: 'upload' | 'qbsd';
  status: 'created' | 'processing' | 'schema_ready' | 'completed' | 'error' | 
          'schema_extracted' | 'documents_uploaded' | 'processing_documents';
  metadata: SessionMetadata;
  schema_query?: string;
  columns: ColumnInfo[];
  statistics?: DataStatistics;
  error_message?: string;
}

export interface DataRow {
  row_name?: string;
  papers?: string[];
  data: Record<string, CellValue>;
}

export interface PaginatedData {
  rows: DataRow[];
  total_count: number;
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
  max_tokens: number;
  temperature: number;
  max_context_tokens?: number;
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

export interface QBSDConfig {
  query: string;
  docs_path: string | string[];
  max_keys_schema: number;
  documents_batch_size: number;
  initial_schema_path?: string;
  schema_creation_backend: LLMConfig;
  value_extraction_backend: LLMConfig;
  retriever?: RetrieverConfig;
  output_path: string;
  document_randomization_seed: number;
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
}

export interface WebSocketMessage {
  type: 'progress' | 'log' | 'error' | 'completed' | 'connected' | 'pong' | 'schema_completed' | 'row_completed' | 'schema_updated' | 'reprocessing_progress' | 'reprocessing_completed';
  timestamp: string;
  session_id?: string;
  message?: string;
  data?: ProgressData | LogData | ErrorData | CompletionData | SchemaCompletionData | RowCompletionData | SchemaUpdatedData | ReprocessingProgressData | ReprocessingCompletedData;
}

// Dual-file upload types
export interface SchemaColumn {
  name: string;
  definition?: string;
  rationale?: string;
}

export interface SchemaValidationResultBasic {
  is_valid: boolean;
  errors: string[];
  warnings: string[];
  detected_columns: string[];
  query?: string;
  schema?: SchemaColumn[];
}

export interface CompatibilityCheck {
  is_compatible: boolean;
  matching_columns: string[];
  missing_in_data: string[];  // Columns in schema but not in data
  extra_in_data: string[];    // Columns in data but not in schema
  schema_count: number;
  data_count: number;
  compatibility_score: number;  // Percentage of matching columns
  detailed_errors: string[];
  suggestions: string[];
}

export interface DualFileUploadResult {
  session_id: string;
  schema_validation: SchemaValidationResultBasic;
  data_validation: FileValidationResult;
  compatibility: CompatibilityCheck;
  requires_column_mapping: boolean;
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
export interface QBSDAnswerWithExcerpts {
  answer: string;
  excerpts: string[];
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
  name: string;
  definition?: string;
  rationale?: string;
  new_name?: string; // For renaming
}

export interface AddColumnRequest {
  name: string;
  definition: string;
  rationale: string;
  document_paths?: string[]; // Specific documents to process
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
  success: boolean;
  message: string;
  updated_columns: ColumnInfo[];
  reprocessing_required?: boolean;
  session_status: string;
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
        'reprocessing_progress' | 'reprocessing_completed';
  data?: ProgressData | LogData | ErrorData | CompletionData | 
         SchemaCompletionData | RowCompletionData | SchemaUpdatedData | 
         ReprocessingProgressData | ReprocessingCompletedData;
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