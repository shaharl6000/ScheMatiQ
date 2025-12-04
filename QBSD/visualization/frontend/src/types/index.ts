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
  status: 'created' | 'processing' | 'completed' | 'error';
  metadata: SessionMetadata;
  schema_query?: string;
  columns: ColumnInfo[];
  statistics?: DataStatistics;
  error_message?: string;
}

export interface DataRow {
  row_name?: string;
  papers?: string[];
  data: Record<string, any>;
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
  sample_data?: Record<string, any>[];
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
  backend: LLMConfig;
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

export interface WebSocketMessage {
  type: 'progress' | 'log' | 'error' | 'completed' | 'connected' | 'pong';
  timestamp: string;
  session_id?: string;
  message?: string;
  data?: any;
}

// Dual-file upload types
export interface SchemaColumn {
  name: string;
  definition?: string;
  rationale?: string;
}

export interface SchemaValidationResult {
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
  schema_validation: SchemaValidationResult;
  data_validation: FileValidationResult;
  compatibility: CompatibilityCheck;
  requires_column_mapping: boolean;
}