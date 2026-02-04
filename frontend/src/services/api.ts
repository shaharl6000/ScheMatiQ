import axios from 'axios';
import {
  VisualizationSession,
  PaginatedData,
  FileValidationResult,
  QBSDConfig,
  QBSDStatus,
  SchemaExtractionResult,
  DocumentUploadResult,
  DocumentProcessingResult,
  ProcessingStatus,
  SchemaData,
  EditColumnRequest,
  AddColumnRequest,
  MergeColumnsRequest,
  ReprocessRequest,
  SchemaEditResponse,
  ReprocessingStatus,
  SchemaValidationResult as SchemaValidationResultType,
  SchemaChangeStatus,
  PaperDiscoveryResult,
  ReextractionRequest,
  ReextractionResponse,
  ReextractionOperationStatus,
  ContinueDiscoveryDocuments,
  ContinueDiscoveryRequest,
  ContinueDiscoveryResponse,
  ContinueDiscoveryStatus,
  ConfirmColumnsRequest,
  ConfirmColumnsResponse,
  DocumentAvailabilityRequest,
  DocumentAvailabilityResponse,
  CostEstimate
} from '../types';
import {
  UnitListResponse,
  UnitSuggestionsResponse,
  MergeUnitsRequest,
  MergeUnitsResponse,
} from '../types/unit';
import { FilterRule, SortColumn } from '../components/DataTable/types/filters';

// Railway backend URL - used when env vars aren't set at build time
const RAILWAY_BACKEND_URL = 'https://backend-production-5a26.up.railway.app';

// Determine API base URL
function getApiBaseUrl(): string {
  // Build-time env var (preferred)
  if (process.env.REACT_APP_API_URL) {
    return `${process.env.REACT_APP_API_URL}/api`;
  }

  // Runtime detection for Railway
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname;
    if (hostname.includes('railway.app') || hostname.includes('up.railway.app')) {
      return `${RAILWAY_BACKEND_URL}/api`;
    }
  }

  // Local development - use relative path (proxy handles it)
  return '/api';
}

const API_BASE = getApiBaseUrl();

// Export for WebSocket service to use
export const getBackendBaseUrl = (): string => {
  if (process.env.REACT_APP_API_URL) {
    return process.env.REACT_APP_API_URL;
  }
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname;
    if (hostname.includes('railway.app') || hostname.includes('up.railway.app')) {
      return RAILWAY_BACKEND_URL;
    }
  }
  return 'http://localhost:8000';
};

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

// Load API (for loading existing QBSD data)
export const loadAPI = {
  uploadFile: async (file: File): Promise<{
    session_id: string;
    validation: FileValidationResult;
    requires_column_mapping: boolean;
  }> => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await api.post('/load/file', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });

    return response.data;
  },

  parseFile: async (sessionId: string, columnMappings?: Record<string, string>): Promise<void> => {
    const payload = columnMappings ? {
      session_id: sessionId,
      column_mappings: columnMappings,
      column_types: {}
    } : null;

    await api.post(`/load/parse/${sessionId}`, payload);
  },

  getData: async (
    sessionId: string,
    page = 0,
    pageSize = 50,
    filters?: FilterRule[],
    sort?: SortColumn[],
    search?: string
  ): Promise<PaginatedData> => {
    // Use POST for filter/sort support
    const response = await api.post(`/load/data/${sessionId}`, {
      filters: filters && filters.length > 0 ? filters : null,
      sort: sort && sort.length > 0 ? sort : null,
      search: search || null
    }, {
      params: { page, page_size: pageSize }
    });
    return response.data;
  },

  getSession: async (sessionId: string): Promise<VisualizationSession> => {
    const response = await api.get(`/load/sessions/${sessionId}`);
    return response.data;
  },

  listSessions: async (): Promise<VisualizationSession[]> => {
    const response = await api.get('/load/sessions');
    return response.data;
  },

  deleteSession: async (sessionId: string): Promise<void> => {
    await api.delete(`/load/sessions/${sessionId}`);
  },

  // Enhanced load workflow methods
  extractSchema: async (sessionId: string, query?: string): Promise<SchemaExtractionResult> => {
    const response = await api.post(`/load/extract-schema/${sessionId}`, null, {
      params: { query: query || '' }
    });
    return response.data;
  },

  addDocuments: async (sessionId: string, files: File[]): Promise<DocumentUploadResult> => {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });

    const response = await api.post(`/load/add-documents/${sessionId}`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });

    return response.data;
  },

  processDocuments: async (sessionId: string, llmConfig?: any): Promise<DocumentProcessingResult> => {
    const requestBody = llmConfig ? { llm_config: llmConfig } : {};
    const response = await api.post(`/load/process-documents/${sessionId}`, requestBody);
    return response.data;
  },

  confirmWebSocketReady: async (sessionId: string): Promise<{ status: string; connections: number; session_id: string }> => {
    const response = await api.post(`/load/sessions/${sessionId}/confirm-websocket`);
    return response.data;
  },

  removeDocument: async (sessionId: string, filename: string): Promise<{
    status: string;
    message: string;
    remaining_documents: string[];
    files_removed: string[];
  }> => {
    const response = await api.delete(`/load/remove-document/${sessionId}`, {
      data: { filename }
    });
    return response.data;
  },

  getProcessingStatus: async (sessionId: string): Promise<ProcessingStatus> => {
    const response = await api.get(`/load/processing-status/${sessionId}`);
    return response.data;
  },

  stopProcessing: async (sessionId: string): Promise<{
    status: string;
    message: string;
    stopped: boolean;
    processed_documents?: number;
    total_documents?: number;
    data_rows_saved?: number;
  }> => {
    const response = await api.post(`/load/stop-processing/${sessionId}`);
    return response.data;
  },

  exportData: async (sessionId: string): Promise<Blob> => {
    const response = await api.get(`/load/export/${sessionId}`, {
      responseType: 'blob'
    });
    return response.data;
  },
};

// QBSD API
export const qbsdAPI = {
  configure: async (config: QBSDConfig): Promise<{ session_id: string; message: string }> => {
    const response = await api.post('/qbsd/configure', config);
    return response.data;
  },

  run: async (sessionId: string): Promise<void> => {
    await api.post(`/qbsd/run/${sessionId}`);
  },

  getStatus: async (sessionId: string): Promise<QBSDStatus> => {
    const response = await api.get(`/qbsd/status/${sessionId}`);
    return response.data;
  },

  getSchema: async (sessionId: string): Promise<SchemaData> => {
    const response = await api.get(`/qbsd/schema/${sessionId}`);
    return response.data;
  },

  getData: async (
    sessionId: string,
    page = 0,
    pageSize = 50,
    filters?: FilterRule[],
    sort?: SortColumn[],
    search?: string
  ): Promise<PaginatedData> => {
    // Use POST for filter/sort support
    const response = await api.post(`/qbsd/data/${sessionId}`, {
      filters: filters && filters.length > 0 ? filters : null,
      sort: sort && sort.length > 0 ? sort : null,
      search: search || null
    }, {
      params: { page, page_size: pageSize }
    });
    return response.data;
  },

  stop: async (sessionId: string): Promise<{
    status: string;
    message: string;
    schema_saved: boolean;
    data_rows_saved: number;
  }> => {
    const response = await api.post(`/qbsd/stop/${sessionId}`);
    return response.data;
  },

  listSessions: async (): Promise<VisualizationSession[]> => {
    const response = await api.get('/qbsd/sessions');
    return response.data;
  },

  getDirectories: async (): Promise<{ value: string; label: string }[]> => {
    const response = await api.get('/qbsd/directories');
    return response.data;
  },

  getSchemaFiles: async (): Promise<{
    value: string;
    label: string;
    columns_count: number;
    preview: string;
    columns: {
      name: string;
      definition: string;
      rationale: string;
      allowed_values?: string[];
    }[];
  }[]> => {
    const response = await api.get('/qbsd/schema-files');
    return response.data;
  },

  export: async (sessionId: string): Promise<void> => {
    const response = await api.get(`/qbsd/export/${sessionId}`, { 
      responseType: 'blob' 
    });
    
    // Create download link
    const blob = new Blob([response.data], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `qbsd_data_${sessionId.slice(0, 8)}.csv`;
    link.click();
    window.URL.revokeObjectURL(url);
  },

  updateCell: async (
    sessionId: string,
    rowName: string,
    column: string,
    value: string
  ): Promise<{ status: string; session_id: string; row_name: string; column: string; value: string }> => {
    const response = await api.put(`/qbsd/cell/${sessionId}`, null, {
      params: { row_name: rowName, column, value }
    });
    return response.data;
  },

  /**
   * Estimate cost for QBSD execution for an existing configured session.
   */
  estimateCost: async (sessionId: string): Promise<CostEstimate> => {
    const response = await api.post(`/qbsd/estimate-cost/${sessionId}`);
    return response.data;
  },

  /**
   * Preview cost estimate without saving a session.
   * Useful for getting estimates before committing to a configuration.
   * @param config - The QBSD configuration
   * @param uploadedFiles - Optional array of uploaded file info (name, size) for estimation
   */
  estimateCostPreview: async (
    config: QBSDConfig, 
    uploadedFiles?: Array<{ name: string; size: number }>
  ): Promise<CostEstimate> => {
    const response = await api.post('/qbsd/estimate-cost-preview', { 
      config, 
      uploaded_files: uploadedFiles 
    });
    return response.data;
  },
};

// Common session API
export const sessionAPI = {
  getSession: async (sessionId: string, type: 'load' | 'qbsd'): Promise<VisualizationSession> => {
    if (type === 'load') {
      return loadAPI.getSession(sessionId);
    } else {
      // For QBSD, we need to construct from multiple endpoints
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const _status = await qbsdAPI.getStatus(sessionId);
      // This would need to be implemented based on actual backend structure
      throw new Error('QBSD session details not yet implemented');
    }
  },

  getData: async (
    sessionId: string,
    type: 'load' | 'qbsd',
    page = 0,
    pageSize = 50,
    filters?: FilterRule[],
    sort?: SortColumn[],
    search?: string
  ): Promise<PaginatedData> => {
    if (type === 'load') {
      return loadAPI.getData(sessionId, page, pageSize, filters, sort, search);
    } else {
      return qbsdAPI.getData(sessionId, page, pageSize, filters, sort, search);
    }
  },
};

// Schema editing API
// Cloud Data API (datasets and templates)
export const cloudAPI = {
  // List available datasets
  getDatasets: async (): Promise<{
    name: string;
    path: string;
    file_count: number;
    description?: string;
  }[]> => {
    const response = await api.get('/cloud/datasets');
    return response.data;
  },

  // List files in a dataset
  getDatasetFiles: async (datasetName: string): Promise<{
    name: string;
    path: string;
    size: number;
    content_type?: string;
  }[]> => {
    const response = await api.get(`/cloud/datasets/${encodeURIComponent(datasetName)}/files`);
    return response.data;
  },

  // List available templates
  getTemplates: async (): Promise<{
    name: string;
    path: string;
    file_type: string;
    description?: string;
    row_count?: number;
    column_count?: number;
  }[]> => {
    const response = await api.get('/cloud/templates');
    return response.data;
  },

  // Get template info
  getTemplate: async (templateName: string): Promise<{
    name: string;
    path: string;
    file_type: string;
    description?: string;
    row_count?: number;
    column_count?: number;
  }> => {
    const response = await api.get(`/cloud/templates/${encodeURIComponent(templateName)}`);
    return response.data;
  },

  // List all cloud documents grouped by dataset
  getCloudDocuments: async (): Promise<{
    dataset: string;
    files: {
      name: string;
      path: string;
      size: number;
      content_type?: string;
    }[];
  }[]> => {
    const response = await api.get('/cloud/documents');
    return response.data;
  },

  // Load a template and create a session
  loadTemplate: async (templateName: string): Promise<{
    session_id: string;
    template_name: string;
    status: string;
    message: string;
    row_count: number;
    column_count: number;
  }> => {
    const response = await api.post(`/load/template/${encodeURIComponent(templateName)}`);
    return response.data;
  },

  // Add cloud documents to a session
  addCloudDocuments: async (sessionId: string, dataset: string, files: string[]): Promise<{
    status: string;
    message: string;
    added_files: string[];
    errors?: string[];
  }> => {
    const response = await api.post(`/load/add-cloud-documents/${sessionId}`, {
      dataset,
      files
    });
    return response.data;
  },

  // List available initial schemas from cloud storage
  getInitialSchemas: async (): Promise<{
    name: string;
    path: string;
    file_type: string;
    columns_count: number;
    preview: string;
    columns: {
      name: string;
      definition: string;
      rationale: string;
      allowed_values?: string[];
    }[];
  }[]> => {
    const response = await api.get('/cloud/initial-schemas');
    return response.data;
  },

  // Get specific initial schema
  getInitialSchema: async (schemaName: string): Promise<{
    name: string;
    path: string;
    file_type: string;
    columns_count: number;
    preview: string;
    columns: {
      name: string;
      definition: string;
      rationale: string;
      allowed_values?: string[];
    }[];
  }> => {
    const response = await api.get(`/cloud/initial-schemas/${encodeURIComponent(schemaName)}`);
    return response.data;
  },

  // Upload a new initial schema file
  uploadInitialSchema: async (file: File): Promise<{
    status: string;
    name: string;
    path: string;
    columns_count: number;
  }> => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await api.post('/cloud/initial-schemas/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
};

// Schema editing API
export const schemaAPI = {
  editColumn: async (sessionId: string, request: EditColumnRequest): Promise<SchemaEditResponse> => {
    const response = await api.put(`/schema/edit-column/${sessionId}`, request);
    return response.data;
  },

  deleteColumn: async (sessionId: string, columnName: string): Promise<SchemaEditResponse> => {
    const response = await api.delete(`/schema/delete-column/${sessionId}/${encodeURIComponent(columnName)}`);
    return response.data;
  },

  addColumn: async (sessionId: string, request: AddColumnRequest): Promise<SchemaEditResponse> => {
    const response = await api.post(`/schema/add-column/${sessionId}`, request);
    return response.data;
  },

  mergeColumns: async (sessionId: string, request: MergeColumnsRequest): Promise<SchemaEditResponse> => {
    const response = await api.post(`/schema/merge-columns/${sessionId}`, request);
    return response.data;
  },

  reprocessDocuments: async (sessionId: string, request: ReprocessRequest): Promise<{ success: boolean; message: string; session_id: string; }> => {
    const response = await api.post(`/schema/reprocess/${sessionId}`, request);
    return response.data;
  },

  getReprocessingStatus: async (sessionId: string): Promise<ReprocessingStatus> => {
    const response = await api.get(`/schema/reprocessing-status/${sessionId}`);
    return response.data;
  },

  validateSchema: async (sessionId: string): Promise<SchemaValidationResultType> => {
    const response = await api.get(`/schema/validation/${sessionId}`);
    return response.data;
  },

  backupSchema: async (sessionId: string): Promise<{ success: boolean; backup_id: string; backup_path: string; created_at: string; includes_data: boolean; }> => {
    const response = await api.post(`/schema/backup/${sessionId}`);
    return response.data;
  },

  restoreSchema: async (sessionId: string, backupId: string): Promise<SchemaEditResponse> => {
    const response = await api.post(`/schema/restore/${sessionId}`, { backup_id: backupId });
    return response.data;
  },

  // Schema Evolution / Suggestions API
  getSuggestions: async (sessionId: string): Promise<{
    session_id: string;
    suggestions: Array<{
      column_name: string;
      pending_values?: Array<{
        value: string;
        document_count: number;
        first_seen: string;
        documents: string[];
      }>;
      current_allowed_values?: string[];
      auto_expand_threshold?: number;
      suggested_values?: string[];
      value_details?: Record<string, {
        value: string;
        document_count: number;
        first_seen: string;
        documents: string[];
      }>;
      auto_approved?: boolean;
    }>;
    total_pending: number;
  }> => {
    const response = await api.get(`/schema/suggestions/${sessionId}`);
    return response.data;
  },

  approveSuggestion: async (sessionId: string, columnName: string, value: string): Promise<{ status: string; message: string }> => {
    const response = await api.post(`/schema/approve-suggestion/${sessionId}`, {
      column_name: columnName,
      value: value
    });
    return response.data;
  },

  rejectSuggestion: async (sessionId: string, columnName: string, value: string): Promise<{ status: string; message: string }> => {
    const response = await api.post(`/schema/reject-suggestion/${sessionId}`, {
      column_name: columnName,
      value: value
    });
    return response.data;
  },

  setAutoExpandThreshold: async (sessionId: string, columnName: string, threshold: number): Promise<{ status: string; message: string; threshold: number }> => {
    const response = await api.put(`/schema/auto-expand-threshold/${sessionId}/${encodeURIComponent(columnName)}`, {
      threshold: threshold
    });
    return response.data;
  },

  bulkApproveSuggestions: async (sessionId: string, columnName?: string): Promise<{ status: string; message: string; approved_count: number }> => {
    const params = columnName ? `?column_name=${encodeURIComponent(columnName)}` : '';
    const response = await api.post(`/schema/bulk-approve/${sessionId}${params}`);
    return response.data;
  },

  // Document availability pre-check
  precheckDocuments: async (sessionId: string, request: DocumentAvailabilityRequest): Promise<DocumentAvailabilityResponse> => {
    const response = await api.post(`/schema/precheck-documents/${sessionId}`, request);
    return response.data;
  },

  // Re-extraction API methods
  getSchemaChangeStatus: async (sessionId: string): Promise<SchemaChangeStatus> => {
    const response = await api.get(`/schema/change-status/${sessionId}`);
    return response.data;
  },

  discoverPapers: async (sessionId: string): Promise<PaperDiscoveryResult> => {
    const response = await api.get(`/schema/discover-papers/${sessionId}`);
    return response.data;
  },

  startReextraction: async (sessionId: string, request: ReextractionRequest): Promise<ReextractionResponse> => {
    const response = await api.post(`/schema/reextract/${sessionId}`, request);
    return response.data;
  },

  getReextractionStatus: async (sessionId: string, operationId: string): Promise<ReextractionOperationStatus> => {
    const response = await api.get(`/schema/reextraction-status/${sessionId}/${operationId}`);
    return response.data;
  },

  stopReextraction: async (sessionId: string, operationId: string): Promise<{
    status: string;
    message: string;
    processed_documents: number;
    total_documents: number;
  }> => {
    const response = await api.post(`/schema/stop-reextraction/${sessionId}/${operationId}`);
    return response.data;
  },

  captureBaseline: async (sessionId: string): Promise<{ status: string; message: string; column_count: number }> => {
    const response = await api.post(`/schema/capture-baseline/${sessionId}`);
    return response.data;
  },

  uploadMissingPapers: async (sessionId: string, files: File[]): Promise<{ status: string; message: string; uploaded_files: string[] }> => {
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    const response = await api.post(`/schema/upload-missing-papers/${sessionId}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
    return response.data;
  },

  // Continue Schema Discovery API methods
  continueDiscovery: {
    getDocuments: async (sessionId: string): Promise<ContinueDiscoveryDocuments> => {
      const response = await api.get(`/schema/continue-discovery/documents/${sessionId}`);
      return response.data;
    },

    start: async (sessionId: string, request: ContinueDiscoveryRequest): Promise<ContinueDiscoveryResponse> => {
      const response = await api.post(`/schema/continue-discovery/start/${sessionId}`, request);
      return response.data;
    },

    getStatus: async (sessionId: string, operationId: string): Promise<ContinueDiscoveryStatus> => {
      const response = await api.get(`/schema/continue-discovery/status/${sessionId}/${operationId}`);
      return response.data;
    },

    confirmColumns: async (sessionId: string, operationId: string, request: ConfirmColumnsRequest): Promise<ConfirmColumnsResponse> => {
      const response = await api.post(`/schema/continue-discovery/confirm/${sessionId}/${operationId}`, request);
      return response.data;
    },

    stop: async (sessionId: string, operationId: string): Promise<{ status: string; phase: string; message: string }> => {
      const response = await api.post(`/schema/continue-discovery/stop/${sessionId}/${operationId}`);
      return response.data;
    },
  },
};

// Observation Unit API
export const observationUnitAPI = {
  list: async (sessionId: string): Promise<{
    session_id: string;
    observation_units: Array<{
      unit_name: string;
      document_id?: string;
      confidence?: number;
    }>;
    count: number;
  }> => {
    const response = await api.get(`/observation-unit/list/${sessionId}`);
    return response.data;
  },

  remove: async (sessionId: string, unitName: string): Promise<{
    status: string;
    message: string;
    session_id: string;
    observation_units: Array<{
      unit_name: string;
      document_id?: string;
      confidence?: number;
    }>;
    row_count: number;
  }> => {
    const response = await api.delete(`/observation-unit/remove/${sessionId}`, {
      data: { unit_name: unitName }
    });
    return response.data;
  },

  add: async (sessionId: string, request: {
    unit_name: string;
    document_id?: string;
    relevant_passages?: string[];
    confidence?: number;
  }): Promise<{
    status: string;
    message: string;
    session_id: string;
    observation_units: Array<{
      unit_name: string;
      document_id?: string;
      confidence?: number;
    }>;
    row_count: number;
  }> => {
    const response = await api.post(`/observation-unit/add/${sessionId}`, request);
    return response.data;
  },

  updateDefinition: async (sessionId: string, request: {
    name: string;
    definition: string;
    example_names?: string[];
  }): Promise<{
    status: string;
    message: string;
    observation_unit: {
      name: string;
      definition: string;
      example_names?: string[];
      source_document?: string;
      discovery_iteration?: number;
    };
    warning?: string;
  }> => {
    const response = await api.patch(`/observation-unit/definition/${sessionId}`, request);
    return response.data;
  },

  removeBulk: async (sessionId: string, unitNames: string[]): Promise<{
    status: string;
    message: string;
    session_id: string;
    deleted_count: number;
    failed: string[];
  }> => {
    const response = await api.delete(`/observation-unit/remove-bulk/${sessionId}`, {
      data: { unit_names: unitNames }
    });
    return response.data;
  },
};

// Units API (Observation Unit View and Merge)
export const unitsAPI = {
  /**
   * List all observation units in a session.
   */
  list: async (sessionId: string): Promise<UnitListResponse> => {
    const response = await api.get(`/units/list/${sessionId}`);
    // Convert snake_case to camelCase for frontend
    const data = response.data;
    return {
      units: data.units.map((u: any) => ({
        name: u.name,
        rowCount: u.row_count,
        sourceDocuments: u.source_documents,
        isMerged: u.is_merged,
        originalUnits: u.original_units,
      })),
      totalUnits: data.total_units,
      totalRows: data.total_rows,
    };
  },

  /**
   * Get paginated data optionally filtered by observation unit.
   */
  getData: async (
    sessionId: string,
    options?: {
      unit?: string;
      page?: number;
      pageSize?: number;
    }
  ): Promise<PaginatedData> => {
    const params = new URLSearchParams();
    if (options?.unit) params.append('unit', options.unit);
    if (options?.page !== undefined) params.append('page', options.page.toString());
    if (options?.pageSize !== undefined) params.append('page_size', options.pageSize.toString());

    const response = await api.get(`/units/data/${sessionId}?${params.toString()}`);
    return response.data;
  },

  /**
   * Merge multiple observation units into one.
   */
  merge: async (sessionId: string, request: MergeUnitsRequest): Promise<MergeUnitsResponse> => {
    const response = await api.post(`/units/merge/${sessionId}`, request);
    const data = response.data;
    return {
      success: data.success,
      message: data.message,
      merged_unit: data.merged_unit ? {
        name: data.merged_unit.name,
        rowCount: data.merged_unit.row_count,
        sourceDocuments: data.merged_unit.source_documents,
        isMerged: data.merged_unit.is_merged,
        originalUnits: data.merged_unit.original_units,
      } : undefined,
      rows_affected: data.rows_affected,
    };
  },

  /**
   * Get suggestions for units that could be merged based on name similarity.
   */
  getSuggestions: async (sessionId: string, threshold: number = 0.8): Promise<UnitSuggestionsResponse> => {
    const response = await api.get(`/units/suggestions/${sessionId}`, {
      params: { threshold }
    });
    const data = response.data;
    return {
      suggestions: data.suggestions.map((s: any) => ({
        units: s.units,
        similarity: s.similarity,
        suggestedName: s.suggested_name,
        reason: s.reason,
      })),
      threshold: data.threshold,
    };
  },
};

export default api;