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
  SchemaValidationResult as SchemaValidationResultType
} from '../types';

// Use REACT_APP_API_URL for full URL (Railway), otherwise default to relative /api path
const API_BASE = process.env.REACT_APP_API_URL
  ? `${process.env.REACT_APP_API_URL}/api`
  : '/api';

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

  getData: async (sessionId: string, page = 0, pageSize = 50): Promise<PaginatedData> => {
    const response = await api.get(`/load/data/${sessionId}`, {
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

  getData: async (sessionId: string, page = 0, pageSize = 50): Promise<PaginatedData> => {
    const response = await api.get(`/qbsd/data/${sessionId}`, {
      params: { page, page_size: pageSize }
    });
    return response.data;
  },

  stop: async (sessionId: string): Promise<void> => {
    await api.post(`/qbsd/stop/${sessionId}`);
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

  getData: async (sessionId: string, type: 'load' | 'qbsd', page = 0, pageSize = 50): Promise<PaginatedData> => {
    if (type === 'load') {
      return loadAPI.getData(sessionId, page, pageSize);
    } else {
      return qbsdAPI.getData(sessionId, page, pageSize);
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
};

export default api;