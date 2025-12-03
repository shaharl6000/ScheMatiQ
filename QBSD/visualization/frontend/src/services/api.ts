import axios from 'axios';
import { 
  VisualizationSession, 
  PaginatedData, 
  FileValidationResult,
  QBSDConfig,
  QBSDStatus
} from '../types';

const API_BASE = process.env.REACT_APP_API_BASE || 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

// Upload API
export const uploadAPI = {
  uploadFile: async (file: File): Promise<{
    session_id: string;
    validation: FileValidationResult;
    requires_column_mapping: boolean;
  }> => {
    const formData = new FormData();
    formData.append('file', file);
    
    const response = await api.post('/upload/file', formData, {
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

    await api.post(`/upload/parse/${sessionId}`, payload);
  },

  getData: async (sessionId: string, page = 0, pageSize = 50): Promise<PaginatedData> => {
    const response = await api.get(`/upload/data/${sessionId}`, {
      params: { page, page_size: pageSize }
    });
    return response.data;
  },

  getSession: async (sessionId: string): Promise<VisualizationSession> => {
    const response = await api.get(`/upload/sessions/${sessionId}`);
    return response.data;
  },

  listSessions: async (): Promise<VisualizationSession[]> => {
    const response = await api.get('/upload/sessions');
    return response.data;
  },

  deleteSession: async (sessionId: string): Promise<void> => {
    await api.delete(`/upload/sessions/${sessionId}`);
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

  getSchema: async (sessionId: string): Promise<any> => {
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
};

// Common session API
export const sessionAPI = {
  getSession: async (sessionId: string, type: 'upload' | 'qbsd'): Promise<VisualizationSession> => {
    if (type === 'upload') {
      return uploadAPI.getSession(sessionId);
    } else {
      // For QBSD, we need to construct from multiple endpoints
      const status = await qbsdAPI.getStatus(sessionId);
      // This would need to be implemented based on actual backend structure
      throw new Error('QBSD session details not yet implemented');
    }
  },

  getData: async (sessionId: string, type: 'upload' | 'qbsd', page = 0, pageSize = 50): Promise<PaginatedData> => {
    if (type === 'upload') {
      return uploadAPI.getData(sessionId, page, pageSize);
    } else {
      return qbsdAPI.getData(sessionId, page, pageSize);
    }
  },
};

export default api;