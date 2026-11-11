/**
 * Application constants for the frontend
 */

// API Configuration
// For Railway deployment: Set REACT_APP_API_URL to backend service URL (e.g., https://backend.up.railway.app)
export const API_BASE_URL = process.env.REACT_APP_API_URL || '';

// WebSocket Configuration - derives from API URL or uses current host
// For Railway: Set REACT_APP_WS_URL to backend WebSocket URL (e.g., wss://backend.up.railway.app)
const getWebSocketBaseUrl = (): string => {
  // Explicit WS URL takes priority
  if (process.env.REACT_APP_WS_URL) {
    return process.env.REACT_APP_WS_URL;
  }
  // Derive from API URL if set (build-time env var)
  if (process.env.REACT_APP_API_URL) {
    const apiUrl = process.env.REACT_APP_API_URL;
    return apiUrl.replace(/^http/, 'ws');
  }
  // Derive from API_BASE_URL if it's set (runtime check)
  if (API_BASE_URL) {
    return API_BASE_URL.replace(/^http/, 'ws');
  }
  // Default: use current host (works with proxy in development)
  return (window.location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + window.location.host;
};
export const WS_BASE_URL = getWebSocketBaseUrl();

// UI Configuration
export const AVAILABLE_PAGE_SIZES = [25, 50, 100, 200, 500, 1000];

// WebSocket Configuration
export const WS_RECONNECT_ATTEMPTS = 5;
export const WS_RECONNECT_DELAY_BASE = 1000; // milliseconds
export const WS_RECONNECT_MAX_DELAY = 10000; // milliseconds

// Data Refresh Intervals
export const SCHEMATIQ_REFRESH_INTERVAL = 3000; // milliseconds for ScheMatiQ data polling
export const PROCESSING_REFRESH_INTERVAL = 2000; // milliseconds for processing status
export const WS_DISCONNECTED_REFRESH_INTERVAL = 20000; // fallback poll when WebSocket is down

// File Upload Configuration
export const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB in bytes

// ScheMatiQ Configuration Defaults
export const DEFAULT_MAX_KEYS_SCHEMA = 25;
export const DEFAULT_DOCUMENTS_BATCH_SIZE = 1;
export const DEFAULT_DOCUMENT_RANDOMIZATION_SEED = 42;
export const DEFAULT_CONVERGENCE_THRESHOLD = 5;

// Document Upload Limits (Public Release)
export const DEFAULT_MAX_DOCUMENTS = 120;
export const ALLOWED_FILE_TYPES = {
  'text/csv': ['.csv'],
  'application/json': ['.json'],
  'text/plain': ['.txt'],
  'text/markdown': ['.md'],
  'application/pdf': ['.pdf'],
  'application/vnd.ms-excel': ['.xls'],
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx']
};

// UI Timing
export const NEW_ROW_HIGHLIGHT_DURATION = 5000; // milliseconds

// LLM Model Definitions
export * from './llmModels';