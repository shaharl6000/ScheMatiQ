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
export const DEFAULT_PAGE_SIZE = 50;
export const AVAILABLE_PAGE_SIZES = [25, 50, 100, 200];
export const DEFAULT_DEBOUNCE_DELAY = 300; // milliseconds

// WebSocket Configuration
export const WS_RECONNECT_ATTEMPTS = 5;
export const WS_RECONNECT_DELAY_BASE = 1000; // milliseconds
export const WS_RECONNECT_MAX_DELAY = 10000; // milliseconds

// Data Refresh Intervals
export const SCHEMATIQ_REFRESH_INTERVAL = 3000; // milliseconds for ScheMatiQ data polling
export const PROCESSING_REFRESH_INTERVAL = 2000; // milliseconds for processing status
export const SESSION_REFRESH_INTERVAL = 3000; // milliseconds

// File Upload Configuration
export const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB in bytes

// ScheMatiQ Configuration Defaults
export const DEFAULT_MAX_KEYS_SCHEMA = 25;
export const DEFAULT_DOCUMENTS_BATCH_SIZE = 1;
export const DEFAULT_DOCUMENT_RANDOMIZATION_SEED = 42;

// Document Upload Limits (Public Release)
export const DEFAULT_MAX_DOCUMENTS = 40;
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
export const NOTIFICATION_AUTO_HIDE_DURATION = 6000; // milliseconds

// Text Limits  
export const MAX_CELL_TEXT_LENGTH = 200;
export const MAX_CELL_LINES = 5; // maximum number of lines in a cell
export const LONG_TEXT_THRESHOLD = 300; // characters
export const MEDIUM_TEXT_THRESHOLD = 150; // characters
export const SHORT_TEXT_THRESHOLD = 300; // characters
export const MAX_PREVIEW_ROWS = 10;
export const MAX_SEARCH_RESULTS = 100;

// Animation Durations
export const FADE_ANIMATION_DURATION = 300; // milliseconds
export const SLIDE_ANIMATION_DURATION = 250; // milliseconds

// Component Sizes
export const FROZEN_COLUMN_WIDTH = 220; // pixels for frozen columns
export const REGULAR_COLUMN_WIDTH = 200; // pixels for regular columns  
export const MIN_COLUMN_WIDTH = 150; // pixels
export const MAX_COLUMN_WIDTH = 400; // pixels
export const TABLE_MAX_HEIGHT = 600; // pixels
export const TABLE_MIN_WIDTH = 800; // pixels
export const SEARCH_FIELD_WIDTH = 300; // pixels
export const TABLE_ROW_MAX_HEIGHT = 120; // pixels

// Status Messages
export const STATUS_MESSAGES = {
  NO_DATA: 'No data available',
  LOADING: 'Loading...',
  ERROR: 'An error occurred',
  SUCCESS: 'Operation completed successfully',
  PROCESSING: 'Processing...',
  LOADING_FILES: 'Loading files...',
  EXTRACTING: 'Extracting schema...',
  ANALYZING: 'Analyzing documents...'
} as const;

// Route Paths
export const ROUTES = {
  HOME: '/',
  LOAD: '/load',
  SCHEMATIQ: '/schematiq',
  VISUALIZE: '/visualize'
} as const;

// LocalStorage Keys
export const STORAGE_KEYS = {
  RECENT_SESSIONS: 'schematiq_recent_sessions',
  USER_PREFERENCES: 'schematiq_user_preferences',
  TABLE_SETTINGS: 'schematiq_table_settings'
} as const;

// LLM Model Definitions
export * from './llmModels';