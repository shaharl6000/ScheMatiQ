"""Application constants."""
import os

# API Configuration
API_TITLE = "ScheMatiQ API"
API_DESCRIPTION = """
## ScheMatiQ API

This API provides endpoints for **ScheMatiQ** - an approach to automatically
discover table schemas from document collections based on user queries.

### Key Features

- **File Upload & Loading**: Upload CSV, JSON, or JSONL files to load existing data
- **ScheMatiQ Pipeline**: Configure and run schema discovery on document collections
- **Schema Editing**: Interactively edit, merge, add, and delete schema columns
- **Real-time Updates**: WebSocket connections for live progress monitoring
- **Data Export**: Export data in CSV, JSON, or ZIP formats with full metadata

### Authentication

Currently, the API does not require authentication. In production deployments,
configure appropriate authentication middleware.

### Rate Limiting

No rate limiting is currently enforced. Consider implementing rate limits for production use.

### WebSocket Endpoints

The API includes WebSocket endpoints for real-time updates:
- `/ws/progress/{session_id}` - Progress updates during processing
- `/ws/logs/{session_id}` - Real-time log streaming

### Contact & Support

For issues and feature requests, visit the project repository.
"""
API_VERSION = "1.0.0"

# Server Configuration
DEFAULT_HOST = os.environ.get("HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("PORT", 8000))

# CORS Configuration - supports comma-separated origins via ALLOWED_ORIGINS env var
_default_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://querydiscovery-production.up.railway.app",  # Production frontend
]
_env_origins = os.environ.get("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = (
    [origin.strip() for origin in _env_origins.split(",") if origin.strip()]
    if _env_origins
    else _default_origins
)

# WebSocket Configuration
WEBSOCKET_RECONNECT_ATTEMPTS = 5
WEBSOCKET_CHECK_INTERVAL = 3000  # milliseconds
PROGRESS_CHECK_INTERVAL = 2  # seconds

# File Upload Configuration
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB in bytes
ALLOWED_FILE_EXTENSIONS = ['.csv', '.json', '.txt', '.md', '.pdf']

# Data Processing Configuration
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
DEFAULT_RETRIEVAL_K = 8
DEFAULT_TIMEOUT = 120  # seconds

# LLM Configuration
# max_output_tokens is auto-detected per model from schematiq.core.model_specs.
# Do NOT hardcode a default here — each model has its own limit.
DEFAULT_TEMPERATURE = 0

# Storage Configuration
DEFAULT_SESSIONS_DIR = "./sessions"
DEFAULT_DATA_DIR = "./data"
DEFAULT_SCHEMATIQ_WORK_DIR = "./schematiq_work"

# Storage Backend Selection
# Options: "local" (default) or "supabase"
STORAGE_BACKEND = os.environ.get("STORAGE_BACKEND", "local")

# Supabase Configuration (required if STORAGE_BACKEND=supabase)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# ── Release Mode vs Developer Mode ──────────────────────────────
# Default: release mode (restricted). Set DEVELOPER_MODE=true to unlock.
DEVELOPER_MODE = os.environ.get("DEVELOPER_MODE", "false").lower() == "true"

# All mode-dependent feature flags live here.
# To add a new release restriction, add a key with its release-mode default.
RELEASE_CONFIG = {
    "max_documents": 20,           # release mode cap
    # LLM configuration for release mode (locked to Gemini)
    "schema_creation_model": "gemini-2.5-flash",
    "value_extraction_model": "gemini-2.5-flash-lite",
    "llm_provider": "gemini",
    "llm_temperature": 0,
}

# Convenience: whether LLM config UI should be shown
ALLOW_LLM_CONFIG = DEVELOPER_MODE

# Effective values (resolved once at startup)
MAX_DOCUMENTS = int(os.environ.get("MAX_DOCUMENTS", str(
    RELEASE_CONFIG["max_documents"] if not DEVELOPER_MODE else 10_000
)))

# ── Research Data Collection (Google Drive) ──────────────────────
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")
GOOGLE_OAUTH_CREDENTIALS_JSON = os.environ.get("GOOGLE_OAUTH_CREDENTIALS_JSON", "")
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_SHEETS_SPREADSHEET_ID = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GOOGLE_SHEETS_LLM_USAGE_ID = os.environ.get("GOOGLE_SHEETS_LLM_USAGE_ID", "")
DATA_COLLECTION_ENABLED = (
    not DEVELOPER_MODE
    and bool(GOOGLE_DRIVE_FOLDER_ID)
    and bool(GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_OAUTH_CREDENTIALS_JSON)
)

# ── LLM Call Quota ───────────────────────────────────────────────
# Global limit on cumulative LLM API calls across all sessions.
# Always enforced in release mode. In developer mode the quota is bypassed.
# Set to 0 to allow unlimited calls (no quota enforced).
LLM_CALL_GLOBAL_LIMIT = int(os.environ.get("LLM_CALL_GLOBAL_LIMIT", "20"))

# ── Quota Alert Email ────────────────────────────────────────────
# Send an email when the LLM quota is exceeded.
# Uses the same Google OAuth credentials as Google Sheets (no extra passwords).
# Set ALERT_EMAIL_TO to enable; leave empty to disable.
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")          # recipient(s), comma-separated

# ── Concurrency Configuration ────────────────────────────────────
MAX_CONCURRENT_SESSIONS = int(os.environ.get("MAX_CONCURRENT_SESSIONS", "5"))
SCHEMATIQ_THREAD_POOL_SIZE = int(os.environ.get(
    "SCHEMATIQ_THREAD_POOL_SIZE",
    os.environ.get("QBSD_THREAD_POOL_SIZE", "6"),
))

# Status Messages
HEALTH_CHECK_MESSAGE = "healthy"
API_ROOT_MESSAGE = "ScheMatiQ API"