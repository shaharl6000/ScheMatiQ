"""Application constants."""
import os

# API Configuration
API_TITLE = "QBSD Visualization API"
API_DESCRIPTION = """
## Query-Based Schema Discovery (QBSD) API

This API provides endpoints for **Query-Based Schema Discovery** - an approach to automatically
discover table schemas from document collections based on user queries.

### Key Features

- **File Upload & Loading**: Upload CSV, JSON, or JSONL files to load existing data
- **QBSD Pipeline**: Configure and run schema discovery on document collections
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
# Increased from 1024 to support longer schema definitions and observation unit descriptions
DEFAULT_MAX_OUTPUT_TOKENS = 4096
DEFAULT_TEMPERATURE = 0

# Storage Configuration
DEFAULT_SESSIONS_DIR = "./sessions"
DEFAULT_DATA_DIR = "./data"
DEFAULT_QBSD_WORK_DIR = "./qbsd_work"

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
    # Future flags go here, e.g.:
    # "enable_experimental_merging": False,
    # "max_iterations": 5,
}

# Effective values (resolved once at startup)
MAX_DOCUMENTS = int(os.environ.get("MAX_DOCUMENTS", str(
    RELEASE_CONFIG["max_documents"] if not DEVELOPER_MODE else 10_000
)))

# Status Messages
HEALTH_CHECK_MESSAGE = "healthy"
API_ROOT_MESSAGE = "QBSD Visualization API"