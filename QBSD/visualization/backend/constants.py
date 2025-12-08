"""Application constants."""

# API Configuration
API_TITLE = "QBSD Visualization API"
API_DESCRIPTION = "Interactive visualization and schema editing for QBSD"
API_VERSION = "1.0.0"

# Server Configuration
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000

# CORS Configuration
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]

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
DEFAULT_MAX_TOKENS = 1024
DEFAULT_TEMPERATURE = 0.3

# Storage Configuration
DEFAULT_SESSIONS_DIR = "./sessions"
DEFAULT_DATA_DIR = "./data"
DEFAULT_QBSD_WORK_DIR = "./qbsd_work"

# Status Messages
HEALTH_CHECK_MESSAGE = "healthy"
API_ROOT_MESSAGE = "QBSD Visualization API"