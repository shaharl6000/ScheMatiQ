"""Constants for value extraction processing."""

# Caching constants
DEFAULT_CACHE_SIZE = 1000
CACHE_EVICTION_BATCH = 100

# Processing constants  
MIN_DOCUMENT_SIZE_FOR_SNIPPETS = 1000
DEFAULT_RETRIEVAL_K = 8
FALLBACK_EXPANDED_K_MULTIPLIER = 2
MAX_SNIPPETS = 8

# Prompt safety margins
SAFETY_MARGIN_ALL_MODE = 512
SAFETY_MARGIN_SINGLE_MODE = 256

# Default processing settings
DEFAULT_MAX_NEW_TOKENS = 512
DEFAULT_MAX_WORKERS = 3