"""Constants for value extraction processing."""

# Caching constants
DEFAULT_CACHE_SIZE = 1000
CACHE_EVICTION_BATCH = 100

# Processing constants
MIN_DOCUMENT_SIZE_FOR_SNIPPETS = 1000
DEFAULT_RETRIEVAL_K = 8
MAX_SNIPPETS = 8

# When True, skip retrieval and send the full document to the LLM, disabling
# chunking/passage-selection. Single source of truth for this mode: imported by
# both paper_processor (runtime behavior) and prompts (which prompt variant to use).
DISABLE_RETRIEVER = True

# Prompt safety margins
SAFETY_MARGIN_ALL_MODE = 512
SAFETY_MARGIN_SINGLE_MODE = 256

# Default processing settings
DEFAULT_MAX_NEW_TOKENS = 8192
DEFAULT_MAX_WORKERS = 3