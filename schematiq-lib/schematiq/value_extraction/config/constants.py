"""Constants for value extraction processing."""

import os

# Caching constants
DEFAULT_CACHE_SIZE = 1000
CACHE_EVICTION_BATCH = 100

# Processing constants
MIN_DOCUMENT_SIZE_FOR_SNIPPETS = 1000
DEFAULT_RETRIEVAL_K = 8
FALLBACK_EXPANDED_K_MULTIPLIER = 2
MAX_SNIPPETS = 8

# When True, skip retrieval and send the full document to the LLM, disabling
# chunking/passage-selection. Single source of truth for this mode: imported by
# both paper_processor (runtime behavior) and prompts (which prompt variant to use).
DISABLE_RETRIEVER = True

# Value-extraction prompt layout (issue #547). Read from the environment so an
# evaluation can run both layouts from the same commit.
#   "document_first" (default): the document opens every extraction prompt and
#       everything that varies per call follows it, so calls on one document
#       share a prefix that Gemini implicit caching can reuse. No explicit cache.
#   "legacy": the layout before #547, unchanged: question first, document
#       inline, plus a Gemini explicit context cache that holds the document
#       again (so it is sent twice). Kept for A/B comparison only.
PROMPT_LAYOUT_DOCUMENT_FIRST = "document_first"
PROMPT_LAYOUT_LEGACY = "legacy"
PROMPT_LAYOUT = (
    os.environ.get("SCHEMATIQ_PROMPT_LAYOUT", PROMPT_LAYOUT_DOCUMENT_FIRST).strip().lower()
)
if PROMPT_LAYOUT not in (PROMPT_LAYOUT_DOCUMENT_FIRST, PROMPT_LAYOUT_LEGACY):
    raise ValueError(
        f"SCHEMATIQ_PROMPT_LAYOUT must be '{PROMPT_LAYOUT_DOCUMENT_FIRST}' or "
        f"'{PROMPT_LAYOUT_LEGACY}', got {PROMPT_LAYOUT!r}"
    )
USE_EXPLICIT_CONTEXT_CACHE = PROMPT_LAYOUT == PROMPT_LAYOUT_LEGACY

# Set SCHEMATIQ_DISABLE_LLM_CACHE=1 to bypass the in-process LLM response cache
# (LLMCache), so repeated runs in one process always call the model. Used for
# evaluation runs that measure run-to-run noise.
DISABLE_LLM_RESPONSE_CACHE = (
    os.environ.get("SCHEMATIQ_DISABLE_LLM_CACHE", "").strip().lower() in ("1", "true", "yes")
)

# Prompt safety margins
SAFETY_MARGIN_ALL_MODE = 512
SAFETY_MARGIN_SINGLE_MODE = 256

# Default processing settings
DEFAULT_MAX_NEW_TOKENS = 8192
DEFAULT_MAX_WORKERS = 3

# LLM Backend defaults
from schematiq.core.model_specs import ModelNames
DEFAULT_GEMINI_MODEL = ModelNames.DEFAULT_VALUE_EXTRACTION
DEFAULT_GEMINI_ROTATION_REQUESTS = 5
EXHAUSTED_KEY_RESET_HOURS = 24  # RPD resets daily