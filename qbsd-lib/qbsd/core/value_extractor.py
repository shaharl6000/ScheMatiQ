"""
Value extraction for QBSD - Refactored version.

This file imports from the new refactored structure.
The original 860-line monolithic file has been split into focused modules:

- value_extraction/core/ - Core classes (JSONResponseParser, LLMCache, PaperProcessor, etc.)
- value_extraction/utils/ - Utility functions (text processing, prompt building)
- value_extraction/config/ - Configuration and constants

FIXED: Resume logic now preserves existing rows exactly as they were,
only adding new rows without modifying existing data.
"""

from pathlib import Path

# Import the main functions from the refactored structure
from value_extraction.main import build_table_jsonl, main

# Import core classes for direct access
from value_extraction.core.table_builder import TableBuilder
from value_extraction.core.paper_processor import PaperProcessor
from value_extraction.core.json_parser import JSONResponseParser
from value_extraction.core.llm_cache import LLMCache
from value_extraction.core.row_manager import RowDataManager

# Import utility classes
from value_extraction.utils.text_processing import TextProcessor
from value_extraction.utils.prompt_builder import PromptBuilder

# Import configuration
from value_extraction.config.constants import *
from value_extraction.config.prompts import SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_VAL_STRICT

# Export main functions and classes
__all__ = [
    'build_table_jsonl',
    'main',
    'TableBuilder',
    'PaperProcessor', 
    'JSONResponseParser',
    'LLMCache',
    'RowDataManager',
    'TextProcessor',
    'PromptBuilder',
    'SYSTEM_PROMPT_VAL',
    'SYSTEM_PROMPT_VAL_STRICT',
]

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])
    else:
        config_path = Path("configurations/valueExtractionConfig.json")
    
    main(config_path)