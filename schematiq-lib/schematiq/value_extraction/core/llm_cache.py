"""Thread-safe LRU cache for LLM responses."""

import hashlib
import threading
from collections import OrderedDict
from typing import Dict, Any, Optional

from ..config.constants import DEFAULT_CACHE_SIZE, CACHE_EVICTION_BATCH


class LLMCache:
    """Thread-safe LRU cache for LLM responses with size limit."""
    
    def __init__(self, max_size: int = DEFAULT_CACHE_SIZE):
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size
    
    def _get_text_hash(self, text: str) -> str:
        """Generate a hash for text content for caching purposes."""
        return hashlib.md5(text.encode('utf-8')).hexdigest()[:16]
    
    def get_cache_key(self, paper_text: str, column_name: str, mode: str, strict: bool) -> str:
        """Generate cache key for LLM responses."""
        text_hash = self._get_text_hash(paper_text)
        return f"{text_hash}:{column_name}:{mode}:{strict}"
    
    def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Thread-safe cache retrieval."""
        with self._lock:
            if cache_key in self._cache:
                # Move to end (most recently used)
                value = self._cache.pop(cache_key)
                self._cache[cache_key] = value
                return value
            return None
    
    def put(self, cache_key: str, response: Dict[str, Any]) -> None:
        """Thread-safe cache storage with size limit."""
        with self._lock:
            if len(self._cache) >= self._max_size:
                # Remove oldest entries (FIFO)
                for _ in range(CACHE_EVICTION_BATCH):
                    if self._cache:
                        self._cache.popitem(last=False)
            
            # Remove if already exists to update position
            if cache_key in self._cache:
                del self._cache[cache_key]
            
            self._cache[cache_key] = response
    
    def clear(self) -> None:
        """Clear all cached responses."""
        with self._lock:
            self._cache.clear()
    
    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)