"""Text processing utilities for value extraction."""

import re
from typing import List
from schematiq.core.schema import Schema, Column

from ..config.constants import MAX_SNIPPETS


class TextProcessor:
    """Handles text processing operations for value extraction."""
    
    def __init__(self):
        pass
    
    def build_retrieval_query(self, schema: Schema, columns: List[Column] = None) -> str:
        """
        Build an effective retrieval query combining the main schema query 
        with specific column information for targeted passage retrieval.
        """
        base_query = schema.query
        
        if columns:
            # Add column-specific context for more targeted retrieval
            column_context = " ".join([
                f"Information about {col.name}: {col.definition}" 
                for col in columns
            ])
            return f"{base_query} {column_context}"
        
        return base_query
    
    def expand_k(self, k: int) -> int:
        """Expand retrieval k value for fallback attempts."""
        return min(max(2 * k, 8), 24)
    
    def heuristic_snippets(self, text: str, keywords: List[str], 
                          max_snippets: int = MAX_SNIPPETS) -> str:
        """Pick top paragraphs by keyword hits as a poor-man's retrieval."""
        paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        if not paras:
            return text
        keys = {w.lower() for w in keywords if len(w) > 2}
        
        def score(p: str) -> int:
            tokens = re.findall(r"[A-Za-z0-9_]+", p.lower())
            return sum(t in keys for t in tokens)
        
        ranked = sorted(paras, key=score, reverse=True)[:max_snippets]
        return "\n\n--- RELEVANT PASSAGE ---\n\n".join(ranked)
    
    def keywords_for_column(self, col: Column) -> List[str]:
        """Extract keywords from column name and definition."""
        base = f"{col.name} {col.definition}"
        return re.findall(r"[A-Za-z0-9_]+", base)