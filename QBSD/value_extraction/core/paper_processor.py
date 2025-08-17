"""Paper processing for value extraction."""

from pathlib import Path
from typing import Dict, Any, Set
from schema import Schema, Column
from llm_backends import LLMInterface
import utils

from .llm_cache import LLMCache
from .json_parser import JSONResponseParser
from ..utils.text_processing import TextProcessor
from ..utils.prompt_builder import PromptBuilder
from ..config.constants import (
    MIN_DOCUMENT_SIZE_FOR_SNIPPETS, 
    SAFETY_MARGIN_ALL_MODE, 
    SAFETY_MARGIN_SINGLE_MODE
)


class PaperProcessor:
    """Handles value extraction from individual papers."""
    
    def __init__(self, llm: LLMInterface, cache: LLMCache = None, retriever=None):
        self.llm = llm
        self.cache = cache or LLMCache()
        self.retriever = retriever
        self.json_parser = JSONResponseParser()
        self.text_processor = TextProcessor()
        self.prompt_builder = PromptBuilder()
    
    def extract_values_for_paper(self,
                                paper_title: str,
                                paper_text: str,
                                schema: Schema,
                                max_new_tokens: int,
                                mode: str = "all",
                                retrieval_k: int = 8) -> Dict[str, Any]:
        """Extract values from a single paper for the given schema."""
        if mode == "one":
            mode = "one_by_one"

        def _retrieve_effective_text(columns_for_query=None, k=None) -> str:
            if self.retriever is None:
                return paper_text
            try:
                retrieval_query = self.text_processor.build_retrieval_query(schema, columns_for_query)
                passages = self.retriever.query([paper_text], retrieval_query, k=(k or retrieval_k))
                if passages:
                    print(f"📖 Retrieved {len(passages)} relevant passages for {paper_title}")
                    return "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
                else:
                    print(f"⚠️  No relevant passages found for {paper_title}, using full text")
                    return paper_text
            except Exception as e:
                print(f"⚠️  Retrieval failed for {paper_title}: {e}, using full text")
                return paper_text

        def _single_column_attempt(col: Column,
                                  strict: bool,
                                  k_override: int | None,
                                  use_snippets: bool = False) -> Dict[str, Any]:
            # prepare text
            if self.retriever is not None:
                eff = _retrieve_effective_text([col], k=k_override)
            else:
                if use_snippets:
                    keywords = self.text_processor.keywords_for_column(col)
                    eff = self.text_processor.heuristic_snippets(paper_text, keywords)
                else:
                    eff = paper_text

            # Check cache first
            cache_key = self.cache.get_cache_key(eff, col.name, "one_by_one", strict)
            cached_result = self.cache.get(cache_key)
            if cached_result is not None:
                return cached_result

            msgs = self.prompt_builder.build_val_messages(
                schema.query, paper_title, eff, [col.to_dict()],
                mode="one_by_one", strict=strict
            )
            trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, 
                                     safety_margins=SAFETY_MARGIN_SINGLE_MODE)
            raw = self.llm.generate(trimmed)
            try:
                parsed = self.json_parser.parse_response(raw)
                cleaned = self.json_parser.postprocess(parsed, [col.name])
                result = cleaned.get(col.name, {})
                
                # Cache the result
                self.cache.put(cache_key, result)
                return result
            except Exception as e:
                print(f"⚠️  parse failure for column {col.name} in {paper_title}: {e}")
                return {}

        if mode == "all":
            # joint retrieval + one call
            eff = _retrieve_effective_text(list(schema.columns))
            msgs = self.prompt_builder.build_val_messages(
                schema.query, paper_title, eff, [c.to_dict() for c in schema.columns],
                mode="all", strict=False
            )
            trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, 
                                     safety_margins=SAFETY_MARGIN_ALL_MODE)
            raw = self.llm.generate(trimmed)
            try:
                parsed = self.json_parser.parse_response(raw)
            except Exception as e:
                print(f"⚠️  parse failure for {paper_title}: {e}")
                parsed = {}

            requested = [c.name for c in schema.columns]
            cleaned = self.json_parser.postprocess(parsed, requested)

            # Fallback for missing columns: retry per-column, stricter + expanded retrieval
            missing = [c for c in schema.columns if c.name not in cleaned]
            if missing:
                print(f"↻ Fallback per-column for {len(missing)} missing: {[c.name for c in missing]}")
            for col in missing:
                # First fallback: expanded k + strict prompt
                expanded_k = self.text_processor.expand_k(retrieval_k)
                col_res = _single_column_attempt(col, strict=True, k_override=expanded_k, use_snippets=False)
                if not col_res:
                    # Second fallback: heuristic snippets (no retriever) or even stricter evidence demand
                    col_res = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
                if col_res:
                    cleaned[col.name] = col_res

            return cleaned

        # mode == "one_by_one" with optimized fallback logic
        row: Dict[str, Any] = {}
        for col in schema.columns:
            # Attempt 1: normal rules, per-column retrieval
            first = _single_column_attempt(col, strict=False, k_override=None, use_snippets=False)

            if first and first.get('answer', '').strip():
                row[col.name] = first
                continue

            # Attempt 2: Only try expanded retrieval if we have a retriever and got empty result
            if self.retriever is not None:
                expanded_k = self.text_processor.expand_k(retrieval_k)
                second = _single_column_attempt(col, strict=True, k_override=expanded_k, use_snippets=False)
                if second and second.get('answer', '').strip():
                    row[col.name] = second
                    continue

            # Attempt 3: Only if previous attempts truly failed and we have substantial text
            if len(paper_text) > MIN_DOCUMENT_SIZE_FOR_SNIPPETS:  # Only for reasonably sized documents
                third = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
                if third and third.get('answer', '').strip():
                    row[col.name] = third

        return row
    
    def process_single_paper(self,
                            doc_path: Path,
                            schema: Schema,
                            max_new_tokens: int,
                            mode: str,
                            retrieval_k: int,
                            processed_papers: Set[str]) -> tuple[str, str, Dict[str, Any] | None]:
        """
        Process a single paper and return (row_name, paper_title, extracted_data).
        Returns None for extracted_data if paper should be skipped or fails.
        """
        from .row_manager import RowDataManager
        row_manager = RowDataManager()
        
        paper_title = doc_path.stem
        row_name = row_manager.extract_row_name_from_filename(doc_path.name)
        
        # Skip if already processed
        if paper_title in processed_papers:
            return row_name, paper_title, None
        
        try:
            paper_text = doc_path.read_text(encoding="utf-8", errors="ignore")
            print(f"🔍 Extracting values for {paper_title} (row: {row_name})...")

            # Extract values for this paper
            paper_data = self.extract_values_for_paper(
                paper_title, paper_text, schema, max_new_tokens, mode, retrieval_k
            )
            
            return row_name, paper_title, paper_data
            
        except Exception as e:
            print(f"⚠️  Error processing {paper_title}: {e}")
            return row_name, paper_title, None