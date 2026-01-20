"""Paper processing for value extraction."""

from pathlib import Path
from typing import Dict, Any, Set, Callable, Optional, List, Iterator
from qbsd.core.schema import Schema, Column
from qbsd.core.llm_backends import LLMInterface
from qbsd.core import utils

from .llm_cache import LLMCache
from .json_parser import JSONResponseParser
from ..utils.text_processing import TextProcessor
from ..utils.prompt_builder import PromptBuilder
from ..config.constants import (
    MIN_DOCUMENT_SIZE_FOR_SNIPPETS,
    SAFETY_MARGIN_ALL_MODE,
    SAFETY_MARGIN_SINGLE_MODE
)


def _chunk_list(lst: List, size: int) -> Iterator[List]:
    """Split list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# Default batch size for fallback column extraction
FALLBACK_BATCH_SIZE = 3

# Type alias for value extracted callback: (row_name, column_name, value) -> None
OnValueExtractedCallback = Callable[[str, str, Any], None]

# Type alias for should_stop callback: () -> bool
ShouldStopCallback = Callable[[], bool]


class StopRequestedException(Exception):
    """Raised when a stop has been requested during extraction."""
    pass


class PaperProcessor:
    """Handles value extraction from individual papers."""

    def __init__(self, llm: LLMInterface, cache: LLMCache = None, retriever=None,
                 on_value_extracted: Optional[OnValueExtractedCallback] = None,
                 should_stop: Optional[ShouldStopCallback] = None):
        self.llm = llm
        self.cache = cache or LLMCache()
        self.retriever = retriever
        self.json_parser = JSONResponseParser()
        self.text_processor = TextProcessor()
        self.prompt_builder = PromptBuilder()
        self.on_value_extracted = on_value_extracted
        self.should_stop = should_stop
        # Schema evolution tracking: {column_name: {value: [list of documents]}}
        self.suggested_values: Dict[str, Dict[str, list]] = {}
        # Cache for fallback retriever (created on-demand, reused across papers)
        self._cached_fallback_retriever = None

    def _check_stop_requested(self) -> bool:
        """Check if stop was requested. Returns True if should stop."""
        if self.should_stop and self.should_stop():
            return True
        return False

    def _track_unmatched_values(self, unmatched: Dict[str, list], document_name: str = None):
        """Track unmatched values for schema evolution suggestions."""
        for col_name, values in unmatched.items():
            if col_name not in self.suggested_values:
                self.suggested_values[col_name] = {}
            for value in values:
                if value not in self.suggested_values[col_name]:
                    self.suggested_values[col_name][value] = []
                if document_name and document_name not in self.suggested_values[col_name][value]:
                    self.suggested_values[col_name][value].append(document_name)

    def get_suggested_values(self, threshold: int = 2) -> Dict[str, Dict[str, Any]]:
        """
        Return values that appear in threshold+ documents.

        Returns:
            Dict mapping column_name to dict of {value: {"count": N, "documents": [...]}}
        """
        result = {}
        for col_name, values in self.suggested_values.items():
            qualified_values = {}
            for value, documents in values.items():
                count = len(documents)
                if count >= threshold:
                    qualified_values[value] = {
                        "count": count,
                        "documents": documents
                    }
            if qualified_values:
                result[col_name] = qualified_values
        return result

    def get_all_suggested_values(self) -> Dict[str, Dict[str, Any]]:
        """Return all suggested values with their document counts (regardless of threshold)."""
        result = {}
        for col_name, values in self.suggested_values.items():
            result[col_name] = {
                value: {"count": len(documents), "documents": documents}
                for value, documents in values.items()
            }
        return result

    def clear_suggested_values(self):
        """Clear all tracked suggested values."""
        self.suggested_values = {}

    def _notify_value_extracted(self, row_name: str, column_name: str, value: Any):
        """Call the on_value_extracted callback if set."""
        print(f"🔔 _notify_value_extracted called: row={row_name}, col={column_name}, callback={'set' if self.on_value_extracted else 'NOT SET'}")
        if self.on_value_extracted:
            try:
                self.on_value_extracted(row_name, column_name, value)
            except Exception as e:
                print(f"⚠️  Callback error for {column_name}: {e}")
        else:
            print(f"⚠️  on_value_extracted callback is NOT SET - cell streaming disabled")

    def _attach_source_to_excerpts(self, data: Dict[str, Any], source_filename: str) -> Dict[str, Any]:
        """Attach source filename to each excerpt in the extracted data.

        Converts plain excerpt strings to objects with source info:
        {"text": "excerpt text", "source": "filename.txt"}
        """
        for col_name, col_value in data.items():
            if isinstance(col_value, dict) and 'excerpts' in col_value:
                excerpts = col_value.get('excerpts', [])
                col_value['excerpts'] = [
                    {"text": exc, "source": source_filename} if isinstance(exc, str) else exc
                    for exc in excerpts
                ]
        return data

    def _should_skip_truncation(self) -> bool:
        """Check if this LLM supports long context and should skip truncation."""
        # Check for Gemini or other long-context models
        model_name = getattr(self.llm, 'model', '').lower()
        provider = getattr(self.llm, '__class__', None)
        
        # Skip truncation for Gemini models or models with "long" in the name
        if provider and 'gemini' in provider.__name__.lower():
            return True
        if 'gemini' in model_name or 'long' in model_name:
            return True
        
        # Check if no retriever is used (indicating long context mode)
        if self.retriever is None:
            return True
            
        return False
    
    def _create_fallback_retriever(self):
        """Create a default retriever for fallback when in long context mode."""
        try:
            # Use default retriever configuration for fallback
            fallback_config = {
                "type": "embedding",
                "model_name": "all-MiniLM-L6-v2",  # Fast, lightweight model
                "k": 8,  # Default retrieval count
                "max_words": 512,
                "batch_size": 32,
                "enable_dynamic_k": True,
                "dynamic_k_threshold": 0.65,
                "dynamic_k_minimum": 2
            }
            return utils.build_retriever(fallback_config)
        except Exception as e:
            print(f"⚠️  Failed to create fallback retriever: {e}")
            return None
    
    def _single_column_attempt_with_retriever(self, 
                                            col: Column,
                                            strict: bool,
                                            k_override: int | None,
                                            retriever,
                                            paper_text: str,
                                            schema: Schema,
                                            paper_title: str,
                                            use_snippets: bool = False) -> Dict[str, Any]:
        """Single column extraction attempt with explicit retriever control."""
        # Prepare text based on retriever availability
        if retriever is not None:
            try:
                retrieval_query = self.text_processor.build_retrieval_query(schema, [col])
                passages = retriever.query([paper_text], retrieval_query, k=(k_override or 8))
                if passages:
                    eff = "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
                else:
                    eff = paper_text
            except Exception as e:
                print(f"⚠️  Fallback retrieval failed for {col.name}: {e}, using full text")
                eff = paper_text
        else:
            if use_snippets:
                keywords = self.text_processor.keywords_for_column(col)
                eff = self.text_processor.heuristic_snippets(paper_text, keywords)
            else:
                eff = paper_text

        # Check cache first
        cache_key = self.cache.get_cache_key(eff, col.name, "fallback", strict)
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        # Build and execute LLM call
        msgs = self.prompt_builder.build_val_messages(
            schema.query, paper_title, eff, [col.to_dict()],
            mode="one_by_one", strict=strict
        )
        
        # Skip truncation for long context models
        should_truncate = not self._should_skip_truncation()
        max_ctx = getattr(self.llm, 'context_window_size', 8192) if hasattr(self.llm, 'context_window_size') else 8192
        trimmed = utils.fit_prompt(msgs, truncate=should_truncate, max_new=512,
                                 safety_margins=SAFETY_MARGIN_SINGLE_MODE,
                                 context_window_size=max_ctx)
        raw = self.llm.generate(trimmed)
        # Check stop immediately after LLM call returns
        if self._check_stop_requested():
            print(f"🛑 Stop requested after LLM call, returning empty")
            return {}
        try:
            parsed = self.json_parser.parse_response(raw)
            # Build allowed_values dict for postprocessing
            column_allowed_values = {col.name: col.allowed_values} if col.allowed_values else {}
            cleaned, unmatched = self.json_parser.postprocess(parsed, [col.name], column_allowed_values)
            result = cleaned.get(col.name, {})

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            # Cache the result
            self.cache.put(cache_key, result)
            return result
        except Exception as e:
            print(f"⚠️  parse failure for column {col.name}: {e}")
            return {}

    def _batch_column_attempt(self,
                              columns: List[Column],
                              retriever,
                              paper_text: str,
                              schema: Schema,
                              paper_title: str,
                              base_k: int = 8) -> Dict[str, Any]:
        """Extract multiple columns in a single LLM call with combined retrieval.

        Args:
            columns: List of columns to extract together (2-4 columns typically)
            retriever: Retriever to use (can be None for snippet fallback)
            paper_text: Full paper text
            schema: Schema containing query
            paper_title: Paper title for logging
            base_k: Base retrieval k, will be scaled by batch size

        Returns:
            Dict mapping column names to their extracted values
        """
        batch_size = len(columns)
        if batch_size == 0:
            return {}

        # Scale k by batch size (more columns = need more passages), cap at 15
        scaled_k = min(base_k * batch_size, 15)

        # Prepare text based on retriever availability
        if retriever is not None:
            try:
                # Combined retrieval query for all columns in batch
                retrieval_query = self.text_processor.build_retrieval_query(schema, columns)
                passages = retriever.query([paper_text], retrieval_query, k=scaled_k)
                if passages:
                    eff = "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
                else:
                    eff = paper_text
            except Exception as e:
                print(f"⚠️  Batch retrieval failed: {e}, using full text")
                eff = paper_text
        else:
            # Fallback: combine keywords from all columns for heuristic snippets
            all_keywords = []
            for col in columns:
                all_keywords.extend(self.text_processor.keywords_for_column(col))
            eff = self.text_processor.heuristic_snippets(paper_text, all_keywords)

        # Build LLM call with all columns (using "all" mode for batch)
        msgs = self.prompt_builder.build_val_messages(
            schema.query, paper_title, eff, [col.to_dict() for col in columns],
            mode="all", strict=True  # strict for fallback
        )

        # Skip truncation for long context models
        should_truncate = not self._should_skip_truncation()
        max_ctx = getattr(self.llm, 'context_window_size', 8192) if hasattr(self.llm, 'context_window_size') else 8192
        trimmed = utils.fit_prompt(msgs, truncate=should_truncate, max_new=512,
                                 safety_margins=SAFETY_MARGIN_ALL_MODE,
                                 context_window_size=max_ctx)
        raw = self.llm.generate(trimmed)
        # Check stop immediately after LLM call returns
        if self._check_stop_requested():
            print(f"🛑 Stop requested after batch LLM call, returning empty")
            return {}

        try:
            parsed = self.json_parser.parse_response(raw)
            requested = [c.name for c in columns]
            # Build allowed_values dict for postprocessing
            column_allowed_values = {
                c.name: c.allowed_values
                for c in columns
                if c.allowed_values
            }
            cleaned, unmatched = self.json_parser.postprocess(parsed, requested, column_allowed_values)

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            return cleaned
        except Exception as e:
            print(f"⚠️  Batch parse failure for columns {[c.name for c in columns]}: {e}")
            return {}

    def extract_values_for_paper(self,
                                paper_title: str,
                                paper_text: str,
                                schema: Schema,
                                max_new_tokens: int,
                                mode: str = "all",
                                retrieval_k: int = 8,
                                row_name: Optional[str] = None) -> Dict[str, Any]:
        """Extract values from a single paper for the given schema.

        Args:
            row_name: If provided, used for streaming callbacks to identify the row.
        """
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
            # Skip truncation for long context models
            should_truncate = not self._should_skip_truncation()
            max_ctx = getattr(self.llm, 'context_window_size', 8192) if hasattr(self.llm, 'context_window_size') else 8192
            trimmed = utils.fit_prompt(msgs, truncate=should_truncate, max_new=max_new_tokens,
                                     safety_margins=SAFETY_MARGIN_SINGLE_MODE,
                                     context_window_size=max_ctx)
            raw = self.llm.generate(trimmed)
            # Check stop immediately after LLM call returns
            if self._check_stop_requested():
                print(f"🛑 Stop requested after single-column LLM call, returning empty")
                return {}
            try:
                parsed = self.json_parser.parse_response(raw)
                # Build allowed_values dict for postprocessing
                column_allowed_values = {col.name: col.allowed_values} if col.allowed_values else {}
                cleaned, unmatched = self.json_parser.postprocess(parsed, [col.name], column_allowed_values)
                result = cleaned.get(col.name, {})

                # Track unmatched values for schema evolution
                self._track_unmatched_values(unmatched, paper_title)

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
            # Skip truncation for long context models
            should_truncate = not self._should_skip_truncation()
            max_ctx = getattr(self.llm, 'context_window_size', 8192) if hasattr(self.llm, 'context_window_size') else 8192
            trimmed = utils.fit_prompt(msgs, truncate=should_truncate, max_new=max_new_tokens,
                                     safety_margins=SAFETY_MARGIN_ALL_MODE,
                                     context_window_size=max_ctx)
            raw = self.llm.generate(trimmed)
            # Check stop immediately after LLM call returns
            if self._check_stop_requested():
                print(f"🛑 Stop requested after all-mode LLM call, returning partial results")
                return cleaned  # Return what we have so far
            try:
                parsed = self.json_parser.parse_response(raw)
            except Exception as e:
                print(f"⚠️  parse failure for {paper_title}: {e}")
                parsed = {}

            requested = [c.name for c in schema.columns]
            # Build allowed_values dict for postprocessing
            column_allowed_values = {
                c.name: c.allowed_values
                for c in schema.columns
                if c.allowed_values
            }
            cleaned, unmatched = self.json_parser.postprocess(parsed, requested, column_allowed_values)

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            # Attach source filename to excerpts
            cleaned = self._attach_source_to_excerpts(cleaned, paper_title)

            # Notify callback for each extracted column (streaming to UI)
            if row_name:
                for col_name, col_value in cleaned.items():
                    self._notify_value_extracted(row_name, col_name, col_value)

            # Fallback for missing columns: retry per-column, stricter + expanded retrieval
            missing = [c for c in schema.columns if c.name not in cleaned]
            if missing:
                # Check for stop request before fallback processing
                if self._check_stop_requested():
                    print(f"🛑 Stop requested before fallback, returning partial results")
                    return cleaned

                print(f"↻ Fallback per-column for {len(missing)} missing: {[c.name for c in missing]}")
                
                # Use cached fallback retriever if we don't have one (long context mode)
                fallback_retriever = self.retriever
                if self.retriever is None:
                    if self._cached_fallback_retriever is None:
                        print(f"📡 Creating fallback retriever (will be cached for future use)")
                        self._cached_fallback_retriever = self._create_fallback_retriever()
                    fallback_retriever = self._cached_fallback_retriever
                
                # First fallback: batched extraction with retriever
                expanded_k = self.text_processor.expand_k(retrieval_k)
                still_missing = []

                if fallback_retriever is not None:
                    for batch in _chunk_list(missing, FALLBACK_BATCH_SIZE):
                        # Check for stop request before each batch
                        if self._check_stop_requested():
                            print(f"🛑 Stop requested during batch fallback, returning partial results")
                            return cleaned

                        print(f"  📦 Batch fallback ({len(batch)} columns): {[c.name for c in batch]}")
                        batch_results = self._batch_column_attempt(
                            batch, retriever=fallback_retriever,
                            paper_text=paper_text, schema=schema,
                            paper_title=paper_title, base_k=expanded_k
                        )
                        # Process batch results
                        for col in batch:
                            col_res = batch_results.get(col.name)
                            if col_res:
                                col_res = self._attach_source_to_excerpts({col.name: col_res}, paper_title).get(col.name, col_res)
                                cleaned[col.name] = col_res
                                if row_name:
                                    self._notify_value_extracted(row_name, col.name, col_res)
                            else:
                                still_missing.append(col)
                else:
                    still_missing = missing

                # Second fallback: heuristic snippets for columns that still failed
                if still_missing:
                    # Check for stop request before snippet fallback
                    if self._check_stop_requested():
                        print(f"🛑 Stop requested before snippet fallback, returning partial results")
                        return cleaned

                    print(f"  📦 Snippet fallback for {len(still_missing)} remaining: {[c.name for c in still_missing]}")
                    for batch in _chunk_list(still_missing, FALLBACK_BATCH_SIZE):
                        # Check for stop request before each snippet batch
                        if self._check_stop_requested():
                            print(f"🛑 Stop requested during snippet fallback, returning partial results")
                            return cleaned

                        batch_results = self._batch_column_attempt(
                            batch, retriever=None,
                            paper_text=paper_text, schema=schema,
                            paper_title=paper_title, base_k=expanded_k
                        )
                        for col in batch:
                            col_res = batch_results.get(col.name)
                            if col_res:
                                col_res = self._attach_source_to_excerpts({col.name: col_res}, paper_title).get(col.name, col_res)
                                cleaned[col.name] = col_res
                                if row_name:
                                    self._notify_value_extracted(row_name, col.name, col_res)

            return cleaned

        # mode == "one_by_one" with optimized fallback logic
        row: Dict[str, Any] = {}
        print(f"🔍 Processing {len(schema.columns)} columns one-by-one: {[c.name for c in schema.columns]}")
        for col in schema.columns:
            # Check for stop request before each column extraction
            if self._check_stop_requested():
                print(f"🛑 Stop requested during column extraction, returning partial results")
                return row

            col_value = None
            print(f"  → Extracting column: {col.name}")

            # Attempt 1: normal rules, per-column retrieval
            first = _single_column_attempt(col, strict=False, k_override=None, use_snippets=False)

            if first and first.get('answer', '').strip():
                col_value = first
            elif self.retriever is not None:
                # Check for stop before attempt 2
                if self._check_stop_requested():
                    print(f"🛑 Stop requested before fallback attempt, returning partial results")
                    return row
                # Attempt 2: Only try expanded retrieval if we have a retriever and got empty result
                expanded_k = self.text_processor.expand_k(retrieval_k)
                second = _single_column_attempt(col, strict=True, k_override=expanded_k, use_snippets=False)
                if second and second.get('answer', '').strip():
                    col_value = second

            # Attempt 3: Only if previous attempts truly failed and we have substantial text
            if col_value is None and len(paper_text) > MIN_DOCUMENT_SIZE_FOR_SNIPPETS:
                # Check for stop before attempt 3
                if self._check_stop_requested():
                    print(f"🛑 Stop requested before snippet attempt, returning partial results")
                    return row
                third = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
                if third and third.get('answer', '').strip():
                    col_value = third

            # Store value and notify callback
            if col_value:
                # Attach source filename to excerpts
                col_value = self._attach_source_to_excerpts({col.name: col_value}, paper_title).get(col.name, col_value)
                row[col.name] = col_value
                print(f"    ✓ Found value for {col.name}: {str(col_value.get('answer', ''))[:50]}...")
                if row_name:
                    self._notify_value_extracted(row_name, col.name, col_value)
            else:
                print(f"    ✗ No value found for {col.name}")

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

            # Extract values for this paper (pass row_name for streaming callbacks)
            paper_data = self.extract_values_for_paper(
                paper_title, paper_text, schema, max_new_tokens, mode, retrieval_k,
                row_name=row_name
            )
            
            return row_name, paper_title, paper_data
            
        except Exception as e:
            print(f"⚠️  Error processing {paper_title}: {e}")
            return row_name, paper_title, None