"""Paper processing for value extraction."""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Set, Callable, Optional, List, Iterator
from schematiq.core.schema import Schema, Column, ObservationUnit, _embed
from schematiq.core.llm_backends import LLMInterface
from sentence_transformers import util as st_util
from schematiq.core.llm_call_tracker import LLMCallTracker
from schematiq.core import utils

from .llm_cache import LLMCache
from .json_parser import JSONResponseParser
from .unit_parser import UnitIdentificationParser, create_retry_prompt_addition
from ..utils.text_processing import TextProcessor
from ..utils.prompt_builder import PromptBuilder
from ..config.constants import (
    MIN_DOCUMENT_SIZE_FOR_SNIPPETS,
    SAFETY_MARGIN_ALL_MODE,
    SAFETY_MARGIN_SINGLE_MODE,
)
from ..config.prompts import (
    SYSTEM_PROMPT_UNIT_IDENTIFICATION,
    USER_PROMPT_TMPL_UNIT_IDENTIFICATION,
    SYSTEM_PROMPT_VAL_WITH_UNIT,
    SYSTEM_PROMPT_VAL_REEXTRACT,
)
from ..utils.schema_builder import build_extraction_response_schema
from ..utils.excerpt_grounder import ExcerptGrounder

# Type alias for warning callback: (paper_title, warning_type, message) -> None
OnWarningCallback = Callable[[str, str, str], None]


def _chunk_list(lst: List, size: int) -> Iterator[List]:
    """Split list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# Default batch size for fallback column extraction
FALLBACK_BATCH_SIZE = 3

# When True, skip retrieval and send the full document to the LLM.
# This effectively disables chunking/passage-selection so the entire
# document is sent together with the schema.
DISABLE_RETRIEVER = True

# When True, use Gemini controlled generation (response_schema) for
# value extraction. Only applies when the LLM backend is Gemini.
ENABLE_CONTROLLED_GENERATION = True

# Type alias for value extracted callback: (row_name, column_name, value) -> None
OnValueExtractedCallback = Callable[[str, str, Any], None]

# Type alias for should_stop callback: () -> bool
ShouldStopCallback = Callable[[], bool]


class StopRequestedException(Exception):
    """Raised when a stop has been requested during extraction."""

    pass


class PaperProcessor:
    """Handles value extraction from individual papers."""

    def __init__(
        self,
        llm: LLMInterface,
        cache: LLMCache = None,
        retriever=None,
        on_value_extracted: Optional[OnValueExtractedCallback] = None,
        should_stop: Optional[ShouldStopCallback] = None,
        on_warning: Optional[OnWarningCallback] = None,
    ):
        self.llm = llm
        self.cache = cache or LLMCache()
        self.retriever = retriever
        self.json_parser = JSONResponseParser()
        self.unit_parser = UnitIdentificationParser()
        self.text_processor = TextProcessor()
        self.prompt_builder = PromptBuilder()
        self.on_value_extracted = on_value_extracted
        self.should_stop = should_stop
        self.on_warning = on_warning
        # Schema evolution tracking: {column_name: {value: [list of documents]}}
        self.suggested_values: Dict[str, Dict[str, list]] = {}
        # Cache for fallback retriever (created on-demand, reused across papers)
        self._cached_fallback_retriever = None
        # Excerpt grounding for hallucination detection
        self.excerpt_grounder = ExcerptGrounder()
        # Active context cache for current document (Gemini only)
        self._active_context_cache = None

    def _check_stop_requested(self) -> bool:
        """Check if stop was requested. Returns True if should stop."""
        if self.should_stop and self.should_stop():
            return True
        return False

    def _is_gemini_backend(self) -> bool:
        """Check if the LLM backend supports controlled generation."""
        return getattr(self.llm, "_provider", "") == "gemini"

    def _build_response_schema(self, columns):
        """Build response_schema for Gemini controlled generation, or None."""
        if not ENABLE_CONTROLLED_GENERATION or not self._is_gemini_backend():
            return None
        return build_extraction_response_schema(columns)

    def _gemini_kwargs(self, thinking_budget: int = 0) -> dict:
        """Build Gemini-specific kwargs (thinking_budget). Returns empty dict for non-Gemini."""
        if not self._is_gemini_backend():
            return {}
        return {"thinking_budget": thinking_budget}

    def _create_document_cache(self, system_prompt: str, paper_text: str):
        """Create a Gemini context cache for a document. Returns cache or None."""
        if not self._is_gemini_backend():
            return None
        # Only cache if document is large enough (~1024 tokens ≈ 4096 chars)
        if len(paper_text) < 4096:
            return None
        if not hasattr(self.llm, 'create_context_cache'):
            return None
        return self.llm.create_context_cache(system_prompt, paper_text)

    def _delete_document_cache(self):
        """Delete the active context cache if one exists."""
        if self._active_context_cache and hasattr(self.llm, 'delete_context_cache'):
            self.llm.delete_context_cache(self._active_context_cache)
            self._active_context_cache = None

    def _generate(self, prompt, **kwargs) -> str:
        """Generate using context cache if available, otherwise regular generate."""
        if self._active_context_cache and hasattr(self.llm, 'generate_with_cache'):
            return self.llm.generate_with_cache(prompt, self._active_context_cache, **kwargs)
        return self.llm.generate(prompt, **kwargs)

    def _track_unmatched_values(
        self, unmatched: Dict[str, list], document_name: str = None
    ):
        """Track unmatched values for schema evolution suggestions."""
        for col_name, values in unmatched.items():
            if col_name not in self.suggested_values:
                self.suggested_values[col_name] = {}
            for value in values:
                if value not in self.suggested_values[col_name]:
                    self.suggested_values[col_name][value] = []
                if (
                    document_name
                    and document_name not in self.suggested_values[col_name][value]
                ):
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
                    qualified_values[value] = {"count": count, "documents": documents}
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
        print(
            f"🔔 _notify_value_extracted called: row={row_name}, col={column_name}, callback={'set' if self.on_value_extracted else 'NOT SET'}"
        )
        if self.on_value_extracted:
            try:
                self.on_value_extracted(row_name, column_name, value)
            except Exception as e:
                print(f"⚠️  Callback error for {column_name}: {e}")
        else:
            print(
                f"⚠️  on_value_extracted callback is NOT SET - cell streaming disabled"
            )

    def _attach_source_to_excerpts(
        self, data: Dict[str, Any], source_filename: str
    ) -> Dict[str, Any]:
        """Attach source filename to each excerpt in the extracted data.

        Converts plain excerpt strings to objects with source info:
        {"text": "excerpt text", "source": "filename.txt"}
        """
        for col_name, col_value in data.items():
            if isinstance(col_value, dict) and "excerpts" in col_value:
                excerpts = col_value.get("excerpts", [])
                col_value["excerpts"] = [
                    (
                        {"text": exc, "source": source_filename}
                        if isinstance(exc, str)
                        else exc
                    )
                    for exc in excerpts
                ]
        return data

    def _should_skip_truncation(self) -> bool:
        """Check if this LLM supports long context and should skip truncation."""
        # Global override: when retriever is disabled, also skip truncation
        if DISABLE_RETRIEVER:
            return True

        # Check for Gemini or other long-context models
        model_name = getattr(self.llm, "model", "").lower()
        provider = getattr(self.llm, "__class__", None)

        # Skip truncation for Gemini models or models with "long" in the name
        if provider and "gemini" in provider.__name__.lower():
            return True
        if "gemini" in model_name or "long" in model_name:
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
                "dynamic_k_minimum": 2,
            }
            return utils.build_retriever(fallback_config)
        except Exception as e:
            print(f"⚠️  Failed to create fallback retriever: {e}")
            return None

    def _single_column_attempt_with_retriever(
        self,
        col: Column,
        strict: bool,
        k_override: int | None,
        retriever,
        paper_text: str,
        schema: Schema,
        paper_title: str,
        use_snippets: bool = False,
    ) -> Dict[str, Any]:
        """Single column extraction attempt with explicit retriever control."""
        # Prepare text based on retriever availability
        if DISABLE_RETRIEVER:
            eff = paper_text
        elif retriever is not None:
            try:
                retrieval_query = self.text_processor.build_retrieval_query(
                    schema, [col]
                )
                passages = retriever.query(
                    [paper_text], retrieval_query, k=(k_override or 8)
                )
                if passages:
                    eff = "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
                else:
                    eff = paper_text
            except Exception as e:
                print(
                    f"⚠️  Fallback retrieval failed for {col.name}: {e}, using full text"
                )
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
            schema.query,
            paper_title,
            eff,
            [col.to_dict()],
            mode="one_by_one",
            strict=strict,
        )

        # Skip truncation for long context models
        should_truncate = not self._should_skip_truncation()
        max_ctx = getattr(self.llm, "context_window_size", None) or 8192
        trimmed = utils.fit_prompt(
            msgs,
            truncate=should_truncate,
            max_new=512,
            safety_margins=SAFETY_MARGIN_SINGLE_MODE,
            context_window_size=max_ctx,
        )
        raw = self._generate(trimmed, **self._gemini_kwargs(thinking_budget=0))
        # Check stop immediately after LLM call returns
        if self._check_stop_requested():
            print(f"🛑 Stop requested after LLM call, returning empty")
            return {}
        try:
            parsed = self.json_parser.parse_response(raw)
            # Build allowed_values dict for postprocessing
            column_allowed_values = (
                {col.name: col.allowed_values} if col.allowed_values else {}
            )
            cleaned, unmatched = self.json_parser.postprocess(
                parsed, [col.name], column_allowed_values
            )
            result = cleaned.get(col.name, {})

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            # Cache the result
            self.cache.put(cache_key, result)
            return result
        except Exception as e:
            print(f"⚠️  parse failure for column {col.name}: {e}")
            return {}

    def _batch_column_attempt(
        self,
        columns: List[Column],
        retriever,
        paper_text: str,
        schema: Schema,
        paper_title: str,
        base_k: int = 8,
    ) -> Dict[str, Any]:
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
        if DISABLE_RETRIEVER:
            eff = paper_text
        elif retriever is not None:
            try:
                # Combined retrieval query for all columns in batch
                retrieval_query = self.text_processor.build_retrieval_query(
                    schema, columns
                )
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
            schema.query,
            paper_title,
            eff,
            [col.to_dict() for col in columns],
            mode="all",
            strict=True,  # strict for fallback
        )

        # Skip truncation for long context models
        should_truncate = not self._should_skip_truncation()
        max_ctx = getattr(self.llm, "context_window_size", None) or 8192
        trimmed = utils.fit_prompt(
            msgs,
            truncate=should_truncate,
            max_new=512,
            safety_margins=SAFETY_MARGIN_ALL_MODE,
            context_window_size=max_ctx,
        )
        raw = self._generate(
            trimmed,
            response_schema=self._build_response_schema(columns),
            **self._gemini_kwargs(thinking_budget=0),
        )
        # Check stop immediately after LLM call returns
        if self._check_stop_requested():
            print(f"🛑 Stop requested after batch LLM call, returning empty")
            return {}

        try:
            parsed = self.json_parser.parse_response(raw)
            requested = [c.name for c in columns]
            # Build allowed_values dict for postprocessing
            column_allowed_values = {
                c.name: c.allowed_values for c in columns if c.allowed_values
            }
            cleaned, unmatched = self.json_parser.postprocess(
                parsed, requested, column_allowed_values
            )

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            return cleaned
        except Exception as e:
            print(
                f"⚠️  Batch parse failure for columns {[c.name for c in columns]}: {e}"
            )
            return {}

    def extract_values_for_paper(
        self,
        paper_title: str,
        paper_text: str,
        schema: Schema,
        max_new_tokens: int,
        mode: str = "all",
        retrieval_k: int = 8,
        row_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Extract values from a single paper for the given schema.

        Args:
            row_name: If provided, used for streaming callbacks to identify the row.

        Note: Document preprocessing is handled by the retriever when configured,
        avoiding redundant preprocessing overhead.
        """
        LLMCallTracker.get_instance().set_stage("value_extraction")
        if mode == "one":
            mode = "one_by_one"

        if DISABLE_RETRIEVER:
            print(
                f"🚫 Retriever disabled → sending full document ({len(paper_text)} chars) for {paper_title}"
            )

        def _retrieve_effective_text(columns_for_query=None, k=None) -> str:
            if DISABLE_RETRIEVER or self.retriever is None:
                return paper_text
            try:
                retrieval_query = self.text_processor.build_retrieval_query(
                    schema, columns_for_query
                )
                passages = self.retriever.query(
                    [paper_text], retrieval_query, k=(k or retrieval_k)
                )
                if passages:
                    print(
                        f"📖 Retrieved {len(passages)} relevant passages for {paper_title}"
                    )
                    return "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
                else:
                    print(
                        f"⚠️  No relevant passages found for {paper_title}, using full text"
                    )
                    return paper_text
            except Exception as e:
                print(f"⚠️  Retrieval failed for {paper_title}: {e}, using full text")
                return paper_text

        def _single_column_attempt(
            col: Column,
            strict: bool,
            k_override: int | None,
            use_snippets: bool = False,
        ) -> Dict[str, Any]:
            # prepare text
            if DISABLE_RETRIEVER:
                eff = paper_text
            elif self.retriever is not None:
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
                schema.query,
                paper_title,
                eff,
                [col.to_dict()],
                mode="one_by_one",
                strict=strict,
            )
            # Skip truncation for long context models
            should_truncate = not self._should_skip_truncation()
            max_ctx = getattr(self.llm, "context_window_size", None) or 8192
            trimmed = utils.fit_prompt(
                msgs,
                truncate=should_truncate,
                max_new=max_new_tokens,
                safety_margins=SAFETY_MARGIN_SINGLE_MODE,
                context_window_size=max_ctx,
            )
            raw = self._generate(
                trimmed,
                response_schema=self._build_response_schema([col]),
                **self._gemini_kwargs(thinking_budget=0),
            )
            # Check stop immediately after LLM call returns
            if self._check_stop_requested():
                print(
                    f"🛑 Stop requested after single-column LLM call, returning empty"
                )
                return {}
            try:
                parsed = self.json_parser.parse_response(raw)
                # Build allowed_values dict for postprocessing
                column_allowed_values = (
                    {col.name: col.allowed_values} if col.allowed_values else {}
                )
                cleaned, unmatched = self.json_parser.postprocess(
                    parsed, [col.name], column_allowed_values
                )
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
                schema.query,
                paper_title,
                eff,
                [c.to_dict() for c in schema.columns],
                mode="all",
                strict=False,
            )
            # Skip truncation for long context models
            should_truncate = not self._should_skip_truncation()
            max_ctx = getattr(self.llm, "context_window_size", None) or 8192
            trimmed = utils.fit_prompt(
                msgs,
                truncate=should_truncate,
                max_new=max_new_tokens,
                safety_margins=SAFETY_MARGIN_ALL_MODE,
                context_window_size=max_ctx,
            )
            raw = self._generate(
                trimmed,
                response_schema=self._build_response_schema(list(schema.columns)),
                **self._gemini_kwargs(thinking_budget=0),
            )
            # Check stop immediately after LLM call returns
            if self._check_stop_requested():
                print(
                    f"🛑 Stop requested after all-mode LLM call, returning partial results"
                )
                return cleaned  # Return what we have so far
            try:
                parsed = self.json_parser.parse_response(raw)
            except Exception as e:
                print(f"⚠️  parse failure for {paper_title}: {e}")
                parsed = {}

            requested = [c.name for c in schema.columns]
            # Build allowed_values dict for postprocessing
            column_allowed_values = {
                c.name: c.allowed_values for c in schema.columns if c.allowed_values
            }
            cleaned, unmatched = self.json_parser.postprocess(
                parsed, requested, column_allowed_values
            )

            # Track unmatched values for schema evolution
            self._track_unmatched_values(unmatched, paper_title)

            # Attach source filename to excerpts
            cleaned = self._attach_source_to_excerpts(cleaned, paper_title)

            # Notify callback for each extracted column (streaming to UI)
            if row_name:
                for col_name, col_value in cleaned.items():
                    self._notify_value_extracted(row_name, col_name, col_value)

            # Column-reordered second pass: re-extract missing columns in reversed
            # order to counteract LLM positional attention bias.
            missing = [c for c in schema.columns if c.name not in cleaned]
            if len(missing) >= 2:
                if self._check_stop_requested():
                    print(
                        f"🛑 Stop requested before reordered pass, returning partial results"
                    )
                    return cleaned

                print(
                    f"↻ Reordered second pass for {len(missing)} missing: {[c.name for c in missing]}"
                )
                reordered = list(reversed(missing))
                reorder_msgs = self.prompt_builder.build_val_messages(
                    schema.query,
                    paper_title,
                    eff,
                    [c.to_dict() for c in reordered],
                    mode="all",
                    strict=True,
                )
                # Replace system prompt with re-extraction prompt
                reorder_msgs[0]["content"] = SYSTEM_PROMPT_VAL_REEXTRACT
                should_truncate_re = not self._should_skip_truncation()
                trimmed_re = utils.fit_prompt(
                    reorder_msgs,
                    truncate=should_truncate_re,
                    max_new=max_new_tokens,
                    safety_margins=SAFETY_MARGIN_ALL_MODE,
                    context_window_size=max_ctx,
                )
                raw_re = self._generate(
                    trimmed_re,
                    response_schema=self._build_response_schema(reordered),
                    **self._gemini_kwargs(thinking_budget=0),
                )
                if not self._check_stop_requested():
                    try:
                        parsed_re = self.json_parser.parse_response(raw_re)
                        reorder_allowed = {
                            c.name: c.allowed_values
                            for c in reordered
                            if c.allowed_values
                        }
                        cleaned_re, unmatched_re = self.json_parser.postprocess(
                            parsed_re,
                            [c.name for c in reordered],
                            reorder_allowed,
                        )
                        self._track_unmatched_values(unmatched_re, paper_title)
                        cleaned_re = self._attach_source_to_excerpts(
                            cleaned_re, paper_title
                        )
                        # Merge: only add columns not already present
                        for col_name, col_val in cleaned_re.items():
                            if col_name not in cleaned:
                                cleaned[col_name] = col_val
                                if row_name:
                                    self._notify_value_extracted(
                                        row_name, col_name, col_val
                                    )
                    except Exception as e:
                        print(f"⚠️  Reordered pass parse failure: {e}")

            # Fallback for missing columns: retry per-column, stricter + expanded retrieval
            missing = [c for c in schema.columns if c.name not in cleaned]
            if missing:
                # Check for stop request before fallback processing
                if self._check_stop_requested():
                    print(
                        f"🛑 Stop requested before fallback, returning partial results"
                    )
                    return cleaned

                print(
                    f"↻ Fallback per-column for {len(missing)} missing: {[c.name for c in missing]}"
                )

                # Use cached fallback retriever if we don't have one (long context mode)
                fallback_retriever = self.retriever
                if self.retriever is None:
                    if self._cached_fallback_retriever is None:
                        print(
                            f"📡 Creating fallback retriever (will be cached for future use)"
                        )
                        self._cached_fallback_retriever = (
                            self._create_fallback_retriever()
                        )
                    fallback_retriever = self._cached_fallback_retriever

                # First fallback: batched extraction with retriever
                expanded_k = self.text_processor.expand_k(retrieval_k)
                still_missing = []

                if fallback_retriever is not None:
                    for batch in _chunk_list(missing, FALLBACK_BATCH_SIZE):
                        # Check for stop request before each batch
                        if self._check_stop_requested():
                            print(
                                f"🛑 Stop requested during batch fallback, returning partial results"
                            )
                            return cleaned

                        print(
                            f"  📦 Batch fallback ({len(batch)} columns): {[c.name for c in batch]}"
                        )
                        batch_results = self._batch_column_attempt(
                            batch,
                            retriever=fallback_retriever,
                            paper_text=paper_text,
                            schema=schema,
                            paper_title=paper_title,
                            base_k=expanded_k,
                        )
                        # Process batch results
                        for col in batch:
                            col_res = batch_results.get(col.name)
                            if col_res:
                                col_res = self._attach_source_to_excerpts(
                                    {col.name: col_res}, paper_title
                                ).get(col.name, col_res)
                                cleaned[col.name] = col_res
                                if row_name:
                                    self._notify_value_extracted(
                                        row_name, col.name, col_res
                                    )
                            else:
                                still_missing.append(col)
                else:
                    still_missing = missing

                # Second fallback: heuristic snippets for columns that still failed
                if still_missing:
                    # Check for stop request before snippet fallback
                    if self._check_stop_requested():
                        print(
                            f"🛑 Stop requested before snippet fallback, returning partial results"
                        )
                        return cleaned

                    print(
                        f"  📦 Snippet fallback for {len(still_missing)} remaining: {[c.name for c in still_missing]}"
                    )
                    for batch in _chunk_list(still_missing, FALLBACK_BATCH_SIZE):
                        # Check for stop request before each snippet batch
                        if self._check_stop_requested():
                            print(
                                f"🛑 Stop requested during snippet fallback, returning partial results"
                            )
                            return cleaned

                        batch_results = self._batch_column_attempt(
                            batch,
                            retriever=None,
                            paper_text=paper_text,
                            schema=schema,
                            paper_title=paper_title,
                            base_k=expanded_k,
                        )
                        for col in batch:
                            col_res = batch_results.get(col.name)
                            if col_res:
                                col_res = self._attach_source_to_excerpts(
                                    {col.name: col_res}, paper_title
                                ).get(col.name, col_res)
                                cleaned[col.name] = col_res
                                if row_name:
                                    self._notify_value_extracted(
                                        row_name, col.name, col_res
                                    )

            # Excerpt grounding: verify excerpts against source text
            grounding_stats = self.excerpt_grounder.ground_all_excerpts(
                cleaned, paper_text
            )
            if grounding_stats.get("not_found", 0) > 0:
                print(
                    f"📍 Excerpt grounding for {paper_title}: "
                    f"{grounding_stats['exact']} exact, "
                    f"{grounding_stats.get('case_insensitive', 0)} case_insensitive, "
                    f"{grounding_stats['fuzzy']} fuzzy, "
                    f"{grounding_stats['not_found']} not found"
                )

            return cleaned

        # mode == "one_by_one" with optimized fallback logic
        row: Dict[str, Any] = {}
        print(
            f"🔍 Processing {len(schema.columns)} columns one-by-one: {[c.name for c in schema.columns]}"
        )
        for col in schema.columns:
            # Check for stop request before each column extraction
            if self._check_stop_requested():
                print(
                    f"🛑 Stop requested during column extraction, returning partial results"
                )
                return row

            col_value = None
            print(f"  → Extracting column: {col.name}")

            # Attempt 1: normal rules, per-column retrieval
            first = _single_column_attempt(
                col, strict=False, k_override=None, use_snippets=False
            )

            if first and first.get("answer", "").strip():
                col_value = first
            elif self.retriever is not None:
                # Check for stop before attempt 2
                if self._check_stop_requested():
                    print(
                        f"🛑 Stop requested before fallback attempt, returning partial results"
                    )
                    return row
                # Attempt 2: Only try expanded retrieval if we have a retriever and got empty result
                expanded_k = self.text_processor.expand_k(retrieval_k)
                second = _single_column_attempt(
                    col, strict=True, k_override=expanded_k, use_snippets=False
                )
                if second and second.get("answer", "").strip():
                    col_value = second

            # Attempt 3: Only if previous attempts truly failed and we have substantial text
            if col_value is None and len(paper_text) > MIN_DOCUMENT_SIZE_FOR_SNIPPETS:
                # Check for stop before attempt 3
                if self._check_stop_requested():
                    print(
                        f"🛑 Stop requested before snippet attempt, returning partial results"
                    )
                    return row
                third = _single_column_attempt(
                    col, strict=True, k_override=None, use_snippets=True
                )
                if third and third.get("answer", "").strip():
                    col_value = third

            # Store value and notify callback
            if col_value:
                # Attach source filename to excerpts
                col_value = self._attach_source_to_excerpts(
                    {col.name: col_value}, paper_title
                ).get(col.name, col_value)
                row[col.name] = col_value
                print(
                    f"    ✓ Found value for {col.name}: {str(col_value.get('answer', ''))[:50]}..."
                )
                if row_name:
                    self._notify_value_extracted(row_name, col.name, col_value)
            else:
                print(f"    ✗ No value found for {col.name}")

        return row

    def process_single_paper(
        self,
        doc_path: Path,
        schema: Schema,
        max_new_tokens: int,
        mode: str,
        retrieval_k: int,
        processed_papers: Set[str],
    ) -> tuple[str, str, Dict[str, Any] | None]:
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
                paper_title,
                paper_text,
                schema,
                max_new_tokens,
                mode,
                retrieval_k,
                row_name=row_name,
            )

            return row_name, paper_title, paper_data

        except Exception as e:
            print(f"⚠️  Error processing {paper_title}: {e}")
            return row_name, paper_title, None

    # ================================================================
    # Observation Unit Methods
    # ================================================================

    def _emit_warning(self, paper_title: str, warning_type: str, message: str):
        """Emit a warning via the on_warning callback if set."""
        print(f"⚠️  [{paper_title}] {warning_type}: {message}")
        if self.on_warning:
            try:
                self.on_warning(paper_title, warning_type, message)
            except Exception as e:
                print(f"⚠️  Warning callback error: {e}")

    def _deduplicate_units(
        self, units: List[Dict[str, Any]], sim_threshold: float = 0.85
    ) -> List[Dict[str, Any]]:
        """Deduplicate units using semantic similarity and substring containment.

        Uses the same embedding model (all-MiniLM-L6-v2) used for schema column
        deduplication, but with a slightly lower threshold (0.85 vs 0.9) since
        unit names are shorter and more varied.

        Greedy merge: iterate in order, merge into first matching cluster.
        Keeps the highest-confidence representative, combines relevant_passages.
        """
        if len(units) <= 1:
            return units

        conf_priority = {"high": 0, "known": 1, "medium": 2, "low": 3}

        # Compute embeddings for all unit names
        names = [u.get("unit_name", "") for u in units]
        embeddings = [_embed(name) for name in names]

        # Greedy clustering
        clusters: List[List[int]] = []  # each cluster is a list of indices
        assigned = set()

        for i in range(len(units)):
            if i in assigned:
                continue
            cluster = [i]
            assigned.add(i)

            for j in range(i + 1, len(units)):
                if j in assigned:
                    continue

                # Check semantic similarity
                sim = st_util.cos_sim(embeddings[i], embeddings[j]).item()
                is_similar = sim >= sim_threshold

                # Check substring containment (case-insensitive)
                name_i = names[i].lower().strip()
                name_j = names[j].lower().strip()
                is_substring = (name_i in name_j or name_j in name_i) and min(
                    len(name_i), len(name_j)
                ) > 2

                if is_similar or is_substring:
                    cluster.append(j)
                    assigned.add(j)

            clusters.append(cluster)

        # Merge clusters: keep highest-confidence representative, combine passages
        deduped = []
        for cluster in clusters:
            # Sort by confidence (best first)
            cluster.sort(
                key=lambda idx: conf_priority.get(
                    units[idx].get("confidence", "medium"), 2
                )
            )
            representative = dict(units[cluster[0]])  # copy best

            # Combine relevant_passages from all cluster members
            if len(cluster) > 1:
                all_passages = []
                for idx in cluster:
                    all_passages.extend(units[idx].get("relevant_passages", []))
                # Deduplicate passages by identity
                seen = set()
                unique_passages = []
                for p in all_passages:
                    p_id = id(p) if not isinstance(p, str) else p[:200]
                    if p_id not in seen:
                        seen.add(p_id)
                        unique_passages.append(p)
                representative["relevant_passages"] = unique_passages

                merged_names = [names[idx] for idx in cluster]
                logging.debug(
                    f"Merged units: {merged_names} → {representative['unit_name']}"
                )

            deduped.append(representative)

        return deduped

    def _attempt_unit_identification(
        self,
        paper_title: str,
        paper_text: str,
        observation_unit: ObservationUnit,
        schema: Schema,
        is_retry: bool = False,
        previous_error: Optional[str] = None,
        previous_format: Optional[str] = None,
    ):
        """Single attempt at unit identification.

        Args:
            paper_title: Title/name of the paper
            paper_text: Full text of the paper
            observation_unit: Definition of what constitutes a single observation unit
            schema: Schema containing query and columns
            is_retry: Whether this is a retry attempt
            previous_error: Error message from previous attempt (for retry prompt)
            previous_format: Detected format from previous attempt (for retry prompt)

        Returns:
            UnitParseResult from the parser
        """
        # Build the prompt for unit identification
        example_names_str = (
            ", ".join(observation_unit.example_names or []) or "None provided"
        )

        # Format the system prompt with unit definition and query context
        system_prompt = SYSTEM_PROMPT_UNIT_IDENTIFICATION.format(
            unit_name=observation_unit.name,
            unit_definition=observation_unit.definition,
            example_names=example_names_str,
            query=schema.query,
        )

        user_content = USER_PROMPT_TMPL_UNIT_IDENTIFICATION.format(
            unit_name=observation_unit.name,
            unit_definition=observation_unit.definition,
            example_names=example_names_str,
            document_text=paper_text,
            query=schema.query,
        )

        # Add retry instructions if this is a retry
        if is_retry and previous_error:
            retry_addition = create_retry_prompt_addition(
                previous_error, previous_format or "unknown"
            )
            user_content = retry_addition + user_content

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        # Fit to context window, using task-specific token budget
        max_ctx = getattr(self.llm, "context_window_size", None) or 8192
        task_tokens = self.llm.max_tokens_for_task("unit_identification")
        trimmed = utils.fit_prompt(
            messages, truncate=True, max_new=task_tokens, context_window_size=max_ctx
        )

        raw_response = self.llm.generate(
            trimmed, max_output_tokens=task_tokens, **self._gemini_kwargs(thinking_budget=1024)
        )

        # Log raw response for diagnostics (truncated for readability)
        preview = raw_response[:500] if raw_response else "(empty)"
        logging.info(f"[{paper_title}] Unit identification raw response: {preview}")

        # Check for stop request
        if self._check_stop_requested():
            from .unit_parser import UnitParseResult

            return UnitParseResult(success=False, error="Stop requested")

        # Parse using dedicated unit parser
        result = self.unit_parser.parse_response(raw_response)

        # Log parse result for diagnostics
        if result.success:
            unit_names = (
                [u.get("unit_name", "?") for u in result.units] if result.units else []
            )
            logging.info(
                f"[{paper_title}] Unit identification result: {len(unit_names)} units found: {unit_names}"
            )
        else:
            logging.warning(
                f"[{paper_title}] Unit identification parse failed: {result.error} (format: {result.detected_format})"
            )

        return result

    def identify_observation_units(
        self,
        paper_title: str,
        paper_text: str,
        observation_unit: ObservationUnit,
        schema: Schema,
        max_retries: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Identify all observation units within a document with retry logic.

        Args:
            paper_title: Title/name of the paper
            paper_text: Full text of the paper
            observation_unit: Definition of what constitutes a single observation unit
            schema: Schema containing query and columns
            max_retries: Maximum retry attempts on format errors (default: 2)

        Returns:
            List of dicts, each containing:
            - unit_name: Descriptive name for this instance (e.g., "GPT-4 on MMLU")
            - relevant_passages: List of passages specific to this unit
            - confidence: "high", "medium", or "low"
        """
        last_error = None
        last_format = None

        for attempt in range(max_retries + 1):
            try:
                is_retry = attempt > 0
                if is_retry:
                    logging.info(
                        f"Retry {attempt}/{max_retries} for unit identification in {paper_title}"
                    )
                    print(
                        f"🔄 Retry {attempt}/{max_retries} for unit identification in {paper_title} (previous error: {last_error})"
                    )

                result = self._attempt_unit_identification(
                    paper_title,
                    paper_text,
                    observation_unit,
                    schema,
                    is_retry=is_retry,
                    previous_error=last_error,
                    previous_format=last_format,
                )

                if result.success:
                    # Log any warnings from successful parse
                    for warning in result.warnings:
                        logging.info(
                            f"Unit identification warning for {paper_title}: {warning}"
                        )

                    if not result.units:
                        # Empty is valid - no units of this type in document
                        logging.info(
                            f"No observation units of type '{observation_unit.name}' found in {paper_title}"
                        )
                        return []

                    # Ensure relevant_passages aren't empty - use full text as fallback
                    for unit in result.units:
                        if not unit.get("relevant_passages"):
                            unit["relevant_passages"] = [paper_text]

                    raw_count = len(result.units)

                    # --- Layer 2a: Confidence filtering ---
                    units = [
                        u
                        for u in result.units
                        if u.get("confidence", "medium") != "low"
                    ]
                    dropped_low = raw_count - len(units)
                    if dropped_low > 0:
                        logging.info(
                            f"[{paper_title}] Confidence filter: {raw_count} → {len(units)} "
                            f"(dropped {dropped_low} low-confidence units)"
                        )

                    # --- Layer 2b: Semantic deduplication ---
                    pre_dedup = len(units)
                    units = self._deduplicate_units(units)
                    if len(units) < pre_dedup:
                        logging.info(
                            f"[{paper_title}] Semantic dedup: {pre_dedup} → {len(units)} units"
                        )

                    # --- Layer 2c: Hard cap ---
                    MAX_UNITS = 15
                    if len(units) > MAX_UNITS:
                        logging.warning(
                            f"[{paper_title}] Unit hard cap: {len(units)} → {MAX_UNITS} "
                            f"(keeping top {MAX_UNITS} by confidence)"
                        )
                        # Sort by confidence priority: high > medium > known > low
                        conf_order = {"high": 0, "known": 1, "medium": 2, "low": 3}
                        units.sort(
                            key=lambda u: conf_order.get(
                                u.get("confidence", "medium"), 2
                            )
                        )
                        units = units[:MAX_UNITS]

                    logging.info(
                        f"[{paper_title}] Unit identification: {raw_count} raw → "
                        f"{raw_count - dropped_low} after confidence → {len(units)} final"
                    )
                    return units

                # Parse failed - record error for retry
                last_error = result.error
                last_format = result.detected_format

                if result.detected_format == "value_extraction":
                    logging.warning(
                        f"LLM confused unit ID with value extraction for {paper_title}"
                    )
                else:
                    logging.warning(
                        f"Unit parse failed for {paper_title}: {result.error}"
                    )

            except Exception as e:
                last_error = str(e)
                last_format = "exception"
                logging.warning(
                    f"Exception in unit identification attempt {attempt + 1} for {paper_title}: {e}"
                )

        # Exhausted retries - emit warning and return empty (skip document)
        self._emit_warning(
            paper_title,
            "unit_identification_failed",
            f"Failed to identify observation units after {max_retries + 1} attempts. "
            f"Last error: {last_error}. Skipping document.",
        )

        return []

    def extract_values_for_unit(
        self,
        unit_name: str,
        relevant_passages: List[str],
        schema: Schema,
        max_new_tokens: int,
        paper_title: str,
    ) -> Dict[str, Any]:
        """
        Extract values for a single observation unit using its relevant passages.

        Args:
            unit_name: Name of the observation unit (e.g., "GPT-4 on MMLU")
            relevant_passages: Passages specific to this unit
            schema: Schema with columns to extract
            max_new_tokens: Max tokens for LLM response
            paper_title: Source document title

        Returns:
            Dict of column values for this unit
        """
        # Combine relevant passages
        eff = "\n\n--- RELEVANT PASSAGE ---\n\n".join(relevant_passages)

        # Build prompt with unit context
        system_prompt = SYSTEM_PROMPT_VAL_WITH_UNIT.format(unit_name=unit_name)

        msgs = self.prompt_builder.build_val_messages(
            schema.query,
            f"{paper_title} - {unit_name}",
            eff,
            [c.to_dict() for c in schema.columns],
            mode="all",
            strict=False,
        )

        # Replace the system prompt with unit-aware version
        msgs[0]["content"] = system_prompt

        # Fit to context and generate, using task-specific token budget
        should_truncate = not self._should_skip_truncation()
        max_ctx = getattr(self.llm, "context_window_size", None) or 8192
        task_tokens = self.llm.max_tokens_for_task("value_extraction")
        effective_max = (
            min(max_new_tokens, task_tokens) if max_new_tokens else task_tokens
        )
        trimmed = utils.fit_prompt(
            msgs,
            truncate=should_truncate,
            max_new=effective_max,
            safety_margins=SAFETY_MARGIN_ALL_MODE,
            context_window_size=max_ctx,
        )

        raw = self._generate(
            trimmed,
            max_output_tokens=effective_max,
            response_schema=self._build_response_schema(list(schema.columns)),
            **self._gemini_kwargs(thinking_budget=0),
        )

        if self._check_stop_requested():
            return {}

        try:
            parsed = self.json_parser.parse_response(raw)
            requested = [c.name for c in schema.columns]
            column_allowed_values = {
                c.name: c.allowed_values for c in schema.columns if c.allowed_values
            }
            cleaned, unmatched = self.json_parser.postprocess(
                parsed, requested, column_allowed_values
            )

            # Track unmatched values
            self._track_unmatched_values(unmatched, paper_title)

            # Attach source to excerpts
            cleaned = self._attach_source_to_excerpts(cleaned, paper_title)

            return cleaned

        except Exception as e:
            logging.warning(f"Error extracting values for unit {unit_name}: {e}")
            return {}

    def extract_values_for_paper_with_units(
        self,
        paper_title: str,
        paper_text: str,
        schema: Schema,
        max_new_tokens: int,
        mode: str = "all",
        retrieval_k: int = 8,
        on_unit_extracted: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        known_units: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract values from a paper, potentially producing multiple rows
        if the observation unit is sub-document-level.

        Args:
            paper_title: Title of the paper
            paper_text: Full paper text
            schema: Schema with observation_unit and columns
            max_new_tokens: Max tokens for extraction
            mode: Extraction mode ("all" or "one_by_one")
            retrieval_k: Number of passages to retrieve
            on_unit_extracted: Callback called when each unit is extracted
            known_units: Optional list of unit names to use instead of LLM discovery.
                When provided, skips the identify_observation_units LLM call and uses
                these names directly with the full paper text as relevant passages.

        Returns:
            List of row dicts, each containing:
            - _unit_name: Descriptive name of the observation unit
            - _source_document: Original document filename
            - _observation_unit: The unit type name
            - <column values>

        Note: Document preprocessing is handled by the retriever when configured,
        avoiding redundant preprocessing overhead.
        """
        LLMCallTracker.get_instance().set_stage("value_extraction")
        observation_unit = schema.observation_unit

        # Observation unit is required
        if not observation_unit:
            raise ValueError("observation_unit required in schema for value extraction")

        if known_units is not None:
            # Skip LLM discovery — use provided unit names directly
            units = [
                {
                    "unit_name": name,
                    "relevant_passages": [paper_text],
                    "confidence": "known",
                }
                for name in known_units
            ]
            print(f"📋 Using {len(units)} known units for {paper_title}: {known_units}")
        else:
            # Identify observation units in the document via LLM
            print(
                f"🔍 Identifying observation units ({observation_unit.name}) in {paper_title}..."
            )
            units = self.identify_observation_units(
                paper_title, paper_text, observation_unit, schema
            )

        if not units:
            print(f"  ⚠️ No units found, skipping document")
            return []

        print(
            f"  📊 Found {len(units)} observation units: {[u['unit_name'] for u in units]}"
        )

        # Create context cache for this document (Gemini only, reused across units)
        cache_system_prompt = SYSTEM_PROMPT_VAL_WITH_UNIT.format(unit_name=observation_unit.name)
        self._active_context_cache = self._create_document_cache(cache_system_prompt, paper_text)
        if self._active_context_cache:
            print(f"  📦 Created context cache for {paper_title}")

        # Extract values for each unit
        import time as time_module

        results = []
        for i, unit in enumerate(units, 1):
            if self._check_stop_requested():
                print(f"🛑 Stop requested during unit extraction")
                self._delete_document_cache()
                break

            unit_name = unit.get("unit_name", f"Unit {i}")
            relevant_passages = unit.get("relevant_passages", [paper_text])
            confidence = unit.get("confidence", "medium")

            # When DISABLE_RETRIEVER is on, always use the full document text
            # instead of the (possibly truncated) relevant_passages from unit identification
            if DISABLE_RETRIEVER:
                relevant_passages = [paper_text]

            print(
                f"  → Extracting values for unit {i}/{len(units)}: {unit_name} (confidence: {confidence})"
            )
            unit_start = time_module.time()

            # Extract values for this specific unit
            unit_values = self.extract_values_for_unit(
                unit_name=unit_name,
                relevant_passages=relevant_passages,
                schema=schema,
                max_new_tokens=max_new_tokens,
                paper_title=paper_title,
            )

            unit_elapsed = time_module.time() - unit_start

            if unit_values:
                # Add metadata fields
                unit_values["_unit_name"] = unit_name
                unit_values["_source_document"] = paper_title
                unit_values["_parent_document"] = paper_title
                unit_values["_observation_unit"] = observation_unit.name
                unit_values["_unit_confidence"] = confidence

                results.append(unit_values)

                # Callback for streaming
                if on_unit_extracted:
                    on_unit_extracted(unit_name, unit_values)

                col_count = len([k for k in unit_values if not k.startswith("_")])
                print(
                    f"    ✓ Extracted {col_count} columns for {unit_name} ({unit_elapsed:.1f}s)"
                )
            else:
                print(
                    f"    ✗ No values extracted for {unit_name} ({unit_elapsed:.1f}s)"
                )

        # Clean up context cache for this document
        self._delete_document_cache()

        print(
            f"  ✅ Completed {paper_title}: {len(results)} rows from {len(units)} units"
        )
        return results
