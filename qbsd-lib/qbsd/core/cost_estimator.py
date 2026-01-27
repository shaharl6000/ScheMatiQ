# cost_estimator.py
"""
Cost estimation for LLM API calls in QBSD pipeline.

Provides pre-execution cost estimates based on:
- Document token counts
- Model pricing configuration
- Pipeline parameters (batch size, retrieval k, etc.)

## Estimation Methodology

### Schema Discovery Phase
- Input = system_prompt + user_prompt_template + query + passages + draft_schema
- The system prompt is ~700 tokens (measured from prompts.py SYSTEM_PROMPT_STANDARD)
- If retriever is used: passages = retriever.k passages × avg_passage_size
- If no retriever: passages = full document content
- Output = JSON with columns (typically 500-2000 tokens depending on schema complexity)

### Value Extraction Phase  
- Input = system_prompt + column_definitions + retrieved_passages
- The system prompt is ~400 tokens (measured from SYSTEM_PROMPT_VAL)
- One API call per document (extracts all columns at once)
- Output = JSON with answers + excerpts (typically 200-500 tokens)
"""

from __future__ import annotations
import json
import math
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
import tiktoken

# Load tiktoken encoder (use gpt-4o as baseline - close enough for all models)
try:
    _enc = tiktoken.encoding_for_model("gpt-4o")
except Exception:
    _enc = tiktoken.get_encoding("cl100k_base")

# Load pricing config
_PRICING_CONFIG_PATH = Path(__file__).parent / "pricing_config.json"
_pricing_config: Optional[Dict[str, Any]] = None

# ============================================================================
# MEASURED PROMPT TOKEN COUNTS (from actual prompts in prompts.py)
# These are computed once at module load to ensure accuracy
# ============================================================================

# Schema discovery prompts (from qbsd/core/prompts.py)
_SCHEMA_SYSTEM_PROMPT_TOKENS: Optional[int] = None
_SCHEMA_USER_TEMPLATE_TOKENS: Optional[int] = None
_OBSERVATION_UNIT_PROMPT_TOKENS: Optional[int] = None

# Value extraction prompts (from qbsd/value_extraction/config/prompts.py)  
_VALUE_EXTRACTION_SYSTEM_TOKENS: Optional[int] = None
_UNIT_IDENTIFICATION_PROMPT_TOKENS: Optional[int] = None


def _measure_prompt_tokens() -> Dict[str, int]:
    """
    Measure actual token counts of our prompt templates.
    
    Returns dict with measured values for documentation/verification.
    """
    global _SCHEMA_SYSTEM_PROMPT_TOKENS, _SCHEMA_USER_TEMPLATE_TOKENS
    global _OBSERVATION_UNIT_PROMPT_TOKENS, _VALUE_EXTRACTION_SYSTEM_TOKENS
    global _UNIT_IDENTIFICATION_PROMPT_TOKENS
    
    # Try to import actual prompts - fall back to estimates if not available
    try:
        from qbsd.core.prompts import (
            SYSTEM_PROMPT_STANDARD, USER_PROMPT_TMPL_STANDARD,
            SYSTEM_PROMPT_OBSERVATION_UNIT
        )
        _SCHEMA_SYSTEM_PROMPT_TOKENS = count_tokens(SYSTEM_PROMPT_STANDARD)
        _SCHEMA_USER_TEMPLATE_TOKENS = count_tokens(USER_PROMPT_TMPL_STANDARD)
        _OBSERVATION_UNIT_PROMPT_TOKENS = count_tokens(SYSTEM_PROMPT_OBSERVATION_UNIT)
    except ImportError:
        # Fallback estimates based on prompt length analysis
        _SCHEMA_SYSTEM_PROMPT_TOKENS = 700
        _SCHEMA_USER_TEMPLATE_TOKENS = 50
        _OBSERVATION_UNIT_PROMPT_TOKENS = 600
    
    try:
        from qbsd.value_extraction.config.prompts import (
            SYSTEM_PROMPT_VAL, SYSTEM_PROMPT_UNIT_IDENTIFICATION
        )
        _VALUE_EXTRACTION_SYSTEM_TOKENS = count_tokens(SYSTEM_PROMPT_VAL)
        _UNIT_IDENTIFICATION_PROMPT_TOKENS = count_tokens(SYSTEM_PROMPT_UNIT_IDENTIFICATION)
    except ImportError:
        # Fallback estimates
        _VALUE_EXTRACTION_SYSTEM_TOKENS = 400
        _UNIT_IDENTIFICATION_PROMPT_TOKENS = 800
    
    return {
        "schema_system_prompt": _SCHEMA_SYSTEM_PROMPT_TOKENS,
        "schema_user_template": _SCHEMA_USER_TEMPLATE_TOKENS,
        "observation_unit_prompt": _OBSERVATION_UNIT_PROMPT_TOKENS,
        "value_extraction_system": _VALUE_EXTRACTION_SYSTEM_TOKENS,
        "unit_identification_prompt": _UNIT_IDENTIFICATION_PROMPT_TOKENS,
    }


def _ensure_prompts_measured():
    """Ensure prompt tokens have been measured."""
    global _SCHEMA_SYSTEM_PROMPT_TOKENS
    if _SCHEMA_SYSTEM_PROMPT_TOKENS is None:
        _measure_prompt_tokens()


def _load_pricing_config() -> Dict[str, Any]:
    """Load pricing configuration from JSON file."""
    global _pricing_config
    if _pricing_config is None:
        with open(_PRICING_CONFIG_PATH, 'r') as f:
            _pricing_config = json.load(f)
    return _pricing_config


def count_tokens(text: str) -> int:
    """Count tokens in a text string using tiktoken."""
    if not text:
        return 0
    try:
        return len(_enc.encode(text))
    except Exception:
        # Fallback: rough estimate of 4 chars per token
        return len(text) // 4


def count_tokens_in_documents(
    documents: List[str],
    token_counts: Optional[List[int]] = None
) -> Tuple[int, int, int]:
    """
    Count tokens across multiple documents.
    
    Args:
        documents: List of document contents
        token_counts: Optional precomputed token counts per document
    
    Returns:
        Tuple of (total_tokens, avg_tokens_per_doc, max_tokens_in_doc)
    """
    if token_counts is not None:
        if not token_counts:
            return 0, 0, 0
        total = sum(token_counts)
        avg = total // len(token_counts)
        max_tokens = max(token_counts)
        return total, avg, max_tokens
    
    if not documents:
        return 0, 0, 0
    
    token_counts = [count_tokens(doc) for doc in documents]
    total = sum(token_counts)
    avg = total // len(documents)
    max_tokens = max(token_counts)
    
    return total, avg, max_tokens


def _estimate_schema_tokens(initial_schema: List[Dict[str, Any]]) -> int:
    """Estimate tokens for schema column definitions using actual text."""
    if not initial_schema:
        return 0
    parts: List[str] = []
    for col in initial_schema:
        name = col.get("name", "")
        definition = col.get("definition", "")
        rationale = col.get("rationale", "")
        allowed_values = col.get("allowed_values") or []
        allowed_values_str = ", ".join(allowed_values) if isinstance(allowed_values, list) else str(allowed_values)
        parts.append(
            f"Name: {name}\nDefinition: {definition}\nRationale: {rationale}\nAllowed: {allowed_values_str}"
        )
    return count_tokens("\n\n".join(parts))


def get_model_pricing(provider: str, model: str) -> Dict[str, float]:
    """
    Get pricing for a specific model.
    
    Args:
        provider: LLM provider (openai, gemini, together)
        model: Model name/identifier
        
    Returns:
        Dict with 'input', 'output' (per 1M tokens), and 'context_window'
    """
    config = _load_pricing_config()
    provider_lower = provider.lower()
    
    # Try exact match first
    if provider_lower in config["providers"]:
        provider_models = config["providers"][provider_lower]
        if model in provider_models:
            return provider_models[model]
        
        # Try partial match for model names
        for model_key, pricing in provider_models.items():
            if model_key.lower() in model.lower() or model.lower() in model_key.lower():
                return pricing
    
    # Return default pricing for unknown models
    return config["defaults"]["unknown_model"]


def get_estimation_constants() -> Dict[str, int]:
    """Get estimation constants from config."""
    config = _load_pricing_config()
    return config.get("estimation_constants", {})


@dataclass
class PhaseEstimate:
    """Cost estimate for a single phase (schema discovery or value extraction)."""
    input_tokens: int = 0
    output_tokens: int = 0
    api_calls: int = 0
    cost_usd: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CostEstimateResult:
    """Complete cost estimate for QBSD execution."""
    schema_discovery: PhaseEstimate = field(default_factory=PhaseEstimate)
    value_extraction: PhaseEstimate = field(default_factory=PhaseEstimate)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_api_calls: int = 0
    total_cost_usd: float = 0.0
    warnings: List[str] = field(default_factory=list)
    document_stats: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_discovery": self.schema_discovery.to_dict(),
            "value_extraction": self.value_extraction.to_dict(),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_api_calls": self.total_api_calls,
            "total_cost_usd": self.total_cost_usd,
            "warnings": self.warnings,
            "document_stats": self.document_stats
        }


def calculate_cost(input_tokens: int, output_tokens: int, pricing: Dict[str, float]) -> float:
    """
    Calculate cost in USD based on token counts and pricing.
    
    Args:
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens
        pricing: Dict with 'input' and 'output' prices per 1M tokens
        
    Returns:
        Cost in USD
    """
    input_cost = (input_tokens / 1_000_000) * pricing.get("input", 1.0)
    output_cost = (output_tokens / 1_000_000) * pricing.get("output", 3.0)
    return round(input_cost + output_cost, 6)


def estimate_schema_discovery_cost(
    documents: List[str],
    batch_size: int,
    max_output_tokens: int,
    provider: str,
    model: str,
    initial_schema_columns: int = 0,
    retrieval_k: int = 15,
    has_retriever: bool = True,
    query: str = "",
    initial_schema_tokens: Optional[int] = None,
    document_token_counts: Optional[List[int]] = None
) -> PhaseEstimate:
    """
    Estimate cost for schema discovery phase.
    
    Schema discovery iterates through document batches, sending relevant
    content to the LLM to discover/refine schema columns.
    
    ## How content selection works (from qbsd.py select_relevant_content):
    - If retriever is configured: retriever.query() returns k passages per document
    - If NO retriever: entire document content is sent to LLM
    
    ## Input composition per API call:
    1. System prompt (~700 tokens, measured from SYSTEM_PROMPT_STANDARD)
    2. User prompt template (~50 tokens)
    3. Query text (variable, typically 50-200 tokens)
    4. Passages/content from batch documents
    5. Draft schema if exists (~50 tokens per existing column)
    
    ## Output:
    - JSON with document_helpful flag + columns array
    - Typically 500-2000 tokens depending on columns discovered
    """
    if not documents and not document_token_counts:
        return PhaseEstimate()
    
    _ensure_prompts_measured()
    constants = get_estimation_constants()
    pricing = get_model_pricing(provider, model)
    
    # Calculate document statistics
    total_doc_tokens, avg_doc_tokens, max_doc_tokens = count_tokens_in_documents(
        documents,
        token_counts=document_token_counts
    )
    num_documents = len(document_token_counts) if document_token_counts is not None else len(documents)
    
    # Number of API calls = number of batches + 1 for observation unit discovery
    num_batches = math.ceil(num_documents / batch_size)
    num_api_calls = num_batches + 1  # +1 for observation unit discovery in first batch
    
    # === INPUT TOKENS CALCULATION ===
    
    # Fixed prompt overhead (measured from actual prompts)
    system_prompt_tokens = _SCHEMA_SYSTEM_PROMPT_TOKENS or 700
    user_template_tokens = _SCHEMA_USER_TEMPLATE_TOKENS or 50
    query_tokens_estimate = count_tokens(query)
    
    # Draft schema tokens (~50 tokens per column for name + definition + rationale)
    # This grows as schema evolves, so use average across iterations
    avg_new_columns = 5
    if initial_schema_tokens is not None and initial_schema_tokens > 0:
        schema_tokens_per_call = initial_schema_tokens + (avg_new_columns * 50)
    else:
        avg_schema_columns = initial_schema_columns + avg_new_columns
        schema_tokens_per_call = avg_schema_columns * 50
    
    # Content tokens per batch - THIS IS THE KEY DIFFERENCE
    if has_retriever:
        # Retriever returns k passages per document in the batch
        # Each passage is typically 200-500 tokens (based on retriever.passage_chars setting)
        passage_tokens = constants.get("retrieval_passage_avg_tokens", 250)
        # Retriever queries each doc in batch and returns k passages total
        content_tokens_per_batch = retrieval_k * passage_tokens * batch_size
    else:
        # No retriever: entire document content is sent
        content_tokens_per_batch = avg_doc_tokens * batch_size
    
    # Total input per call
    input_tokens_per_call = (
        system_prompt_tokens +
        user_template_tokens +
        query_tokens_estimate +
        schema_tokens_per_call +
        content_tokens_per_batch
    )
    
    # === OUTPUT TOKENS CALCULATION ===
    # Based on observed schema responses:
    # - Empty response (document_helpful: false): ~50 tokens
    # - Response with 1-3 new columns: ~200-600 tokens
    # - Response with many columns (rare): ~1000+ tokens
    # 
    # Conservative estimate: 300 tokens average (most batches add 0-2 columns)
    avg_output_tokens = constants.get("schema_discovery_avg_output_tokens", 300)
    output_tokens_per_call = min(avg_output_tokens, max_output_tokens)
    
    # Add observation unit discovery (separate call with different prompt)
    obs_unit_input = (_OBSERVATION_UNIT_PROMPT_TOKENS or 600) + query_tokens_estimate + (
        retrieval_k * constants.get("retrieval_passage_avg_tokens", 250) if has_retriever else avg_doc_tokens
    )
    obs_unit_output = 200  # Observation unit response is typically small
    
    # Total tokens
    total_input = (input_tokens_per_call * num_batches) + obs_unit_input
    total_output = (output_tokens_per_call * num_batches) + obs_unit_output
    
    # Calculate cost
    cost = calculate_cost(total_input, total_output, pricing)
    
    return PhaseEstimate(
        input_tokens=total_input,
        output_tokens=total_output,
        api_calls=num_api_calls,
        cost_usd=cost
    )


def estimate_value_extraction_cost(
    documents: List[str],
    num_columns: int,
    retrieval_k: int,
    max_output_tokens: int,
    provider: str,
    model: str,
    skip_value_extraction: bool = False,
    has_observation_units: bool = True,
    query: str = "",
    column_def_tokens: Optional[int] = None,
    document_token_counts: Optional[List[int]] = None
) -> PhaseEstimate:
    """
    Estimate cost for value extraction phase.
    
    Value extraction processes each document to extract column values.
    
    ## How value extraction works (from value_extraction/main.py):
    1. For each document:
       a. Identify observation units (if multi-unit schema) - 1 API call
       b. For each observation unit: extract all columns at once - 1 API call per unit
    
    ## Input composition per extraction call:
    1. System prompt (~400 tokens, measured from SYSTEM_PROMPT_VAL)
    2. Column definitions with allowed_values (~80 tokens per column)
    3. Retrieved passages (retrieval_k × avg_passage_size)
    4. Query context (~100 tokens)
    
    ## Output:
    - JSON with column answers + supporting excerpts
    - Typically 50-100 tokens per column (answer + 1-2 short excerpts)
    
    ## Observation unit identification (if applicable):
    - Additional API call per document to identify units
    - Input: ~800 tokens (prompt) + document content
    - Output: ~200 tokens (list of identified units)
    """
    if (not documents and not document_token_counts) or skip_value_extraction or num_columns == 0:
        return PhaseEstimate()
    
    _ensure_prompts_measured()
    constants = get_estimation_constants()
    pricing = get_model_pricing(provider, model)
    
    # Calculate document statistics for unit identification
    total_doc_tokens, avg_doc_tokens, max_doc_tokens = count_tokens_in_documents(
        documents,
        token_counts=document_token_counts
    )
    num_documents = len(document_token_counts) if document_token_counts is not None else len(documents)
    
    # === API CALLS CALCULATION ===
    # Base: 1 extraction call per document
    # If observation units: +1 unit identification call per document
    # Average ~1.5 units per document (some docs have multiple, some have 1)
    avg_units_per_doc = 1.5 if has_observation_units else 1.0
    
    if has_observation_units:
        # Unit identification calls + extraction calls
        unit_id_calls = num_documents
        extraction_calls = int(num_documents * avg_units_per_doc)
        num_api_calls = unit_id_calls + extraction_calls
    else:
        num_api_calls = num_documents
        extraction_calls = num_documents
    
    # === INPUT TOKENS CALCULATION ===
    
    # Value extraction system prompt (measured from actual prompt)
    system_prompt_tokens = _VALUE_EXTRACTION_SYSTEM_TOKENS or 400
    
    # Column definitions: name + definition + allowed_values
    # ~80 tokens per column is a good estimate
    if column_def_tokens is None:
        column_def_tokens = num_columns * 80
    
    # Query context
    query_tokens = count_tokens(query)
    
    # Retrieved passages for extraction
    passage_tokens = constants.get("retrieval_passage_avg_tokens", 250)
    retrieved_content_tokens = retrieval_k * passage_tokens
    
    # Input per extraction call
    extraction_input_per_call = (
        system_prompt_tokens +
        column_def_tokens +
        query_tokens +
        retrieved_content_tokens
    )
    
    # Unit identification input (if applicable)
    unit_id_input_per_call = 0
    if has_observation_units:
        unit_id_prompt_tokens = _UNIT_IDENTIFICATION_PROMPT_TOKENS or 800
        # Unit ID gets more document content (not just retrieved passages)
        unit_id_content = min(avg_doc_tokens, 4000)  # Capped for context window
        unit_id_input_per_call = unit_id_prompt_tokens + unit_id_content
    
    # === OUTPUT TOKENS CALCULATION ===
    # Based on observed extraction responses:
    # - Each column value: ~30-50 tokens (answer + 1-2 excerpt quotes)
    # - Empty columns are omitted (saves tokens)
    # - Assume ~70% of columns have values on average
    # 
    # Per column with value: ~40 tokens
    # Expected columns with values: num_columns * 0.7
    tokens_per_column = constants.get("tokens_per_extracted_column", 40)
    fill_rate = constants.get("value_extraction_fill_rate", 0.7)
    extraction_output_per_call = int(num_columns * fill_rate * tokens_per_column)
    extraction_output_per_call = max(100, min(extraction_output_per_call, max_output_tokens))
    
    # Unit identification output
    unit_id_output = 200 if has_observation_units else 0
    
    # Total tokens
    total_input = (extraction_input_per_call * extraction_calls)
    if has_observation_units:
        total_input += (unit_id_input_per_call * num_documents)
    
    total_output = (extraction_output_per_call * extraction_calls)
    if has_observation_units:
        total_output += (unit_id_output * num_documents)
    
    # Calculate cost
    cost = calculate_cost(total_input, total_output, pricing)
    
    return PhaseEstimate(
        input_tokens=total_input,
        output_tokens=total_output,
        api_calls=num_api_calls,
        cost_usd=cost
    )


def estimate_qbsd_cost(
    documents: List[str],
    query: str,
    batch_size: int = 1,
    retrieval_k: int = 15,
    schema_creation_provider: str = "gemini",
    schema_creation_model: str = "gemini-2.5-flash",
    schema_creation_max_output_tokens: int = 8192,
    value_extraction_provider: str = "gemini",
    value_extraction_model: str = "gemini-2.5-flash",
    value_extraction_max_output_tokens: int = 8192,
    initial_schema_columns: int = 0,
    estimated_columns: int = 10,
    skip_value_extraction: bool = False,
    has_retriever: bool = True,
    initial_schema_tokens: Optional[int] = None,
    document_token_counts: Optional[List[int]] = None
) -> CostEstimateResult:
    """
    Estimate total cost for running QBSD pipeline.
    
    Args:
        documents: List of document contents
        query: The schema discovery query
        batch_size: Documents per batch in schema discovery
        retrieval_k: Number of passages to retrieve per extraction
        schema_creation_provider: LLM provider for schema discovery
        schema_creation_model: Model for schema discovery
        schema_creation_max_output_tokens: Max output tokens for schema discovery
        value_extraction_provider: LLM provider for value extraction
        value_extraction_model: Model for value extraction
        value_extraction_max_output_tokens: Max output tokens for value extraction
        initial_schema_columns: Number of columns in initial schema (if any)
        estimated_columns: Estimated number of columns to discover
        skip_value_extraction: Whether value extraction will be skipped
        
    Returns:
        CostEstimateResult with detailed breakdown
    """
    result = CostEstimateResult()
    warnings = []
    
    # Document statistics
    if documents or document_token_counts:
        total_tokens, avg_tokens, max_tokens = count_tokens_in_documents(
            documents,
            token_counts=document_token_counts
        )
        result.document_stats = {
            "num_documents": len(document_token_counts) if document_token_counts is not None else len(documents),
            "total_tokens": total_tokens,
            "avg_tokens_per_document": avg_tokens,
            "max_tokens_in_document": max_tokens
        }
        
        # Check for context window issues
        schema_pricing = get_model_pricing(schema_creation_provider, schema_creation_model)
        value_pricing = get_model_pricing(value_extraction_provider, value_extraction_model)
        
        schema_context = schema_pricing.get("context_window", 8192)
        value_context = value_pricing.get("context_window", 8192)
        
        if max_tokens > schema_context * 0.8:
            warnings.append(
                f"Largest document ({max_tokens:,} tokens) may exceed {schema_creation_model} "
                f"context window ({schema_context:,} tokens). Consider splitting large documents."
            )
        
        if avg_tokens * batch_size > schema_context * 0.7:
            warnings.append(
                f"Batch size {batch_size} with avg document size ({avg_tokens:,} tokens) "
                f"may exceed context window. Consider reducing batch size."
            )
    else:
        result.document_stats = {
            "num_documents": 0,
            "total_tokens": 0,
            "avg_tokens_per_document": 0,
            "max_tokens_in_document": 0
        }
        warnings.append("No documents provided - cost estimate is for schema discovery from query only.")
    
    # Estimate schema discovery phase
    result.schema_discovery = estimate_schema_discovery_cost(
        documents=documents,
        batch_size=batch_size,
        max_output_tokens=schema_creation_max_output_tokens,
        provider=schema_creation_provider,
        model=schema_creation_model,
        initial_schema_columns=initial_schema_columns,
        retrieval_k=retrieval_k,
        has_retriever=has_retriever,
        query=query,
        initial_schema_tokens=initial_schema_tokens,
        document_token_counts=document_token_counts
    )
    
    # Estimate value extraction phase
    # Use estimated_columns if no initial schema, otherwise use initial + some new columns
    num_columns = initial_schema_columns + estimated_columns if initial_schema_columns == 0 else initial_schema_columns + 5
    
    result.value_extraction = estimate_value_extraction_cost(
        documents=documents,
        num_columns=num_columns,
        retrieval_k=retrieval_k,
        max_output_tokens=value_extraction_max_output_tokens,
        provider=value_extraction_provider,
        model=value_extraction_model,
        skip_value_extraction=skip_value_extraction,
        has_observation_units=True,  # Most schemas use observation units
        query=query,
        column_def_tokens=initial_schema_tokens,
        document_token_counts=document_token_counts
    )
    
    # Calculate totals
    result.total_input_tokens = result.schema_discovery.input_tokens + result.value_extraction.input_tokens
    result.total_output_tokens = result.schema_discovery.output_tokens + result.value_extraction.output_tokens
    result.total_api_calls = result.schema_discovery.api_calls + result.value_extraction.api_calls
    result.total_cost_usd = round(result.schema_discovery.cost_usd + result.value_extraction.cost_usd, 6)
    
    # Add cost-related warnings
    if result.total_cost_usd > 10.0:
        warnings.append(
            f"Estimated cost (${result.total_cost_usd:.2f}) is relatively high. "
            f"Consider using a smaller/cheaper model or processing fewer documents."
        )
    
    if result.total_cost_usd == 0.0 and documents:
        warnings.append(
            "Using free tier model - no API costs, but may have rate limits."
        )
    
    result.warnings = warnings
    
    return result


def estimate_from_config(
    documents: List[str],
    config: Dict[str, Any],
    document_token_counts: Optional[List[int]] = None
) -> CostEstimateResult:
    """
    Estimate cost from a QBSD configuration dictionary.
    
    This is a convenience function that extracts parameters from
    the standard QBSDConfig format used by the backend.
    """
    # Extract schema creation backend config
    schema_backend = config.get("schema_creation_backend", {})
    schema_provider = schema_backend.get("provider", "gemini")
    schema_model = schema_backend.get("model", "gemini-2.5-flash")
    schema_max_tokens = schema_backend.get("max_output_tokens", 8192)
    
    # Extract value extraction backend config
    value_backend = config.get("value_extraction_backend", {})
    value_provider = value_backend.get("provider", schema_provider)
    value_model = value_backend.get("model", schema_model)
    value_max_tokens = value_backend.get("max_output_tokens", 8192)
    
    # Extract other config
    batch_size = config.get("documents_batch_size", 1)
    retriever_config = config.get("retriever", {})
    retrieval_k = retriever_config.get("k", 15)
    has_retriever = bool(retriever_config)  # True if retriever is configured
    skip_value_extraction = config.get("skip_value_extraction", False)
    
    # Count initial schema columns if provided
    initial_schema = config.get("initial_schema", [])
    initial_columns = len(initial_schema) if initial_schema else 0
    initial_schema_tokens = _estimate_schema_tokens(initial_schema) if initial_schema else None
    
    return estimate_qbsd_cost(
        documents=documents,
        query=config.get("query", ""),
        batch_size=batch_size,
        retrieval_k=retrieval_k,
        schema_creation_provider=schema_provider,
        schema_creation_model=schema_model,
        schema_creation_max_output_tokens=schema_max_tokens,
        value_extraction_provider=value_provider,
        value_extraction_model=value_model,
        value_extraction_max_output_tokens=value_max_tokens,
        initial_schema_columns=initial_columns,
        skip_value_extraction=skip_value_extraction,
        has_retriever=has_retriever,
        initial_schema_tokens=initial_schema_tokens,
        document_token_counts=document_token_counts
    )

