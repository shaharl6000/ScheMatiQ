"""
Query-Based Schema Discovery
============================
Given a user *query* and a list of *documents*, iteratively discover a table
schema (column headers + short rationales) that best captures information
needed to answer the query.

Key pipeline steps
------------------
1. select_relevant_content – find passages likely to inform the schema.
2. generate_schema          – ask the LLM to propose / refine a schema.
3. merge_schemas            – unify schemas across iterations.
4. evaluate_schema_convergence – decide when the schema is “good enough”.

Replace the two stubs:
    • EmbeddingRetriever          (for passage selection)
    • LLMInterface.generate(...)  (for prompt → completion)
"""

from qbsd.core import utils
# from __future__ import annotations
from typing import List, Dict, Sequence, Tuple, Any, Union, Optional
import itertools
import argparse
import json
import logging, re
import time
import random
from pathlib import Path
from qbsd.core.schema import Schema, Column, SchemaEvolution, ObservationUnit
from qbsd.core.prompts import (
    get_prompts, SchemaMode, DRAFT_SCHEMA_TMPL,
    SYSTEM_PROMPT_OBSERVATION_UNIT, USER_PROMPT_TMPL_OBSERVATION_UNIT,
    OBSERVATION_UNIT_CONTEXT_TMPL
)


##############################################################################
# Core pipeline                                                           #
##############################################################################

def select_relevant_content(
    docs: Sequence[str],
    query: str,
    retriever,
) -> List[str]:
    """Return a flat list of passages drawn from all docs, or whole docs if no retriever."""
    if retriever is None:
        # No retriever configured - return whole documents
        return list(docs)
    
    # Use retriever to select relevant passages
    passages = []
    for doc in docs:
        passages.extend(retriever.query([doc], question=query))
    return passages


_CODE_FENCE = re.compile(r"```(?:\s*json)?\s*(.*?)\s*```", re.DOTALL)

def _extract_json(text: str) -> str:
    """
    Return the JSON payload inside a Markdown code-block if one exists,
    otherwise return the original string (stripped of leading/trailing space).
    Also normalizes double braces ({{ }}) to single braces ({ }).
    """
    match = _CODE_FENCE.search(text)
    result = match.group(1) if match else text.strip()
    # Normalize double braces (LLM may copy from prompt examples which use {{ }} for Python escaping)
    result = result.replace('{{', '{').replace('}}', '}')
    return result

def _parse_schema_from_llm(raw_text: str,
                           query: str,
                           max_keys_schema: int,
                           ) -> tuple[Schema, bool, List[Dict[str, Any]]]:
    """
    Parse schema from LLM response, now including document helpfulness assessment.
    Returns (Schema, document_helpful_flag, suggested_value_additions)
    """
    cleaned = _extract_json(raw_text)
    document_helpful = True  # Default assumption
    columns = []
    suggested_value_additions = []

    try:
        payload = json.loads(cleaned)

        # Handle new format with document_helpful field
        if isinstance(payload, dict) and "columns" in payload:
            document_helpful = payload.get("document_helpful", True)
            columns_data = payload["columns"]
            if columns_data:
                columns = [
                    Column(
                        name=c["name"],
                        definition=c.get("definition", ""),
                        rationale=c.get("rationale", ""),
                        allowed_values=c.get("allowed_values") if c.get("allowed_values") else None
                    )
                    for c in columns_data
                ]
            # Extract suggested value additions for schema evolution
            suggested_value_additions = payload.get("suggested_value_additions", [])
        # Handle legacy format (direct list of columns)
        elif isinstance(payload, list):
            columns = [
                Column(
                    name=c["name"],
                    definition=c.get("definition", ""),
                    rationale=c.get("rationale", ""),
                    allowed_values=c.get("allowed_values") if c.get("allowed_values") else None
                )
                for c in payload
            ]
        else:
            raise ValueError("Unexpected payload format")

    except (json.JSONDecodeError, TypeError, KeyError, ValueError) as e:
        # Fallback: lenient parsing for old models / bad outputs
        print(f"❌ JSON parsing failed ({e}). Cleaned text: '{cleaned}'")
        print(f"📝 Original raw text (first 500 chars): '{raw_text[:500]}'")
        columns = []

        # JSON field names to skip (these are format spec, not actual columns)
        json_field_names = {'name', 'definition', 'rationale', 'allowed_values',
                           'document_helpful', 'columns', 'suggested_value_additions',
                           'column_name', 'new_values', 'reason'}

        for line in raw_text.splitlines():
            line = line.strip()
            # Skip empty lines and JSON syntax
            if not line or line in '{}[],' or line.startswith('{') or line.startswith('}'):
                continue
            if ":" in line:
                name, rationale = [s.strip() for s in line.split(":", 1)]
                # Skip if name is quoted (JSON field)
                if name.startswith('"') and name.endswith('"'):
                    continue
                # Skip known JSON field names
                if name.lower().strip('"') in json_field_names:
                    continue
                # Skip if name is empty or too short
                if not name or len(name) < 2:
                    continue
                columns.append(Column(
                    name=name,
                    definition="",
                    rationale=rationale
                ))
        print(f"📊 Extracted {len(columns)} columns from fallback parsing")

    return Schema(query=query, max_keys=max_keys_schema, columns=columns), document_helpful, suggested_value_additions


def _parse_observation_unit_from_llm(raw_text: str) -> ObservationUnit:
    """
    Parse observation unit from LLM response.

    Returns:
        ObservationUnit with name, definition, and example_names
    """
    cleaned = _extract_json(raw_text)

    try:
        payload = json.loads(cleaned)

        if isinstance(payload, dict) and "observation_unit" in payload:
            unit_data = payload["observation_unit"]
            return ObservationUnit(
                name=unit_data.get("name", "Document"),
                definition=unit_data.get("definition", "Each document is treated as one observation unit"),
                example_names=unit_data.get("example_names", []),
            )
        else:
            logging.warning("Unexpected observation unit response format, using default")
            return ObservationUnit.default()

    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logging.warning(f"Failed to parse observation unit response: {e}")
        return ObservationUnit.default()


def _discover_observation_unit(
    query: str,
    passages: List[str],
    llm,
    context_window_size: int = 8192,
    source_document: str = None,
) -> ObservationUnit:
    """
    Discover the appropriate observation unit for the schema based on query and sample passages.

    The observation unit defines what each row represents:
    - Document-level: Each document = one row (default)
    - Sub-document-level: Each document may contain multiple units (e.g., "Model on Benchmark")

    Args:
        query: User query describing what information to extract
        passages: Sample document passages to analyze
        llm: LLM interface for generation
        context_window_size: Maximum context window size
        source_document: Document name that provided the passages

    Returns:
        ObservationUnit with name, definition, and example_names
    """
    if not query or not query.strip():
        logging.info("No query provided, using default document-level observation unit")
        return ObservationUnit.default()

    if not passages:
        logging.info("No passages provided, using default document-level observation unit")
        return ObservationUnit.default()

    # Build messages for observation unit discovery
    user_content = USER_PROMPT_TMPL_OBSERVATION_UNIT.format(
        query=query.strip(),
        joined_passages="\n\n".join(p.strip() for p in passages[:5])  # Use first 5 passages as sample
    )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_OBSERVATION_UNIT},
        {"role": "user", "content": user_content},
    ]

    # Trim to fit context window
    trimmed = utils.fit_prompt(messages, truncate=True, context_window_size=context_window_size)

    try:
        llm_response = llm.generate(trimmed)
        observation_unit = _parse_observation_unit_from_llm(llm_response)

        # Add source tracking
        if source_document:
            observation_unit.source_document = source_document
        observation_unit.discovery_iteration = 1

        logging.info(f"Discovered observation unit: {observation_unit.name} - {observation_unit.definition}")
        return observation_unit

    except Exception as e:
        logging.warning(f"Failed to discover observation unit: {e}, using default")
        return ObservationUnit.default()


def build_messages(query: str | None,
                   passages: list[str],
                   draft_schema=None,
                   observation_unit: ObservationUnit = None) -> tuple[list[dict], SchemaMode]:
    """
    Build LLM messages for schema discovery with automatic mode detection.

    Args:
        query: User query (may be None or empty for document-only mode)
        passages: Document passages (may be empty for query-only mode)
        draft_schema: Existing schema to refine (optional)

    Returns:
        Tuple of (messages, mode) where messages is the LLM conversation
        and mode indicates which prompt variant was used.

    Raises:
        ValueError: If neither query nor passages are provided
    """
    has_passages = bool(passages)
    system_prompt, user_prompt_tmpl, mode = get_prompts(query, has_passages)

    # Build user prompt based on mode
    if mode == SchemaMode.STANDARD:
        user_content = user_prompt_tmpl.format(
            query=query.strip(),
            joined_passages="\n\n".join(p.strip() for p in passages)
        )
    elif mode == SchemaMode.DOCUMENT_ONLY:
        user_content = user_prompt_tmpl.format(
            joined_passages="\n\n".join(p.strip() for p in passages)
        )
    elif mode == SchemaMode.QUERY_ONLY:
        user_content = user_prompt_tmpl.format(
            query=query.strip()
        )
    else:
        raise ValueError(f"Unknown schema mode: {mode}")

    user_parts = [user_content]

    # Add observation unit context if provided (helps LLM understand row granularity)
    if observation_unit and not observation_unit.is_default():
        example_names_section = ""
        if observation_unit.example_names:
            example_names_section = f"Example instances: {', '.join(observation_unit.example_names)}"

        obs_unit_context = OBSERVATION_UNIT_CONTEXT_TMPL.format(
            unit_name=observation_unit.name,
            unit_definition=observation_unit.definition,
            example_names_section=example_names_section,
            unit_name_lower=observation_unit.name.lower()
        )
        user_parts.append(obs_unit_context)

    if draft_schema:
        serialisable = draft_schema.to_llm_dict()
        user_parts.append(
            DRAFT_SCHEMA_TMPL.format(
                json_schema=json.dumps(serialisable,
                                       indent=2,
                                       ensure_ascii=False)
            )
        )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": "\n\n".join(user_parts).strip()},
    ]

    return messages, mode


def generate_schema(
    passages: List[str],
    query: str | None,
    max_keys_schema: int,
    current_schema: Schema | None,
    llm,
    context_window_size: int = 8192,
    observation_unit: ObservationUnit = None,
) -> tuple[Schema, bool, List[Dict[str, Any]], SchemaMode]:
    """
    Feed passages + (optional) current schema to the LLM, ask for additions.

    Args:
        passages: Document passages (may be empty for query-only mode)
        query: User query (may be None/empty for document-only mode)
        max_keys_schema: Maximum number of schema columns
        current_schema: Existing schema to refine
        llm: LLM interface for generation
        context_window_size: Maximum context window size
        observation_unit: What each row represents (helps LLM understand granularity)

    Returns:
        Tuple of (Schema, document_helpful_flag, suggested_value_additions, mode)
        Note: document_helpful is always True for QUERY_ONLY mode (no documents to assess)
    """
    messages, mode = build_messages(query, passages, current_schema, observation_unit)
    trimmed = utils.fit_prompt(messages, truncate=True, context_window_size=context_window_size)
    llm_response = llm.generate(trimmed)

    # For query-only mode, document_helpful doesn't apply
    schema, document_helpful, suggested_value_additions = _parse_schema_from_llm(
        llm_response, query=query or "", max_keys_schema=max_keys_schema
    )

    # In QUERY_ONLY mode, there are no documents to assess helpfulness
    if mode == SchemaMode.QUERY_ONLY:
        document_helpful = True  # Not applicable, default to True

    return schema, document_helpful, suggested_value_additions, mode


def evaluate_schema_convergence(prev: Schema, new: Schema, thresh: float = 0.9) -> bool:
    """
    Stop when Jaccard similarity ≥ thresh AND no new columns were added.
    """
    overlap = prev.jaccard(new)
    no_growth = len(new) == len(prev)
    return overlap >= thresh and no_growth


def load_initial_schema(initial_schema_path: Path, query: str, max_keys_schema: int) -> Schema:
    """Load initial schema from JSON file."""
    if not initial_schema_path.exists():
        logging.info("No initial schema file found at %s, starting with empty schema", initial_schema_path)
        return Schema(query=query, max_keys=max_keys_schema)
    
    try:
        data = json.loads(initial_schema_path.read_text(encoding="utf-8"))
        columns = [Column(**col) for col in data]
        logging.info("Loaded initial schema with %d columns from %s", len(columns), initial_schema_path)
        return Schema(query=query, columns=columns, max_keys=max_keys_schema)
    except Exception as e:
        logging.warning("Failed to load initial schema from %s: %s. Starting with empty schema.", initial_schema_path, e)
        return Schema(query=query, max_keys=max_keys_schema)


def discover_schema(
    query: str | None,
    documents: List[str],
    filenames: List[str],
    max_keys_schema : int,
    llm,
    retriever,
    documents_batch_size,
    context_window_size,
    initial_schema: Schema | None = None,
    max_iters: int = 6,
    initial_observation_unit: ObservationUnit | None = None,
    discover_observation_unit: bool = True,
) -> tuple[Schema, List[str], List[str], SchemaEvolution]:
    """
    Main orchestration loop with document contribution tracking.

    Supports three modes:
    - STANDARD: Both query and documents provided
    - DOCUMENT_ONLY: Documents provided, no query
    - QUERY_ONLY: Query provided, no documents

    Args:
        query: User query (optional - can be None/empty for document-only mode)
        documents: List of document contents (optional - can be empty for query-only mode)
        filenames: List of document filenames (parallel to documents)
        max_keys_schema: Maximum number of schema columns
        llm: LLM interface
        retriever: Content retriever (optional)
        documents_batch_size: Number of documents per batch
        context_window_size: LLM context window size
        initial_schema: Starting schema (optional)
        max_iters: Maximum iterations
        initial_observation_unit: Pre-defined observation unit (optional)
        discover_observation_unit: Whether to discover observation unit in first iteration (default True)

    Returns:
        (Schema, contributing_files, non_contributing_files, schema_evolution)

    Raises:
        ValueError: If neither query nor documents are provided
    """
    # Validate inputs - at least one must be provided
    has_query = bool(query and query.strip())
    has_documents = bool(documents)
    if not has_query and not has_documents:
        raise ValueError("At least one of query or documents must be provided")

    logging.info("Starting schema discovery…")
    schema = initial_schema or Schema(query=query or "", max_keys=max_keys_schema)

    # Initialize observation unit (will be discovered in first iteration if not provided)
    observation_unit = initial_observation_unit or (schema.observation_unit if initial_schema else None)
    if observation_unit:
        schema.observation_unit = observation_unit
        logging.info("Using pre-defined observation unit: %s", observation_unit.name)

    logging.info("Starting with schema containing %d columns", len(schema))

    # Track document contributions
    contributing_files = []
    non_contributing_files = []

    # Track schema evolution
    evolution = SchemaEvolution()
    cumulative_docs = 0

    # Record initial columns (if any) as iteration 0
    if len(schema.columns) > 0:
        initial_column_names = [col.name for col in schema.columns]
        evolution.add_snapshot(
            iteration=0,
            documents=["initial_schema"],
            total_columns=len(schema.columns),
            new_columns=initial_column_names,
            cumulative_documents=0
        )
        # Mark initial columns as from initial schema
        for col in schema.columns:
            if col.source_document is None:
                col.source_document = "initial_schema"
            if col.discovery_iteration is None:
                col.discovery_iteration = 0
            evolution.record_column_source(col.name, col.source_document)

    # Handle QUERY_ONLY mode: no documents, just generate schema from query
    if not has_documents:
        logging.info("QUERY_ONLY mode: Generating schema from query without documents")
        # In query-only mode, use default document-level observation unit
        if not observation_unit:
            observation_unit = ObservationUnit.default()
            schema.observation_unit = observation_unit

        proposed, _, _, mode = generate_schema(
            passages=[], query=query, max_keys_schema=max_keys_schema,
            current_schema=schema, llm=llm, context_window_size=context_window_size,
            observation_unit=observation_unit
        )

        # Merge proposed schema
        merged = schema.merge(proposed)
        new_columns = [col.name for col in merged.columns if col.name not in {c.name for c in schema.columns}]

        # Tag new columns
        for col in merged.columns:
            if col.name in new_columns:
                col.source_document = "query_only"
                col.discovery_iteration = 1
                evolution.record_column_source(col.name, "query_only")

        # Record evolution snapshot
        evolution.add_snapshot(
            iteration=1,
            documents=["query_only"],
            total_columns=len(merged.columns),
            new_columns=new_columns,
            cumulative_documents=0
        )

        logging.info("QUERY_ONLY mode completed with %d columns: %s",
                     len(merged), [col.name for col in merged.columns])
        return merged, contributing_files, non_contributing_files, evolution

    # Simple batching; one doc may be chunked if > batch_size
    doc_iter = iter(list(zip(documents, filenames)))
    batches = [list(itertools.islice(doc_iter, documents_batch_size))
               for _ in range((len(documents)+documents_batch_size-1)//documents_batch_size)]

    for it, batch_docs_with_names in enumerate(batches[:max_iters], start=1):
        batch_docs = [doc for doc, _ in batch_docs_with_names]
        batch_filenames = [fname for _, fname in batch_docs_with_names]
        cumulative_docs += len(batch_filenames)

        # Track column names before this iteration
        columns_before = {col.name.lower() for col in schema.columns}

        try:
            passages = select_relevant_content(batch_docs, query, retriever)
        except Exception as exc:
            print(f"Failed to retrieve {exc}")
            # Mark these documents as non-contributing due to retrieval failure
            non_contributing_files.extend(batch_filenames)
            # Still record snapshot (no changes)
            evolution.add_snapshot(
                iteration=it,
                documents=batch_filenames,
                total_columns=len(schema.columns),
                new_columns=[],
                cumulative_documents=cumulative_docs
            )
            continue

        # Discover observation unit in first iteration if not already set
        if it == 1 and not observation_unit and discover_observation_unit and has_query:
            logging.info("Discovering observation unit from first batch...")
            source_doc = batch_filenames[0] if batch_filenames else None
            observation_unit = _discover_observation_unit(
                query=query,
                passages=passages,
                llm=llm,
                context_window_size=context_window_size,
                source_document=source_doc
            )
            schema.observation_unit = observation_unit
            logging.info("Set observation unit: %s - %s", observation_unit.name, observation_unit.definition)

        proposed, document_helpful, suggested_value_additions, _ = generate_schema(
            passages, query, max_keys_schema, schema, llm, context_window_size,
            observation_unit=observation_unit
        )

        # Apply suggested value additions to existing schema columns
        if suggested_value_additions:
            for suggestion in suggested_value_additions:
                col_name = suggestion.get("column_name", "")
                new_values = suggestion.get("new_values", [])
                reason = suggestion.get("reason", "")
                if col_name and new_values:
                    # Find the column in the current schema and add new values
                    for col in schema.columns:
                        if col.name.lower() == col_name.lower() and col.allowed_values is not None:
                            # Only add values that don't already exist
                            existing_lower = {v.lower() for v in col.allowed_values}
                            added = []
                            for val in new_values:
                                if val.lower() not in existing_lower:
                                    col.allowed_values.append(val)
                                    existing_lower.add(val.lower())
                                    added.append(val)
                            if added:
                                logging.info("Iteration %d — Added values %s to column '%s': %s",
                                           it, added, col_name, reason)
                            break

        # Tag proposed columns with source document info BEFORE merging
        # Use first document in batch as source (representative)
        source_doc = batch_filenames[0] if batch_filenames else "unknown"
        for col in proposed.columns:
            col.source_document = source_doc
            col.discovery_iteration = it

        # Track document contributions
        if document_helpful and (len(proposed.columns) > 0 or suggested_value_additions):
            contributing_files.extend(batch_filenames)
            logging.info("Iteration %d — Helpful documents: %s", it, batch_filenames)
        else:
            non_contributing_files.extend(batch_filenames)
            if not document_helpful:
                logging.info("Iteration %d — Non-helpful documents (LLM assessed): %s", it, batch_filenames)
            else:
                logging.info("Iteration %d — No new columns from: %s", it, batch_filenames)

        merged = schema.merge(proposed)

        # Identify NEW columns added in this iteration
        columns_after = {col.name.lower() for col in merged.columns}
        new_column_names_lower = columns_after - columns_before
        new_columns = [col.name for col in merged.columns if col.name.lower() in new_column_names_lower]

        # Record column sources for new columns
        for col in merged.columns:
            if col.name.lower() in new_column_names_lower:
                evolution.record_column_source(col.name, source_doc)

        # Add snapshot to evolution
        evolution.add_snapshot(
            iteration=it,
            documents=batch_filenames,
            total_columns=len(merged.columns),
            new_columns=new_columns,
            cumulative_documents=cumulative_docs
        )

        logging.info("Iteration %d — columns: %d → %d (J=%.2f), new columns: %s",
                     it, len(schema), len(merged), schema.jaccard(merged), new_columns)

        if evaluate_schema_convergence(schema, merged):
            logging.info("Converged at iteration %d", it)
            return merged, contributing_files, non_contributing_files, evolution

        schema = merged  # update and continue

    return schema, contributing_files, non_contributing_files, evolution


# ------------------------------------------------------------------------ #
# Helpers                                                                  #
# ------------------------------------------------------------------------ #
def load_documents(paths: Union[Path, List[Path]], seed: Optional[int] = None) -> tuple[List[str], List[str]]:
    """Load documents from single path or multiple paths and return (content_list, filename_list)"""
    # Handle both single path and multiple paths
    if isinstance(paths, (str, Path)):
        paths = [Path(paths)]
    else:
        paths = [Path(p) for p in paths]
    
    exts = {".txt", ".md", ".html", ".htm"}
    docs = []
    filenames = []
    
    # Load from all paths
    for path in paths:
        logging.info(f"Loading documents from: {path}")
        count_from_path = 0
        for p in path.rglob("*"):
            if p.suffix.lower() in exts and p.is_file():
                docs.append(p.read_text(encoding="utf-8", errors="ignore"))
                filenames.append(p.name)  # Just filename, not full path
                count_from_path += 1
        logging.info(f"Loaded {count_from_path} documents from {path}")
    
    if not docs:
        paths_str = ", ".join(str(p) for p in paths)
        raise RuntimeError(f"No text files found under {paths_str}")
    
    # Randomize order if seed provided
    if seed is not None:
        random.seed(seed)
        combined = list(zip(docs, filenames))
        random.shuffle(combined)
        docs, filenames = zip(*combined)
        docs, filenames = list(docs), list(filenames)
        logging.info(f"Randomized {len(docs)} documents with seed {seed}")
    
    logging.info(f"Total documents loaded: {len(docs)}")
    return docs, filenames


def save_schema(
    out_path: Path,
    query: str,
    retriever_cfg: Dict[str, Any],
    backend_cfg: Dict[str, Any],
    docs_path: Union[str, List[str]],
    schema: Schema,
    contributing_files: List[str] = None,
    non_contributing_files: List[str] = None,
    randomization_seed: Optional[int] = None,
    schema_evolution: SchemaEvolution = None,
) -> None:
    artefact: Dict[str, Any] = {
        "query": query,
        "docs_path": docs_path,
        "backend": backend_cfg,
        "retriever": retriever_cfg,
        "schema": [col.to_dict() for col in schema],
    }

    # Add observation_unit if present
    if schema.observation_unit:
        artefact["observation_unit"] = schema.observation_unit.to_dict()

    # Add randomization metadata if seed was used
    if randomization_seed is not None:
        artefact["document_randomization_seed"] = randomization_seed

    # Add document contribution tracking if available
    if contributing_files is not None:
        artefact["document_contributions"] = {
            "contributing_files": contributing_files,
            "non_contributing_files": non_contributing_files or [],
            "total_files": len(contributing_files) + len(non_contributing_files or []),
            "contribution_rate": len(contributing_files) / (len(contributing_files) + len(non_contributing_files or [])) if (contributing_files or non_contributing_files) else 0.0
        }

    # Add schema evolution tracking if available
    if schema_evolution is not None:
        artefact["schema_evolution"] = schema_evolution.to_dict()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(artefact, indent=2, ensure_ascii=False))
    logging.info("Saved schema JSON to %s", out_path.resolve())


# ------------------------------------------------------------------------ #
# Main                                                                     #
# ------------------------------------------------------------------------ #
def main(cfg_path: Path) -> None:
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    logging.info("Loaded config from %s", cfg_path)

    query = cfg["query"]
    
    # Handle both single path and multiple paths for documents
    docs_path_config = cfg["docs_path"]
    if isinstance(docs_path_config, list):
        docs_paths = [Path(p) for p in docs_path_config]
    else:
        docs_paths = Path(docs_path_config)
    
    # Extract randomization seed with default value of 42
    randomization_seed = cfg.get("document_randomization_seed", 42)
    
    backend_cfg = cfg.get("backend", {})
    documents_batch_size = cfg.get("documents_batch_size", 4)
    retriever_cfg = cfg.get("retriever", {})
    max_keys_schema = cfg["max_keys_schema"]
    output_path = Path(cfg.get("output_path", "schema_output.json"))
    
    # Load initial schema if specified
    initial_schema = None
    if "initial_schema_path" in cfg:
        initial_schema_path = Path(cfg["initial_schema_path"])
        # Make path relative to config file directory if not absolute
        if not initial_schema_path.is_absolute():
            initial_schema_path = cfg_path.parent / initial_schema_path
        initial_schema = load_initial_schema(initial_schema_path, query, max_keys_schema)

    # Set up components
    llm_for_schema = utils.build_llm(backend_cfg)
    retriever = None
    if retriever_cfg:  # Only build retriever if configuration exists
        retriever = utils.build_retriever(retriever_cfg, llm_for_schema)
        logging.info("Using retriever: %s", retriever_cfg.get("type", "unknown"))
    else:
        logging.info("No retriever configured - will use whole documents")

    # Load docs with filenames and potential randomization
    docs, filenames = load_documents(docs_paths, seed=randomization_seed)

    # Run discovery with contribution tracking
    start_time = time.time()
    context_window_size = backend_cfg.get("context_window_size", backend_cfg.get("max_context_tokens", 8192))
    schema, contributing_files, non_contributing_files, schema_evolution = discover_schema(
        query=query, documents=docs, filenames=filenames,
        max_keys_schema=max_keys_schema, llm=llm_for_schema, retriever=retriever,
        context_window_size=context_window_size,
        documents_batch_size=documents_batch_size,
        initial_schema=initial_schema
    )
    elapsed_time = time.time() - start_time

    # Log timing results
    total_docs = len(docs)
    logging.info("Schema discovery completed for %d documents in %.2f seconds (%.2f minutes)", total_docs, elapsed_time, elapsed_time / 60)

    # Prepare docs_path for saving (convert paths to strings)
    if isinstance(docs_paths, list):
        docs_path_for_save = [str(p) for p in docs_paths]
    else:
        docs_path_for_save = str(docs_paths)

    # Persist artefact with contribution tracking and evolution
    save_schema(output_path, query, retriever_cfg, backend_cfg, docs_path_for_save, schema,
                contributing_files, non_contributing_files, randomization_seed, schema_evolution)

    # Print results
    print(f"\nSchema discovery completed for {total_docs} documents in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"\n📊 Document Contributions:")
    print(f"  • Contributing files: {len(contributing_files)}")
    print(f"  • Non-contributing files: {len(non_contributing_files)}")
    if total_docs > 0:
        contribution_rate = len(contributing_files) / total_docs * 100
        print(f"  • Contribution rate: {contribution_rate:.1f}%")
    
    if contributing_files:
        print(f"\n✅ Files that contributed to schema:")
        for filename in contributing_files:
            print(f"  • {filename}")
    
    if non_contributing_files:
        print(f"\n❌ Files that did not contribute:")
        for filename in non_contributing_files:
            print(f"  • {filename}")

    print(f"\nFinal schema ({len(schema)} columns)\n------------")
    for col in schema:
        print(f"• {col.name}: {col.rationale}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Query-Based Schema Discovery runner")
    parser.add_argument("--config", required=True, type=Path, help="Path to JSON config")

    main(parser.parse_args().config)

