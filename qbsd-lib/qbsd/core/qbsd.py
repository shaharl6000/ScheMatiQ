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
from qbsd.core.schema import Schema, Column, SchemaEvolution


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
    """
    match = _CODE_FENCE.search(text)
    return match.group(1) if match else text.strip()

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
        # ← fallback: lenient parsing for old models / bad outputs
        print(f"❌ JSON parsing failed ({e}). Cleaned text: '{cleaned}'")
        print(f"📝 Original raw text (first 500 chars): '{raw_text[:500]}'")
        columns = []
        for line in raw_text.splitlines():
            if ":" in line:
                name, rationale = [s.strip() for s in line.split(":", 1)]
                if name:
                    columns.append(Column(
                        name=name,
                        definition="",
                        rationale=rationale
                    ))
        print(f"📊 Extracted {len(columns)} columns from fallback parsing")

    return Schema(query=query, max_keys=max_keys_schema, columns=columns), document_helpful, suggested_value_additions


SYSTEM_PROMPT = """
You are *SchemaLLM*, a minimalist schema designer. Your default response is NO NEW COLUMNS.
Only add a column when it is clearly missing from the existing schema and provides real value.

### CRITICAL: Default to Empty
Most of the time, the correct response is: {{"document_helpful": true, "columns": []}}
Adding columns should be RARE, not routine. When in doubt, DO NOT add.

### Task

**Step 1: Assess document relevance**
If passages are off-topic or lack useful information:
→ Return {{"document_helpful": false, "columns": []}}

**Step 2: If passages ARE relevant**
- **If an existing schema is provided:**
  - Assume the schema is already COMPLETE unless proven otherwise
  - Ask: "Is there a MAJOR gap that makes the schema unable to answer the query?"
  - If no major gap exists → return {{"document_helpful": true, "columns": []}}
  - Only propose columns for genuinely MISSING dimensions
- **If no existing schema is provided:**
  - Create ONLY the essential columns (aim for minimal set)
  - Return {{"document_helpful": true, "columns": [...]}}

### Column Rejection Checklist — REJECT if ANY is true:
1. ❌ An existing column could capture this (even loosely, with broader scope, or different wording)
2. ❌ It's a variation of an existing column (e.g., "model_accuracy" when "accuracy" exists)
3. ❌ It's overly specific (e.g., "f1_micro" when "f1_score" would suffice)
4. ❌ It overlaps semantically with existing columns
5. ❌ It's "nice to have" rather than essential for answering the query

**Only add if ALL of these are true:**
- ✅ The schema has a CLEAR GAP — this dimension is completely absent
- ✅ This column provides genuine benefit for understanding how to answer the query
- ✅ No existing column covers this, even partially

### Output Format
Return valid JSON only:
{{
  "document_helpful": true | false,
  "columns": [
    {{
      "name": "snake_case_name",
      "definition": "One-sentence definition",
      "rationale": "Why this is ESSENTIAL for answering the query",
      "allowed_values": ["val1", "val2"] | ["0-100"] | null
    }}
  ],
  "suggested_value_additions": []
}}

### allowed_values
| Type | Format | Examples |
|------|--------|----------|
| Categorical | list | ["yes", "no"], ["cnn", "rnn", "transformer"] |
| Numeric range | ["min-max"] | ["0-100"], ["0.0-1.0"] |
| Any number | ["number"] | Unconstrained numeric |
| Free-form | null | Titles, names, descriptions |

### Evolving allowed_values
If passages reveal new categorical values for an existing column:
{{"column_name": "...", "new_values": ["..."], "reason": "..."}}

### Remember
- **FEWER columns = BETTER schema**
- When uncertain, return empty columns
- Every column must justify its existence as ESSENTIAL
""".strip()

USER_PROMPT_TMPL = """
<QUERY>
{query}
</QUERY>

<PASSAGES>
{joined_passages}
</PASSAGES>
""".strip()

DRAFT_SCHEMA_TMPL = """
A draft schema already exists. Review it first – then append columns as needed.

<DRAFT_SCHEMA>
{json_schema}
</DRAFT_SCHEMA>
""".strip()

def build_messages(query: str,
                   passages: list[str],
                   draft_schema=None):
    user_parts = [USER_PROMPT_TMPL.format(
        query=query.strip(),
        joined_passages="\n\n".join(p.strip() for p in passages)
    )]

    if draft_schema:
        serialisable = draft_schema.to_llm_dict()
        user_parts.append(
            DRAFT_SCHEMA_TMPL.format(
                json_schema=json.dumps(serialisable,
                                       indent=2,
                                       ensure_ascii=False)
            )
        )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": "\n\n".join(user_parts).strip()},
    ]


def generate_schema(
    passages: List[str],
    query: str,
    max_keys_schema: int,
    current_schema: Schema | None,
    llm,
    context_window_size: int = 8192,
) -> tuple[Schema, bool, List[Dict[str, Any]]]:
    """
    Feed passages + (optional) current schema to the LLM, ask for additions.
    Returns (Schema, document_helpful_flag, suggested_value_additions)
    """
    prompt = build_messages(query, passages, current_schema)
    trimmed = utils.fit_prompt(prompt, truncate=True, context_window_size=context_window_size)
    llm_response = llm.generate(trimmed)
    return _parse_schema_from_llm(llm_response, query=query, max_keys_schema=max_keys_schema)


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
    query: str,
    documents: List[str],
    filenames: List[str],
    max_keys_schema : int,
    llm,
    retriever,
    documents_batch_size,
    context_window_size,
    initial_schema: Schema | None = None,
    max_iters: int = 6
) -> tuple[Schema, List[str], List[str], SchemaEvolution]:
    """
    Main orchestration loop with document contribution tracking.
    Returns (Schema, contributing_files, non_contributing_files, schema_evolution)
    """
    logging.info("Starting schema discovery…")
    schema = initial_schema or Schema(query=query, max_keys=max_keys_schema)
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

        proposed, document_helpful, suggested_value_additions = generate_schema(passages, query, max_keys_schema, schema, llm, context_window_size)

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

