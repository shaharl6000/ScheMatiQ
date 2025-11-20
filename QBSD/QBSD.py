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

import utils
# from __future__ import annotations
from typing import List, Dict, Sequence, Tuple, Any
import itertools
import argparse
import json
import logging, re
import time
from pathlib import Path
from schema import Schema, Column


##############################################################################
# Core pipeline                                                           #
##############################################################################

def select_relevant_content(
    docs: Sequence[str],
    query: str,
    retriever,
) -> List[str]:
    """Return a flat list of passages drawn from all docs."""
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
                           ) -> Schema:
    """
    Very lenient parser: lines that look like "Column: rationale".
    Adapt this to your favorite JSON-only format if you prefer.
    """
    cleaned = _extract_json(raw_text)
    try:
        payload = json.loads(cleaned)
        columns = [Column(**c) for c in payload]
        # print(f"cleaned raw text good: {raw_text}")
    except (json.JSONDecodeError, TypeError, KeyError):
        # ← fallback: lenient parsing for old models / bad outputs
        print(f"❌ JSON parsing failed. Cleaned text: '{cleaned}'")
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
    return Schema(query=query, max_keys=max_keys_schema, columns=columns)


SYSTEM_PROMPT = """
You are *SchemaLLM*, a senior data analyst who discovers NEW table columns to extend existing schemas.

### Task
1. **Silently reason** about the user's query and the supplied passages.
2. **If an existing schema is provided:**
   - Review the existing columns to understand what is already covered
   - Identify NEW aspects not covered by existing columns
   - Do NOT repeat or include existing columns in your output
   - Return ONLY new columns that complement the existing schema
3. **If no existing schema is provided:**
   - Create initial columns based on the query and passages
4. Identify only those aspects whose answers can be found in the provided
   passages — do **not** invent information that is absent from the text.
5. Return **only** a JSON list of NEW columns; do **not** expose your reasoning.
   
### Output JSON spec
[
  {{
    "name":        "<snake_case_column_name>",
    "definition":  "<one‑sentence definition of what data belongs here>",
    "rationale":   "<one‑sentence on why this column helps answer the query>"
  }},
  ...
]

### Critical Guidelines
* Return ONLY new columns that are missing from the existing schema
* Keep `name` concise (3–5 words, snake_case)
* Avoid creating near-duplicates of existing columns
* Focus on discovering gaps and missing information
* Do not write markdown, comments, or any text outside the JSON
* If no new columns are needed, return an empty JSON array: []
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
    max_context_tokens: int = 8192,
) -> Schema:
    """
    Feed passages + (optional) current schema to the LLM, ask for additions.
    """
    prompt = build_messages(query, passages, current_schema)
    trimmed = utils.fit_prompt(prompt, truncate=True, max_context_tokens=max_context_tokens)
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
    max_keys_schema : int,
    llm,
    retriever,
    documents_batch_size,
    max_context_tokens,
    initial_schema: Schema | None = None,
    max_iters: int = 6
) -> Schema:
    """
    Main orchestration loop.
    """
    logging.info("Starting schema discovery…")
    schema = initial_schema or Schema(query=query, max_keys=max_keys_schema)
    logging.info("Starting with schema containing %d columns", len(schema))
    
    # Simple batching; one doc may be chunked if > batch_size
    doc_iter = iter(documents)
    batches = [list(itertools.islice(doc_iter, documents_batch_size))
               for _ in range((len(documents)+documents_batch_size-1)//documents_batch_size)]

    for it, batch_docs in enumerate(batches[:max_iters], start=1):
        try:
            passages = select_relevant_content(batch_docs, query, retriever)
        except Exception as exc:
            print(f"Failed to retrieve {exc}")
            continue

        proposed = generate_schema(passages, query, max_keys_schema, schema, llm, max_context_tokens)
        merged = schema.merge(proposed)

        logging.info("Iteration %d — columns: %d → %d (J=%.2f)",
                     it, len(schema), len(merged), schema.jaccard(merged))

        if evaluate_schema_convergence(schema, merged):
            logging.info("Converged at iteration %d", it)
            return merged

        schema = merged  # update and continue

    return schema


# ------------------------------------------------------------------------ #
# Helpers                                                                  #
# ------------------------------------------------------------------------ #
def load_documents(path: Path) -> List[str]:
    exts = {".txt", ".md", ".html", ".htm"}
    docs = []
    for p in path.rglob("*"):
        if p.suffix.lower() in exts and p.is_file():
            docs.append(p.read_text(encoding="utf-8", errors="ignore"))
    if not docs:
        raise RuntimeError(f"No text files found under {path}")
    return docs


def save_schema(
    out_path: Path,
    query: str,
    retriever_cfg: Dict[str, Any],
    backend_cfg: Dict[str, Any],
    docs_path: str,
    schema: Schema,
) -> None:
    artefact = {
        "query": query,
        "docs_path": docs_path,
        "backend": backend_cfg,
        "retriever": retriever_cfg,
        "schema": [col.to_dict() for col in schema],
    }
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
    docs_path = Path(cfg["docs_path"])
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
    retriever = utils.build_retriever(retriever_cfg, llm_for_schema)

    # Load docs
    docs = load_documents(docs_path)

    # Run discovery
    start_time = time.time()
    max_context_tokens = backend_cfg.get("max_context_tokens", 8192)
    schema = discover_schema(query=query, documents=docs,
                             max_keys_schema=max_keys_schema, llm=llm_for_schema, retriever=retriever,
                             max_context_tokens=max_context_tokens,
                             documents_batch_size=documents_batch_size,
                             initial_schema=initial_schema)
    elapsed_time = time.time() - start_time

    # Log timing results
    logging.info("Schema discovery completed for %d documents in %.2f seconds (%.2f minutes)", len(docs), elapsed_time, elapsed_time / 60)

    # Persist artefact
    save_schema(output_path, query, retriever_cfg, backend_cfg, str(docs_path), schema)

    print(f"\nSchema discovery completed for {len(docs)} documents in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print("\nFinal schema\n------------")
    for col in schema:
        print(f"• {col.name}: {col.rationale}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Query-Based Schema Discovery runner")
    parser.add_argument("--config", required=True, type=Path, help="Path to JSON config")

    main(parser.parse_args().config)

