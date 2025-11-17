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
        print(f"cleaned fallback to no definition. raw text: {cleaned}")
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
    return Schema(query=query, max_keys=max_keys_schema, columns=columns)


SYSTEM_PROMPT = """
You are *SchemaLLM*, a senior data analyst who converts collections of papers
into research‑ready table schemas.

### Task
1. **Silently reason** about the user’s query and the supplied passages.
2. Select the most important aspects that, if turned into table columns,
   would best support answering the query.
3. Identify only those aspects whose answers can be found in the provided
   passages — do **not** invent information that is absent from the text.
4. Return **only** a JSON list; do **not** expose your reasoning.
   
### Output JSON spec
[
  {{
    "name":        "<snake_case_column_name>",
    "definition":  "<one‑sentence definition of what data belongs here>",
    "rationale":   "<one‑sentence on why this column helps answer the query>"
  }},
  ...
]

### Constraints
* Keep `name` concise (3–5 words, snake_case).
* Do not write markdown, comments, or any text outside the JSON.
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
        serialisable = utils._to_jsonable(draft_schema)
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
) -> Schema:
    """
    Feed passages + (optional) current schema to the LLM, ask for additions.
    """
    prompt = build_messages(query, passages, current_schema)
    trimmed = utils.fit_prompt(prompt, truncate=True)
    llm_response = llm.generate(trimmed)
    return _parse_schema_from_llm(llm_response, query=query, max_keys_schema=max_keys_schema)


def evaluate_schema_convergence(prev: Schema, new: Schema, thresh: float = 0.9) -> bool:
    """
    Stop when Jaccard similarity ≥ thresh AND no new columns were added.
    """
    overlap = prev.jaccard(new)
    no_growth = len(new) == len(prev)
    return overlap >= thresh and no_growth


def discover_schema(
    query: str,
    documents: List[str],
    max_keys_schema : int,
    llm,
    retriever,
    batch_size: int = 4,
    max_iters: int = 6,
) -> Schema:
    """
    Main orchestration loop.
    """
    logging.info("Starting schema discovery…")
    schema = Schema(query=query, max_keys=max_keys_schema)
    # Simple batching; one doc may be chunked if > batch_size
    doc_iter = iter(documents)
    batches = [list(itertools.islice(doc_iter, batch_size)) for _ in range((len(documents)+batch_size-1)//batch_size)]

    for it, batch_docs in enumerate(batches[:max_iters], start=1):
        try:
            passages = select_relevant_content(batch_docs, query, retriever)
        except Exception as exc:
            print(f"Failed to retrieve {exc}")
            continue

        proposed = generate_schema(passages, query, max_keys_schema, schema, llm)
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
    retriever_cfg = cfg.get("retriever", {})
    max_keys_schema = cfg["max_keys_schema"]
    output_path = Path(cfg.get("output_path", "schema_output.json"))

    # Set up components
    llm_for_schema = utils.build_llm(backend_cfg)
    retriever = utils.build_retriever(retriever_cfg, llm_for_schema)

    # Load docs
    docs = load_documents(docs_path)

    # Run discovery
    start_time = time.time()
    schema = discover_schema(query, docs, max_keys_schema, llm_for_schema, retriever)
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

