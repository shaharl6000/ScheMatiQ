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

from __future__ import annotations
from typing import List, Dict, Sequence, Tuple, Any
import itertools
from retrievers import Retriever, EmbeddingRetriever, PromptingRetriever, test_retriever_stability
from llm_backends import LLMInterface, TogetherLLM, OpenAILLM
import argparse
import json
import logging
from pathlib import Path
from schema import Schema, Column

##############################################################################
# Core pipeline                                                           #
##############################################################################

def select_relevant_content(
    docs: Sequence[str],
    query: str,
    retriever: EmbeddingRetriever,
    passages_per_doc: int = 3,
) -> List[str]:
    """Return a flat list of passages drawn from all docs."""
    passages = []
    for doc in docs:
        passages.extend(retriever.query([doc], question=query, k=passages_per_doc))
    return passages


def _parse_schema_from_llm(text: str,
                           query: str,
                           max_keys_schema: int,
                           ) -> Schema:
    """
    Very lenient parser: lines that look like "Column: rationale".
    Adapt this to your favorite JSON-only format if you prefer.
    """
    columns = []
    for line in text.splitlines():
        if ":" in line:
            name, rationale = line.split(":", 1)
            name, rationale = name.strip(), rationale.strip()
            if name:
                columns.append(Column(name=name, rationale=rationale))
    return Schema(query=query, max_keys=max_keys_schema, columns=columns)


SYSTEM_MSG = """You are an expert data analyst. 
Given a user *query* and one or more *passages*, output ONLY a set
of column headers with a one-sentence explanation each. 
Use the format: <Column>: <Why this helps answer the query>. 
Return NO extra commentary.
"""

def generate_schema(
    passages: List[str],
    query: str,
    max_keys_schema: int,
    current_schema: Schema | None,
    llm: LLMInterface,
) -> Schema:
    """
    Feed passages + (optional) current schema to the LLM, ask for additions.
    """
    prompt_parts = [SYSTEM_MSG, f"User query:\n{query}\n"]
    if current_schema and len(current_schema):
        prompt_parts.append("Current draft schema:\n" + repr(current_schema) + "\n")
        prompt_parts.append("You may *extend* or *refine* this schema if needed.\n")
    joined_passages = "\n\n".join(passages)
    prompt_parts.append("Passages:\n" + joined_passages + "\n")
    prompt_parts.append("### Proposed schema\n")
    prompt = "\n".join(prompt_parts)

    llm_response = llm.generate(prompt)
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
    llm: LLMInterface,
    retriever: EmbeddingRetriever,
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
        passages = select_relevant_content(batch_docs, query, retriever)
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


def build_llm(cfg: Dict[str, Any]) -> LLMInterface:
    provider = cfg.get("provider", "together").lower()
    if provider == "together":
        return TogetherLLM(
            model=cfg.get("model", "mistralai/Mixtral-8x7B-Instruct-v0.1"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),               # falls back to env var
        )
    elif provider == "openai":
        return OpenAILLM(
            model=cfg.get("model", "gpt-4o-mini"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),
        )
    else:
        raise ValueError(f"Unknown backend provider: {provider}")


def build_retriever(cfg: Dict[str, Any], llm_for_prompting: LLMInterface = None) -> Retriever:
    rtype = cfg.get("type", "embedding").lower()
    if rtype == "embedding":
        return EmbeddingRetriever(
            model_name=cfg.get("model_name", "all-MiniLM-L6-v2"),
        )
    elif rtype == "prompting":
        if llm_for_prompting is None:
            raise ValueError("Unknown llm_for_prompting for prompting retriever")
        return PromptingRetriever(
            llm=llm_for_prompting,
            sentences_per_doc=cfg.get("sentences_per_doc", 3),
            max_doc_chars=cfg.get("max_doc_chars", 4000),
        )
    else:
        raise ValueError(f"Unknown retriever type: {rtype}")


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
    llm_for_schema = build_llm(backend_cfg)
    retriever = build_retriever(retriever_cfg, llm_for_schema)

    # Load docs
    docs = load_documents(docs_path)

    # Run discovery
    schema = discover_schema(query, docs, max_keys_schema, llm_for_schema, retriever)

    # Persist artefact
    save_schema(output_path, query, retriever_cfg, backend_cfg, str(docs_path), schema)

    print("\nFinal schema\n------------")
    for col in schema:
        print(f"• {col.name}: {col.rationale}")


if __name__ == "__main__":
    TEST = False

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Query-Based Schema Discovery runner")
    parser.add_argument("--config", required=True, type=Path, help="Path to JSON config")

    if TEST:
        # test:
        cfg_path = parser.parse_args().config
        cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
        logging.info("Loaded config from %s", cfg_path)

        query = cfg["query"]
        retriever = build_retriever(cfg.get("retriever", {}))
        docs = load_documents(Path(cfg["docs_path"]))

        test_retriever_stability(retriever, docs, query)
    else:
        # run QBSD
        main(parser.parse_args().config)

