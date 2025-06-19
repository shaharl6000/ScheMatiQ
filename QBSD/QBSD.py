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
from dataclasses import dataclass, field
from typing import List, Dict, Sequence, Tuple
import logging
import itertools
import json
from retrievers import EmbeddingRetriever
from llm_backends import LLMInterface, TogetherLLM, OpenAILLM

##############################################################################
# 1. Model-agnostic helper layers                                            #
##############################################################################


##############################################################################
# 2. Data classes                                                            #
##############################################################################

@dataclass
class Column:
    name: str
    rationale: str

    def __hash__(self):
        return hash(self.name.lower())

    def to_dict(self) -> Dict[str, str]:
        return {"column": self.name, "explanation": self.rationale}


@dataclass
class Schema:
    columns: List[Column] = field(default_factory=list)

    def merge(self, other: "Schema") -> "Schema":
        """
        Union by column *name*; prefer longer rationales.
        """
        combined = {c.name.lower(): c for c in self.columns}
        for c in other.columns:
            key = c.name.lower()
            if key not in combined or len(c.rationale) > len(combined[key].rationale):
                combined[key] = c
        return Schema(list(combined.values()))

    def jaccard(self, other: "Schema") -> float:
        a, b = {c.name.lower() for c in self.columns}, {c.name.lower() for c in other.columns}
        inter = len(a & b)
        union = len(a | b) or 1
        return inter / union

    # Convenience
    def __len__(self):              return len(self.columns)
    def __iter__(self):             return iter(self.columns)
    def __repr__(self):             return json.dumps([c.to_dict() for c in self.columns], indent=2)


##############################################################################
# 3. Core pipeline                                                           #
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


def _parse_schema_from_llm(text: str) -> Schema:
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
    return Schema(columns)


SYSTEM_MSG = """You are an expert data analyst. 
Given a user *query* and one or more *passages*, output ONLY a set
of column headers with a one-sentence explanation each. 
Use the format: <Column>: <Why this helps answer the query>. 
Return NO extra commentary.
"""

def generate_schema(
    passages: List[str],
    query: str,
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
    return _parse_schema_from_llm(llm_response)


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
    llm: LLMInterface,
    retriever: EmbeddingRetriever,
    batch_size: int = 4,
    max_iters: int = 6,
) -> Schema:
    """
    Main orchestration loop.
    """
    logging.info("Starting schema discovery…")
    schema = Schema()
    # Simple batching; one doc may be chunked if > batch_size
    doc_iter = iter(documents)
    batches = [list(itertools.islice(doc_iter, batch_size)) for _ in range((len(documents)+batch_size-1)//batch_size)]

    for it, batch_docs in enumerate(batches[:max_iters], start=1):
        passages = select_relevant_content(batch_docs, query, retriever)
        proposed = generate_schema(passages, query, schema, llm)
        merged = schema.merge(proposed)

        logging.info("Iteration %d — columns: %d → %d (J=%.2f)",
                     it, len(schema), len(merged), schema.jaccard(merged))

        if evaluate_schema_convergence(schema, merged):
            logging.info("Converged at iteration %d", it)
            return merged

        schema = merged  # update and continue

    return schema


##############################################################################
# 4. Example usage (wired to do nothing until you add concrete back-ends)    #
##############################################################################

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --------------------------------------------------------------------- #
    # ❶ Plug in your real retriever & LLM here.                              #
    # --------------------------------------------------------------------- #
    llm = LLMInterface()           # ← replace with OpenAI/Together/HF client
    retriever = EmbeddingRetriever()

    docs = ["<long doc1 …>", "<long doc2 …>", "<long doc3 …>"]
    question = "How did climate change affect wheat yields in Europe after 2010?"

    final_schema = discover_schema(question, docs, llm, retriever)
    print("\nFinal schema:\n", final_schema)