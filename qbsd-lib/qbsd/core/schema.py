from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import json

from sentence_transformers import SentenceTransformer, util

# -------------------------------------------------------------------- #
# Globals                                                              #
# -------------------------------------------------------------------- #
EMB_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
SIM_THRESHOLD = 0.9            # paraphrase deduplication
MAX_KEYS_DEFAULT = None         # unlimited unless specified


def _embed(text: str):
    """Small helper – add caching in production."""
    return EMB_MODEL.encode(text, normalize_embeddings=True)


# -------------------------------------------------------------------- #
# Data classes                                                         #
# -------------------------------------------------------------------- #
@dataclass
class Column:
    name: str
    rationale: str
    definition: str

    def to_dict(self) -> Dict[str, str]:
        return {"column": self.name, "explanation": self.rationale, "definition": self.definition}

    # Needed for `set` / dict keys
    def __hash__(self): return hash(self.name.lower())

@dataclass
class Schema:
    """
    Holds the evolving table schema.

    Parameters
    ----------
    query       – the user’s natural-language question (used for relevance).
    columns     – initial columns, if any.
    max_keys    – keep at most this many after every merge (None = unlimited).
    """
    query: str
    columns: List[Column] = field(default_factory=list)
    max_keys: Optional[int] = MAX_KEYS_DEFAULT

    # Internal (not part of JSON serialisation)
    _q_emb: list = field(init=False, repr=False)

    # -------------------------------------------------------------- #
    # Life-cycle                                                    #
    # -------------------------------------------------------------- #
    def __post_init__(self):
        self._q_emb = _embed(self.query)
        self._prune()  # Enforce max_keys limit immediately


    # ------------------------------------------------------------------ #
    # Merge with paraphrase-aware deduplication + pruning                 #
    # ------------------------------------------------------------------ #
    def merge(self, other: "Schema") -> "Schema":
        combined: Dict[str, Column] = {c.name: c for c in self.columns}
        combined_emb = {k: _embed(k) for k in combined}

        for cand in other.columns:
            # 1) Exact match (case-insensitive)
            key = next((k for k in combined if k.lower() == cand.name.lower()), None)

            # 2) Semantic match
            if key is None:
                c_emb = _embed(cand.name)
                key = next(
                    (
                        k
                        for k, e in combined_emb.items()
                        if util.cos_sim(c_emb, e) >= SIM_THRESHOLD
                    ),
                    None,
                )

            # 3) Update / insert
            if key:  # already present – maybe replace rationale
                if len(cand.rationale) > len(combined[key].rationale):
                    combined[key].rationale = cand.rationale
            else:    # brand-new column
                combined[cand.name] = cand
                combined_emb[cand.name] = _embed(cand.name)

        merged = Schema(columns=list(combined.values()), query=self.query, max_keys=self.max_keys)
        merged._prune()        # enforce key budget, if any
        return merged

    # ------------------------------------------------------------------ #
    # Utility methods                                                    #
    # ------------------------------------------------------------------ #
    def _prune(self, query: str | None = None) -> None:
        """
        Trim to `max_keys` columns, keeping those most semantically
        relevant to *query*.  If `query` is None, fall back to previous
        “longest rationale” logic.
        """
        if self.max_keys is None or len(self.columns) <= self.max_keys:
            return

        if query is None:
            # Fallback heuristic
            self.columns.sort(key=lambda c: len(c.rationale), reverse=True)
            self.columns = self.columns[: self.max_keys]
            return

        # --- query-aware scoring ------------------------------------------- #
        q_emb = _embed(query)

        def _score(col):
            col_emb = _embed(f"{col.name}: {col.rationale}")
            sim = util.cos_sim(q_emb, col_emb).item()  # relevance
            length_bonus = 0.05 * len(col.rationale) / 100.0  # tiny tie-break
            return sim + length_bonus

        self.columns.sort(key=_score, reverse=True)
        self.columns = self.columns[: self.max_keys]

    def jaccard(self, other: "Schema") -> float:
        a = {c.name.lower() for c in self.columns}
        b = {c.name.lower() for c in other.columns}
        inter = len(a & b)
        union = len(a | b) or 1
        return inter / union

    def to_llm_dict(self) -> List[Dict[str, str]]:
        """Serialize schema for LLM prompts, excluding internal fields like _q_emb."""
        return [
            {
                "name": col.name,
                "definition": col.definition,
                "rationale": col.rationale
            }
            for col in self.columns
        ]

    # Convenience dunders
    def __len__(self):              return len(self.columns)
    def __iter__(self):             return iter(self.columns)
    def __repr__(self):             return json.dumps(
        [c.to_dict() for c in self.columns], indent=2, ensure_ascii=False
    )
