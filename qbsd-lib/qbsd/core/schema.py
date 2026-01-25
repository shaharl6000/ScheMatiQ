from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
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
class ObservationUnit:
    """
    Defines what constitutes a single row (observation unit) in the extracted table.

    For example, if analyzing benchmark papers:
    - name: "Model-Benchmark Evaluation"
    - definition: "A single evaluation of one model on one benchmark dataset"
    - example_names: ["GPT-4 on MMLU", "Claude on HumanEval", "LLaMA on GSM8K"]

    This allows a single document to produce multiple rows when it contains
    multiple observation units (e.g., a paper comparing 5 models on 3 benchmarks
    could produce 15 rows).
    """
    name: str                                    # e.g., "Model-Benchmark Evaluation"
    definition: str                              # What constitutes one row
    example_names: Optional[List[str]] = None    # ["GPT-4 on MMLU", "Claude on HumanEval"]
    source_document: Optional[str] = None        # Document that helped define this unit
    discovery_iteration: Optional[int] = None    # Iteration when this unit was discovered

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        result = {
            "name": self.name,
            "definition": self.definition,
        }
        if self.example_names:
            result["example_names"] = self.example_names
        if self.source_document is not None:
            result["source_document"] = self.source_document
        if self.discovery_iteration is not None:
            result["discovery_iteration"] = self.discovery_iteration
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ObservationUnit":
        """Deserialize from dictionary."""
        return cls(
            name=data.get("name", "Document"),
            definition=data.get("definition", "Each document is treated as one observation unit"),
            example_names=data.get("example_names"),
            source_document=data.get("source_document"),
            discovery_iteration=data.get("discovery_iteration"),
        )

    @classmethod
    def default(cls) -> "ObservationUnit":
        """Return the default observation unit (one row per document)."""
        return cls(
            name="Document",
            definition="Each document is treated as one observation unit",
            example_names=None,
        )

    def is_default(self) -> bool:
        """Check if this is the default document-level observation unit."""
        return self.name == "Document" and "one observation unit" in self.definition.lower()


@dataclass
class Column:
    name: str
    rationale: str
    definition: str
    source_document: Optional[str] = None      # Document that first added this column
    discovery_iteration: Optional[int] = None  # Iteration when this column was discovered
    allowed_values: Optional[List[str]] = None  # Closed set of valid values for categorical columns
    auto_expand_threshold: Optional[int] = 2   # Auto-add new value if seen in N+ docs (None/0 = disabled)

    def to_dict(self) -> Dict[str, str]:
        result = {"column": self.name, "explanation": self.rationale, "definition": self.definition}
        # Include source tracking fields if set
        if self.source_document is not None:
            result["source_document"] = self.source_document
        if self.discovery_iteration is not None:
            result["discovery_iteration"] = self.discovery_iteration
        if self.allowed_values is not None:
            result["allowed_values"] = self.allowed_values
        if self.auto_expand_threshold is not None:
            result["auto_expand_threshold"] = self.auto_expand_threshold
        return result

    # Needed for `set` / dict keys
    def __hash__(self): return hash(self.name.lower())


@dataclass
class SchemaSnapshot:
    """Snapshot of schema state at a point during discovery."""
    iteration: int
    documents_processed: List[str]
    total_columns: int
    new_columns: List[str]  # Names of columns added in this iteration
    cumulative_documents: int = 0  # Total documents processed so far

    def to_dict(self) -> Dict:
        return {
            "iteration": self.iteration,
            "documents_processed": self.documents_processed,
            "total_columns": self.total_columns,
            "new_columns": self.new_columns,
            "cumulative_documents": self.cumulative_documents
        }


@dataclass
class SchemaEvolution:
    """Tracks how the schema evolved during discovery."""
    snapshots: List[SchemaSnapshot] = field(default_factory=list)
    column_sources: Dict[str, str] = field(default_factory=dict)  # column_name -> source_document

    def to_dict(self) -> Dict:
        return {
            "snapshots": [s.to_dict() for s in self.snapshots],
            "column_sources": self.column_sources
        }

    def add_snapshot(self, iteration: int, documents: List[str],
                     total_columns: int, new_columns: List[str],
                     cumulative_documents: int) -> None:
        """Add a new snapshot to the evolution history."""
        snapshot = SchemaSnapshot(
            iteration=iteration,
            documents_processed=documents,
            total_columns=total_columns,
            new_columns=new_columns,
            cumulative_documents=cumulative_documents
        )
        self.snapshots.append(snapshot)

    def record_column_source(self, column_name: str, source_document: str) -> None:
        """Record which document contributed a column."""
        if column_name not in self.column_sources:
            self.column_sources[column_name] = source_document


@dataclass
class Schema:
    """
    Holds the evolving table schema.

    Parameters
    ----------
    query       – the user's natural-language question (used for relevance).
    columns     – initial columns, if any.
    max_keys    – keep at most this many after every merge (None = unlimited).
    observation_unit – what constitutes a single row (default: one row per document).
    """
    query: str
    columns: List[Column] = field(default_factory=list)
    max_keys: Optional[int] = MAX_KEYS_DEFAULT
    observation_unit: Optional[ObservationUnit] = None  # Defines what each row represents

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
                # Merge allowed_values (union)
                if cand.allowed_values:
                    existing_av = combined[key].allowed_values or []
                    merged_av = list(dict.fromkeys(existing_av + cand.allowed_values))  # Preserve order, dedupe
                    combined[key].allowed_values = merged_av if merged_av else None
                # Keep existing auto_expand_threshold unless candidate has it and existing doesn't
                if cand.auto_expand_threshold is not None and combined[key].auto_expand_threshold is None:
                    combined[key].auto_expand_threshold = cand.auto_expand_threshold
            else:    # brand-new column
                combined[cand.name] = cand
                combined_emb[cand.name] = _embed(cand.name)

        # Preserve observation_unit from self (the base schema) or take from other if self doesn't have one
        obs_unit = self.observation_unit or other.observation_unit
        merged = Schema(columns=list(combined.values()), query=self.query, max_keys=self.max_keys, observation_unit=obs_unit)
        merged._prune()        # enforce key budget, if any
        return merged

    # ------------------------------------------------------------------ #
    # Utility methods                                                    #
    # ------------------------------------------------------------------ #
    def _prune(self, query: str | None = None) -> None:
        """
        Trim to `max_keys` columns, keeping those most semantically
        relevant to *query*.  If `query` is None, use self.query.
        Only fall back to "longest rationale" heuristic if no query is available.
        """
        if self.max_keys is None or len(self.columns) <= self.max_keys:
            return

        # Use provided query, or fall back to self.query
        if query is None:
            query = self.query

        if not query:
            # Only use fallback heuristic if there's truly no query available
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
        result = []
        for col in self.columns:
            col_dict = {
                "name": col.name,
                "definition": col.definition,
                "rationale": col.rationale
            }
            if col.allowed_values is not None:
                col_dict["allowed_values"] = col.allowed_values
            # Include auto_expand_threshold for LLM awareness
            if col.auto_expand_threshold is not None:
                col_dict["auto_expand_threshold"] = col.auto_expand_threshold
            result.append(col_dict)
        return result

    def to_full_dict(self) -> Dict[str, Any]:
        """Serialize schema including observation_unit for storage/export."""
        result = {
            "query": self.query,
            "columns": [col.to_dict() for col in self.columns],
            "max_keys": self.max_keys,
        }
        if self.observation_unit:
            result["observation_unit"] = self.observation_unit.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schema":
        """Deserialize schema from dictionary."""
        columns = [
            Column(
                name=col.get("column") or col.get("name"),
                definition=col.get("definition", ""),
                rationale=col.get("explanation", "") or col.get("rationale", ""),
                source_document=col.get("source_document"),
                discovery_iteration=col.get("discovery_iteration"),
                allowed_values=col.get("allowed_values"),
                auto_expand_threshold=col.get("auto_expand_threshold"),
            )
            for col in data.get("schema", data.get("columns", []))
        ]

        observation_unit = None
        if "observation_unit" in data:
            observation_unit = ObservationUnit.from_dict(data["observation_unit"])

        return cls(
            query=data.get("query", ""),
            columns=columns,
            max_keys=data.get("max_keys"),
            observation_unit=observation_unit,
        )

    # Convenience dunders
    def __len__(self):              return len(self.columns)
    def __iter__(self):             return iter(self.columns)
    def __repr__(self):             return json.dumps(
        [c.to_dict() for c in self.columns], indent=2, ensure_ascii=False
    )
