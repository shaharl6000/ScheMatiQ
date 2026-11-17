from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Sequence
import json
import re
import threading

from sentence_transformers import util

from schematiq.core.embedding_model import load_sentence_transformer, serialize_if_mps

# -------------------------------------------------------------------- #
# Globals                                                              #
# -------------------------------------------------------------------- #
# Defaults for column deduplication (merge) and relevance pruning (_prune).
# A Schema can override both per instance (see Schema.embedding_model and
# Schema.similarity_threshold), e.g. a multilingual model for non-English corpora.
EMB_MODEL_NAME = "all-MiniLM-L6-v2"
SIM_THRESHOLD = 0.9            # paraphrase deduplication
MAX_KEYS_DEFAULT = None         # unlimited unless specified

# The SentenceTransformer is loaded lazily on first embedding use rather than at
# import time. Importing this module (directly or transitively) no longer pulls
# in torch/transformers or downloads model weights, which keeps unrelated imports
# — tests, CLI tools, the FastAPI app startup — fast and dependency-light.
_EMB_MODEL = None
# Non-default models, loaded lazily and cached by name.
_EMB_MODELS: Dict[str, Any] = {}
# Guards lazy model initialization in _get_emb_model across worker threads.
_EMB_MODEL_LOCK = threading.Lock()


def _get_emb_model(model_name: Optional[str] = None):
    """Return a shared SentenceTransformer, loading it once on first use.

    ``None`` or ``EMB_MODEL_NAME`` returns the default model. Any other name is
    loaded once and cached, so schemas configured with the same model share it.
    """
    global _EMB_MODEL
    if model_name is None or model_name == EMB_MODEL_NAME:
        if _EMB_MODEL is None:
            with _EMB_MODEL_LOCK:
                if _EMB_MODEL is None:
                    _EMB_MODEL = load_sentence_transformer(EMB_MODEL_NAME)
        return _EMB_MODEL

    model = _EMB_MODELS.get(model_name)
    if model is None:
        with _EMB_MODEL_LOCK:
            model = _EMB_MODELS.get(model_name)
            if model is None:
                model = load_sentence_transformer(model_name)
                _EMB_MODELS[model_name] = model
    return model


def validate_similarity_threshold(value: Optional[float]) -> Optional[float]:
    """Return ``value`` as a float, or raise if it is not a cosine threshold in (0, 1]."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"similarity_threshold must be a number in (0, 1], got {value!r}")
    threshold = float(value)
    if not 0.0 < threshold <= 1.0:
        raise ValueError(f"similarity_threshold must be in (0, 1], got {threshold}")
    return threshold


_DIGIT_RUN = re.compile(r"\d+")


def _numbers_conflict(name_a: str, name_b: str) -> bool:
    """Return True when both names contain numbers and the numbers differ.

    Embedding similarity cannot separate numbered columns: names such as
    "justice_1" / "justice_2" or "lawyer_r1" / "lawyer_r2" score above
    SIM_THRESHOLD, yet they are different columns. Numbers are compared as
    integers, so "item_01" and "item_1" do not conflict.
    """
    nums_a = [int(n) for n in _DIGIT_RUN.findall(name_a)]
    nums_b = [int(n) for n in _DIGIT_RUN.findall(name_b)]
    return bool(nums_a) and bool(nums_b) and nums_a != nums_b


def _embed(text: str, model_name: Optional[str] = None):
    """Embed ``text`` with ``model_name`` (``None`` = the default model)."""
    model = _get_emb_model(model_name)
    with serialize_if_mps(model):
        return model.encode(
            text, normalize_embeddings=True, show_progress_bar=False
        )


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
        """Deserialize from dictionary.

        Raises:
            KeyError: If required fields 'name' or 'definition' are missing.
        """
        return cls(
            name=data["name"],  # Required - no default
            definition=data["definition"],  # Required - no default
            example_names=data.get("example_names"),
            source_document=data.get("source_document"),
            discovery_iteration=data.get("discovery_iteration"),
        )


@dataclass
class Column:
    name: str
    rationale: str = ""
    definition: str = ""
    source_document: Optional[str] = None      # Document that first added this column
    discovery_iteration: Optional[int] = None  # Iteration when this column was discovered
    allowed_values: Optional[List[str]] = None  # Closed set of valid values for categorical columns
    value_kind: Optional[str] = None           # category | identifier | free_text | number | date (from discovery)
    auto_expand_threshold: Optional[int] = 2   # Auto-add new value if seen in N+ docs (None/0 = disabled)
    locked: bool = False                       # User-seeded column protected from prune/merge changes

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"column": self.name, "explanation": self.rationale, "definition": self.definition}
        # Include source tracking fields if set
        if self.source_document is not None:
            result["source_document"] = self.source_document
        if self.discovery_iteration is not None:
            result["discovery_iteration"] = self.discovery_iteration
        if self.allowed_values is not None:
            result["allowed_values"] = self.allowed_values
        if self.value_kind is not None:
            result["value_kind"] = self.value_kind
        if self.auto_expand_threshold is not None:
            result["auto_expand_threshold"] = self.auto_expand_threshold
        if self.locked:
            result["locked"] = True
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Column":
        """Deserialize a Column from a dict, accepting both current and legacy keys.

        Legacy keys: ``column`` for ``name``, ``explanation`` for ``rationale``.
        """
        name = data.get("column") or data.get("name")
        if not name:
            raise KeyError("Column dict must contain 'name' or 'column' key")
        rationale = data.get("explanation") or data.get("rationale") or ""
        return cls(
            name=name,
            rationale=rationale,
            definition=data.get("definition", ""),
            source_document=data.get("source_document"),
            discovery_iteration=data.get("discovery_iteration"),
            allowed_values=data.get("allowed_values"),
            value_kind=data.get("value_kind"),
            auto_expand_threshold=data.get("auto_expand_threshold"),
            locked=data.get("locked", False),
        )

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
    contributing_files: List[str] = field(default_factory=list)
    non_contributing_files: List[str] = field(default_factory=list)
    skipped_batches_by_reason: Dict[str, int] = field(default_factory=dict)
    skipped_filenames: List[str] = field(default_factory=list)

    @property
    def total_skipped_batches(self) -> int:
        """Total number of skipped batches (or single-doc split leaves)."""
        return sum(self.skipped_batches_by_reason.values())

    def to_dict(self) -> Dict:
        return {
            "snapshots": [s.to_dict() for s in self.snapshots],
            "column_sources": self.column_sources,
            "skipped_batches_by_reason": dict(self.skipped_batches_by_reason),
            "skipped_filenames": list(self.skipped_filenames),
        }

    def record_skipped_batch(self, reason: str, filenames: Sequence[str]) -> None:
        """Record a skipped batch (or skipped single document) and its filenames."""
        self.skipped_batches_by_reason[reason] = (
            self.skipped_batches_by_reason.get(reason, 0) + 1
        )
        self.skipped_filenames.extend(filenames)

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
    embedding_model – SentenceTransformer used by merge and _prune
                      (None = EMB_MODEL_NAME). Runtime setting, not serialized.
    similarity_threshold – cosine similarity at which merge treats two column
                      names as paraphrases (None = SIM_THRESHOLD). Runtime
                      setting, not serialized.
    """
    query: str
    columns: List[Column] = field(default_factory=list)
    max_keys: Optional[int] = MAX_KEYS_DEFAULT
    observation_unit: Optional[ObservationUnit] = None  # Defines what each row represents
    embedding_model: Optional[str] = field(default=None, compare=False)
    similarity_threshold: Optional[float] = field(default=None, compare=False)

    # -------------------------------------------------------------- #
    # Life-cycle                                                    #
    # -------------------------------------------------------------- #
    def __post_init__(self):
        self.similarity_threshold = validate_similarity_threshold(self.similarity_threshold)
        # No embedding work here: _prune embeds the query only when it has to
        # cut columns, so constructing a Schema does not load the model.
        self._prune()  # Enforce max_keys limit immediately

    def apply_embedding_settings(
        self,
        embedding_model: Optional[str] = None,
        similarity_threshold: Optional[float] = None,
    ) -> "Schema":
        """Set the dedup/pruning model and threshold in place; ``None`` leaves a setting unchanged."""
        if embedding_model is not None:
            self.embedding_model = embedding_model
        if similarity_threshold is not None:
            self.similarity_threshold = validate_similarity_threshold(similarity_threshold)
        return self


    # ------------------------------------------------------------------ #
    # Merge with paraphrase-aware deduplication + pruning                 #
    # ------------------------------------------------------------------ #
    def merge(self, other: "Schema") -> "Schema":
        # Settings come from self, else from other, so a configured schema keeps
        # its settings whichever side of the merge it is on.
        model_name = self.embedding_model if self.embedding_model is not None else other.embedding_model
        threshold = (
            self.similarity_threshold
            if self.similarity_threshold is not None
            else other.similarity_threshold
        )
        effective_threshold = threshold if threshold is not None else SIM_THRESHOLD

        combined: Dict[str, Column] = {c.name: c for c in self.columns}
        combined_emb = {k: _embed(k, model_name) for k in combined}

        for cand in other.columns:
            # 1) Exact match (case-insensitive)
            key = next((k for k in combined if k.lower() == cand.name.lower()), None)

            # 2) Semantic match (never across names whose numbers differ)
            if key is None:
                c_emb = _embed(cand.name, model_name)
                key = next(
                    (
                        k
                        for k, e in combined_emb.items()
                        if not _numbers_conflict(cand.name, k)
                        and util.cos_sim(c_emb, e) >= effective_threshold
                    ),
                    None,
                )

            # 3) Update / insert
            if key:  # already present – maybe replace rationale
                existing = combined[key]
                if len(cand.rationale) > len(existing.rationale):
                    existing.rationale = cand.rationale
                if existing.value_kind is None and cand.value_kind is not None:
                    existing.value_kind = cand.value_kind
                # Merge allowed_values (union). A list the user set on a
                # locked column is kept as is, and a column whose values are
                # not categories (a name, an ID, free text) never gets one.
                not_categorical = existing.value_kind in ("identifier", "free_text")
                if (
                    cand.allowed_values
                    and not (existing.locked and existing.allowed_values)
                    and not not_categorical
                ):
                    existing_av = existing.allowed_values or []
                    merged_av = list(dict.fromkeys(existing_av + cand.allowed_values))  # Preserve order, dedupe
                    merged_av = [v for v in merged_av if v is not None]  # Filter nulls from LLM output
                    existing.allowed_values = merged_av if merged_av else None
                # Keep existing auto_expand_threshold unless candidate has it and existing doesn't
                if cand.auto_expand_threshold is not None and existing.auto_expand_threshold is None:
                    existing.auto_expand_threshold = cand.auto_expand_threshold
                # Locked columns: name and locked flag are preserved (already true since we're updating in place).
                # If existing is locked but candidate's definition is not empty and existing's is, fill it.
                if existing.locked and not existing.definition and cand.definition:
                    existing.definition = cand.definition
            else:    # brand-new column
                combined[cand.name] = cand
                combined_emb[cand.name] = _embed(cand.name, model_name)

        # Preserve observation_unit from self (the base schema) or take from other if self doesn't have one
        obs_unit = self.observation_unit or other.observation_unit
        merged = Schema(
            columns=list(combined.values()),
            query=self.query,
            max_keys=self.max_keys,
            observation_unit=obs_unit,
            embedding_model=model_name,
            similarity_threshold=threshold,
        )
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

        Locked columns are always retained, even if it would push the total
        beyond ``max_keys``. The budget cut applies only to unlocked columns.
        """
        if self.max_keys is None or len(self.columns) <= self.max_keys:
            return

        locked = [c for c in self.columns if c.locked]
        unlocked = [c for c in self.columns if not c.locked]

        # Budget for unlocked columns after reserving slots for locked ones.
        unlocked_budget = max(0, self.max_keys - len(locked))
        if unlocked_budget == 0:
            self.columns = locked
            return

        if query is None:
            query = self.query

        if not query:
            # Only use fallback heuristic if there's truly no query available
            unlocked.sort(key=lambda c: len(c.rationale), reverse=True)
            self.columns = locked + unlocked[:unlocked_budget]
            return

        # --- query-aware scoring ------------------------------------------- #
        q_emb = _embed(query, self.embedding_model)

        def _score(col):
            col_emb = _embed(f"{col.name}: {col.rationale}", self.embedding_model)
            sim = util.cos_sim(q_emb, col_emb).item()  # relevance
            length_bonus = 0.05 * len(col.rationale) / 100.0  # tiny tie-break
            return sim + length_bonus

        unlocked.sort(key=_score, reverse=True)
        self.columns = locked + unlocked[:unlocked_budget]

    def jaccard(self, other: "Schema") -> float:
        a = {c.name.lower() for c in self.columns}
        b = {c.name.lower() for c in other.columns}
        inter = len(a & b)
        union = len(a | b) or 1
        return inter / union

    def to_llm_dict(self) -> List[Dict[str, Any]]:
        """Serialize schema for LLM prompts (name, definition, rationale and metadata)."""
        result: List[Dict[str, Any]] = []
        for col in self.columns:
            col_dict: Dict[str, Any] = {
                "name": col.name,
                "definition": col.definition,
                "rationale": col.rationale
            }
            if col.allowed_values is not None:
                col_dict["allowed_values"] = col.allowed_values
            if col.locked:
                col_dict["locked"] = True
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
            Column.from_dict(col)
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
