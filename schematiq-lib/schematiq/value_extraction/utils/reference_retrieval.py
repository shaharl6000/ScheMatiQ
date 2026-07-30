"""Retrieval over an external reference document for value extraction.

An external reference is supplementary material the user attaches (e.g. a table
mapping judges to the president who appointed them, or a long prose handbook).
It can be far larger than the model's context window, so instead of injecting the
whole document into every extraction prompt we index it once and, per observation
unit, retrieve only the passages relevant to what we are trying to fill.

Design goals:
- Domain-agnostic: no assumption that the reference is a table, or that any
  particular column is a key. The only thing we rely on is what the extractor
  always knows anyway: the observation unit and the column(s) being filled.
- Structure-aware chunking that works for both tabular text (keep the header with
  each row group) and free prose (pack paragraphs to a word budget).
- Lexical BM25 ranking: strong, deterministic and explainable for the dominant
  case where a unit's identifier (a name, an id) appears in the relevant passage.
  The scorer is isolated behind ``ReferenceRetriever`` so a semantic/hybrid stage
  can be layered on later without changing callers.
"""

from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass, field
from typing import List, Optional

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")

# BM25 free parameters (standard Okapi defaults).
_BM25_K1 = 1.5
_BM25_B = 0.75


def _tokenize(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _looks_tabular(lines: List[str]) -> bool:
    """Heuristic: a delimited header plus rows with a consistent field count.

    Field counts come from ``csv.reader`` rather than a raw delimiter count, so a
    field that legitimately contains the delimiter inside quotes (e.g.
    ``"Doe, Jane"``) is parsed as one field instead of inflating the count. Naive
    counting makes such rows look inconsistent, misclassifies the whole file as
    prose, and drops header replication — which is what left every retrieved row
    without its column names.

    Deliberately conservative — if unsure, we treat the text as prose, which is
    always safe (chunking still works, just without header replication).
    """
    non_empty = [ln for ln in lines if ln.strip()]
    if len(non_empty) < 3:
        return False
    sample = non_empty[:20]
    for delim in (",", "\t", "|"):
        try:
            counts = [len(row) for row in csv.reader(sample, delimiter=delim)]
        except csv.Error:
            continue
        if not counts:
            continue
        # A real table has >= 2 fields per line (>= 1 delimiter) and a field
        # count that is stable across most of the sampled lines.
        if counts[0] >= 2 and sum(c == counts[0] for c in counts) >= max(3, int(0.7 * len(counts))):
            return True
    return False


def _word_count(text: str) -> int:
    return len(text.split())


def _split_record(record: str, max_words: int) -> List[str]:
    """Split one record into word-bounded pieces no longer than ``max_words``.

    A record is normally a single row or paragraph. But a delimited file that is
    misdetected as prose collapses to one record (there are no blank lines to
    split on), which would otherwise become a single chunk containing the whole
    file and blow past the model's token limit downstream. Splitting over-long
    records keeps every chunk bounded regardless of tabular/prose detection.
    """
    words = record.split()
    if len(words) <= max_words:
        return [record]
    return [" ".join(words[k:k + max_words]) for k in range(0, len(words), max_words)]


@dataclass
class ReferenceChunk:
    text: str
    index: int


def chunk_reference(
    text: str, max_words: int = 120, overlap_records: int = 1
) -> List[ReferenceChunk]:
    """Split reference text into retrievable chunks, structure-aware.

    Tabular text: the header line is kept out of the record stream and prepended
    to every emitted chunk, so each chunk stays interpretable on its own. Prose:
    split into paragraphs (blank-line separated), falling back to single lines.
    Records are packed up to ``max_words`` with a small record-level overlap so a
    fact spanning a boundary still appears whole in one chunk.
    """
    if not text or not text.strip():
        return []

    raw_lines = text.splitlines()
    tabular = _looks_tabular(raw_lines)

    header = ""
    if tabular:
        # First non-empty line is the header; records are the remaining rows.
        records: List[str] = []
        seen_header = False
        for ln in raw_lines:
            if not ln.strip():
                continue
            if not seen_header:
                header = ln.strip()
                seen_header = True
                continue
            records.append(ln.strip())
    else:
        # Prose: paragraphs first; if there are none, fall back to lines.
        paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        records = paras if paras else [ln.strip() for ln in raw_lines if ln.strip()]

    if not records:
        # Header-only tabular file, or nothing usable.
        return [ReferenceChunk(text=header, index=0)] if header else []

    # Split any record longer than the budget so a single oversized record (e.g.
    # a delimited file misread as prose, making the whole file one record) cannot
    # become one giant chunk.
    records = [piece for rec in records for piece in _split_record(rec, max_words)]

    header_words = _word_count(header)
    chunks: List[ReferenceChunk] = []
    i = 0
    n = len(records)
    while i < n:
        bucket: List[str] = []
        words = header_words
        j = i
        while j < n:
            rw = _word_count(records[j])
            if bucket and words + rw > max_words:
                break
            bucket.append(records[j])
            words += rw
            j += 1
        body = "\n".join(bucket)
        chunk_text = f"{header}\n{body}" if header else body
        chunks.append(ReferenceChunk(text=chunk_text, index=len(chunks)))
        if j >= n:
            break
        # Advance with a small overlap so boundary records aren't isolated.
        i = max(j - overlap_records, i + 1)
    return chunks


@dataclass
class ReferenceRetriever:
    """BM25 retriever over the chunks of a single reference document.

    Build once per reference (chunking + statistics are computed up front); call
    :meth:`retrieve` cheaply per observation unit.
    """

    text: str
    max_words: int = 120
    overlap_records: int = 1
    chunks: List[ReferenceChunk] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.chunks = chunk_reference(self.text, self.max_words, self.overlap_records)
        self._tokens: List[List[str]] = [_tokenize(c.text) for c in self.chunks]
        self._len: List[int] = [len(t) for t in self._tokens]
        self._avgdl: float = (sum(self._len) / len(self._len)) if self._len else 0.0
        # Document frequency per term.
        self._df: dict[str, int] = {}
        for toks in self._tokens:
            for term in set(toks):
                self._df[term] = self._df.get(term, 0) + 1
        self._n = len(self._tokens)

    def _idf(self, term: str) -> float:
        df = self._df.get(term, 0)
        if df == 0:
            return 0.0
        # BM25 idf with +1 to stay non-negative.
        return math.log(1 + (self._n - df + 0.5) / (df + 0.5))

    def _score(self, query_terms: List[str], doc_idx: int) -> float:
        if self._avgdl == 0:
            return 0.0
        toks = self._tokens[doc_idx]
        if not toks:
            return 0.0
        dl = self._len[doc_idx]
        tf: dict[str, int] = {}
        for t in toks:
            tf[t] = tf.get(t, 0) + 1
        score = 0.0
        for term in query_terms:
            f = tf.get(term, 0)
            if f == 0:
                continue
            idf = self._idf(term)
            denom = f + _BM25_K1 * (1 - _BM25_B + _BM25_B * dl / self._avgdl)
            score += idf * (f * (_BM25_K1 + 1)) / denom
        return score

    def retrieve(self, query: str, k: int = 5, rel_threshold: float = 0.3) -> List[str]:
        """Return up to ``k`` chunk texts most relevant to ``query``.

        Only chunks with a positive score are considered, and — after the best
        match — a chunk is kept only if its score is at least ``rel_threshold`` of
        the top score. This drops chunks that matched merely on ubiquitous terms
        (e.g. a column word that appears in every row), so a query that truly
        matches one region doesn't drag in unrelated rows just to fill ``k``.
        """
        if not self.chunks:
            return []
        query_terms = _tokenize(query)
        if not query_terms:
            return []
        scored = [
            (self._score(query_terms, idx), idx) for idx in range(len(self.chunks))
        ]
        scored = [s for s in scored if s[0] > 0]
        if not scored:
            return []
        scored.sort(key=lambda s: (-s[0], s[1]))
        top = scored[0][0]
        cutoff = top * rel_threshold
        kept = [(score, idx) for score, idx in scored[:k] if score >= cutoff]
        return [self.chunks[idx].text for _, idx in kept]


def build_reference_query(
    unit_descriptor: str,
    columns: Optional[List] = None,
) -> str:
    """Compose the retrieval query from the observation unit and the columns.

    The unit descriptor (its name/identifier, and definition if available) is the
    part the existing source-document query builder omits, yet it is exactly the
    join signal for a reference lookup. ``columns`` may be Column-like objects
    (with ``.name`` / ``.definition``) or plain strings.
    """
    parts: List[str] = []
    if unit_descriptor:
        parts.append(unit_descriptor)
    for col in columns or []:
        if isinstance(col, str):
            name, definition = col, None
        elif isinstance(col, dict):
            # build_val_messages passes column dicts keyed by "column"; Column.to_dict
            # uses "name". Accept either.
            name = col.get("column") or col.get("name")
            definition = col.get("definition")
        else:
            name = getattr(col, "name", None)
            definition = getattr(col, "definition", None)
        if name:
            parts.append(name)
        if definition:
            parts.append(definition)
    return " ".join(p for p in parts if p).strip()
