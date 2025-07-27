# retrievers.py
"""
Two retrieval strategies under a single interface:

    Retriever           – abstract base class
      ├─ EmbeddingRetriever   (dense-vector similarity)
      └─ PromptingRetriever   (LLM-extraction of relevant sentences)
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Sequence, Iterable, Any, Dict, Callable, Tuple
from dataclasses import dataclass
import copy
import random
import re
import json
from transformers import AutoTokenizer
import torch

_SENT_SPLIT_RE = re.compile(r"(?<=[\.\?!])\s+")
JSON_FENCE = re.compile(r"```json(.*?)```", re.S | re.I)
FIRST_OBJ = re.compile(r"\{.*\}", re.S)

def safe_parse_json(text: str) -> Dict[str, Any]:
    m = JSON_FENCE.search(text)
    if m:
        candidate = m.group(1).strip()
    else:
        m = FIRST_OBJ.search(text)
        candidate = m.group(0).strip() if m else ""
    # lightweight fix
    candidate = candidate.replace("\n", " ").strip()
    return json.loads(candidate)

def _to_unicode(x: Any) -> str:
    """
    Return a well‑formed Unicode string.
    • bytes          → decoded    (try utf‑8, then latin‑1, finally ignore errors)
    • everything else→ str(x)
    """
    if isinstance(x, bytes):
        for enc in ("utf-8", "latin-1"):
            try:
                return x.decode(enc)
            except UnicodeDecodeError:
                pass
        return x.decode("utf-8", errors="ignore")
    return str(x)

_SECTION_RE = re.compile(
    r"""(?imx)
    ^\s*(?:                # common section markers
        \d{1,2}\.?\s+|     # "1 "  or "1. "
        \\section\*?\{|    # LaTeX \section{...}
        \\subsection\*?\{| # LaTeX \subsection{...}
        \#\s+|             # Markdown #
        (?:abstract|introduction|related\ work|background|method[s]?|
           approach|dataset[s]?|experiment[s]?|results?|discussion|
           conclusion[s]?|limitations?|future\ work|appendix)\b
    )
    """,
    re.I | re.M,
)

_TABLE_FIG_RE = re.compile(r"(?im)^\s*(table|figure|algorithm)\s+\d+[:.]")

def _split_by_structure(doc: str) -> List[str]:
    """
    Heuristically split the document into coarse 'blocks' (sections, tables, etc.).
    Fallback to the whole doc if no matches found.
    """
    # Insert a delimiter before lines that look like headings/captions.
    marked = []
    for line in doc.splitlines():
        if _SECTION_RE.match(line) or _TABLE_FIG_RE.match(line):
            marked.append("§§§" + line)
        else:
            marked.append(line)
    chunks = "\n".join(marked).split("§§§")
    return [c.strip() for c in chunks if c.strip()]

def _sliding_windows(words: List[str], max_len: int, overlap: int) -> Iterable[List[str]]:
    """
    Yield word windows with overlap. max_len >= 1, 0 <= overlap < max_len
    """
    if not words:
        return
    step = max_len - overlap
    if step <= 0:
        step = max_len  # avoid infinite loop
    for start in range(0, len(words), step):
        window = words[start:start + max_len]
        if window:
            yield window
##############################################################################
# Abstract base                                                              #
##############################################################################

class Retriever(ABC):
    @abstractmethod
    def query(self, docs: Sequence[str], question: str, k: int = 3) -> List[str]:
        """Return up to *k* passages relevant to *question*."""
        ...

    def _improved_chunk(self, doc: str, *, overlap_words: int = 64, respect_structure: bool = True) -> List[str]:
        """
        Split *doc* into passages of ≤ self.max_words_chunk words.
        Improvements:
        - Optional sliding window with `overlap_words`.
        - Optional structure-aware pre-split (sections, tables, captions).
        """
        max_w = self.max_words_chunk  # per-passage budget
        ovlp = max(0, min(overlap_words, max_w - 1))  # keep sane

        # 1. High-level split by structure
        blocks = _split_by_structure(doc) if respect_structure else [doc]

        out: List[str] = []

        for block in blocks:
            # 2. Sentence split → word budget packing (like your original), but keep windows
            sentences = _split_sentences(block)
            sent_words = [s.split() for s in sentences]

            # Pack sentences into word lists no longer than max_w (like your current logic)
            packed: List[List[str]] = []
            current: List[str] = []
            count = 0
            for wlist in sent_words:
                wlen = len(wlist)
                if wlen <= max_w:
                    if count + wlen <= max_w:
                        current.extend(wlist)
                        count += wlen
                    else:
                        if current:
                            packed.append(current)
                        current = wlist[:]
                        count = wlen
                else:
                    # sentence longer than budget → slice
                    if current:
                        packed.append(current)
                        current, count = [], 0
                    # use original helper to slice long sentences
                    for slice_words in _yield_word_slices(wlist, max_w):
                        packed.append(slice_words)
            if current:
                packed.append(current)

            # 3. Sliding windows over packed blocks
            #    First flatten each packed list then window it
            for p in packed:
                for win in _sliding_windows(p, max_w, ovlp):
                    out.append(" ".join(win).strip())

        return out


##############################################################################
# 1. Dense-embedding implementation (identical logic, just subclassed)       #
##############################################################################

try:
    import numpy as np
    from sentence_transformers import SentenceTransformer, CrossEncoder
except ImportError:
    # Delay hard dependency – only needed if you actually instantiate this class
    SentenceTransformer = None        # type: ignore


def _split_sentences(text: str) -> List[str]:
    """Very lightweight sentence tokenizer (avoids NLTK install)."""
    return [s.strip() for s in _SENT_SPLIT_RE.split(text.strip()) if s.strip()]


def _yield_word_slices(words: List[str], size: int) -> Iterable[str]:
    """Yield sub-lists of <size> words (used for over-long sentences)."""
    for i in range(0, len(words), size):
        piece = words[i: i + size]
        if piece:
            yield " ".join(piece)


class EmbeddingRetriever(Retriever):
    """
    Dense-vector retriever that chunks documents by **word count**, not characters.
    Each passage contains up to `max_words` words (tries to fill the budget as
    tightly as possible).
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        max_words: int = 512,
        batch_size: int = 32,
        k: int = 3,
        device: str | None = None,
    ):
        if SentenceTransformer is None:
            raise ImportError(
                "sentence-transformers not installed. "
                "pip install 'sentence-transformers>=2.5 numpy'"
            )

        self.k = k
        self.is_cross_encoder = False
        self.model_name = model_name
        self.batch_size = batch_size
        self.is_first_run = True

        if "Qwen" in model_name:
            self.model = CrossEncoder(model_name, device="cuda", trust_remote_code=True, max_length=8192)
            self.is_cross_encoder = True
            tokenizer = AutoTokenizer.from_pretrained(self.model_name, trust_remote_code=True)
            if tokenizer.pad_token_id is None:
                tokenizer.pad_token = tokenizer.eos_token
            self.model.tokenizer = tokenizer
            self.model.model.config.pad_token_id = tokenizer.pad_token_id
            self.batch_size = 2

        else:
            self.model = SentenceTransformer(model_name, device=device)
        self.max_words = max_words
        self.max_words_chunk = min(int(max_words / self.k), 128)


        print(f"-------Create EmbeddingRetriever, with model: {model_name}, k: {self.k}, "
              f"max_words: {self.max_words}, batch_size: {self.batch_size}")


    # ---- Retriever API -------------------------------------------------- #
    def _query_sentence_transformer(self,passages: list[str], question: str, k: int | None = None) -> list[str]:
        q_emb = self.model.encode([_to_unicode(question)], show_progress_bar=False)[0]
        p_embs = self.model.encode(
            passages,
            batch_size=self.batch_size,
            show_progress_bar=False,
        )

        # Cosine similarity
        q_norm = q_emb / (np.linalg.norm(q_emb) + 1e-8)
        p_norm = p_embs / (
            np.linalg.norm(p_embs, axis=1, keepdims=True) + 1e-8
        )
        sims = p_norm @ q_norm
        chosen_k = self.k if k is None else k
        top = sims.argsort()[-chosen_k:][::-1]
        return [passages[i] for i in top]

    def _rerank_passages(self, pairs: list[tuple[str, str]], chunk: int) -> np.ndarray:
        """
        Call self.model.predict on <chunk>‑sized slices to avoid GPU OOM.
        Clears CUDA cache after each slice.
        """
        scores: list[float] = []
        for i in range(0, len(pairs), chunk):
            sub = pairs[i: i + chunk]
            # len(sub) may be smaller than chunk for the last slice
            scores.extend(
                self.model.predict(
                    sub,
                    batch_size=len(sub),  # safe even without pad token
                    show_progress_bar=False,
                )
            )
            if torch.cuda.is_available():
                torch.cuda.empty_cache()  # reclaim reserved-but-unused blocks
        return np.asarray(scores)

    # --------------------------------------------------------------------- #
    # Re‑written _query_cross_encoder using the helper
    # --------------------------------------------------------------------- #
    def _query_cross_encoder(
            self,
            passages: list[str],
            question: str,
            k: int | None = None,
    ) -> list[str]:
        pairs = [(question, p) for p in passages]

        scores = self._rerank_passages(pairs, self.batch_size)

        chosen_k = min(self.k if k is None else k, len(passages))
        top_idx = scores.argsort()[-chosen_k:][::-1]
        return [passages[i] for i in top_idx]


    def query(self, docs: Sequence[str], question: str, k: int | None = None) -> list[str]:
        # ---- 1. collect & sanitise chunks ----------------------------------
        passages: list[str] = []
        for d in docs:
            d = _to_unicode(d)  # 🆕 make sure the doc itself is clean
            for chunk in self._improved_chunk(d):  #self._chunk(d):
                # flatten lists / tuples
                if isinstance(chunk, (list, tuple)):
                    passages.extend(_to_unicode(c) for c in chunk)
                else:
                    passages.append(_to_unicode(chunk))  # 🆕

        passages = [p.strip() for p in passages if p and p.strip()]
        if not passages:
            return []

        if self.is_first_run:
            print(f"-------first query run, k: {k}")
            self.is_first_run = False

        # ---- 2. encode safely ----------------------------------------------
        if self.is_cross_encoder:
            return self._query_cross_encoder(passages, question, k)
        else:
            return self._query_sentence_transformer(passages, question, k)

    # ---- Helpers -------------------------------------------------------- #

    def _chunk(self, doc: str) -> List[str]:
        """
        Split *doc* into passages of ≤ `self.max_words` words, trying to
        keep each passage close to the limit by concatenating sentences.
        """
        sentences = _split_sentences(doc)
        chunks: List[str] = []
        current: List[str] = []
        count = 0

        for sent in sentences:
            words = sent.split()
            wlen = len(words)

            # Sentence itself fits budget → accumulate
            if wlen <= self.max_words_chunk:
                if count + wlen <= self.max_words_chunk:
                    current.append(sent)
                    count += wlen
                else:
                    # flush and start new passage
                    if current:
                        chunks.append(" ".join(current).strip())
                    current = [sent]
                    count = wlen
            else:
                # Sentence longer than budget → slice by words
                if current:
                    chunks.append(" ".join(current).strip())
                    current, count = [], 0
                chunks.extend(_yield_word_slices(words, self.max_words_chunk))

        if current:
            chunks.append(" ".join(current).strip())

        return chunks


##############################################################################
# 2. Prompt-based implementation                                            #
##############################################################################

from llm_backends import LLMInterface   # assumes the base class lives there


LLM_RANK_INSTRUCTION = """
You are *RetrievalJudgeLLM*. Your goal is to pick and rank the text chunks that
will best help another model derive **informative aspects/columns** for a table
schema answering the user's query.

### INPUT
- QUERY: the user's information need
- CHUNKS: numbered passages from papers

### WHAT TO DO
1. For each chunk, judge how *useful* it is for identifying important, answerable
   aspects (e.g., task, dataset, metrics, model details, hyperparameters, results).
2. Assign a score 0–1 (float). 1 = essential, 0 = useless.
3. Prefer chunks with concrete definitions, enumerations, tables, metrics, or
   explicit descriptions of entities/attributes.
4. Ignore chunks with only generic motivation or unrelated content.

### OUTPUT (ONLY JSON)
{
  "ranked": [
    {"i": <chunk_index>, "score": <float 0-1>},
    ...
  ]
}
No extra keys, no comments.
""".strip()

# --------------------------------------------------------------------------
# Retriever Class
# --------------------------------------------------------------------------

@dataclass
class PromptingRetrieverConfig:
    k: int = 5
    max_new_tokens: int = 512
    temperature: float = 0.0
    stop: List[str] | None = None
    batch_size: int = 40          # how many passages to present per LLM call
    finalist_factor: float = 2.0  # keep top k*factor from each batch for final rerank
    mode: str = "sampled_rank"    # "rank" (single call) or "sampled_rank"
    overlap_words: int = 64
    respect_structure: bool = True

class PromptingRetriever(Retriever):
    """
    LLM-based retriever/reranker.

    `generate(messages, max_tokens, temperature, stop)` must be provided externally.
    """

    def __init__(self,
                 generate: Callable[..., str],
                 config: PromptingRetrieverConfig | None = None):
        self.generate = generate
        self.cfg = config or PromptingRetrieverConfig()
        self.is_first_run = True

    # ---- Public API ----------------------------------------------------- #
    def query(self, docs: Sequence[str], question: str, k: int | None = None) -> List[str]:
        # 1. chunk
        passages: List[str] = []
        for d in docs:
            d = _to_unicode(d)
            for ch in self._chunk(d,
                                  overlap_words=self.cfg.overlap_words,
                                  respect_structure=self.cfg.respect_structure):
                if isinstance(ch, (list, tuple)):
                    passages.extend(_to_unicode(c) for c in ch)
                else:
                    passages.append(_to_unicode(ch))
        passages = [p.strip() for p in passages if p.strip()]
        if not passages:
            return []

        if self.is_first_run:
            print(f"PromptingRetriever first run: {len(passages)} passages, mode={self.cfg.mode}")
            self.is_first_run = False

        # 2. rank
        scores = self._rank_passages_with_llm(passages, question)

        # 3. pick top-k
        chosen_k = min(k or self.cfg.k, len(passages))
        top_idx = np.argsort(scores)[-chosen_k:][::-1]
        return [passages[i] for i in top_idx]

    # ---- Core ranking logic -------------------------------------------- #
    def _rank_passages_with_llm(self, passages: List[str], query: str) -> np.ndarray:
        if self.cfg.mode == "rank":
            return self._single_llm_rank(passages, query)
        else:
            return self._batched_two_stage_rank(passages, query)

    def _single_llm_rank(self, passages: List[str], query: str) -> np.ndarray:
        payload = self._build_chunk_payload(passages)
        messages = self._build_messages(query, payload)

        raw = self.generate(
            messages=messages,
            max_tokens=self.cfg.max_new_tokens,
            temperature=self.cfg.temperature,
            stop=self.cfg.stop or []
        ).strip()

        parsed = self._parse_rank_json(raw, len(passages))
        return self._scores_from_parsed(parsed, len(passages))

    def _batched_two_stage_rank(self, passages: List[str], query: str) -> np.ndarray:
        bs = self.cfg.batch_size
        finals: List[Tuple[int, float]] = []

        # 1st stage: batch-wise scoring
        for start in range(0, len(passages), bs):
            end = min(start + bs, len(passages))
            sub_pass = passages[start:end]
            payload = self._build_chunk_payload(sub_pass, offset=start)
            messages = self._build_messages(query, payload)
            raw = self.generate(
                messages=messages,
                max_tokens=self.cfg.max_new_tokens,
                temperature=self.cfg.temperature,
                stop=self.cfg.stop or []
            ).strip()
            parsed = self._parse_rank_json(raw, len(sub_pass), offset=start)
            scored = [(d["i"], float(d["score"])) for d in parsed["ranked"]]
            # keep top local finalists
            keep = max(1, int(self.cfg.finalist_factor * self.cfg.k))
            scored_sorted = sorted(scored, key=lambda x: x[1], reverse=True)[:keep]
            finals.extend(scored_sorted)

        # 2nd stage: rerank finalists if we reduced
        unique_final_ids = sorted({i for i, _ in finals})
        if len(unique_final_ids) <= self.cfg.batch_size:
            # direct rerank
            sub_pass = [passages[i] for i in unique_final_ids]
            payload = self._build_chunk_payload(sub_pass, offset=0)
            messages = self._build_messages(query, payload)
            raw = self.generate(
                messages=messages,
                max_tokens=self.cfg.max_new_tokens,
                temperature=self.cfg.temperature,
                stop=self.cfg.stop or []
            ).strip()
            parsed = self._parse_rank_json(raw, len(sub_pass), offset=0)
            reranked_scores = {d["i"]: float(d["score"]) for d in parsed["ranked"]}
            scores = np.zeros(len(passages), dtype=float)
            for i, s in reranked_scores.items():
                scores[unique_final_ids[i]] = s
        else:
            # No second pass; just aggregate first-pass scores
            scores = np.zeros(len(passages), dtype=float)
            for i, s in finals:
                scores[i] = max(scores[i], s)

        return scores

    # ---- Prompt builders & parsing -------------------------------------- #
    def _build_messages(self, query: str, chunk_payload: str) -> List[Dict[str, str]]:
        return [
            {"role": "system", "content": LLM_RANK_INSTRUCTION},
            {
                "role": "user",
                "content": f"QUERY:\n{query}\n\nCHUNKS:\n{chunk_payload}\n\nReturn JSON only."
            },
        ]

    @staticmethod
    def _build_chunk_payload(passages: List[str], offset: int = 0) -> str:
        lines = []
        for i, p in enumerate(passages, start=offset):
            # Escape double quotes minimally
            p_clean = p.replace("\n", " ").strip()
            lines.append(f"[{i}] {p_clean}")
        return "\n".join(lines)

    @staticmethod
    def _parse_rank_json(raw: str, n_passages: int, offset: int = 0) -> Dict[str, Any]:
        parsed = safe_parse_json(raw)
        if "ranked" not in parsed or not isinstance(parsed["ranked"], list):
            raise ValueError(f"Bad LLM JSON (no 'ranked'): {raw}")
        # basic check
        for d in parsed["ranked"]:
            if "i" not in d or "score" not in d:
                raise ValueError(f"Rank item missing keys: {d}")
            if not (offset <= int(d["i"]) < offset + n_passages):
                # allow 0-based indexing across docs
                pass
        return parsed

    @staticmethod
    def _scores_from_parsed(parsed: Dict[str, Any], total: int) -> np.ndarray:
        scores = np.zeros(total, dtype=float)
        for d in parsed["ranked"]:
            i = int(d["i"])
            s = float(d["score"])
            if 0 <= i < total:
                scores[i] = s
        return scores

    # ---- Chunker wrapper (reuse your improved version) ------------------- #
    def _chunk(self, doc: str, *, overlap_words: int, respect_structure: bool) -> List[str]:
        # assuming you already implemented this improved version:
        return self._improved_chunk(doc, overlap_words=overlap_words, respect_structure=respect_structure)


def test_retriever_stability(
    retriever: Retriever,
    docs: Sequence[str],
    question: str,
    k: int = 5,
    seed: int = 42,
) -> None:
    """
    1. Computes cosine-similarity scores for *all* passages.
    2. Prints the top-k and bottom-k passages.
    3. Shuffles `docs` and re-runs the query.
    4. Asserts that the same top-k passages are returned (order-agnostic).

    Raises
    ------
    AssertionError
        If shuffling causes a change in the retrieved top-k set.
    """

    # --- helper to get all passages + sims ------------------------------- #
    passages = []
    for d in docs:
        passages.extend(retriever._chunk(d))

        if not passages:
            print("⚠️  No passages produced from the documents.")
            return

        q_emb = retriever.model.encode([question], show_progress_bar=False)[0]
        p_embs = retriever.model.encode(
            passages, batch_size=retriever.batch_size, show_progress_bar=False
        )

        q_norm = q_emb / (np.linalg.norm(q_emb) + 1e-8)
        p_norm = p_embs / (np.linalg.norm(p_embs, axis=1, keepdims=True) + 1e-8)
        sims = p_norm @ q_norm

        # --- top & bottom k --------------------------------------------------- #
        top_idx = sims.argsort()[-k:][::-1]
        bot_idx = sims.argsort()[:k]

        top_passages = [passages[i] for i in top_idx]
        bot_passages = [passages[i] for i in bot_idx]

        # print("\n🏆 Top-{} passages:".format(k))
        # print("\n---\n".join(top_passages))
        #
        # print("\n🪫 Bottom-{} passages:".format(k))
        # print("\n---\n".join(bot_passages))

        # --- shuffle docs & re-query ----------------------------------------- #
        bot_and_top = top_passages + bot_passages
        shuffled_docs = copy.copy(bot_and_top)
        # print(f"num of shuffled_docs {len(shuffled_docs)}")
        random.Random(seed).shuffle(shuffled_docs)
        # print(f"query is: {question}")

        top_after_shuffle = retriever.query(shuffled_docs, question, k=k)

        # --- check stability -------------------------------------------------- #
        if set(top_passages) == set(top_after_shuffle):
            print("\n✅  Retrieval stable after shuffling.")
        else:
            print("\n❌  Top-{} passages changed after shuffling!".format(k))
            diff_old = set(top_passages) - set(top_after_shuffle)
            diff_new = set(top_after_shuffle) - set(top_passages)
            print("   Lost after shuffle:", diff_old or "—")
            print("   Gained after shuffle:", diff_new or "—")
            raise AssertionError("Retriever is not order-invariant!")