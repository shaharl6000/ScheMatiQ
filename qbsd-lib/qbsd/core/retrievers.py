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
import hashlib
from transformers import AutoTokenizer
import torch
import tiktoken

_enc = tiktoken.encoding_for_model("gpt-4o")
_SENT_SPLIT_RE = re.compile(r"(?<=[\.\?!])\s+")
JSON_FENCE = re.compile(r"```json(.*?)```", re.S | re.I)
FIRST_OBJ = re.compile(r"\{.*\}", re.S)
DEFAULT_MAX_INPUT_TOKENS = 7800
DEFAULT_TRUNCATE_WORDS   = 250
TOGETHER_LIMIT = 8193
SAFETY_MARGIN  = 512
CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

def safe_parse_json(text: str, fallback_key: str | None = None) -> Dict[str, Any]:
    """
    Try hard to parse a JSON object. If it fails and `fallback_key` is given,
    salvage that list with regex.
    """
    # ---- extract candidate block ----
    m = JSON_FENCE.search(text)
    candidate = m.group(1).strip() if m else (FIRST_OBJ.search(text).group(0).strip() if FIRST_OBJ.search(text) else text.strip())

    # ---- normalize ----
    candidate = CONTROL_CHARS.sub(" ", candidate)
    candidate = candidate.replace("“", '"').replace("”", '"').replace("’", "'")
    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)  # trailing commas

    # ---- first try ----
    try:
        return json.loads(candidate)
    except Exception:
        if not fallback_key:
            raise

    # ---- salvage path ----
    if fallback_key == "ranked":
        ranked = []
        # find {"i": 3, "score": 0.87} like objects
        for m in re.finditer(r'\{\s*"i"\s*:\s*(\d+).*?"score"\s*:\s*([0-9.]+)', candidate, re.S):
            ranked.append({"i": int(m.group(1)), "score": float(m.group(2))})
        if ranked:
            return {"ranked": ranked}

    raise ValueError(f"Cannot parse JSON:\n{candidate}")

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


def _approx_tokens(txt: str) -> int:
    return len(_enc.encode(txt))

##############################################################################
# Abstract base                                                              #
##############################################################################

class Retriever(ABC):
    def __init__(
        self,
        max_words_chunk: int = 512,
    ):
        self.max_words_chunk = max_words_chunk

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
                        packed.append(slice_words.split())
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

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", max_words: int = 512, batch_size: int = 32, k: int = 3,
                 device: str | None = None, enable_dynamic_k: bool = False,
                 dynamic_k_threshold: float = 0.65, dynamic_k_minimum: int = 2,
                 enable_embedding_cache: bool = True, max_cache_size: int = 100):
        super().__init__()
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

        # Dynamic k parameters
        self.enable_dynamic_k = enable_dynamic_k
        self.dynamic_k_threshold = dynamic_k_threshold
        self.dynamic_k_minimum = dynamic_k_minimum

        # Embedding cache: stores {doc_hash: {'passages': [...], 'embeddings': np.array}}
        # This avoids recomputing embeddings for the same document across multiple queries
        self.enable_embedding_cache = enable_embedding_cache
        self.max_cache_size = max_cache_size
        self._embedding_cache: Dict[str, Dict[str, Any]] = {}

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
        self.max_words_chunk = max(int(max_words / self.k), 128)


        print(f"-------Create EmbeddingRetriever, with model: {model_name}, k: {self.k}, "
              f"max_words: {self.max_words}, batch_size: {self.batch_size}, "
              f"embedding_cache: {enable_embedding_cache}")

    def _compute_doc_hash(self, doc: str) -> str:
        """Compute a hash for the document to use as cache key."""
        return hashlib.md5(doc.encode('utf-8', errors='ignore')).hexdigest()

    def _get_cached_embeddings(self, doc_hash: str) -> Dict[str, Any] | None:
        """Get cached passages and embeddings for a document hash."""
        return self._embedding_cache.get(doc_hash)

    def _cache_embeddings(self, doc_hash: str, passages: List[str], embeddings: Any) -> None:
        """Cache passages and embeddings for a document hash."""
        # Simple LRU-like eviction: if cache is full, remove oldest entry
        if len(self._embedding_cache) >= self.max_cache_size:
            # Remove the first (oldest) entry
            oldest_key = next(iter(self._embedding_cache))
            del self._embedding_cache[oldest_key]

        self._embedding_cache[doc_hash] = {
            'passages': passages,
            'embeddings': embeddings
        }

    def clear_cache(self) -> None:
        """Clear the embedding cache."""
        self._embedding_cache.clear()


    # ---- Retriever API -------------------------------------------------- #
    def _query_sentence_transformer(self, passages: list[str], question: str, k: int | None = None) -> list[str]:
        """Original method without caching - kept for backward compatibility."""
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

        if self.enable_dynamic_k:
            num_above = int((sims > self.dynamic_k_threshold).sum())
            chosen_k = max(self.dynamic_k_minimum, num_above)
            print(f"---- chosen k = {chosen_k}, num_above: {num_above}, thresh: {self.dynamic_k_threshold} ----")

        top = sims.argsort()[-chosen_k:][::-1]
        return [passages[i] for i in top]

    def _query_sentence_transformer_with_cache(
        self,
        passages: list[str],
        question: str,
        k: int | None = None,
        cached_embeddings: Any = None,
        doc_hash: str | None = None
    ) -> list[str]:
        """
        Query with caching support.

        If cached_embeddings is provided, skip passage embedding computation.
        Otherwise compute embeddings and cache them if doc_hash is provided.
        """
        # 1. Encode query (always needed, but fast ~10ms)
        q_emb = self.model.encode([_to_unicode(question)], show_progress_bar=False)[0]

        # 2. Get or compute passage embeddings
        if cached_embeddings is not None:
            p_embs = cached_embeddings
        else:
            # Compute passage embeddings (the expensive part: ~30 sec)
            p_embs = self.model.encode(
                passages,
                batch_size=self.batch_size,
                show_progress_bar=False,
            )
            # Cache for future queries on this document
            if self.enable_embedding_cache and doc_hash is not None:
                self._cache_embeddings(doc_hash, passages, p_embs)

        # 3. Cosine similarity (fast ~50ms)
        q_norm = q_emb / (np.linalg.norm(q_emb) + 1e-8)
        p_norm = p_embs / (
            np.linalg.norm(p_embs, axis=1, keepdims=True) + 1e-8
        )
        sims = p_norm @ q_norm

        chosen_k = self.k if k is None else k

        if self.enable_dynamic_k:
            num_above = int((sims > self.dynamic_k_threshold).sum())
            chosen_k = max(self.dynamic_k_minimum, num_above)
            print(f"---- chosen k = {chosen_k}, num_above: {num_above}, thresh: {self.dynamic_k_threshold} ----")

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
        # ---- 1. Check cache for single document case -----------------------
        # Caching works best when querying a single document multiple times
        cached_passages = None
        cached_embeddings = None
        doc_hash = None

        if self.enable_embedding_cache and len(docs) == 1 and not self.is_cross_encoder:
            doc_hash = self._compute_doc_hash(docs[0])
            cached = self._get_cached_embeddings(doc_hash)
            if cached is not None:
                cached_passages = cached['passages']
                cached_embeddings = cached['embeddings']
                # print(f"---- CACHE HIT: reusing embeddings for doc (hash: {doc_hash[:8]}...) ----")

        # ---- 2. collect & sanitise chunks (or use cached) ------------------
        if cached_passages is not None:
            passages = cached_passages
        else:
            passages: list[str] = []
            for d in docs:
                d = _to_unicode(d)  # make sure the doc itself is clean
                for chunk in self._improved_chunk(d):
                    # flatten lists / tuples
                    if isinstance(chunk, (list, tuple)):
                        passages.extend(_to_unicode(c) for c in chunk)
                    else:
                        passages.append(_to_unicode(chunk))

            passages = [p.strip() for p in passages if p and p.strip()]

        if not passages:
            return []

        if self.is_first_run:
            print(f"-------first query run, k: {k}")
            self.is_first_run = False

        # ---- 3. encode safely ----------------------------------------------
        if self.is_cross_encoder:
            return self._query_cross_encoder(passages, question, k)
        else:
            return self._query_sentence_transformer_with_cache(
                passages, question, k,
                cached_embeddings=cached_embeddings,
                doc_hash=doc_hash
            )



##############################################################################
# 2. Prompt-based implementation                                            #
##############################################################################

from qbsd.core.llm_backends import LLMInterface   # assumes the base class lives there


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
    respect_structure: bool = True,
    truncate_words_per_chunk: int = 512,
    max_input_tokens: int = 7000   #TODO: shahar to not truncate but split doc or something..

class PromptingRetriever(Retriever):
    """
    LLM-based retriever/reranker.

    `generate(messages, max_tokens, temperature, stop)` must be provided externally.
    """

    def __init__(self, generate: Callable[..., str], config: PromptingRetrieverConfig | None = None):
        super().__init__()
        self.generate = generate
        self.is_first_run = True
        self.cfg = config or PromptingRetrieverConfig()
        # sanitize config fields
        for attr, default in [("max_input_tokens", 7800), ("truncate_words_per_chunk", 250)]:
            val = getattr(self.cfg, attr, default)
            if isinstance(val, (list, tuple)):
                val = val[0]
            setattr(self.cfg, attr, int(val))

    # ---- Public API ----------------------------------------------------- #
    def query(self, docs: Sequence[str], question: str, k: int | None = None) -> List[str]:
        # 1. chunk
        passages: List[str] = []
        for d in docs:
            d = _to_unicode(d)
            for ch in self._improved_chunk(d,
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

        raw = self.generate(messages).strip()

        parsed = self._parse_rank_json(raw, len(passages), offset=0, total_len=len(passages))
        return self._scores_from_parsed(parsed, len(passages))

    def _call_llm(self, messages: List[Dict[str, str]]) -> str:
        """
        Send messages to the LLM, but first ensure `inputs + max_new_tokens <= 8193`.
        We only trim the *last user message* content.
        """
        hard_cap = TOGETHER_LIMIT - getattr(self.cfg, "max_new_tokens", 128) - SAFETY_MARGIN

        def msgs_tok_len(msgs: List[Dict[str, str]]) -> int:
            return sum(_approx_tokens(m["role"]) + _approx_tokens(m["content"]) for m in msgs)

        # Trim if needed
        if msgs_tok_len(messages) > hard_cap:
            # pick the last user message (fallback: last message)
            user_idx = next((i for i in reversed(range(len(messages)))
                             if messages[i]["role"] == "user"), len(messages) - 1)
            content = messages[user_idx]["content"]
            lines = content.splitlines()
            # drop 10% lines until under cap (at least 1 each loop)
            while lines and msgs_tok_len(messages) > hard_cap:
                drop = max(1, int(len(lines) * 0.1))
                lines = lines[:-drop]
                messages[user_idx]["content"] = "\n".join(lines)
            if not lines:
                raise ValueError("Unable to fit any payload under token budget.")

        return self.generate(messages).strip()


    def _truncate_chunk(self, text: str) -> str:
        limit = getattr(self.cfg, "truncate_words_per_chunk", 250)
        # normalize bad types
        if isinstance(limit, (list, tuple)):
            limit = limit[0]
        if limit is None:
            limit = 250
        limit = int(limit)

        words = text.split()
        if len(words) <= limit:
            return text
        return " ".join(words[:limit])

    def _batched_two_stage_rank(self, passages: List[str], query: str) -> np.ndarray:
        finals: List[Tuple[int, float]] = []
        # ---- 1st stage: token-bounded mini-batches ----
        start = 0
        while start < len(passages):
            # pack as many chunks as will fit into max_input_tokens
            cur_payload_chunks: List[str] = []
            cur_indices: List[int] = []
            token_budget = getattr(self.cfg, "max_input_tokens", DEFAULT_MAX_INPUT_TOKENS)
            while start < len(passages) and len(cur_indices) < self.cfg.batch_size:
                idx = start
                candidate = self._truncate_chunk(passages[idx].replace("\n", " ").strip())
                chunk_line = f"[{idx}] {candidate}"
                need = _approx_tokens(chunk_line)
                if need > token_budget and cur_indices:
                    break  # send what we have
                if need > token_budget and not cur_indices:
                    # single huge chunk: send anyway (or skip)
                    pass
                cur_payload_chunks.append(chunk_line)
                cur_indices.append(idx)
                token_budget -= need
                start += 1

            payload = "\n".join(cur_payload_chunks)
            messages = self._build_messages(query, payload)
            raw = self._call_llm(messages)
            parsed = self._parse_rank_json(raw, len(cur_indices), offset=cur_indices[0], total_len=len(passages))
            scored = [(d["i"], d["score"]) for d in parsed["ranked"]]

            scored = [(int(d["i"]), float(d["score"])) for d in parsed["ranked"]]
            keep = max(1, int(self.cfg.finalist_factor * self.cfg.k))
            finals.extend(sorted(scored, key=lambda x: x[1], reverse=True)[:keep])

        # ---- 2nd stage: rerank finalists if small enough ----
        unique_ids = sorted({i for i, _ in finals})
        scores = np.zeros(len(passages), dtype=float)

        if len(unique_ids) <= self.cfg.batch_size:
            sub_pass = [passages[i] for i in unique_ids]
            payload = self._build_chunk_payload(sub_pass, offset=0)
            messages = self._build_messages(query, payload)
            raw = self._call_llm(messages)
            parsed = self._parse_rank_json(raw, len(sub_pass), offset=0, total_len=len(passages))
            local = {int(d["i"]): float(d["score"]) for d in parsed["ranked"]}
            for li, sc in local.items():
                gi = unique_ids[li]
                scores[gi] = sc
        else:
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


    def _parse_rank_json(self, raw: str, n_passages: int, offset: int, total_len: int) -> Dict[str, Any]:
        parsed = safe_parse_json(raw)
        if "ranked" not in parsed or not isinstance(parsed["ranked"], list):
            raise ValueError(f"Bad LLM JSON (no 'ranked'): {raw}")

        fixed = []
        for d in parsed["ranked"]:
            i = int(d["i"])
            s = float(d["score"])

            # local -> global
            if 0 <= i < n_passages:
                gi = i + offset
            else:
                # maybe already global
                gi = i

            # clamp / skip invalid
            if 0 <= gi < total_len:
                fixed.append({"i": gi, "score": s})

        return {"ranked": fixed}

    @staticmethod
    def _scores_from_parsed(parsed: Dict[str, Any], total: int) -> np.ndarray:
        scores = np.zeros(total, dtype=float)
        for d in parsed["ranked"]:
            i = int(d["i"])
            s = float(d["score"])
            if 0 <= i < total:
                scores[i] = s
        return scores



def test_retriever_stability(
    retriever: EmbeddingRetriever,
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
        passages.extend(retriever._improved_chunk(d))

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