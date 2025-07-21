# retrievers.py
"""
Two retrieval strategies under a single interface:

    Retriever           – abstract base class
      ├─ EmbeddingRetriever   (dense-vector similarity)
      └─ PromptingRetriever   (LLM-extraction of relevant sentences)
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Sequence, Iterable, Any
import logging
import copy
import random
import re
from transformers import AutoTokenizer
import torch

_SENT_SPLIT_RE = re.compile(r"(?<=[\.\?!])\s+")


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

##############################################################################
# Abstract base                                                              #
##############################################################################

class Retriever(ABC):
    @abstractmethod
    def query(self, docs: Sequence[str], question: str, k: int = 3) -> List[str]:
        """Return up to *k* passages relevant to *question*."""
        ...


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
        max_words: int = 248,
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
        self.max_words_chunk = int(max_words / self.k)


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
            for chunk in self._chunk(d):
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


class PromptingRetriever(Retriever):
    """
    Uses an LLM to *extract* relevant sentences from each document.

    Example
    -------
     llm = OpenAILLM(model="gpt-4o-mini")        # any LLMInterface subclass
     r = PromptingRetriever(llm)
     passages = r.query(docs, "What is the F1 score of BERT?")
    """

    SYSTEM_TEMPLATE = (
        "You are a helpful research assistant. "
        "Given a QUESTION and a DOCUMENT, return ONLY the sentences from the "
        "document that help answer the question. Output each sentence on a "
        "separate line, with NO commentary, bullet points, numbering or extra text."
    )

    # taken from attribute-first paper
    content_selection_general_template = (
        "In this task, you are presented with several documents, which need to be summarized. As an intermediate step, "
        "you need to identify salient content within the documents. For each document, copy verbatim the salient spans, "
        "and use <SPAN_DELIM> as a delimiter between each consecutive span. "
        "IMPORTANT: The output must be of the format Document [<DOC_ID>]: <SPAN_DELIM>-delimited consecutive salient spans. "
        "IMPORTANT: Each salient content must be a single consecutive verbatim span from the corresponding passages. "
        "IMPORTANT: make sure the total number of copied words (from all documents) is around 200 words, and not more than 900."
    )

    def __init__(
        self,
        llm: LLMInterface,
        sentences_per_doc: int = 3,
        max_doc_chars: int = 1000,
    ):
        self.llm = llm
        self.sentences_per_doc = sentences_per_doc
        self.max_doc_chars = max_doc_chars

    # ---- Retriever API -------------------------------------------------- #
    def query(self, docs: Sequence[str], question: str, k: int = 3) -> List[str]:
        extracted: List[str] = []

        for doc in docs:
            doc_trimmed = doc[: self.max_doc_chars]
            prompt = self._build_prompt(question, doc_trimmed)
            try:
                raw = self.llm.generate(prompt, max_tokens=512, temperature=0)
            except Exception as e:
                logging.warning("LLM extraction failed: %s", e)
                continue

            sentences = self._parse_sentences(raw)
            extracted.extend(sentences[: self.sentences_per_doc])

        # Return the *k* longest (simple heuristic) to mimic “most informative”
        extracted.sort(key=len, reverse=True)
        return extracted[:k]

    # ---- Helpers -------------------------------------------------------- #
    def _build_prompt(self, question: str, document: str) -> str:
        return (
            f"{self.SYSTEM_TEMPLATE}\n\n"
            f"QUESTION:\n{question}\n\n"
            f"DOCUMENT:\n{document}\n\n"
            f"### Relevant sentences\n"
        )

    @staticmethod
    def _parse_sentences(text: str) -> List[str]:
        """
        Split LLM output into individual sentences and scrub mild punctuation /
        bullet-style clutter.
        """
        # Drop leading bullets / numbers on each line
        cleaned = re.sub(r"^[\s•\-\d\)\.]+", "", text, flags=re.MULTILINE)

        # Split on newlines *or* sentence boundaries like ". "
        parts = re.split(r"\n+|(?<=[.!?])\s+", cleaned)

        # Characters we want to strip from each side
        strip_chars = ' \t\r\n"\'“”‘’()`[]{}<>*•-.,;:'

        sentences = [
            s.strip(strip_chars)  # remove cruft
            for s in parts
            if len(s.split()) > 2  # keep non-trivial sentences
        ]
        return sentences


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