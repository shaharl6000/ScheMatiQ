# retrievers.py
"""
Two retrieval strategies under a single interface:

    Retriever           – abstract base class
      ├─ EmbeddingRetriever   (dense-vector similarity)
      └─ PromptingRetriever   (LLM-extraction of relevant sentences)
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Sequence
import logging
import textwrap
import re

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
    from sentence_transformers import SentenceTransformer
except ImportError:
    # Delay hard dependency – only needed if you actually instantiate this class
    SentenceTransformer = None        # type: ignore


class EmbeddingRetriever(Retriever):
    """
    Fast dense-vector retriever (see earlier version for details).
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        passage_chars: int = 512,
        overlap: int = 50,
        batch_size: int = 32,
        device: str | None = None,
    ):
        if SentenceTransformer is None:
            raise ImportError(
                "sentence-transformers not installed. "
                "pip install 'sentence-transformers>=2.5 numpy'"
            )

        self.model = SentenceTransformer(model_name, device=device)
        self.passage_chars = passage_chars
        self.overlap = overlap
        self.batch_size = batch_size

    # ---- Retriever API -------------------------------------------------- #
    def query(self, docs: Sequence[str], question: str, k: int = 3) -> List[str]:
        passages = []
        for d in docs:
            passages.extend(self._chunk(d))

        if not passages:
            return []

        # Encode
        q_emb = self.model.encode([question], show_progress_bar=False)[0]
        p_embs = self.model.encode(passages, batch_size=self.batch_size,
                                   show_progress_bar=False)
        # Cosine sim
        q_norm = q_emb / (np.linalg.norm(q_emb) + 1e-8)
        p_norm = p_embs / (np.linalg.norm(p_embs, axis=1, keepdims=True) + 1e-8)
        sims = p_norm @ q_norm
        top = sims.argsort()[-k:][::-1]
        return [passages[i] for i in top]

    # ---- Helpers -------------------------------------------------------- #
    def _chunk(self, doc: str) -> List[str]:
        step = self.passage_chars - self.overlap
        return [doc[i:i + self.passage_chars].strip()
                for i in range(0, len(doc), step) if doc[i:i + self.passage_chars].strip()]


##############################################################################
# 2. Prompt-based implementation                                            #
##############################################################################

from llm_backends import LLMInterface   # assumes the base class lives there


class PromptingRetriever(Retriever):
    """
    Uses an LLM to *extract* relevant sentences from each document.

    Example
    -------
    >>> llm = OpenAILLM(model="gpt-4o-mini")        # any LLMInterface subclass
    >>> r = PromptingRetriever(llm)
    >>> passages = r.query(docs, "What is the F1 score of BERT?")
    """

    SYSTEM_TEMPLATE = (
        "You are a helpful research assistant. "
        "Given a QUESTION and a DOCUMENT, return ONLY the sentences from the "
        "document that help answer the question. Output each sentence on a "
        "separate line, with NO commentary, bullet points, numbering or extra text."
    )

    def __init__(
        self,
        llm: LLMInterface,
        sentences_per_doc: int = 3,
        max_doc_chars: int = 4000,
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
