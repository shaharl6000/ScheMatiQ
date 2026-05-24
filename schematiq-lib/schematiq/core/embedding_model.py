"""Load SentenceTransformer models without noisy HuggingFace state-dict load reports."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


@contextmanager
def _suppress_transformers_load_noise():
    """Silence harmless HF load reports (e.g. embeddings.position_ids UNEXPECTED)."""
    try:
        from transformers.utils import logging as hf_logging
    except ImportError:
        yield
        return

    prev_verbosity = hf_logging.get_verbosity()
    hf_logging.set_verbosity_error()
    transformers_logger = logging.getLogger("transformers")
    prev_level = transformers_logger.level
    transformers_logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        hf_logging.set_verbosity(prev_verbosity)
        transformers_logger.setLevel(prev_level)


def load_sentence_transformer(
    model_name: str = "all-MiniLM-L6-v2", **kwargs: Any
) -> SentenceTransformer:
    """Load a SentenceTransformer, suppressing known-harmless HF checkpoint warnings."""
    from sentence_transformers import SentenceTransformer

    with _suppress_transformers_load_noise():
        return SentenceTransformer(model_name, **kwargs)
