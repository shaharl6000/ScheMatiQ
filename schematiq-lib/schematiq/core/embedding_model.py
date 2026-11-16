"""Load SentenceTransformer models without noisy HuggingFace state-dict load reports."""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

# Serialize SentenceTransformer instantiation across worker threads to avoid
# concurrent HuggingFace logger mutation and PyTorch meta-tensor initialization races.
_LOAD_LOCK = threading.Lock()


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


def is_mps_model(model: Any) -> bool:
    """Return True when ``model`` is on an Apple Silicon ``mps`` device."""
    device = getattr(model, "device", None)
    if device is None:
        inner = getattr(model, "model", None)
        device = getattr(inner, "device", None)
    if device is None:
        return False
    dev_type = getattr(device, "type", None)
    if isinstance(dev_type, str):
        return dev_type.lower() == "mps"
    if isinstance(device, str):
        return device.lower().startswith("mps")
    return False


@contextmanager
def serialize_if_mps(model: Any, lock: Any):
    """Acquire ``lock`` around inference only when ``model`` runs on ``mps``."""
    if is_mps_model(model):
        with lock:
            yield
    else:
        yield


def load_sentence_transformer(
    model_name: str = "all-MiniLM-L6-v2", **kwargs: Any
) -> SentenceTransformer:
    """Load a SentenceTransformer, suppressing known-harmless HF checkpoint warnings."""
    from sentence_transformers import SentenceTransformer

    with _LOAD_LOCK:
        with _suppress_transformers_load_noise():
            return SentenceTransformer(model_name, **kwargs)
