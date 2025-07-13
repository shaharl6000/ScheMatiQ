
from retrievers import Retriever, EmbeddingRetriever, PromptingRetriever, test_retriever_stability
from llm_backends import LLMInterface, TogetherLLM, OpenAILLM
from typing import List, Dict, Sequence, Tuple, Any
from dataclasses import asdict, is_dataclass
import numpy as np

def build_llm(cfg: Dict[str, Any]) -> LLMInterface:
    provider = cfg.get("provider", "together").lower()
    if provider == "together":
        return TogetherLLM(
            model=cfg.get("model", "mistralai/Mixtral-8x7B-Instruct-v0.1"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),               # falls back to env var
        )
    elif provider == "openai":
        return OpenAILLM(
            model=cfg.get("model", "gpt-4o-mini"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),
        )
    else:
        raise ValueError(f"Unknown backend provider: {provider}")


def build_retriever(cfg: Dict[str, Any], llm_for_prompting: LLMInterface = None) -> Retriever:
    rtype = cfg.get("type", "embedding").lower()
    if rtype == "embedding":
        return EmbeddingRetriever(
            model_name=cfg.get("model_name", "all-MiniLM-L6-v2"),
        )
    elif rtype == "prompting":
        if llm_for_prompting is None:
            raise ValueError("Unknown llm_for_prompting for prompting retriever")
        return PromptingRetriever(
            llm=llm_for_prompting,
            sentences_per_doc=cfg.get("sentences_per_doc", 3),
            max_doc_chars=cfg.get("max_doc_chars", 4000),
        )
    else:
        raise ValueError(f"Unknown retriever type: {rtype}")


def _to_jsonable(obj: Any):  # ← re‑use helper from QBSD
    from dataclasses import is_dataclass
    if is_dataclass(obj):
        obj = asdict(obj)
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(x) for x in obj]
    return obj