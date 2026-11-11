from __future__ import annotations

import functools
import os
import re
from typing import Any, Dict

import tiktoken

from schematiq.core.llm_backends import (
    GeminiLLM,
    HuggingFaceLLM,
    LLMInterface,
    OpenAILLM,
    TogetherLLM,
)
from schematiq.core.model_specs import ModelNames
from schematiq.core.retrievers import (
    EmbeddingRetriever,
    PromptingRetriever,
    PromptingRetrieverConfig,
    Retriever,
)

MAX_CTX_TOKENS = 8192
SAFETY_MARGIN = 512
DEFAULT_SENTENCE_LEVELS = (11, 9, 7, 5, 3, 1)
MAX_NEW_TOKENS = 512
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")


@functools.lru_cache(maxsize=1)
def _get_encoding() -> tiktoken.Encoding:
    return tiktoken.encoding_for_model(ModelNames.TIKTOKEN_ENCODING_MODEL)


def build_llm(cfg: Dict[str, Any]) -> LLMInterface:
    """
    Build an LLM instance from configuration.

    Token limits (max_output_tokens, context_window_size) are auto-detected
    from model specs when not explicitly provided in the config.
    """
    provider = cfg.get("provider", "together").lower()
    print(f"-------provider: {provider}")

    # Get optional token limits - pass None to let LLM classes auto-detect
    max_output_tokens = cfg.get("max_output_tokens") or cfg.get("max_tokens")
    context_window_size = cfg.get("context_window_size") or cfg.get("max_context_tokens")

    if provider == "together":
        return TogetherLLM(
            model=cfg.get("model", ModelNames.DEFAULT_TOGETHER),
            max_output_tokens=max_output_tokens,  # None = auto-detect
            temperature=cfg.get("temperature", 0.3),
            context_window_size=context_window_size,  # None = auto-detect
            api_key=cfg.get("api_key", TOGETHER_API_KEY),
        )
    elif provider == "openai":
        return OpenAILLM(
            model=cfg.get("model", ModelNames.DEFAULT_OPENAI),
            max_output_tokens=max_output_tokens,  # None = auto-detect
            temperature=cfg.get("temperature", 0.3),
            context_window_size=context_window_size,  # None = auto-detect
            api_key=cfg.get("api_key"),
        )
    elif provider == "hf":
        return HuggingFaceLLM(
            model=cfg.get("model", ModelNames.DEFAULT_HF),
            max_output_tokens=max_output_tokens or 1024,  # HF doesn't have auto-detect
            temperature=cfg.get("temperature", 0.3),
        )
    elif provider == "gemini":
        model_name = cfg.get("model", ModelNames.DEFAULT_VALUE_EXTRACTION)
        llm = GeminiLLM(
            model=model_name,
            max_output_tokens=max_output_tokens,  # None = auto-detect
            temperature=cfg.get("temperature", 0.3),
            context_window_size=context_window_size,  # None = auto-detect
            api_key=cfg.get("api_key"),
        )
        print(f"🤖 Gemini model: {model_name}, max_output_tokens: {llm.max_output_tokens}, context_window: {llm.context_window_size}")
        return llm
    else:
        raise ValueError(f"Unknown backend provider: {provider}")


def build_retriever(cfg: Dict[str, Any], llm_for_prompting: LLMInterface = None) -> Retriever:
    rtype = cfg.get("type", "embedding").lower()
    if rtype == "embedding":
        return EmbeddingRetriever(
            model_name=cfg.get("model_name", "all-MiniLM-L6-v2"),
            k=cfg.get("k", 15),
            max_words=cfg.get("max_words", 512),
            batch_size=cfg.get("batch_size", 32),
            device=cfg.get("device"),
            enable_dynamic_k=cfg.get("enable_dynamic_k", False),
            dynamic_k_threshold=cfg.get("dynamic_k_threshold", 0.65),
            dynamic_k_minimum=cfg.get("dynamic_k_minimum", 2)
        )
    elif rtype == "prompting":
        if llm_for_prompting is None:
            raise ValueError("Unknown llm_for_prompting for prompting retriever")
        prompting_config = PromptingRetrieverConfig(
            k=cfg.get("k", 5),
            max_new_tokens=cfg.get("max_new_tokens", 512),
            temperature=cfg.get("temperature", 0.0),
            stop=cfg.get("stop"),
            batch_size=cfg.get("batch_size", 40),
            finalist_factor=cfg.get("finalist_factor", 2.0),
            mode=cfg.get("mode", "sampled_rank"),
            overlap_words=cfg.get("overlap_words", 64),
            respect_structure=cfg.get("respect_structure", True),
            truncate_words_per_chunk=cfg.get("truncate_words_per_chunk", 512),
            max_input_tokens=cfg.get("max_input_tokens", 7000)
        )
        return PromptingRetriever(
            generate=llm_for_prompting.generate,
            config=prompting_config
        )
    else:
        raise ValueError(f"Unknown retriever type: {rtype}")


def fit_prompt(
    messages: list[dict[str, str]],
    max_new: int = MAX_NEW_TOKENS,
    safety_margins: int = SAFETY_MARGIN,
    sentence_levels: tuple[int, ...] = DEFAULT_SENTENCE_LEVELS,
    truncate: bool = False,
    context_window_size: int = MAX_CTX_TOKENS,
) -> list[dict[str, str]]:
    """
    Ensure (prompt_tokens + max_new) ≤ context_window_size–SAFETY_MARGIN.
    """
    enc = _get_encoding()

    def n_tokens(s: str) -> int:
        return len(enc.encode(s))

    # ------------------------------------------------------------------ #
    # Fast path: if we only want raw truncation
    # ------------------------------------------------------------------ #
    allowed_prompt = context_window_size - safety_margins - max_new
    user_msg = messages[1]["content"]

    if truncate:
        if n_tokens(user_msg) > allowed_prompt:
            # keep only the first `allowed_prompt` tokens
            kept = enc.encode(user_msg)[:allowed_prompt]
            messages[1]["content"] = enc.decode(kept)
            # optional: let the caller know
            print(f"✂️  Hard‑truncated prompt to {allowed_prompt} tokens.")
        return messages

    # ------------------------------------------------------------------ #
    # Original logic (shorten abstracts → drop papers → header‑only)
    # ------------------------------------------------------------------ #
    def shorten(block: str, sentence_cap: int) -> str:
        title, sep, abstract = block.partition("Paper content:")
        if not sep:                       # separator missing → leave as‑is
            return block
        sentences = re.split(r"(?<=[.!?])\s+", abstract.strip())
        short_abs = " ".join(sentences[:sentence_cap])
        return f"{title}{sep} {short_abs}"

    header, _, papers_blob = user_msg.partition("\n\nPapers:\n\n")
    blocks = papers_blob.split("\n\n") if papers_blob else []

    full_len = n_tokens(user_msg) + max_new
    if full_len <= context_window_size - SAFETY_MARGIN:
        return messages                                        # fits as‑is ✅

    # 1) shorten Paper contents ----------------------------------------
    for cap in sentence_levels:
        new_blocks = [shorten(b, cap) for b in blocks]
        short_prompt = header + "\n\nPapers:\n\n" + "\n\n".join(new_blocks)
        if n_tokens(short_prompt) + max_new <= context_window_size - safety_margins:
            print(f"✂️  Trimmed Paper contents to ≤ {cap} sentences each.")
            messages[1]["content"] = short_prompt
            return messages

    # 2) drop papers from the end --------------------------------------
    while blocks:
        blocks.pop()
        short_prompt = header + "\n\nPapers:\n\n" + "\n\n".join(blocks)
        if n_tokens(short_prompt) + max_new <= context_window_size - safety_margins:
            print(f"📄  Dropped papers – {len(blocks)} remain.")
            messages[1]["content"] = short_prompt
            return messages

    # 3) Fallback: all papers gone; keep header only -------------------
    print(f"⚠️  All papers trimmed; only the header remains, call truncation! \n Header: {header}")
    messages[1]["content"] = header
    return fit_prompt(messages, max_new=max_new, sentence_levels=sentence_levels, truncate=True, context_window_size=context_window_size)
