
from retrievers import Retriever, EmbeddingRetriever, PromptingRetriever, test_retriever_stability
from llm_backends import LLMInterface, TogetherLLM, OpenAILLM, HuggingFaceLLM, GeminiLLM
from typing import List, Dict, Any
from pathlib import Path
import io, difflib
import numpy as np
import requests, arxiv
from PyPDF2 import PdfReader
from dataclasses import asdict, is_dataclass
import platform                 #  NEW  (place near your other imports)
import unicodedata, re
from difflib import SequenceMatcher
import tiktoken
import os

def _best_title_match(query_t, results):
    return max(
        results,
        key=lambda r: SequenceMatcher(
            None, query_t, _canonical_title(r.title)
        ).ratio(),
    )


_RE_LATEX_MATH = re.compile(r"\$[^$]+\$")          # very coarse but good enough
_RE_PUNCT      = re.compile(r"[^\w\s-]")

MAX_CTX_TOKENS = 8192
SAFETY_MARGIN   = 512
DEFAULT_SENTENCE_LEVELS = (11, 9, 7, 5, 3, 1)
ENC = tiktoken.encoding_for_model("gpt-4o")  # close enough
MAX_NEW_TOKENS = 512

def _canonical_title(t: str) -> str:
    """Strip LaTeX, normalise Unicode, drop exotic punctuation, squash spaces."""
    t = _RE_LATEX_MATH.sub(" ", t)                 # remove  $\\alpha$  etc.
    t = unicodedata.normalize("NFKD", t)           # é → é  etc.
    t = _RE_PUNCT.sub(" ", t)
    return re.sub(r"\s+", " ", t).strip().lower()

_IS_WINDOWS = "win" in platform.system().lower()
_CACHE_ENABLED = not _IS_WINDOWS

print(f"-------------_CACHE_ENABLED: {_CACHE_ENABLED}")

CACHE_DIR = Path("data_arxiv")                    # configurable if you like
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_SAFE_CHARS = re.compile(r"[^A-Za-z0-9 _.-]+")
def _safe_filename(title: str, max_len: int = 120) -> str:
    """
    Turn an arbitrary paper title into a filesystem‑safe slug.
    """
    slug = _SAFE_CHARS.sub("", title).strip().replace(" ", "_")
    return (slug[:max_len] or "untitled") + ".txt"

def _cache_path(title: str) -> Path:
    return CACHE_DIR / _safe_filename(title)


TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")
CTRL_CHARS = re.compile(r"[\u0000-\u001F\u007F-\u009F]")         # ASCII C0 + DEL

def _clean_pdf_text(txt: str) -> str:
    """
    • normalise to NFKC (merges full‑width & compatible glyphs)
    • drop isolated UTF‑16 surrogates and other illegal codepoints
    • strip control characters
    """
    if not txt:
        return ""
    txt = unicodedata.normalize("NFKC", txt)
    txt = txt.encode("utf-8", "ignore").decode("utf-8", "ignore")  # purge surrogates
    txt = CTRL_CHARS.sub("", txt)
    return txt

def _download_ok(pdf: bytes) -> bool:
    """Quick sanity check: real PDF header + footer present."""
    return pdf.startswith(b"%PDF") and b"%%EOF" in pdf[-1024:]


def build_llm(cfg: Dict[str, Any]) -> LLMInterface:
    provider = cfg.get("provider", "together").lower()
    print(f"-------provider: {provider}")
    if provider == "together":
        return TogetherLLM(
            model=cfg.get("model", "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free"),
            max_tokens=cfg.get("max_tokens", 512),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key", TOGETHER_API_KEY),               # falls back to env var
        )
    elif provider == "openai":
        return OpenAILLM(
            model=cfg.get("model", "gpt-4o"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),
        )
    elif provider == "hf":
        return HuggingFaceLLM(
            model=cfg.get("model", "meta-llama/Llama-3.3-70B-Instruct"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key"),
        )
    elif provider == "gemini":
        return GeminiLLM(
            model=cfg.get("model", "gemini-1.5-flash"),
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
        # Create PromptingRetrieverConfig from cfg
        from retrievers import PromptingRetrieverConfig
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


def get_paper_from_arxiv_id(arxiv_id: str, client: arxiv.Client) -> str:
    """
    Download a PDF from arXiv and return a cleaned Unicode string
    that is safe for tokenisers / LLMs.
    """
    try:
        record  = next(client.results(arxiv.Search(id_list=[arxiv_id])))
        pdf_url = record.pdf_url

        pdf_bytes   = requests.get(pdf_url, timeout=30).content
        reader      = PdfReader(io.BytesIO(pdf_bytes))
        pages_text: List[str] = []

        for page in reader.pages:
            raw = page.extract_text() or ""
            pages_text.append(_clean_pdf_text(raw))

        return "\n".join(pages_text)
    except Exception as e:
        print(f"pdf extraction failed: {e}, return empty string")
        return ""

def _smart_search_arxiv_by_title(title: str,
                                 client: arxiv.Client,
                                 *,
                                 max_results_each: int = 5) -> list[arxiv.Result]:
    """
    Progressive query loosening + local fuzzy re‑ranking.
    Returns [] if nothing found.
    """
    t = _canonical_title(title)
    steps = [
        f'ti:"{t}"',                                  # exact
        f'ti:"{" ".join(t.split()[:10])}"',           # prefix
        " AND ".join(f'ti:"{w}"' for w in t.split()[:3]),  # key nouns
        f'all:"{t}"',                                 # anywhere
    ]

    for q in steps:
        try:
            hits = list(client.results(arxiv.Search(
                query=q,
                max_results=max_results_each,
                sort_by=arxiv.SortCriterion.Relevance,
            )))
        except arxiv.UnexpectedEmptyPageError:
            hits = []

        if hits:                                     # fuzzy sort on original title
            hits.sort(
                key=lambda r: difflib.SequenceMatcher(None,
                                                     title.lower(),
                                                     r.title.lower()).ratio(),
                reverse=True,
            )
            return hits
    return []


def search_arxiv_by_title(title: str,
                          client: arxiv.Client,
                          *,
                          max_results: int = 10,
                          exact: bool = False) -> list[arxiv.Result]:
    """
    First try a simple arXiv title query.
    If that returns no hits (or an empty‑page error), fall back to the
    smarter progressive strategy defined above.
    """
    quoted = f'"{title}"' if exact else title
    query  = f"ti:{quoted}"

    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.Relevance,
    )
    try:
        hits = list(client.results(search))
    except Exception as e:
        print(f"❌ Failed to download full text from {query}: {e}")
        hits = []

    # local fuzzy re‑rank when using the loose (non‑exact) query
    if not exact and hits:
        hits.sort(
            key=lambda r: difflib.SequenceMatcher(None,
                                                  title.lower(),
                                                  r.title.lower()).ratio(),
            reverse=True,
        )

    # return immediately if we found something; otherwise run fallback
    if hits:
        return hits

    # ── Fallback: smarter multi‑step search ──
    return _smart_search_arxiv_by_title(title, client, max_results_each=max_results)


def get_paper_from_title(title: str,
                         client: arxiv.Client,
                         *,
                         exact: bool = True) -> str | None:
    """
    • On Linux/macOS  → use the on‑disk cache under ``data/``.
    • On Windows      → skip all caching and always hit the arXiv API.
    """
    # ---------- Windows: fall back to plain (uncached) lookup ----------
    if not _CACHE_ENABLED:
        results = search_arxiv_by_title(title, client, exact=exact, max_results=5)
        if not results:
            return None
        arxiv_id = results[0].get_short_id()
        return get_paper_from_arxiv_id(arxiv_id, client) or None

    # ---------- Non‑Windows: full cache path ----------
    cache_file = _cache_path(title)

    # ① Cache hit?
    if cache_file.exists():
        cached = cache_file.read_text(encoding="utf-8")
        return cached if cached else None

    # ② Cache miss → query arXiv
    results = search_arxiv_by_title(title, client, exact=exact, max_results=5)
    if not results:
        cache_file.touch()        # negative cache
        return None

    arxiv_id  = results[0].get_short_id()
    pdf_text  = get_paper_from_arxiv_id(arxiv_id, client) or ""

    # ③ Persist result (empty string allowed)
    cache_file.write_text(pdf_text, encoding="utf-8")
    return pdf_text or None


def fit_prompt(
    messages: list[dict[str, str]],
    max_new: int = MAX_NEW_TOKENS,
    safety_margins: int = SAFETY_MARGIN,
    sentence_levels: tuple[int, ...] = DEFAULT_SENTENCE_LEVELS,
    truncate: bool = False,
) -> list[dict[str, str]]:
    """
    Ensure (prompt_tokens + max_new) ≤ MAX_CTX_TOKENS–SAFETY_MARGIN.
    """
    def n_tokens(s: str) -> int:
        return len(ENC.encode(s))

    # ------------------------------------------------------------------ #
    # Fast path: if we only want raw truncation
    # ------------------------------------------------------------------ #
    allowed_prompt = MAX_CTX_TOKENS - safety_margins - max_new
    user_msg = messages[1]["content"]

    if truncate:
        if n_tokens(user_msg) > allowed_prompt:
            # keep only the first `allowed_prompt` tokens
            kept = ENC.encode(user_msg)[:allowed_prompt]
            messages[1]["content"] = ENC.decode(kept)
            # optional: let the caller know
            print(f"✂️  Hard‑truncated prompt to {allowed_prompt} tokens.")
        return messages

    # ------------------------------------------------------------------ #
    # Original logic (shorten abstracts → drop papers → header‑only)
    # ------------------------------------------------------------------ #
    def shorten(block: str, cap: int) -> str:
        title, sep, abstract = block.partition("Paper content:")
        if not sep:                       # separator missing → leave as‑is
            return block
        sentences = re.split(r"(?<=[.!?])\\s+", abstract.strip())
        short_abs = " ".join(sentences[:cap])
        return f"{title}{sep} {short_abs}"

    header, _, papers_blob = user_msg.partition("\\n\\nPapers:\\n\\n")
    blocks = papers_blob.split("\\n\\n") if papers_blob else []

    full_len = n_tokens(user_msg) + max_new
    if full_len <= MAX_CTX_TOKENS - SAFETY_MARGIN:
        return messages                                        # fits as‑is ✅

    # 1) shorten Paper contents ----------------------------------------
    for cap in sentence_levels:
        new_blocks = [shorten(b, cap) for b in blocks]
        short_prompt = header + "\\n\\nPapers:\\n\\n" + "\\n\\n".join(new_blocks)
        if n_tokens(short_prompt) + max_new <= MAX_CTX_TOKENS - safety_margins:
            print(f"✂️  Trimmed Paper contents to ≤ {cap} sentences each.")
            messages[1]["content"] = short_prompt
            return messages

    # 2) drop papers from the end --------------------------------------
    while blocks:
        blocks.pop()
        short_prompt = header + "\\n\\nPapers:\\n\\n" + "\\n\\n".join(blocks)
        if n_tokens(short_prompt) + max_new <= MAX_CTX_TOKENS - safety_margins:
            print(f"📄  Dropped papers – {len(blocks)} remain.")
            messages[1]["content"] = short_prompt
            return messages

    # 3) Fallback: all papers gone; keep header only -------------------
    print(f"⚠️  All papers trimmed; only the header remains, call truncation! \n Header: {header}")
    messages[1]["content"] = header
    return fit_prompt(messages, max_new=max_new, sentence_levels=sentence_levels, truncate=True)
