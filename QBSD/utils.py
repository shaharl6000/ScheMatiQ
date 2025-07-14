
from retrievers import Retriever, EmbeddingRetriever, PromptingRetriever, test_retriever_stability
from llm_backends import LLMInterface, TogetherLLM, OpenAILLM
from typing import List, Dict, Sequence, Tuple, Any
from dataclasses import asdict, is_dataclass
import numpy as np
import arxiv
import io
import requests
from PyPDF2 import PdfReader
from dataclasses import asdict, is_dataclass
from pathlib import Path                    #  NEW
import os, re, unicodedata, io, difflib
import numpy as np
import requests, arxiv
from PyPDF2 import PdfReader
from dataclasses import asdict, is_dataclass
import platform                 #  NEW  (place near your other imports)

_IS_WINDOWS = platform.system().lower().startswith("win")
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


TOGETHER_API_KEY="tgp_v1_CDqcNiCaTIOYlU4lmCcW8ovlO9PHcbhGNOgk_q5p4-A"
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
    if provider == "together":
        return TogetherLLM(
            model=cfg.get("model", "mistralai/Mixtral-8x7B-Instruct-v0.1"),
            max_tokens=cfg.get("max_tokens", 1024),
            temperature=cfg.get("temperature", 0.3),
            api_key=cfg.get("api_key", TOGETHER_API_KEY),               # falls back to env var
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

def search_arxiv_by_title(
    title: str,
    client: arxiv.Client,
    *,
    max_results: int = 10,
    exact: bool = True,
) -> list[arxiv.Result]:
    quoted = f'"{title}"' if exact else title
    query  = f"ti:{quoted}"

    # ② Hit the API.
    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.Relevance,
    )
    try:
        hits = list(client.results(search))

        # ③ If we asked for a fuzzy match, re‑rank locally by
        #    string similarity so the best‑looking title is first.
        if not exact and hits:
            hits.sort(
                key=lambda r: difflib.SequenceMatcher(None, title.lower(), r.title.lower()).ratio(),
                reverse=True,
            )
        return hits
    except arxiv.UnexpectedEmptyPageError:
        return []


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