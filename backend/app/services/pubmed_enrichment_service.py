"""Background service for enriching documents with PubMed/DOI links.

When source documents are listed (GET /units/documents), any documents
without URLs trigger a background EuropePMC lookup. Results are stored
in session.metadata.document_metadata keyed by the exact source document
name so the existing frontend rendering pipeline picks them up automatically.
"""

import asyncio
import functools
import logging
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests

from app.core.config import DEFAULT_DATA_DIR
from app.services.session_manager import SessionManager

logger = logging.getLogger(__name__)

EUROPEPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
PUBMED_DIRECT_URL = "https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

# Typographic → ASCII: EuropePMC's TITLE:"..." is character-exact, so curly
# quotes and special dashes extracted from PDF markdown miss their indexed
# ASCII equivalents. Maps cover the characters observed across ~500 real
# research titles in this repo (see research/data/{NES_new,NPC_hyp,novel_nes}).
_TYPOGRAPHIC_MAP = str.maketrans({
    "‘": "'", "’": "'", "‚": "'", "‛": "'",
    "“": '"', "”": '"', "„": '"', "‟": '"',
    "‐": "-", "‑": "-", "‒": "-",
    "–": "-", "—": "-", "―": "-", "−": "-",
    " ": " ", " ": " ", " ": " ",
})

# Footnote glyphs and missing-glyph placeholders — content noise, not title.
_NOISE_CHARS = frozenset("†‡§¶•□�")


def _normalize_for_search(text: str) -> str:
    """Fold typographic characters to ASCII for exact-match title lookup."""
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(_TYPOGRAPHIC_MAP)
    text = "".join(" " if c in _NOISE_CHARS else c for c in text)
    return " ".join(text.split())


def _clean_source_doc_to_title(source_doc: str) -> Optional[str]:
    """Convert a source document name to a searchable paper title.

    Handles two naming patterns:

    1. Source document names from extraction (with observation-unit prefix):
       'co-A Saturated FG-Repeat Hydrogel' -> 'A Saturated FG-Repeat Hydrogel'
       'amb-Capturing directed molecular motion in' -> 'Capturing directed molecular motion in'

    2. Raw uploaded filenames (underscore-separated with PMID):
       'ATM_phosphorylates_PP2A_36434396_full.txt' -> 'ATM phosphorylates PP2A'
    """
    name = source_doc

    # Strip observation-unit prefix (2-5 lowercase chars followed by dash)
    name = re.sub(r"^[a-z]{2,5}-", "", name)

    # Handle underscore-separated filenames (raw uploads)
    if "_" in name:
        name = Path(name).stem
        name = re.sub(r"_full$", "", name)
        name = re.sub(r"_\d{6,}$", "", name)  # trailing PMID (6+ digits)
        name = re.sub(r"_\d{4}$", "", name)   # trailing 4-digit year
        name = name.replace("_", " ")

    # Strip file extension if present
    name = re.sub(r"\.(txt|pdf|doc|docx|md|rtf)$", "", name, flags=re.IGNORECASE)

    name = name.strip()
    if len(name) < 10:
        return None
    return name


def _title_matches(query_title: str, result_title: str, threshold: float = 0.8) -> bool:
    """Check if the query title matches a PubMed result title.

    Uses word-overlap rather than sequence matching because source
    document names often truncate the real title.
    """
    q_words = set(re.findall(r"[a-z0-9]+", query_title.lower()))
    r_words = set(re.findall(r"[a-z0-9]+", result_title.lower()))
    if not q_words:
        return False
    overlap = len(q_words & r_words) / len(q_words)
    return overlap >= threshold


def _search_europepmc(title: str) -> Optional[Dict[str, str]]:
    """Search EuropePMC for a paper matching the given title.

    Returns {"url": "https://doi.org/...", "doi": "..."} or None.
    If the full title yields no results, retries with the last word
    dropped (source document names often truncate mid-word).
    """
    # Try full title first, then drop last word as fallback
    queries = [title]
    words = title.rsplit(" ", 1)
    if len(words) == 2 and len(words[0]) >= 10:
        queries.append(words[0])

    for query_title in queries:
        normalized = _normalize_for_search(query_title)
        try:
            params = {"query": f'TITLE:"{normalized}"', "format": "json", "pageSize": 3}
            resp = requests.get(EUROPEPMC_SEARCH_URL, params=params, timeout=15)
            resp.raise_for_status()
            results = resp.json().get("resultList", {}).get("result", [])

            for result in results:
                doi = result.get("doi")
                if doi and _title_matches(normalized, result.get("title", "")):
                    return {"url": f"https://doi.org/{doi}", "doi": doi}
        except Exception:
            logger.warning("[pubmed-enrichment] Lookup failed for: %s", normalized[:60], exc_info=True)

    return None


def _extract_pmid_from_filename(source_doc: str) -> Optional[str]:
    """Extract a PubMed ID from a source document name if present.

    Matches patterns like ``Bach1_15175654_full`` or ``CALM_24397609.txt`` —
    a trailing 6-9 digit number after an underscore, optionally followed
    by ``_full`` and a file extension. Rejects 4-digit runs (likely years).
    """
    name = re.sub(r"^[a-z]{2,5}-", "", source_doc)
    name = re.sub(r"\.(txt|pdf|doc|docx|md|rtf)$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"_full$", "", name, flags=re.IGNORECASE)

    m = re.search(r"(?:^|_)(\d{6,9})$", name)
    return m.group(1) if m else None


def _lookup_by_pmid(pmid: str) -> Optional[Dict[str, str]]:
    """Look up a paper by its PubMed ID via EuropePMC.

    Prefers the DOI URL; falls back to a direct PubMed link if the record
    lacks a DOI. Returns None only on network error or no results.
    """
    try:
        params = {
            "query": f"EXT_ID:{pmid} AND SRC:MED",
            "format": "json",
            "pageSize": 1,
        }
        resp = requests.get(EUROPEPMC_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("resultList", {}).get("result", [])
        if not results:
            return None

        doi = results[0].get("doi")
        if doi:
            return {"url": f"https://doi.org/{doi}", "doi": doi}
        return {"url": PUBMED_DIRECT_URL.format(pmid=pmid), "doi": None}
    except Exception:
        logger.warning("[pubmed-enrichment] PMID lookup failed for: %s", pmid, exc_info=True)
        return None


def _extract_title_from_file(file_path: Path) -> Optional[str]:
    """Extract a paper title from the first markdown heading in a file.

    Reads the first ~3 KB and returns the first ``#``-prefixed line,
    stripped of hashes and bold markers. Returns None if nothing
    title-shaped (15-250 chars) is found.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read(3000)
    except Exception:
        logger.warning("[pubmed-enrichment] Could not read %s", file_path, exc_info=True)
        return None

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line.startswith("#"):
            continue
        title = re.sub(r"^#+\s*", "", line)
        title = re.sub(r"\*+", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        if 15 <= len(title) <= 250:
            return title
    return None


class PubMedEnrichmentService:
    """Fire-and-forget service that enriches documents with DOI links.

    Triggered lazily when source documents are listed. Only starts lookups for
    document names with no entry yet in ``document_metadata`` (a missing entry
    means "never tried"; ``{"url": None}`` means "tried, no link" and stops
    repeat polling).
    """

    def __init__(self, session_manager: SessionManager, data_dir: str = DEFAULT_DATA_DIR):
        self._session_manager = session_manager
        self._data_dir = Path(data_dir)
        self._active_tasks: Set[asyncio.Task] = set()
        # Track sessions currently being enriched to avoid duplicate work
        self._in_progress: Set[str] = set()

    def _resolve_doc_path(self, session_id: str, doc_name: str) -> Optional[Path]:
        """Locate the source document on disk for a given session.

        The source_document field in extracted rows typically omits the file
        extension (e.g. ``BMAL1_16980631_full``), so we probe common extensions.
        """
        docs_dir = self._data_dir / session_id / "documents"
        if not docs_dir.exists():
            return None

        direct = docs_dir / doc_name
        if direct.exists():
            return direct

        for ext in (".txt", ".pdf", ".md", ".docx", ".doc", ".rtf"):
            candidate = docs_dir / f"{doc_name}{ext}"
            if candidate.exists():
                return candidate
        return None

    def _resolve_doc(self, session_id: str, doc_name: str) -> Optional[Dict[str, str]]:
        """Run lookup strategies for a single document, strongest signal first.

        Order:
          1. PMID extracted from the filename (deterministic, no false positives).
          2. Title extracted from the file's first markdown heading.
          3. Title derived from the filename (legacy heuristic).

        Each step is skipped silently if it doesn't apply. Returns the first
        successful {"url": ..., "doi": ...} dict, or None if all strategies fail.
        """
        pmid = _extract_pmid_from_filename(doc_name)
        if pmid:
            result = _lookup_by_pmid(pmid)
            if result:
                return result

        file_path = self._resolve_doc_path(session_id, doc_name)
        if file_path:
            title = _extract_title_from_file(file_path)
            if title:
                result = _search_europepmc(title)
                if result:
                    return result

        title = _clean_source_doc_to_title(doc_name)
        if title:
            return _search_europepmc(title)

        return None

    def is_active(self, session_id: str) -> bool:
        """Check if enrichment is currently running for a session."""
        return session_id in self._in_progress

    async def enrich_session(self, session_id: str) -> None:
        """Enrich all source documents in a session. Called at pipeline completion."""
        from app.services.unit_view_service import unit_view_service
        try:
            documents = unit_view_service.get_source_documents(session_id)
            doc_names = [d["name"] for d in documents]
            if doc_names:
                await self.enrich_missing(session_id, doc_names)
        except Exception:
            logger.warning(
                "[pubmed-enrichment] Failed to enrich session %s",
                session_id[:8], exc_info=True,
            )

    async def enrich_missing(self, session_id: str, doc_names: List[str]) -> bool:
        """Enrich documents that don't have URLs yet. Non-blocking, deduped.

        Returns True if enrichment was triggered, False otherwise.
        """
        if session_id in self._in_progress:
            return False

        session = self._session_manager.get_session(session_id)
        if not session:
            return False

        existing_metadata = getattr(session.metadata, "document_metadata", {}) or {}
        # Treat any per-document metadata entry as "already handled" — including
        # {"url": None} after a failed lookup — so polling GET /units/documents
        # does not restart enrichment forever when EuropePMC finds no match.
        missing = [name for name in doc_names if name not in existing_metadata]

        if not missing:
            return False

        self._in_progress.add(session_id)
        task = asyncio.create_task(self._enrich_documents(session_id, missing))
        self._active_tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._active_tasks.discard(t)
            self._in_progress.discard(session_id)

        task.add_done_callback(_on_done)
        logger.info(
            "[pubmed-enrichment] Started enrichment for %d/%d documents in session %s",
            len(missing), len(doc_names), session_id[:8],
        )
        return True

    async def _enrich_documents(self, session_id: str, doc_names: List[str]) -> None:
        """Background coroutine: resolve each document name to a DOI URL."""
        from app.services import schematiq_thread_pool

        loop = asyncio.get_running_loop()
        found = 0
        resolved_names: Set[str] = set()  # docs that got a real URL

        try:
            for doc_name in doc_names:
                result = await loop.run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(self._resolve_doc, session_id, doc_name),
                )

                if result:
                    self._store_url(session_id, doc_name, result["url"])
                    resolved_names.add(doc_name)
                    found += 1

                # Rate limit: EuropePMC recommends max ~3 req/sec.
                # _resolve_doc may have made up to 2 requests; 0.4s keeps the
                # burst rate safe regardless of which strategy fired.
                await asyncio.sleep(0.4)

            logger.info(
                "[pubmed-enrichment] Finished session %s: %d/%d documents enriched",
                session_id[:8], found, len(doc_names),
            )
        except Exception:
            logger.warning(
                "[pubmed-enrichment] Error enriching session %s",
                session_id[:8], exc_info=True,
            )
        finally:
            # Write url=null for every doc without a real URL — including docs
            # skipped because an exception aborted the loop early.  This makes
            # ``name not in document_metadata`` false for the full doc list, so
            # the next GET /units/documents poll won't re-trigger enrichment.
            all_misses = [n for n in doc_names if n not in resolved_names]
            self._store_enrichment_misses_batch(session_id, all_misses)
            if all_misses:
                logger.info(
                    "[pubmed-enrichment] Stored url=null for %d/%d document(s) "
                    "in session %s (polling will stop)",
                    len(all_misses), len(doc_names), session_id[:8],
                )

    def _store_url(self, session_id: str, doc_name: str, url: str) -> None:
        """Persist a DOI URL into session metadata."""
        session = self._session_manager.get_session(session_id)
        if not session:
            return
        if not session.metadata.document_metadata:
            session.metadata.document_metadata = {}
        session.metadata.document_metadata[doc_name] = {"url": url}
        self._session_manager.update_session(session)

    def _store_enrichment_misses_batch(self, session_id: str, doc_names: List[str]) -> None:
        """Record failed lookups in one persist (avoids N disk writes when nothing matched)."""
        if not doc_names:
            return
        session = self._session_manager.get_session(session_id)
        if not session:
            return
        if not session.metadata.document_metadata:
            session.metadata.document_metadata = {}
        for doc_name in doc_names:
            session.metadata.document_metadata[doc_name] = {"url": None}
        self._session_manager.update_session(session)
