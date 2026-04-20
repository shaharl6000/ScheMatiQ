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
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests

from app.services.session_manager import SessionManager

logger = logging.getLogger(__name__)


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
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    # Try full title first, then drop last word as fallback
    queries = [title]
    words = title.rsplit(" ", 1)
    if len(words) == 2 and len(words[0]) >= 10:
        queries.append(words[0])

    for query_title in queries:
        try:
            params = {"query": f'TITLE:"{query_title}"', "format": "json", "pageSize": 3}
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            results = resp.json().get("resultList", {}).get("result", [])

            for result in results:
                doi = result.get("doi")
                if doi and _title_matches(query_title, result.get("title", "")):
                    return {"url": f"https://doi.org/{doi}", "doi": doi}
        except Exception:
            logger.warning("[pubmed-enrichment] Lookup failed for: %s", query_title[:60], exc_info=True)

    return None


class PubMedEnrichmentService:
    """Fire-and-forget service that enriches documents with DOI links.

    Triggered lazily when source documents are listed. Only searches for
    documents that don't already have URLs in session metadata.
    """

    def __init__(self, session_manager: SessionManager):
        self._session_manager = session_manager
        self._active_tasks: Set[asyncio.Task] = set()
        # Track sessions currently being enriched to avoid duplicate work
        self._in_progress: Set[str] = set()

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

        try:
            for doc_name in doc_names:
                title = _clean_source_doc_to_title(doc_name)
                if not title:
                    continue

                result = await loop.run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(_search_europepmc, title),
                )

                if result:
                    self._store_url(session_id, doc_name, result["url"])
                    found += 1

                # Rate limit: EuropePMC recommends max ~3 req/sec
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

    def _store_url(self, session_id: str, doc_name: str, url: str) -> None:
        """Persist a DOI URL into session metadata."""
        session = self._session_manager.get_session(session_id)
        if not session:
            return
        if not session.metadata.document_metadata:
            session.metadata.document_metadata = {}
        session.metadata.document_metadata[doc_name] = {"url": url}
        self._session_manager.update_session(session)
