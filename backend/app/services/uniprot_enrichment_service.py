"""Background service for enriching protein rows with UniProt data.

Triggered at the same completion points as PubMedEnrichmentService (end of
value extraction, continue-discovery, re-extraction, document upload). For any
session whose observation unit looks protein-like, every row missing a UniProt
accession is resolved against the UniProt REST API and the row is augmented
with 10 columns (accession, url, gene symbol, GO terms, PDB IDs, AlphaFold URL,
etc.) plus per-cell `_cell_status: "external_source"` provenance — which the
frontend DataTable already styles with a blue border.

Rows already carrying a `uniprot_accession` are left untouched, so adding new
documents to an enriched session only triggers lookups for the new rows.
"""

import asyncio
import functools
import json
import logging
from pathlib import Path
from typing import Set

from app.services.data_utils import row_dedup_key
from app.services.session_data_store import save_session_data_file, session_write_lock
from app.services.session_manager import SessionManager
from app.services.uniprot_lookup import (
    EXTERNAL,
    NO_CHANGE,
    UNIPROT_SCHEMA_COLUMNS,
    extract_value,
    find_input_columns,
    is_protein_like_unit,
    lookup_protein,
    parse_hit,
)

logger = logging.getLogger(__name__)


class UniProtEnrichmentService:
    """Fire-and-forget service that enriches rows with UniProt data."""

    def __init__(self, session_manager: SessionManager):
        self._session_manager = session_manager
        self._active_tasks: Set[asyncio.Task] = set()
        self._in_progress: Set[str] = set()

    def is_active(self, session_id: str) -> bool:
        return session_id in self._in_progress

    async def enrich_session(self, session_id: str) -> None:
        """Enrich unprocessed rows in a session. Fire-and-forget; never raises."""
        if session_id in self._in_progress:
            return
        try:
            session = self._session_manager.get_session(session_id)
            if not session:
                return

            unit = session.observation_unit
            if not unit or not is_protein_like_unit(unit.name, unit.definition):
                logger.info(
                    "[uniprot-enrichment] Skipping session %s: observation unit %r is not protein-like",
                    session_id[:8], getattr(unit, "name", None),
                )
                return

            from app.services import find_session_data_file

            jsonl_path = await find_session_data_file(session_id)
            if jsonl_path is None:
                return

            # Peek at the first row to decide if we have a usable protein column.
            # Runtime jsonl stores cells at the row top level (ScheMatiQ flat format).
            first_row = _read_first_row(jsonl_path)
            if first_row is None:
                return
            pcol, _ocol, _acol = find_input_columns(first_row)
            if not pcol:
                logger.warning(
                    "[uniprot-enrichment] Skipping session %s: no protein-name column found",
                    session_id[:8],
                )
                return

            self._in_progress.add(session_id)
            task = asyncio.create_task(self._enrich_rows(session_id, jsonl_path))
            self._active_tasks.add(task)

            def _on_done(t: asyncio.Task) -> None:
                self._active_tasks.discard(t)
                self._in_progress.discard(session_id)

            task.add_done_callback(_on_done)
        except Exception:
            self._in_progress.discard(session_id)
            logger.warning(
                "[uniprot-enrichment] Failed to schedule enrichment for %s",
                session_id[:8], exc_info=True,
            )

    async def _enrich_rows(self, session_id: str, jsonl_path: Path) -> None:
        """Background coroutine: resolve each pending row against UniProt."""
        from app.services import schematiq_thread_pool

        loop = asyncio.get_running_loop()

        try:
            rows = _load_jsonl(jsonl_path)
            if not rows:
                return

            pcol, ocol, acol = find_input_columns(rows[0])
            if not pcol:
                return

            pending_indices = [
                i for i, r in enumerate(rows)
                if not r.get("uniprot_accession")
            ]
            if not pending_indices:
                logger.info(
                    "[uniprot-enrichment] Session %s has no pending rows; nothing to do",
                    session_id[:8],
                )
                return

            logger.info(
                "[uniprot-enrichment] Started enrichment for %d/%d rows in session %s",
                len(pending_indices), len(rows), session_id[:8],
            )

            # Per-session cache so repeated proteins within a session skip HTTP.
            cache: dict = {}
            enriched = 0
            # (row index, row key, protein looked up, parsed hit or None). The
            # lookups are slow and rate limited, so the rows are not written
            # here: _merge_results re-reads the file under the session write
            # lock and applies these, keeping edits made in the meantime.
            results: list = []

            for idx in pending_indices:
                row = rows[idx]
                protein = extract_value(row.get(pcol)) if pcol else None
                if not protein:
                    results.append((idx, row_dedup_key(row), protein, None))
                    continue

                organism = row.get(ocol) if ocol else None
                alt_names = row.get(acol) if acol else None

                cache_key = (str(protein).strip().lower(), _cache_key_for_organism(organism))
                if cache_key in cache:
                    parsed = cache[cache_key]
                else:
                    hit, _tier, _err = await loop.run_in_executor(
                        schematiq_thread_pool,
                        functools.partial(lookup_protein, protein, organism, alt_names),
                    )
                    parsed = parse_hit(hit) if hit else None
                    cache[cache_key] = parsed
                    await asyncio.sleep(0.2)  # UniProt rate limit

                if parsed:
                    enriched += 1
                results.append((idx, row_dedup_key(row), protein, parsed))

            applied = await _merge_results(session_id, jsonl_path, pcol, results)
            self._ensure_columns_on_session(session_id)

            logger.info(
                "[uniprot-enrichment] Finished session %s: %d/%d rows enriched (%d rows written)",
                session_id[:8], enriched, len(pending_indices), applied,
            )
        except Exception:
            logger.warning(
                "[uniprot-enrichment] Error enriching session %s",
                session_id[:8], exc_info=True,
            )

    def _ensure_columns_on_session(self, session_id: str) -> None:
        """Register the 10 UniProt columns on the session if they aren't already."""
        from app.models.session import ColumnInfo
        session = self._session_manager.get_session(session_id)
        if not session:
            return
        existing = {c.name for c in session.columns}
        added = False
        for name, definition, data_type in UNIPROT_SCHEMA_COLUMNS:
            if name not in existing:
                session.columns.append(ColumnInfo(
                    name=name,
                    definition=definition,
                    data_type=data_type,
                    source_document="__uniprot_enrichment__",
                ))
                added = True
        if added:
            self._session_manager.update_session(session)


# ── module-private helpers ───────────────────────────────────────────


def _read_first_row(jsonl_path: Path):
    try:
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    return json.loads(line)
    except Exception:
        return None
    return None


def _load_jsonl(jsonl_path: Path):
    rows = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


async def _merge_results(session_id: str, jsonl_path: Path, pcol, results) -> int:
    """Apply lookup results to the current file, under the session write lock.

    Rows are matched by (row name, source document), or by index for rows
    without a name. A row is skipped if it gained an accession or its protein
    cell changed since the lookup started (the next enrichment run covers it).
    The file is written atomically and persisted. Returns rows written.
    """
    async with session_write_lock(session_id):
        rows = _load_jsonl(jsonl_path)
        by_key: dict = {}
        for i, row in enumerate(rows):
            key = row_dedup_key(row)
            if key[0]:
                by_key.setdefault(key, []).append(i)

        applied = 0
        for idx, key, protein, parsed in results:
            if key[0]:
                targets = by_key.get(key, [])
            elif idx < len(rows) and not row_dedup_key(rows[idx])[0]:
                targets = [idx]
            else:
                targets = []
            for i in targets:
                row = rows[i]
                if row.get("uniprot_accession"):
                    continue
                current = extract_value(row.get(pcol)) if pcol else None
                if current != protein:
                    continue
                _apply_result(row, parsed)
                applied += 1

        if applied:
            await save_session_data_file(session_id, jsonl_path, rows, ensure_ascii=False)
        return applied


def _apply_result(row: dict, parsed) -> None:
    status = row.get("_cell_status")
    if not isinstance(status, dict):
        status = {}
        row["_cell_status"] = status
    if not parsed:
        _fill_empty(row, status)
        return
    for col, _defn, _dtype in UNIPROT_SCHEMA_COLUMNS:
        val = parsed.get(col)
        row[col] = val
        status[col] = EXTERNAL if val is not None else NO_CHANGE


def _fill_empty(row: dict, status: dict) -> None:
    for col, _defn, _dtype in UNIPROT_SCHEMA_COLUMNS:
        if col not in row:
            row[col] = None
        status[col] = NO_CHANGE


def _cache_key_for_organism(organism):
    """Produce a stable, hashable cache token for an organism cell."""
    val = extract_value(organism)
    if val is None:
        return None
    if isinstance(val, list):
        return tuple(sorted(str(x).strip().lower() for x in val if x))
    return str(val).strip().lower()
