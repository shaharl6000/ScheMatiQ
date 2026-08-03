"""Shared utilities for reading and deduplicating data rows across file locations."""

import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from schematiq.value_extraction.utils.schema_builder import sanitize_column_name

logger = logging.getLogger(__name__)


def canonicalize_column_name(raw_name: str) -> Tuple[str, Optional[str]]:
    """Split a user-typed column name into (canonical_name, display_name).

    The *canonical* name is the value stored and used everywhere internally
    (schema keys, data-row keys, baselines, re-extraction merge, filters/sort).
    It is the user's text with characters invalid for downstream schema keys
    (anything outside ``[a-zA-Z0-9_]``) replaced by ``_`` — the same rule used
    for Gemini controlled-generation property names, so the two stay consistent.

    The *display_name* preserves the exact text the user typed and is returned
    **only when it differs** from the canonical name. When it is ``None`` the
    canonical name is already display-ready (callers should fall back to their
    normal label formatting).
    """
    raw = (raw_name or "").strip()
    canonical = sanitize_column_name(raw)
    display = raw if raw != canonical else None
    return canonical, display


def row_dedup_key(row: dict) -> Tuple[str, str]:
    """Build a composite dedup key from a data row.

    The same observation-unit name (e.g. "Chief Justice Roberts") can appear in
    multiple source documents. Using only ``_row_name`` as a unique identifier
    causes one document's data to silently overwrite another's.  This helper
    returns ``(row_name, source_document)`` so that callers can disambiguate.

    *source_document* is resolved in priority order:
      ``_source_document`` > ``source_document`` > first entry of
      ``_papers``/``papers`` (stem only) > ``""``.
    """
    row_name: str = row.get('_row_name') or row.get('row_name') or ''
    src = _resolve_source_document(row)
    return (row_name, src)


def row_name_of(row: dict) -> Optional[str]:
    """Return a data row's name, tolerating both 'row_name' and '_row_name'.

    Non-underscore key takes precedence to match existing call sites; rows
    written by the library carry '_row_name', API-shaped rows carry 'row_name',
    and in practice only one is present.
    """
    return row.get('row_name') or row.get('_row_name')


def extract_papers(row: dict) -> List[str]:
    """Extract source-document references from a data.jsonl row.

    Tolerates every shape the pipeline emits: ``papers`` / ``_papers`` /
    ``Papers`` (top-level or nested under ``data``), the ScheMatiQ
    answer-wrapped dict (``{"answer": [...]}``), and a bare string. Empty
    entries are dropped. Returns an empty list when no references are present.
    """
    papers_raw = (
        row.get('papers') or
        row.get('_papers') or
        row.get('Papers') or
        row.get('data', {}).get('Papers') or
        row.get('data', {}).get('papers') or
        []
    )
    if isinstance(papers_raw, dict) and 'answer' in papers_raw:
        papers_raw = papers_raw.get('answer', [])
    if isinstance(papers_raw, str):
        return [papers_raw] if papers_raw else []
    if isinstance(papers_raw, list):
        return [p for p in papers_raw if p]
    return []


def _resolve_source_document(row: dict) -> str:
    """Extract the source-document identifier from *row* regardless of format."""
    src = row.get('_source_document') or row.get('source_document') or ''
    if not src:
        papers = extract_papers(row)
        if papers:
            src = Path(papers[0]).stem
    return src

# Resolve directory paths relative to the module location for reliability
# across Docker/Railway and local dev environments.
_MODULE_DIR = Path(__file__).parent        # app/services/
_APP_DIR = _MODULE_DIR.parent              # app/
_BACKEND_DIR = _APP_DIR.parent             # backend/


def dev_instance_dirs(subdir: str) -> List[Path]:
    """dev.sh isolation dirs: ``.dev-data/instance-*/<subdir>`` (sorted, deduped).

    Returns an empty list when there is no ``.dev-data`` root (Docker / prod).
    """
    dev_root = _BACKEND_DIR.parent / ".dev-data"
    if not dev_root.is_dir():
        return []
    seen: set[Path] = set()
    dirs: List[Path] = []
    for instance_dir in sorted(dev_root.glob(f"instance-*/{subdir}")):
        resolved = instance_dir.resolve()
        if resolved not in seen:
            seen.add(resolved)
            dirs.append(instance_dir)
    return dirs


def candidate_work_dirs() -> List[Path]:
    """Search order for schematiq_work — CWD first (dev.sh / Docker), then fallbacks."""
    seen: set[Path] = set()
    dirs: List[Path] = []
    for raw in (Path.cwd() / "schematiq_work", _BACKEND_DIR / "schematiq_work",
                *dev_instance_dirs("schematiq_work")):
        resolved = raw.resolve()
        if resolved not in seen:
            seen.add(resolved)
            dirs.append(raw)
    return dirs


def candidate_data_dirs() -> List[Path]:
    """Search order for session data/ — CWD first (dev.sh / Docker), then fallbacks."""
    seen: set[Path] = set()
    dirs: List[Path] = []
    for raw in (Path.cwd() / "data", _BACKEND_DIR / "data", _APP_DIR / "data",
                *dev_instance_dirs("data")):
        resolved = raw.resolve()
        if resolved not in seen:
            seen.add(resolved)
            dirs.append(raw)
    return dirs


def get_schematiq_work_dir() -> Path:
    """Runtime schematiq_work directory (CWD-relative — matches dev.sh and Docker WORKDIR)."""
    work_dir = Path.cwd() / "schematiq_work"
    work_dir.mkdir(parents=True, exist_ok=True)
    return work_dir.resolve()


def get_data_dir() -> Path:
    """Runtime data directory (CWD-relative — matches dev.sh and Docker WORKDIR)."""
    data_dir = Path.cwd() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir.resolve()


def collect_all_data_rows(session_id: str, work_dir: Path = None, data_dir: Path = None) -> List[dict]:
    """Read and deduplicate data rows from all possible file locations.

    Data can exist in multiple locations:
    - work_dir/{session_id}/extracted_data.jsonl  (original ScheMatiQ value extraction)
    - work_dir/{session_id}/data.jsonl            (fallback location)
    - data_dir/{session_id}/data.jsonl            (document processing, continue discovery, reextraction)

    Deduplication: rows from earlier files take priority. Rows are identified by
    the composite key (row_name, source_document) via ``row_dedup_key``. Within a
    single file, duplicates are kept (matching get_data() behavior); only
    cross-file duplicates are removed.

    Args:
        session_id: The session ID
        work_dir: Path to schematiq_work directory
        data_dir: Path to data directory

    Returns:
        Combined, deduplicated list of raw row dicts
    """
    if work_dir is None:
        work_dir = get_schematiq_work_dir()
    if data_dir is None:
        data_dir = get_data_dir()

    data_files = []

    # 1. Check schematiq_work for extracted_data.jsonl
    extracted_file = work_dir / session_id / "extracted_data.jsonl"
    if extracted_file.exists():
        data_files.append(extracted_file)

    # 2. Check schematiq_work for data.jsonl (only if extracted_data.jsonl doesn't exist)
    if not data_files:
        schematiq_data_file = work_dir / session_id / "data.jsonl"
        if schematiq_data_file.exists():
            data_files.append(schematiq_data_file)

    # 3. Always check data directory (may contain additional documents)
    data_dir_file = data_dir / session_id / "data.jsonl"
    if data_dir_file.exists() and data_dir_file not in data_files:
        data_files.append(data_dir_file)

    if not data_files:
        return []

    all_rows = []
    seen_keys: set = set()

    for data_file in data_files:
        file_keys: set = set()
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        row_data = json.loads(line.strip())
                        key = row_dedup_key(row_data)
                        if key[0] and key in seen_keys:
                            continue
                        if key[0]:
                            file_keys.add(key)
                        all_rows.append(row_data)
                    except (json.JSONDecodeError, TypeError):
                        pass
        except Exception as e:
            logger.warning("Error reading data file %s: %s", data_file, e)
        seen_keys.update(file_keys)

    return all_rows


def enumerate_session_data_files(
    session_id: str,
    work_dir: Path = None,
    data_dir: Path = None,
) -> List[Path]:
    """Return all session data JSONL paths that may hold table rows.

    Matches the set processed by ``reextraction_service._merge_reextracted_data``:
    schematiq_work/extracted_data.jsonl, schematiq_work/data.jsonl (fallback),
    and data/data.jsonl (additional documents).

    When *work_dir* is omitted, searches ``candidate_work_dirs()`` in order so
    dev.sh isolation (``.dev-data/instance-N/schematiq_work``) is found even if
    a stale copy was hydrated under ``backend/schematiq_work``.
    """
    data_files: List[Path] = []
    work_dirs = [work_dir] if work_dir is not None else candidate_work_dirs()

    for wd in work_dirs:
        extracted_file = wd / session_id / "extracted_data.jsonl"
        if extracted_file.exists():
            data_files.append(extracted_file)
            break

    if not data_files:
        for wd in work_dirs:
            schematiq_data_file = wd / session_id / "data.jsonl"
            if schematiq_data_file.exists():
                data_files.append(schematiq_data_file)
                break

    data_dirs = [data_dir] if data_dir is not None else candidate_data_dirs()
    seen = {f.resolve() for f in data_files}
    for dd in data_dirs:
        load_data_file = dd / session_id / "data.jsonl"
        if load_data_file.exists() and load_data_file.resolve() not in seen:
            data_files.append(load_data_file)
            break

    return data_files


def session_data_file_candidates(
    session_id: str,
    work_dir: Path = None,
    data_dir: Path = None,
) -> List[Path]:
    """All data JSONL paths that may exist for a session (before hydration)."""
    if work_dir is None:
        work_dir = get_schematiq_work_dir()
    if data_dir is None:
        data_dir = get_data_dir()

    return [
        work_dir / session_id / "extracted_data.jsonl",
        work_dir / session_id / "data.jsonl",
        data_dir / session_id / "data.jsonl",
    ]


async def session_has_stored_data(
    session_id: str,
    storage=None,
    work_dir: Path = None,
    data_dir: Path = None,
) -> bool:
    """Return True if any session data JSONL object exists in remote storage."""
    if storage is None:
        from app.storage import get_storage

        storage = get_storage()

    for local_path in session_data_file_candidates(session_id, work_dir, data_dir):
        storage_path = storage_path_for_data_file(local_path, session_id)
        if storage_path and await storage.file_exists("data", storage_path):
            return True
    return False


async def resolve_session_data_files(
    session_id: str,
    work_dir: Path = None,
    data_dir: Path = None,
    storage=None,
) -> List[Path]:
    """Return existing local data files, hydrating from storage when needed."""
    if work_dir is None:
        work_dir = get_schematiq_work_dir()
    if data_dir is None:
        data_dir = get_data_dir()
    if storage is None:
        from app.storage import get_storage

        storage = get_storage()

    # Prefer any on-disk session data (including dev.sh instance dirs) before hydrating.
    existing = enumerate_session_data_files(session_id)
    if existing:
        return existing

    for path in session_data_file_candidates(session_id, work_dir, data_dir):
        if not path.exists():
            await ensure_session_data_file_local(session_id, path, storage=storage)

    return enumerate_session_data_files(session_id, work_dir=work_dir, data_dir=data_dir)


async def resolve_primary_session_data_file(
    session_id: str,
    work_dir: Path = None,
    data_dir: Path = None,
    storage=None,
) -> Optional[Path]:
    """Return the primary session data JSONL path, hydrating from storage when needed."""
    files = await resolve_session_data_files(
        session_id,
        work_dir=work_dir,
        data_dir=data_dir,
        storage=storage,
    )
    return files[0] if files else None


def resolve_primary_session_data_file_sync(session_id: str) -> Optional[Path]:
    """Sync entry point for thread-pool callers without a running event loop."""
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(resolve_primary_session_data_file(session_id))
    raise RuntimeError(
        "resolve_primary_session_data_file_sync cannot be called from a running event loop; "
        "use resolve_primary_session_data_file instead."
    )


def storage_path_for_data_file(local_path: Path, session_id: str) -> Optional[str]:
    """Map a local data JSONL path to its Supabase ``data`` bucket object key."""
    name = local_path.name
    if name == "extracted_data.jsonl":
        return f"{session_id}/extracted_data.jsonl"
    if name == "data.jsonl" and "schematiq_work" not in local_path.parts:
        return f"{session_id}/data.jsonl"
    return None


async def ensure_session_data_file_local(
    session_id: str,
    local_path: Path,
    storage=None,
) -> bool:
    """Ensure a data JSONL file exists locally, hydrating from storage when needed."""
    if local_path.exists():
        return True

    storage_path = storage_path_for_data_file(local_path, session_id)
    if not storage_path:
        return False

    if storage is None:
        from app.storage import get_storage

        storage = get_storage()

    try:
        data = await storage.download_file("data", storage_path)
    except Exception as exc:
        logger.debug(
            "Storage download failed for session %s path %s: %s",
            session_id,
            storage_path,
            exc,
        )
        return False

    if not data:
        return False

    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(data)
    logger.info(
        "Hydrated %s from storage data/%s for session %s",
        local_path,
        storage_path,
        session_id,
    )
    return True


async def persist_session_data_file(
    session_id: str,
    local_path: Path,
    storage=None,
) -> None:
    """Write a data JSONL file back to the active storage backend."""
    if not local_path.exists():
        return

    storage_path = storage_path_for_data_file(local_path, session_id)
    if not storage_path:
        return

    if storage is None:
        from app.storage import get_storage

        storage = get_storage()

    content = local_path.read_bytes()
    try:
        await storage.upload_file(
            "data",
            storage_path,
            content,
            content_type="application/x-ndjson",
        )
    except Exception as exc:
        logger.warning(
            "Failed to persist %s to storage data/%s for session %s: %s",
            local_path,
            storage_path,
            session_id,
            exc,
        )


def rename_column_keys_in_row(row: dict, old_name: str, new_name: str) -> bool:
    """Rename a column key (and its excerpt column) within a single data row."""
    updated = False
    old_excerpt = f"{old_name}_excerpt"
    new_excerpt = f"{new_name}_excerpt"

    if "data" in row and isinstance(row["data"], dict):
        if old_name in row["data"]:
            row["data"][new_name] = row["data"].pop(old_name)
            updated = True
        if old_excerpt in row["data"]:
            row["data"][new_excerpt] = row["data"].pop(old_excerpt)
    else:
        if old_name in row:
            row[new_name] = row.pop(old_name)
            updated = True
        if old_excerpt in row:
            row[new_excerpt] = row.pop(old_excerpt)
    return updated


def remove_column_keys_in_row(row: dict, column_name: str) -> None:
    """Remove a column key (and its excerpt column) from a single data row."""
    excerpt_column = f"{column_name}_excerpt"
    if "data" in row and isinstance(row["data"], dict):
        row["data"].pop(column_name, None)
        row["data"].pop(excerpt_column, None)
    else:
        row.pop(column_name, None)
        row.pop(excerpt_column, None)


def get_extraction_column_value(row: dict, column_name: str):
    """Return a column value from re-extraction output using schema or sanitized key.

    Controlled generation may leave values under sanitized keys (e.g.
    ``judge_full_name`` for schema column ``judge full name``).  Lookup is
    deterministic — same rules as ``align_extraction_keys_to_schema``.
    """
    from schematiq.value_extraction.utils.schema_builder import sanitize_column_name

    containers = [row]
    nested = row.get("data")
    if isinstance(nested, dict):
        containers.append(nested)

    sanitized = sanitize_column_name(column_name)
    for container in containers:
        if column_name in container:
            return container[column_name]
        if sanitized != column_name and sanitized in container:
            return container[sanitized]
    return None


def normalize_row_data(row: dict) -> dict:
    """Extract column data from a row, handling both formats.

    Some rows use a DataRow wrapper: {row_name, papers, data: {col: val, ...}}
    Others store columns directly: {_row_name, _papers, col: val, ...}

    Returns the dict containing column->value mappings.
    """
    return row.get('data', row)


def is_local_path(path: str) -> bool:
    """Check if a path looks like a local filesystem path rather than a cloud storage path."""
    if not path:
        return False
    local_indicators = [
        '/app/', '/data/', '/backend/', 'pending_documents',
        '/Users/', '/home/', 'C:\\', 'D:\\', './', '../',
        'schematiq_work/',
    ]
    for indicator in local_indicators:
        if indicator in path:
            return True
    if path.startswith('/') and path.count('/') > 2:
        return True
    return False
