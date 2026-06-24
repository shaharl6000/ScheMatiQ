"""Shared utilities for reading and deduplicating data rows across file locations."""

import json
import logging
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


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


def _resolve_source_document(row: dict) -> str:
    """Extract the source-document identifier from *row* regardless of format."""
    src = row.get('_source_document') or row.get('source_document') or ''
    if not src:
        papers = row.get('_papers') or row.get('papers') or []
        if isinstance(papers, str):
            papers = [papers]
        if papers:
            src = Path(papers[0]).stem
    return src

# Resolve directory paths relative to the module location for reliability
# across Docker/Railway and local dev environments.
_MODULE_DIR = Path(__file__).parent        # app/services/
_APP_DIR = _MODULE_DIR.parent              # app/
_BACKEND_DIR = _APP_DIR.parent             # backend/


def get_schematiq_work_dir() -> Path:
    """Get the schematiq_work directory path, resolved from module location."""
    return _BACKEND_DIR / "schematiq_work"


def get_data_dir() -> Path:
    """Get the data directory path, resolved from module location.

    Checks both Docker/Railway location (backend/data) and local dev
    location (backend/app/data), preferring whichever has content.
    """
    docker_data_dir = _BACKEND_DIR / "data"
    local_data_dir = _APP_DIR / "data"

    if docker_data_dir.exists() and any(docker_data_dir.iterdir()):
        return docker_data_dir
    elif local_data_dir.exists() and any(local_data_dir.iterdir()):
        return local_data_dir
    else:
        docker_data_dir.mkdir(exist_ok=True)
        return docker_data_dir


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
    """
    if work_dir is None:
        work_dir = get_schematiq_work_dir()
    if data_dir is None:
        data_dir = get_data_dir()

    data_files: List[Path] = []
    extracted_file = work_dir / session_id / "extracted_data.jsonl"
    schematiq_data_file = work_dir / session_id / "data.jsonl"
    load_data_file = data_dir / session_id / "data.jsonl"

    if extracted_file.exists():
        data_files.append(extracted_file)
    if not data_files and schematiq_data_file.exists():
        data_files.append(schematiq_data_file)
    if load_data_file.exists() and load_data_file.resolve() not in {
        f.resolve() for f in data_files
    }:
        data_files.append(load_data_file)
    return data_files


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
