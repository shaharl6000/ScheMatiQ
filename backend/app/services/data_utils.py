"""Shared utilities for reading and deduplicating data rows across file locations."""

import json
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

# Resolve directory paths relative to the module location for reliability
# across Docker/Railway and local dev environments.
_MODULE_DIR = Path(__file__).parent        # app/services/
_APP_DIR = _MODULE_DIR.parent              # app/
_BACKEND_DIR = _APP_DIR.parent             # backend/


def get_qbsd_work_dir() -> Path:
    """Get the qbsd_work directory path, resolved from module location."""
    return _BACKEND_DIR / "qbsd_work"


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
    - work_dir/{session_id}/extracted_data.jsonl  (original QBSD value extraction)
    - work_dir/{session_id}/data.jsonl            (fallback location)
    - data_dir/{session_id}/data.jsonl            (document processing, continue discovery, reextraction)

    Deduplication: rows from earlier files take priority. Rows are identified by
    '_row_name' or 'row_name' fields. Within a single file, duplicates are kept
    (matching get_data() behavior); only cross-file duplicates are removed.

    Args:
        session_id: The session ID
        work_dir: Path to qbsd_work directory
        data_dir: Path to data directory

    Returns:
        Combined, deduplicated list of raw row dicts
    """
    if work_dir is None:
        work_dir = get_qbsd_work_dir()
    if data_dir is None:
        data_dir = get_data_dir()

    data_files = []

    # 1. Check qbsd_work for extracted_data.jsonl
    extracted_file = work_dir / session_id / "extracted_data.jsonl"
    if extracted_file.exists():
        data_files.append(extracted_file)

    # 2. Check qbsd_work for data.jsonl (only if extracted_data.jsonl doesn't exist)
    if not data_files:
        qbsd_data_file = work_dir / session_id / "data.jsonl"
        if qbsd_data_file.exists():
            data_files.append(qbsd_data_file)

    # 3. Always check data directory (may contain additional documents)
    data_dir_file = data_dir / session_id / "data.jsonl"
    if data_dir_file.exists() and data_dir_file not in data_files:
        data_files.append(data_dir_file)

    if not data_files:
        return []

    all_rows = []
    seen_row_names: set = set()

    for data_file in data_files:
        file_row_names: set = set()
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        row_data = json.loads(line.strip())
                        # Cross-file dedup: skip if row_name was in a previous file
                        row_name = row_data.get('_row_name') or row_data.get('row_name')
                        if row_name and row_name in seen_row_names:
                            continue
                        if row_name:
                            file_row_names.add(row_name)
                        all_rows.append(row_data)
                    except (json.JSONDecodeError, TypeError):
                        pass
        except Exception as e:
            logger.warning("Error reading data file %s: %s", data_file, e)
        # After each file, register its row names for cross-file dedup
        seen_row_names.update(file_row_names)

    return all_rows


def normalize_row_data(row: dict) -> dict:
    """Extract column data from a row, handling both formats.

    Some rows use a DataRow wrapper: {row_name, papers, data: {col: val, ...}}
    Others store columns directly: {_row_name, _papers, col: val, ...}

    Returns the dict containing column->value mappings.
    """
    return row.get('data', row)
