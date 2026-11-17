"""Shared write path for session data JSONL files.

Several services rewrite a session's data file: cell edits (data_editor),
column removal/merge/reprocess (schema_manager), unit merge/rename
(unit_view_service), observation unit removal (observation_unit_manager) and
continue discovery. Each one does a read-modify-write of the whole file.

Two rules keep those writes safe:

1. One lock per session. Every read-modify-write of a session's data files
   runs under ``session_write_lock(session_id)``, so two writers cannot both
   read the old file and have the second write drop the first one's change.
   The lock is an ``asyncio.Lock``: hold it on the event loop and run blocking
   work inside it with ``asyncio.to_thread`` (never take it from a worker
   thread).
2. Atomic write, then persist. ``save_session_data_file`` writes through
   ``write_jsonl_atomic`` (readers never see a truncated file) and then uploads
   the file with ``persist_session_data_file``, so the edit survives a
   redeploy like every other data-file write.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from app.services.atomic_jsonl import write_jsonl_atomic
from app.services.data_utils import persist_session_data_file

logger = logging.getLogger(__name__)

_session_write_locks: Dict[str, asyncio.Lock] = {}


def session_write_lock(session_id: str) -> asyncio.Lock:
    """Return the per-session lock that serializes data-file rewrites."""
    lock = _session_write_locks.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _session_write_locks[session_id] = lock
    return lock


async def save_session_data_file(
    session_id: str,
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    ensure_ascii: bool = True,
) -> None:
    """Atomically rewrite a session data file and persist it to storage.

    The caller must hold ``session_write_lock(session_id)`` for the whole
    read-modify-write.
    """
    write_jsonl_atomic(Path(path), rows, ensure_ascii=ensure_ascii)
    await persist_session_data_file(session_id, Path(path))


FileSignature = Tuple[int, int]


def file_signatures(paths: Iterable[Path]) -> Dict[Path, Optional[FileSignature]]:
    """Return (mtime_ns, size) for each path, or None when it does not exist."""
    result: Dict[Path, Optional[FileSignature]] = {}
    for p in paths:
        try:
            st = Path(p).stat()
            result[Path(p)] = (st.st_mtime_ns, st.st_size)
        except OSError:
            result[Path(p)] = None
    return result


async def persist_changed_files(
    session_id: str,
    before: Mapping[Path, Optional[FileSignature]],
    paths: Iterable[Path],
) -> List[Path]:
    """Persist every file in *paths* whose signature differs from *before*.

    For writers that rewrite files in a worker thread (and so cannot call the
    async persist themselves): snapshot ``file_signatures`` before the work,
    then call this afterwards under the same lock.
    """
    changed: List[Path] = []
    after = file_signatures(paths)
    for p, sig in after.items():
        if sig is not None and before.get(p) != sig:
            await persist_session_data_file(session_id, p)
            changed.append(p)
    return changed

