"""Atomic rewrite helper for session data JSONL files.

Rewriting a data file with ``open(path, "w")`` truncates it before the new rows
are written, so any concurrent reader — ``GET /data/{session_id}`` on a
background poll, the unit view, statistics — can observe an empty or partially
written file and report zero rows. In the Workspace grid that surfaces as
column headers with no data underneath, because the columns come from the
schema while the rows come from the data file.

Writing to a sibling temporary file and swapping it in with ``os.replace``
makes the update atomic: a reader sees either the old file or the new one, never
a truncated one. The temp file is created in the same directory so the rename
stays on one filesystem, and is dot-prefixed with a ``.tmp`` suffix so the data
file enumeration in ``data_utils`` (which matches exact filenames) ignores it.
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)


def write_jsonl_atomic(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    ensure_ascii: bool = True,
) -> None:
    """Rewrite *path* with *rows*, one JSON object per line, atomically.

    ``ensure_ascii`` mirrors the ``json.dumps`` default so call sites keep the
    exact on-disk encoding they had before switching to this helper.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=ensure_ascii) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink()
        except OSError:
            logger.debug("Could not remove temp file %s", tmp_path)
        raise
