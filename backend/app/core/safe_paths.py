"""Turn client-supplied file names into names that are safe to join to a path.

Upload routes receive a file name from the client (the multipart
``filename`` or a JSON field). Neither FastAPI nor Starlette strips directory
parts from it, so ``"../../x.json"`` or ``"/etc/x"`` joined to a session dir
would write outside it (CWE-22, path traversal). Browsers send only the bare
name (``File.name``), so keeping the last path component never changes a
legitimate upload.
"""

from pathlib import Path
from typing import Optional


def safe_upload_name(raw: Optional[str]) -> str:
    """Return the bare file name of *raw*, without any directory part.

    Both ``/`` and ``\\`` count as separators, so Windows-style paths are
    handled too. The name is otherwise kept as sent (no stripping), so a
    stored name always matches what the client sends back later. Raises
    ``ValueError`` when nothing usable is left (empty or blank, ``.``,
    ``..``, or a NUL byte).
    """
    name = (raw or "").replace("\\", "/").rsplit("/", 1)[-1]
    if name.strip() in ("", ".", "..") or "\x00" in name:
        raise ValueError(f"Invalid file name: {raw!r}")
    return name


def is_within(base: Path, candidate: Path) -> bool:
    """Whether *candidate* resolves to *base* or a path inside it."""
    base_resolved = base.resolve()
    target = candidate.resolve()
    return target == base_resolved or base_resolved in target.parents
