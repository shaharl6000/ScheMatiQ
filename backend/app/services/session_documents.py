"""Canonical layout of a session's on-disk source documents.

A session keeps its source documents in two sibling directories:

- ``pending_documents/`` — files uploaded but NOT yet part of a successful
  pipeline run. This is a staging area for "unverified" inputs.
- ``documents/`` — files that were part of at least one run that completed
  successfully. These are the "committed" project documents.

The move ``pending_documents/`` -> ``documents/`` happens exactly once, after a
run succeeds, in ``SchematiqRunner._move_pending_documents``. Because the folder
a file lives in *is* its commit status, the atomic ``shutil.move`` used there is
also the commit/rollback boundary: if a run fails, files simply stay pending.

This module is the single place that names those two directories and encodes
their search order. Every reader/resolver and (eventually) writer/committer that
used to build ``session_dir / "documents"`` and ``session_dir / "pending_documents"``
literally should route through here instead, so the two-directory assumption
lives in one file rather than being copied across the codebase. Keeping the
assumption centralized is what makes a future storage-model change (e.g. a single
directory plus a ``status`` field) a localized edit rather than a codebase sweep.

Note on ordering: the correct search order is caller-dependent and is NOT
uniform across the codebase. Bundle/preview resolvers look at ``documents/``
first (committed originals win); the pipeline config resolver lists pending
first. ``local_document_dirs`` therefore exposes the order via ``pending_first``
rather than baking in a single order.
"""

from pathlib import Path
from typing import Iterator

from app.services.data_utils import candidate_data_dirs

# The two directory names, defined once. Nothing else in the codebase should
# spell these string literals.
COMMITTED_DIRNAME = "documents"
PENDING_DIRNAME = "pending_documents"


def committed_docs_dir(session_dir: Path) -> Path:
    """Committed-documents dir for a session directory (``.../{session_id}``)."""
    return session_dir / COMMITTED_DIRNAME


def pending_docs_dir(session_dir: Path) -> Path:
    """Pending-documents (staging) dir for a session directory."""
    return session_dir / PENDING_DIRNAME


def local_document_dirs(
    session_id: str, *, pending_first: bool = False
) -> Iterator[Path]:
    """Yield the session's document directories that exist on the local disk.

    Iterates every candidate data root (``candidate_data_dirs``) and yields the
    ``documents/`` and ``pending_documents/`` dirs that are present, so callers
    keep a single search skeleton instead of re-deriving the layout. Only
    existing directories are yielded; a caller that also needs the matching logic
    applies it to the files inside each yielded dir.

    ``pending_first`` selects the per-root order: default yields committed then
    pending (originals win on de-dup); ``True`` yields pending then committed.
    """
    for base in candidate_data_dirs():
        session_dir = base / session_id
        ordered = (
            (pending_docs_dir(session_dir), committed_docs_dir(session_dir))
            if pending_first
            else (committed_docs_dir(session_dir), pending_docs_dir(session_dir))
        )
        for doc_dir in ordered:
            if doc_dir.is_dir():
                yield doc_dir
