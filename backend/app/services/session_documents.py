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

# Per-document artifacts live in a sibling subdirectory keyed by document stem:
# ``{pending,committed}/{subdir}/{stem}/``. They ride the pending -> committed
# commit together with the document's ``.txt`` (see
# SchematiqRunner._move_pending_documents), so a failed run leaves a document's
# artifacts uncommitted alongside its text rather than orphaning them in
# documents/. A new per-document artifact type is committed automatically by
# adding its subdirectory name here — no other code changes.
PER_DOCUMENT_ARTIFACT_SUBDIRS = ("figures",)


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


def link_document_artifacts_into_view(
    source_docs_dir: Path, view_dir: Path, stem: str
) -> None:
    """Mirror a document's per-document artifacts into a derived view directory.

    The pipeline derives a document's artifact dir as ``<docs_dir>/{subdir}/{stem}``
    from whichever directory it reads the document's text (see schematiq-lib
    ``table_builder``). A run redirected to a derived view — ``capped_documents/``
    (document cap) or ``documents_filtered/`` (incremental extraction) — reads the
    text from the view, so each artifact ``{stem}`` dir must be present there too
    or it is silently dropped. Symlink every registered artifact
    (``PER_DOCUMENT_ARTIFACT_SUBDIRS``) into the view, the same way the view's
    documents themselves are linked: a single mechanism with no per-platform
    branching. The link is live, so it reflects later changes to the source and a
    view that is not rebuilt between runs stays correct; re-linking is a no-op.
    """
    for subdir in PER_DOCUMENT_ARTIFACT_SUBDIRS:
        src = source_docs_dir / subdir / stem
        if not src.is_dir():
            continue
        dest = view_dir / subdir / stem
        if dest.exists() or dest.is_symlink():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.symlink_to(src.resolve(), target_is_directory=True)
