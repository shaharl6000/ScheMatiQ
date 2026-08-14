"""Resolve and read the ORIGINAL bytes of a session's source documents.

Used to assemble the portable project bundle (export ``format=bundle``). Mirrors
the document viewer's resolution order so anything previewable is also
bundle-able: local ``documents/`` and ``pending_documents/`` across the candidate
data dirs first, then the Supabase ``datasets`` bucket under the session's
``cloud_dataset``. Returns originals (no PDF->text conversion) so re-imported
bundles preview natively.

NOTE: ``_find_local_document`` duplicates the helper of the same name in
``app.api.routes.units``; a future refactor can fold both onto this module.
"""

from pathlib import Path
from typing import List, Optional, Tuple

from app.services.data_utils import candidate_data_dirs
from app.services.unit_view_service import unit_view_service
from app.storage import get_storage


def _find_local_document(session_id: str, name: str) -> Optional[Path]:
    """Locate an uploaded source document on the local filesystem.

    Searches documents/ and pending_documents/ across every known data dir.
    Matches the exact filename first, then falls back to stem matching since the
    recorded source_document name may omit the extension.
    """
    target_stem = Path(name).stem.lower()
    target_name = name.lower()
    for base in candidate_data_dirs():
        for sub in ("documents", "pending_documents"):
            doc_dir = base / session_id / sub
            if not doc_dir.is_dir():
                continue
            exact = doc_dir / name
            if exact.is_file():
                return exact
            for f in doc_dir.iterdir():
                if not f.is_file() or f.name.startswith("."):
                    continue
                if f.name.lower() == target_name or f.stem.lower() == target_stem:
                    return f
    return None


async def gather_source_documents(session, session_id: str) -> List[Tuple[str, bytes]]:
    """Return ``(filename, original_bytes)`` for each source document in a session.

    Order mirrors the viewer: local files first, then the session's cloud dataset
    in Supabase storage. Documents that cannot be located are silently skipped
    (the bundle still imports; those previews simply stay unavailable).

    Row-backed documents (``get_source_documents``) are only part of the story: a
    document that was skipped during extraction has no rows, and a file the user
    re-attached via "Show source document" lands on disk without necessarily being
    referenced by a row. Both must still travel inside the bundle so a re-imported
    project can preview them and re-run discovery against them. We therefore also
    include the session's skipped-document names (for cloud lookup) and sweep the
    local document dirs for any on-disk file not already gathered.
    """
    try:
        names = [d["name"] for d in unit_view_service.get_source_documents(session_id)]
    except Exception:
        names = []

    # Skipped documents have no rows, so append their names too (deduped below).
    try:
        stats = getattr(session, "statistics", None)
        for skipped in (stats.skipped_documents if stats and stats.skipped_documents else []):
            doc_name = getattr(skipped, "document", None)
            if doc_name:
                names.append(doc_name)
    except Exception:
        pass

    out: List[Tuple[str, bytes]] = []
    seen_stems: set = set()
    cloud_dataset = getattr(session.metadata, "cloud_dataset", None)
    storage = None

    def _record(filename: str, content: bytes) -> None:
        out.append((filename, content))
        seen_stems.add(Path(filename).stem.lower())

    for name in names:
        if not name or Path(name).stem.lower() in seen_stems:
            continue

        local = _find_local_document(session_id, name)
        if local is not None:
            try:
                _record(local.name, local.read_bytes())
                continue
            except Exception:
                pass

        if cloud_dataset:
            if storage is None:
                storage = get_storage()
            try:
                content = await storage.download_file("datasets", f"{cloud_dataset}/{name}")
            except Exception:
                content = None
            if content:
                _record(name, content)

    # Sweep the local document dirs for any on-disk file not yet gathered. This
    # captures re-attached originals and skipped documents whose files exist
    # locally but are not referenced by any row.
    for base in candidate_data_dirs():
        for sub in ("documents", "pending_documents"):
            doc_dir = base / session_id / sub
            if not doc_dir.is_dir():
                continue
            for f in sorted(doc_dir.iterdir()):
                if not f.is_file() or f.name.startswith("."):
                    continue
                if f.stem.lower() in seen_stems:
                    continue
                try:
                    _record(f.name, f.read_bytes())
                except Exception:
                    pass

    return out
