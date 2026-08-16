"""Resolve and read the ORIGINAL bytes of a session's source documents.

Used to assemble the portable project bundle (export ``format=bundle``). Mirrors
the document viewer's resolution order so anything previewable is also
bundle-able: local ``documents/`` and ``pending_documents/`` across the candidate
data dirs first, then the Supabase ``datasets`` bucket under the session's
``cloud_dataset``. Returns originals (no PDF->text conversion) so re-imported
bundles preview natively.

The bundled set is the UNION of three sources, so a re-imported project can
become extraction-capable rather than merely previewable:
  1. Row-referenced documents (``unit_view_service.get_source_documents``).
  2. Documents skipped during extraction
     (``session.statistics.skipped_documents``) — these carry no rows, so they
     are invisible to (1), yet they are exactly the files a user re-imports in
     order to retry them.
  3. Any remaining source file present on local disk under the session's
     ``documents/`` / ``pending_documents/`` dirs that (1) and (2) missed
     (e.g. uploaded-but-not-yet-extracted files).

NOTE: ``_find_local_document`` duplicates the helper of the same name in
``app.api.routes.units``; a future refactor can fold both onto this module.
"""

from pathlib import Path
from typing import Iterator, List, Optional, Tuple

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


def _iter_local_documents(session_id: str) -> Iterator[Path]:
    """Yield every source file on local disk for a session.

    Searches documents/ and pending_documents/ across every candidate data dir,
    the same locations as ``_find_local_document``. De-duplicates by filename so
    the same file discovered under multiple candidate roots is yielded once.
    """
    seen_names: set = set()
    for base in candidate_data_dirs():
        for sub in ("documents", "pending_documents"):
            doc_dir = base / session_id / sub
            if not doc_dir.is_dir():
                continue
            for f in sorted(doc_dir.iterdir()):
                if f.is_file() and not f.name.startswith(".") and f.name not in seen_names:
                    seen_names.add(f.name)
                    yield f


def _skipped_document_names(session) -> List[str]:
    """Filenames of documents skipped during extraction (they carry no rows)."""
    stats = getattr(session, "statistics", None) if session else None
    skipped = getattr(stats, "skipped_documents", None) or []
    names: List[str] = []
    for doc in skipped:
        raw = doc.get("document") if isinstance(doc, dict) else getattr(doc, "document", None)
        if raw:
            names.append(Path(raw).name)
    return names


async def gather_source_documents(session, session_id: str) -> List[Tuple[str, bytes]]:
    """Return ``(filename, original_bytes)`` for each source document in a session.

    Order mirrors the viewer: local files first, then the session's cloud dataset
    in Supabase storage. Documents that cannot be located are silently skipped
    (the bundle still imports; those previews simply stay unavailable).
    """
    try:
        names = [d["name"] for d in unit_view_service.get_source_documents(session_id)]
    except Exception:
        names = []

    # Skipped documents carry no rows, so get_source_documents never lists them;
    # add them explicitly so a re-imported project can retry them.
    names.extend(_skipped_document_names(session))

    out: List[Tuple[str, bytes]] = []
    seen: set = set()       # requested names already processed
    added: set = set()      # actual filenames placed into the bundle
    cloud_dataset = getattr(session.metadata, "cloud_dataset", None)
    storage = None

    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)

        local = _find_local_document(session_id, name)
        if local is not None:
            try:
                content = local.read_bytes()
                out.append((local.name, content))
                added.add(local.name)
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
                out.append((name, content))
                added.add(name)

    # Sweep any remaining on-disk source files that neither the rows nor the
    # skipped list referenced (e.g. uploaded-but-not-yet-extracted files). Local
    # bytes only; keyed by the actual filename so nothing already bundled above
    # is duplicated.
    for local in _iter_local_documents(session_id):
        if local.name in added:
            continue
        added.add(local.name)
        try:
            out.append((local.name, local.read_bytes()))
        except Exception:
            pass

    return out
