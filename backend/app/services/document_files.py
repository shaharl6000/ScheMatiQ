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
    """
    try:
        names = [d["name"] for d in unit_view_service.get_source_documents(session_id)]
    except Exception:
        names = []

    out: List[Tuple[str, bytes]] = []
    seen: set = set()
    cloud_dataset = getattr(session.metadata, "cloud_dataset", None)
    storage = None

    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)

        local = _find_local_document(session_id, name)
        if local is not None:
            try:
                out.append((local.name, local.read_bytes()))
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

    return out
