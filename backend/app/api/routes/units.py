"""API routes for observation unit view and merge operations."""

import io
import mimetypes
from pathlib import Path
from typing import Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.models.unit import (
    UnitListResponse,
    DocumentListResponse,
    DocumentSummary,
    MergeUnitsRequest,
    MergeUnitsResponse,
    UnitSuggestionsResponse,
)
from app.models.session import PaginatedData, DataRow, FilterSortRequest
from app.services.unit_view_service import unit_view_service
from app.services import session_manager, pubmed_enrichment_service
from app.services.data_utils import candidate_data_dirs
from app.storage import get_storage

router = APIRouter()


@router.get(
    "/list/{session_id}",
    response_model=UnitListResponse,
    summary="List observation units",
    description="Get a list of all observation units in a session with statistics"
)
async def list_units(session_id: str):
    """
    List all observation units in a session.

    Returns summary information for each unit including:
    - Unit name
    - Row count
    - Source documents
    - Whether this unit was created from a merge
    """
    # Verify session exists
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        return unit_view_service.get_units_summary(session_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing units: {str(e)}")


@router.get(
    "/documents/{session_id}",
    response_model=DocumentListResponse,
    summary="List source documents",
    description="Get a list of unique source documents with row counts"
)
async def list_source_documents(session_id: str):
    """
    List all unique source documents in a session.

    Returns a list of documents with their row counts, useful for
    filtering data by source document.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        documents = unit_view_service.get_source_documents(session_id)
        total_rows = sum(d["row_count"] for d in documents)

        # Enrich with URLs from session document_metadata
        doc_metadata = getattr(session.metadata, 'document_metadata', {}) or {}
        doc_summaries = []
        for d in documents:
            meta = doc_metadata.get(d["name"], {})
            raw_url = meta.get("url")
            url = (
                raw_url
                if pubmed_enrichment_service.document_qualifies_for_pubmed_link(session, d["name"])
                else None
            )
            doc_summaries.append(DocumentSummary(
                name=d["name"],
                row_count=d["row_count"],
                url=url,
            ))

        # Trigger background PubMed enrichment for documents missing URLs
        doc_names = [d["name"] for d in documents]
        enrichment_triggered = await pubmed_enrichment_service.enrich_missing(session_id, doc_names)
        enrichment_active = enrichment_triggered or pubmed_enrichment_service.is_active(session_id)

        return DocumentListResponse(
            documents=doc_summaries,
            total_documents=len(documents),
            total_rows=total_rows,
            enrichment_pending=enrichment_active,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing documents: {str(e)}")


# Extensions served with an explicit, browser-renderable content type so the
# document viewer can show them inline in an <iframe>. Anything else falls back
# to a generic type and is offered as a download by the frontend.
_INLINE_MEDIA_TYPES = {
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".htm": "text/html",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".csv": "text/csv",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
}

# Media types whose content can carry executable markup (scripts, embeds). When
# we serve these inline we add a sandboxing CSP so user-uploaded files cannot
# run script in the application origin.
_SANDBOXED_MEDIA_TYPES = {"text/html", "image/svg+xml"}


def _media_type_for(name: str) -> str:
    ext = Path(name).suffix.lower()
    if ext in _INLINE_MEDIA_TYPES:
        return _INLINE_MEDIA_TYPES[ext]
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


def _find_local_document(session_id: str, name: str) -> Optional[Path]:
    """Locate an uploaded source document on the local filesystem.

    Searches documents/ and pending_documents/ across every known data dir
    (CWD, backend, dev.sh instances). Matches the exact filename first, then
    falls back to stem matching since the recorded source_document name may
    omit the extension.
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


async def _load_document_bytes(session, session_id: str, name: str) -> Tuple[Optional[bytes], Optional[str]]:
    """Return (bytes, media_type) for a source document, or (None, None).

    Tries the local filesystem first (dev / freshly uploaded), then Supabase
    storage's ``datasets`` bucket under the session's cloud dataset. Richer
    per-document cloud-folder resolution lives in reextraction_service if this
    fallback ever proves insufficient.
    """
    local = _find_local_document(session_id, name)
    if local is not None:
        return local.read_bytes(), _media_type_for(local.name)

    cloud_dataset = getattr(session.metadata, "cloud_dataset", None)
    if cloud_dataset:
        storage = get_storage()
        try:
            content = await storage.download_file("datasets", f"{cloud_dataset}/{name}")
        except Exception:
            content = None
        if content:
            return content, _media_type_for(name)

    return None, None


@router.get(
    "/document-content/{session_id}",
    summary="Serve a source document inline",
    description="Stream the raw bytes of an uploaded source document for inline viewing",
)
async def get_document_content(
    session_id: str,
    name: str = Query(..., description="Source document filename"),
):
    """Serve an uploaded source document's raw bytes for the document viewer.

    The ``name`` is validated against the session's known source documents to
    prevent path traversal; only files that actually appear in the session are
    served. HTML/SVG is served with a sandboxing CSP so user-uploaded markup
    cannot execute scripts in the app origin.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Allowlist: only serve names that are real source documents for this session.
    known = {d["name"] for d in unit_view_service.get_source_documents(session_id)}
    if name not in known:
        raise HTTPException(status_code=404, detail="Document not found in this session")

    content, media_type = await _load_document_bytes(session, session_id, name)
    if content is None:
        raise HTTPException(status_code=404, detail="Document file is not available")

    headers = {
        "Content-Disposition": f'inline; filename="{Path(name).name}"',
        "Cache-Control": "private, max-age=60",
    }
    if media_type in _SANDBOXED_MEDIA_TYPES:
        headers["Content-Security-Policy"] = (
            "sandbox; default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'"
        )

    return StreamingResponse(io.BytesIO(content), media_type=media_type, headers=headers)


@router.post(
    "/data/{session_id}",
    response_model=PaginatedData,
    summary="Get data by unit",
    description="Get paginated data optionally filtered/searched/sorted, grouped by observation unit"
)
async def get_unit_data(
    session_id: str,
    units: Optional[str] = Query(None, description="Comma-separated unit names to filter by"),
    page: int = Query(0, ge=0, description="Page number (0-indexed)"),
    page_size: int = Query(50, ge=1, le=1000, description="Units per page"),
    request: Optional[FilterSortRequest] = None,
):
    """
    Get paginated data grouped by observation unit.

    Search/filters/sort are applied at the row level across the full session
    (so a match on any page surfaces in the result). Pagination is by unit;
    units with zero matching rows drop out of the view.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    unit_filter = [u.strip() for u in units.split(',') if u.strip()] if units else None

    filters = None
    sort = None
    search = None
    if request:
        filters = [f.dict() for f in request.filters] if request.filters else None
        sort = [s.dict() for s in request.sort] if request.sort else None
        search = request.search

    try:
        rows, total_unit_count, filtered_unit_count, total_row_count = unit_view_service.get_unit_grouped_data(
            session_id=session_id,
            unit_filter=unit_filter,
            page=page,
            page_size=page_size,
            search=search,
            filters=filters,
            sort=sort,
        )

        # Convert to DataRow objects
        data_rows = []
        for row_data in rows:
            # Handle both flat and nested data formats
            if 'data' in row_data:
                data_row = DataRow(
                    row_name=row_data.get('row_name'),
                    papers=row_data.get('papers', []),
                    data=row_data.get('data', {}),
                    unit_name=row_data.get('_unit_name') or row_data.get('unit_name'),
                    source_document=row_data.get('_source_document') or row_data.get('source_document'),
                    parent_document=row_data.get('_parent_document'),
                    cell_status=row_data.get('_cell_status'),
                )
            else:
                # Flat format - extract special fields
                flat_data = dict(row_data)
                row_name = flat_data.pop('row_name', None)
                papers = flat_data.pop('papers', [])
                unit_name = flat_data.pop('_unit_name', None) or flat_data.pop('unit_name', None)
                source_document = flat_data.pop('_source_document', None) or flat_data.pop('source_document', None)
                parent_document = flat_data.pop('_parent_document', None)
                cell_status = flat_data.pop('_cell_status', None)
                flat_data.pop('_original_units', None)  # Remove internal merge tracking

                data_row = DataRow(
                    row_name=row_name,
                    papers=papers if isinstance(papers, list) else [papers] if papers else [],
                    data=flat_data,
                    unit_name=unit_name,
                    source_document=source_document,
                    parent_document=parent_document,
                    cell_status=cell_status,
                )
            data_rows.append(data_row)

        # Pagination is unit-based: counts reflect units, not rows
        any_filter_active = bool(unit_filter or search or filters)
        effective_count = filtered_unit_count if any_filter_active else total_unit_count
        has_more = (page + 1) * page_size < effective_count

        return PaginatedData(
            rows=data_rows,
            total_count=total_unit_count,
            filtered_count=filtered_unit_count if any_filter_active else None,
            page=page,
            page_size=page_size,
            has_more=has_more
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting unit data: {str(e)}")


@router.post(
    "/merge/{session_id}",
    response_model=MergeUnitsResponse,
    summary="Merge observation units",
    description="Merge multiple observation units into a single unit"
)
async def merge_units(session_id: str, request: MergeUnitsRequest):
    """
    Merge multiple observation units into one.

    All rows belonging to the source units will be updated to belong to the target unit.
    The original unit names are preserved in _original_units for potential undo.

    Args:
        session_id: The session ID
        request: Merge request with:
            - source_units: List of unit names to merge (at least 2)
            - target_unit: Name for the merged unit
            - strategy: 'rename' (default) or 'combine'
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Validate that we have at least 2 source units
    if len(request.source_units) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least 2 source units are required for merge"
        )

    # Validate that target unit name is not empty
    if not request.target_unit.strip():
        raise HTTPException(
            status_code=400,
            detail="Target unit name cannot be empty"
        )

    try:
        result = unit_view_service.merge_units(session_id, request)

        if not result.success:
            raise HTTPException(status_code=400, detail=result.message)

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error merging units: {str(e)}")


@router.get(
    "/suggestions/{session_id}",
    response_model=UnitSuggestionsResponse,
    summary="Get merge suggestions",
    description="Find similar observation units that could be merged"
)
async def get_merge_suggestions(
    session_id: str,
    threshold: float = Query(
        0.8,
        ge=0.0,
        le=1.0,
        description="Minimum similarity threshold (0-1)"
    ),
    auto_merge: bool = Query(
        False,
        description="Auto-merge 100% similarity matches before returning suggestions"
    )
):
    """
    Get suggestions for units that could be merged based on name similarity.

    Uses string similarity algorithms to find units with similar names that
    might represent the same entity (e.g., typos, different formatting).

    Args:
        session_id: The session ID
        threshold: Minimum similarity score (0-1) to include in suggestions.
                   Default is 0.8 (80% similar).
        auto_merge: If True, automatically merge units with 100% similarity
                    before returning remaining suggestions.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        return unit_view_service.suggest_similar_units(session_id, threshold, auto_merge)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting suggestions: {str(e)}")
