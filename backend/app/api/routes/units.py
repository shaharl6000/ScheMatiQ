"""API routes for observation unit view and merge operations."""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app.models.unit import (
    UnitListResponse,
    MergeUnitsRequest,
    MergeUnitsResponse,
    UnitSuggestionsResponse,
)
from app.models.session import PaginatedData, DataRow
from app.services.unit_view_service import unit_view_service
from app.services import session_manager

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
        return {
            "documents": documents,
            "total_documents": len(documents),
            "total_rows": total_rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing documents: {str(e)}")


@router.get(
    "/data/{session_id}",
    response_model=PaginatedData,
    summary="Get data by unit",
    description="Get paginated data optionally filtered by observation unit"
)
async def get_unit_data(
    session_id: str,
    units: Optional[str] = Query(None, description="Comma-separated unit names to filter by"),
    page: int = Query(0, ge=0, description="Page number (0-indexed)"),
    page_size: int = Query(50, ge=1, le=500, description="Items per page")
):
    """
    Get paginated data optionally filtered by observation unit(s).

    When a units filter is provided, only rows belonging to those units are returned.
    Rows are sorted by unit name for consistent grouping.
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Parse comma-separated unit names
    unit_filter = [u.strip() for u in units.split(',') if u.strip()] if units else None

    try:
        rows, total_unit_count, total_row_count = unit_view_service.get_unit_grouped_data(
            session_id=session_id,
            unit_filter=unit_filter,
            page=page,
            page_size=page_size
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
                    parent_document=row_data.get('_parent_document')
                )
            else:
                # Flat format - extract special fields
                flat_data = dict(row_data)
                row_name = flat_data.pop('row_name', None)
                papers = flat_data.pop('papers', [])
                unit_name = flat_data.pop('_unit_name', None) or flat_data.pop('unit_name', None)
                source_document = flat_data.pop('_source_document', None) or flat_data.pop('source_document', None)
                parent_document = flat_data.pop('_parent_document', None)
                flat_data.pop('_original_units', None)  # Remove internal merge tracking

                data_row = DataRow(
                    row_name=row_name,
                    papers=papers if isinstance(papers, list) else [papers] if papers else [],
                    data=flat_data,
                    unit_name=unit_name,
                    source_document=source_document,
                    parent_document=parent_document
                )
            data_rows.append(data_row)

        # Pagination is unit-based: total_count = number of units
        has_more = (page + 1) * page_size < total_unit_count

        return PaginatedData(
            rows=data_rows,
            total_count=total_unit_count,
            filtered_count=len(unit_filter) if unit_filter else None,
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
