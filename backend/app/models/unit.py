"""Unit view models for observation unit grouping and merging."""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field


class UnitSummary(BaseModel):
    """Summary information for a single observation unit."""
    name: str
    row_count: int
    source_documents: List[str] = Field(default_factory=list)
    is_merged: bool = False
    original_units: Optional[List[str]] = None


class UnitListResponse(BaseModel):
    """Response containing list of all observation units with statistics."""
    units: List[UnitSummary]
    total_units: int
    total_rows: int


class MergeUnitsRequest(BaseModel):
    """Request to merge multiple observation units into one."""
    source_units: List[str] = Field(..., min_length=2, description="Units to merge (at least 2)")
    target_unit: str = Field(..., min_length=1, description="Name for the merged unit")
    strategy: Literal['rename', 'combine'] = Field(
        default='rename',
        description="Merge strategy: 'rename' updates unit names, 'combine' concatenates row data"
    )


class MergeUnitsResponse(BaseModel):
    """Response after merging units."""
    success: bool
    message: str
    merged_unit: Optional[UnitSummary] = None
    rows_affected: int = 0


class UnitSimilarity(BaseModel):
    """Suggested merge based on similarity between units."""
    units: List[str] = Field(..., min_length=2, description="Similar unit names")
    similarity: float = Field(..., ge=0.0, le=1.0, description="Similarity score (0-1)")
    suggested_name: str = Field(..., description="Suggested name for merged unit")
    reason: str = Field(..., description="Explanation for why these units are similar")


class UnitSuggestionsResponse(BaseModel):
    """Response containing merge suggestions."""
    suggestions: List[UnitSimilarity]
    threshold: float = Field(..., description="Minimum similarity threshold used")
