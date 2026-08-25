"""Figure extraction models — PDFFigures 2.0 output for a source PDF's images.

Persisted as manifest.json under documents/figures/{doc_stem}/ so a later
step (a vision-LLM read of each image, joined back into the table via
source_document — see DataRow.source_document in app.models.session) can
consume it without re-parsing the PDF.
"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class FigureBBox(BaseModel):
    """Bounding box in PDFFigures 2.0's pixel space (top-left origin, 72 DPI)."""
    left: float
    top: float
    right: float
    bottom: float
    page_no: int


class FigureContextParagraph(BaseModel):
    """A body paragraph near a figure, used to give it textual context."""
    text: str
    position: Literal["before", "after"]
    distance: int  # index gap from the figure in the document's section/paragraph ordering


class ExtractedFigure(BaseModel):
    """One captioned figure or table pulled from a PDF via PDFFigures 2.0."""
    figure_id: str  # f"{doc_stem}_fig{seq:03d}"
    source_document: str
    figure_label: Literal["Figure", "Table"]  # PDFFigures 2.0's figType
    page_no: int
    region_bbox: Optional[FigureBBox] = None   # regionBoundary — the saved image's bbox
    caption_bbox: Optional[FigureBBox] = None  # captionBoundary
    image_filename: str  # relative to the manifest's own directory
    caption: Optional[str] = None
    image_text: List[str] = Field(default_factory=list)  # text found inside the figure itself
    nearby_heading: Optional[str] = None
    context_paragraphs: List[FigureContextParagraph] = Field(default_factory=list)
    origin_name: Optional[str] = None  # PDFFigures 2.0's own figure "name" (e.g. "1") — traceability
    # "caption_fallback" means PDFFigures 2.0's own region_bbox was implausible (e.g. a
    # sliver unrelated to the figure) and we re-rendered a caption-anchored crop ourselves
    # instead — see _is_region_degenerate() in figure_extraction_service.py.
    region_source: Literal["detected", "caption_fallback"] = "detected"
    extracted_at: datetime = Field(default_factory=datetime.now)

    # Left empty here on purpose — a future vision-LLM step (out of scope for this pass) fills
    # these in place, so that step is a manifest.json rewrite rather than a schema migration.
    vision_description: Optional[str] = None
    vision_model: Optional[str] = None
    vision_extracted_at: Optional[datetime] = None


class FigureExtractionManifest(BaseModel):
    """Per-document manifest written alongside the extracted figure images."""
    source_document: str
    extracted_at: datetime = Field(default_factory=datetime.now)
    figure_count: int
    figures: List[ExtractedFigure] = Field(default_factory=list)
    status: Literal["ok", "failed"]
    error: Optional[str] = None
