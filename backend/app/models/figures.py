"""Figure extraction models — Docling output for a source PDF's images.

Persisted as manifest.json under documents/figures/{doc_stem}/ so the
vision-LLM step in schematiq-lib (paper_processor.py's
_load_document_figures) can consume it without re-parsing the PDF.
"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class ExtractedFigure(BaseModel):
    """One captioned figure or table pulled from a PDF via Docling."""
    figure_id: str  # f"{doc_stem}_fig{seq:03d}"
    source_document: str
    figure_label: Literal["Figure", "Table"]  # PictureItem vs TableItem
    page_no: int
    image_filename: str  # relative to the manifest's own directory
    caption: Optional[str] = None
    origin_name: Optional[str] = None  # Docling's own node ref (e.g. "#/pictures/3") — traceability
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
