"""Cloud data API endpoints for datasets and templates."""

from typing import List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.storage import get_storage, DatasetInfo, FileInfo, TemplateInfo

router = APIRouter(prefix="/cloud", tags=["cloud-data"])


# ==================
# Response Models
# ==================

class DatasetResponse(BaseModel):
    """Response model for dataset information."""
    name: str
    path: str
    file_count: int
    description: Optional[str] = None


class FileResponse(BaseModel):
    """Response model for file information."""
    name: str
    path: str
    size: int
    content_type: Optional[str] = None


class TemplateResponse(BaseModel):
    """Response model for template information."""
    name: str
    path: str
    file_type: str
    description: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None


# ==================
# Dataset Endpoints
# ==================

@router.get("/datasets", response_model=List[DatasetResponse])
async def list_datasets():
    """List available datasets (document collections).

    Returns a list of datasets that can be used for QBSD processing.
    Each dataset contains multiple documents.

    Returns:
        List of dataset information including name, path, and file count.
    """
    storage = get_storage()
    datasets = await storage.list_datasets()

    return [
        DatasetResponse(
            name=d.name,
            path=d.path,
            file_count=d.file_count,
            description=d.description
        )
        for d in datasets
    ]


@router.get("/datasets/{dataset_name}/files", response_model=List[FileResponse])
async def list_dataset_files(dataset_name: str):
    """List files in a specific dataset.

    Args:
        dataset_name: Name of the dataset (e.g., 'abstracts', 'full_text')

    Returns:
        List of file information for files in the dataset.
    """
    storage = get_storage()
    files = await storage.list_dataset_files(dataset_name)

    if not files:
        # Check if dataset exists
        datasets = await storage.list_datasets()
        dataset_names = [d.name for d in datasets]
        if dataset_name not in dataset_names:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_name}' not found. Available: {dataset_names}"
            )

    return [
        FileResponse(
            name=f.name,
            path=f.path,
            size=f.size,
            content_type=f.content_type
        )
        for f in files
    ]


# ==================
# Template Endpoints
# ==================

@router.get("/templates", response_model=List[TemplateResponse])
async def list_templates():
    """List available templates (pre-made tables).

    Templates are pre-created QBSD results that can be loaded directly
    without running the QBSD pipeline.

    Returns:
        List of template information including name, file type, and row/column counts.
    """
    storage = get_storage()
    templates = await storage.list_templates()

    return [
        TemplateResponse(
            name=t.name,
            path=t.path,
            file_type=t.file_type,
            description=t.description,
            row_count=t.row_count,
            column_count=t.column_count
        )
        for t in templates
    ]


@router.get("/templates/{template_name}")
async def get_template_info(template_name: str):
    """Get information about a specific template.

    Args:
        template_name: Name of the template

    Returns:
        Template information if found.
    """
    storage = get_storage()
    templates = await storage.list_templates()

    for t in templates:
        if t.name == template_name:
            return TemplateResponse(
                name=t.name,
                path=t.path,
                file_type=t.file_type,
                description=t.description,
                row_count=t.row_count,
                column_count=t.column_count
            )

    raise HTTPException(
        status_code=404,
        detail=f"Template '{template_name}' not found"
    )


# ==================
# Cloud Documents for Sessions
# ==================

class CloudDocumentsResponse(BaseModel):
    """Response model for cloud documents grouped by dataset."""
    dataset: str
    files: List[FileResponse]


@router.get("/documents", response_model=List[CloudDocumentsResponse])
async def list_cloud_documents():
    """List all cloud documents available for adding to sessions.

    Returns documents grouped by dataset for easy selection.

    Returns:
        List of datasets with their files.
    """
    storage = get_storage()
    datasets = await storage.list_datasets()

    result = []
    for dataset in datasets:
        files = await storage.list_dataset_files(dataset.name)
        result.append(CloudDocumentsResponse(
            dataset=dataset.name,
            files=[
                FileResponse(
                    name=f.name,
                    path=f.path,
                    size=f.size,
                    content_type=f.content_type
                )
                for f in files
            ]
        ))

    return result
