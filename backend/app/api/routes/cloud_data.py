"""Cloud data API endpoints for datasets, templates, and initial schemas."""

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel

from app.storage import get_storage, DatasetInfo, FileInfo, TemplateInfo, InitialSchemaInfo

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


# ==================
# Initial Schema Endpoints
# ==================

class InitialSchemaColumnResponse(BaseModel):
    """Response model for a column in an initial schema."""
    name: str
    definition: str
    rationale: str
    allowed_values: Optional[List[str]] = None


class InitialSchemaResponse(BaseModel):
    """Response model for initial schema information."""
    name: str
    path: str
    file_type: str
    columns_count: int
    preview: str
    columns: List[Dict[str, Any]]


class InitialSchemaUploadResponse(BaseModel):
    """Response model for initial schema upload."""
    status: str
    name: str
    path: str
    columns_count: int


@router.get("/initial-schemas", response_model=List[InitialSchemaResponse])
async def list_initial_schemas():
    """List available initial schema files.

    Initial schemas are JSON files containing column definitions
    that can be used to seed the QBSD schema discovery process.

    Returns:
        List of initial schema information including name, columns count, and preview.
    """
    storage = get_storage()
    schemas = await storage.list_initial_schemas()

    return [
        InitialSchemaResponse(
            name=s.name,
            path=s.path,
            file_type=s.file_type,
            columns_count=s.columns_count,
            preview=s.preview,
            columns=s.columns
        )
        for s in schemas
    ]


@router.get("/initial-schemas/{schema_name}")
async def get_initial_schema(schema_name: str):
    """Get information about a specific initial schema.

    Args:
        schema_name: Name of the initial schema

    Returns:
        Initial schema information if found.
    """
    storage = get_storage()
    schemas = await storage.list_initial_schemas()

    for s in schemas:
        if s.name == schema_name:
            return InitialSchemaResponse(
                name=s.name,
                path=s.path,
                file_type=s.file_type,
                columns_count=s.columns_count,
                preview=s.preview,
                columns=s.columns
            )

    raise HTTPException(
        status_code=404,
        detail=f"Initial schema '{schema_name}' not found"
    )


@router.post("/initial-schemas/upload", response_model=InitialSchemaUploadResponse)
async def upload_initial_schema(file: UploadFile = File(...)):
    """Upload a new initial schema file.

    The file should be a JSON file containing an array of column definitions,
    where each column has: name, definition, rationale, and optional allowed_values.

    Args:
        file: The JSON file to upload

    Returns:
        Upload status and schema information.
    """
    import json

    # Validate file type
    if not file.filename or not file.filename.endswith('.json'):
        raise HTTPException(
            status_code=400,
            detail="File must be a JSON file (.json extension)"
        )

    # Read and parse the file
    try:
        content = await file.read()
        data = json.loads(content.decode('utf-8'))
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid JSON file: {str(e)}"
        )

    # Validate schema structure
    columns = []
    if isinstance(data, list):
        columns = data
    elif isinstance(data, dict):
        if 'columns' in data:
            columns = data['columns']
        elif 'schema' in data:
            columns = data['schema']
        else:
            raise HTTPException(
                status_code=400,
                detail="Schema must be a JSON array or an object with a 'columns' or 'schema' key"
            )
    else:
        raise HTTPException(
            status_code=400,
            detail="Schema must be a JSON array or an object with a 'columns' or 'schema' key"
        )

    # Validate columns have required fields
    for i, col in enumerate(columns):
        if not isinstance(col, dict):
            raise HTTPException(
                status_code=400,
                detail=f"Column {i} must be an object"
            )
        if 'name' not in col or 'definition' not in col or 'rationale' not in col:
            raise HTTPException(
                status_code=400,
                detail=f"Column {i} must have 'name', 'definition', and 'rationale' fields"
            )

    if len(columns) == 0:
        raise HTTPException(
            status_code=400,
            detail="Schema must contain at least one column"
        )

    # Upload the file
    storage = get_storage()
    schema_name = file.filename.replace('.json', '')

    try:
        path = await storage.upload_initial_schema(schema_name, content, "application/json")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload schema: {str(e)}"
        )

    return InitialSchemaUploadResponse(
        status="success",
        name=schema_name,
        path=path,
        columns_count=len(columns)
    )
