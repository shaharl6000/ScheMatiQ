"""FastAPI application for QBSD visualization module."""

import sys
from pathlib import Path

# Add parent directories to path for imports
backend_dir = Path(__file__).parent.parent
project_root = backend_dir.parent
sys.path.insert(0, str(backend_dir))
sys.path.insert(0, str(project_root / "qbsd-lib"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.load import router as load_router
from app.api.routes.qbsd import router as qbsd_router
from app.api.routes.websocket import router as websocket_router
from app.api.routes.schema import router as schema_router
from app.core.config import (
    API_TITLE, API_DESCRIPTION, API_VERSION,
    ALLOWED_ORIGINS, DEFAULT_HOST, DEFAULT_PORT,
    HEALTH_CHECK_MESSAGE, API_ROOT_MESSAGE
)

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=[
        {
            "name": "root",
            "description": "Root and health check endpoints"
        },
        {
            "name": "load",
            "description": "File upload and data loading operations. Supports CSV, JSON, JSONL files with schema extraction and document processing."
        },
        {
            "name": "qbsd",
            "description": "Query-Based Schema Discovery operations. Configure and run QBSD pipelines to discover schemas from document collections."
        },
        {
            "name": "schema",
            "description": "Schema editing operations. Edit, add, delete, and merge columns. Trigger document reprocessing."
        },
        {
            "name": "websocket",
            "description": "WebSocket endpoints for real-time progress updates and log streaming."
        }
    ]
)

# CORS middleware for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(load_router, prefix="/api/load", tags=["load"])
app.include_router(qbsd_router, prefix="/api/qbsd", tags=["qbsd"])
app.include_router(schema_router, prefix="/api/schema", tags=["schema"])
app.include_router(websocket_router, prefix="/ws", tags=["websocket"])

@app.get("/", tags=["root"], summary="API Root", description="Returns API information and version")
async def root():
    """Root endpoint returning API info and version."""
    return {"message": API_ROOT_MESSAGE, "version": API_VERSION}

@app.get("/health", tags=["root"], summary="Health Check", description="Returns the health status of the API")
async def health_check():
    """Health check endpoint for monitoring and load balancer probes."""
    return {"status": HEALTH_CHECK_MESSAGE}

if __name__ == "__main__":
    import os
    import uvicorn
    # Use reload only in development (when RAILWAY_ENVIRONMENT is not set)
    is_production = os.environ.get("RAILWAY_ENVIRONMENT") is not None
    uvicorn.run("main:app", host=DEFAULT_HOST, port=DEFAULT_PORT, reload=not is_production)