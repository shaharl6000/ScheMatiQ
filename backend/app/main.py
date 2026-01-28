"""FastAPI application for QBSD visualization module."""

import logging
import sys
from pathlib import Path

# Configure logging to show in container logs
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s',
    stream=sys.stdout,
    force=True
)

# Add qbsd-lib to Python path (sibling directory to backend)
_QBSD_LIB_PATH = Path(__file__).parent.parent.parent / "qbsd-lib"
if _QBSD_LIB_PATH.exists() and str(_QBSD_LIB_PATH) not in sys.path:
    sys.path.insert(0, str(_QBSD_LIB_PATH))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.load import router as load_router
from app.api.routes.qbsd import router as qbsd_router
from app.api.routes.websocket import router as websocket_router
from app.api.routes.schema import router as schema_router
from app.api.routes.cloud_data import router as cloud_data_router
from app.api.routes.observation_unit import router as observation_unit_router
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
        },
        {
            "name": "cloud-data",
            "description": "Cloud data endpoints for datasets and templates. List and access pre-uploaded datasets and template tables."
        },
        {
            "name": "observation-unit",
            "description": "Observation unit management operations. Add or remove rows (observation units) from extracted tables."
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
    expose_headers=["Content-Disposition"],  # Allow frontend to read download filename
)

# Include routers
app.include_router(load_router, prefix="/api/load", tags=["load"])
app.include_router(qbsd_router, prefix="/api/qbsd", tags=["qbsd"])
app.include_router(schema_router, prefix="/api/schema", tags=["schema"])
app.include_router(websocket_router, prefix="/ws", tags=["websocket"])
app.include_router(cloud_data_router, prefix="/api", tags=["cloud-data"])
app.include_router(observation_unit_router, prefix="/api/observation-unit", tags=["observation-unit"])

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