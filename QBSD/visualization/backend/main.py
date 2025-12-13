"""FastAPI application for QBSD visualization module."""

import sys
from pathlib import Path

# Add parent directory to path for QBSD imports
sys.path.append(str(Path(__file__).parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.load import router as load_router
from api.qbsd import router as qbsd_router
from api.websocket import router as websocket_router
from api.schema import router as schema_router
from constants import (
    API_TITLE, API_DESCRIPTION, API_VERSION,
    ALLOWED_ORIGINS, DEFAULT_HOST, DEFAULT_PORT,
    HEALTH_CHECK_MESSAGE, API_ROOT_MESSAGE
)

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION
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

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": API_ROOT_MESSAGE, "version": API_VERSION}

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": HEALTH_CHECK_MESSAGE}

if __name__ == "__main__":
    import os
    import uvicorn
    # Use reload only in development (when RAILWAY_ENVIRONMENT is not set)
    is_production = os.environ.get("RAILWAY_ENVIRONMENT") is not None
    uvicorn.run("main:app", host=DEFAULT_HOST, port=DEFAULT_PORT, reload=not is_production)