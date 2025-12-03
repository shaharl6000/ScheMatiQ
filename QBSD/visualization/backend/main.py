"""FastAPI application for QBSD visualization module."""

import sys
from pathlib import Path

# Add parent directory to path for QBSD imports
sys.path.append(str(Path(__file__).parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.upload import router as upload_router
from api.qbsd import router as qbsd_router
from api.websocket import router as websocket_router

app = FastAPI(
    title="QBSD Visualization API",
    description="Interactive visualization and schema editing for QBSD",
    version="1.0.0"
)

# CORS middleware for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(upload_router, prefix="/api/upload", tags=["upload"])
app.include_router(qbsd_router, prefix="/api/qbsd", tags=["qbsd"])
app.include_router(websocket_router, prefix="/ws", tags=["websocket"])

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "QBSD Visualization API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)