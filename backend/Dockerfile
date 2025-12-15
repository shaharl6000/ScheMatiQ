# Backend Dockerfile for QueryDiscovery
# Build from project root: docker build -f backend/Dockerfile -t qbsd-backend .

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for ML libraries (torch, transformers, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy qbsd-lib first (dependency for backend)
COPY qbsd-lib/ /app/qbsd-lib/

# Install CPU-only PyTorch first (saves ~2GB vs CUDA version)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install qbsd-lib as editable package (torch already satisfied)
RUN pip install --no-cache-dir -e /app/qbsd-lib/

# Copy backend requirements and install (separate layer for caching)
COPY backend/requirements.txt /app/backend/
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Copy backend source code (updated 2025-12-15)
COPY backend/ /app/backend/

WORKDIR /app/backend

# Railway sets PORT env var, default to 8000 for local
ENV PORT=8000
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Use shell form to allow variable substitution for PORT
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
