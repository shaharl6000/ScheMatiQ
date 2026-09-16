# Backend Dockerfile for ScheMatiQ
# Build from project root: docker build -f backend/Dockerfile -t schematiq-backend .

# ---------------------------------------------------------------------------
# Final image
# ---------------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for ML libraries (torch, transformers, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libreoffice-writer \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Copy schematiq-lib first (dependency for backend)
COPY schematiq-lib/ /app/schematiq-lib/

# Install CPU-only PyTorch first (saves ~2GB vs CUDA version)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install schematiq-lib as editable package (torch already satisfied)
RUN pip install --no-cache-dir -e /app/schematiq-lib/

# Copy backend requirements and install (separate layer for caching)
COPY backend/requirements.txt /app/backend/
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Prefetch Docling's model weights (layout + table-structure models) into the
# image at build time, so figure extraction doesn't pay a multi-hundred-MB
# HuggingFace download on the first request and the container works fully
# offline at runtime. See app/core/config.py:DOCLING_ARTIFACTS_PATH — this
# path must match what's passed there.
ENV DOCLING_ARTIFACTS_PATH=/opt/docling-models
RUN docling-tools models download --output-dir ${DOCLING_ARTIFACTS_PATH}

# Copy backend source code
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
