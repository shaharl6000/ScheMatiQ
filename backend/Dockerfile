# Backend Dockerfile for QueryDiscovery
# Can be built standalone from backend/ directory

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install CPU-only PyTorch first (saves ~2GB vs CUDA version)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Copy and install qbsd-lib from the qbsd_lib subdirectory (copied during deployment)
# For local dev, we'll install from requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install qbsd-lib from git if needed (fallback)
RUN pip install --no-cache-dir git+https://github.com/shaharl6000/QueryDiscovery.git#subdirectory=qbsd-lib || true

# Copy application code
COPY . .

# Railway sets PORT env var, default to 8000 for local
ENV PORT=8000
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Start command
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
