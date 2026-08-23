# Backend Dockerfile for ScheMatiQ
# Build from project root: docker build -f backend/Dockerfile -t schematiq-backend .

# ---------------------------------------------------------------------------
# Builder stage: compile the PDFFigures 2.0 (allenai) assembly jar. No prebuilt
# release is published upstream, so it's built from source once here; only the
# resulting jar is copied into the final image (the JDK/sbt/Ivy cache are not).
#
# Pinned to a `master` commit, NOT the `v0.0.11` tag: that tag's build (sbt
# 0.13.8 + the `allenai-sbt-plugins` plugin) depends on Bintray, which shut
# down in 2021, so it can no longer resolve its dependencies. `master` was
# modernized (sbt 1.7.1, Maven-Central-only deps) after that tag was cut and
# is what actually builds today — verified locally on 2026-08-23.
# ---------------------------------------------------------------------------
FROM eclipse-temurin:11-jdk AS pdffigures2-builder

RUN apt-get update && apt-get install -y curl git && rm -rf /var/lib/apt/lists/*

# Official sbt distribution (sbt itself pulls the Scala 2.12 toolchain build.sbt pins)
RUN curl -fL https://github.com/sbt/sbt/releases/download/v1.9.9/sbt-1.9.9.tgz | tar xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:${PATH}"

RUN git clone https://github.com/allenai/pdffigures2.git /opt/pdffigures2-src && \
    cd /opt/pdffigures2-src && \
    git checkout 3d7ad46753d4a315cccd1c2bcab398380e88c534
WORKDIR /opt/pdffigures2-src
# build.sbt sets a custom `assemblyOutputPath` — the jar lands at the project
# root as `pdffigures2.jar`, not under target/scala-2.12/ (sbt-assembly's default).
RUN sbt assembly && cp pdffigures2.jar /opt/pdffigures2-assembly.jar

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
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# PDFFigures 2.0 assembly jar built above — see app/core/config.py:PDFFIGURES2_JAR_PATH.
# Figure extraction no-ops gracefully if this (or java) is ever unavailable.
COPY --from=pdffigures2-builder /opt/pdffigures2-assembly.jar /opt/pdffigures2/pdffigures2-assembly.jar

# Copy schematiq-lib first (dependency for backend)
COPY schematiq-lib/ /app/schematiq-lib/

# Install CPU-only PyTorch first (saves ~2GB vs CUDA version)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install schematiq-lib as editable package (torch already satisfied)
RUN pip install --no-cache-dir -e /app/schematiq-lib/

# Copy backend requirements and install (separate layer for caching)
COPY backend/requirements.txt /app/backend/
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

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
