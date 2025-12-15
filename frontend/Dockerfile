# Frontend Dockerfile for QueryDiscovery
# Build from frontend directory: docker build -t qbsd-frontend .
# Cache bust: v2

# ============================================================================
# Stage 1: Build
# ============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files first (better layer caching)
COPY package*.json ./

# Install dependencies (including dev dependencies for build)
RUN npm install --legacy-peer-deps

# Copy frontend source code
COPY . .

# Build arguments for environment variables (set at build time)
ARG REACT_APP_API_URL
ARG REACT_APP_WS_URL
ARG REACT_APP_ENABLE_DEBUG=false

# Set environment variables for build
ENV REACT_APP_API_URL=${REACT_APP_API_URL}
ENV REACT_APP_WS_URL=${REACT_APP_WS_URL}
ENV REACT_APP_ENABLE_DEBUG=${REACT_APP_ENABLE_DEBUG}

# Build the production app
RUN npm run build

# ============================================================================
# Stage 2: Production
# ============================================================================
FROM node:20-alpine

WORKDIR /app

# Install serve globally for serving static files
RUN npm install -g serve

# Copy built assets from builder stage
COPY --from=builder /app/build ./build

# Railway sets PORT env var, default to 3000 for local
ENV PORT=3000
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:${PORT}/ || exit 1

# Use shell form to allow variable substitution for PORT
CMD serve -s build -l ${PORT}
