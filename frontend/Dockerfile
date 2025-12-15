# Frontend Dockerfile for QueryDiscovery
# Build from frontend directory: docker build -t qbsd-frontend .
# Cache bust: v2

# ============================================================================
# Stage 1: Build
# ============================================================================
FROM node:20 AS builder

WORKDIR /app

# Copy package.json only (not package-lock.json to avoid platform-specific deps)
COPY package.json ./

# Install dependencies fresh for Linux platform
# Force reinstall of rollup to get correct platform binaries
RUN npm install --legacy-peer-deps && \
    npm rebuild @rollup/rollup || true

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
FROM node:20-slim

WORKDIR /app

# Install serve globally for serving static files
RUN npm install -g serve

# Copy built assets from builder stage
COPY --from=builder /app/build ./build

# Railway sets PORT env var, default to 3000 for local
ENV PORT=3000
EXPOSE ${PORT}

# Health check (use curl instead of wget for slim image)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD node -e "require('http').get('http://localhost:' + process.env.PORT + '/', (r) => process.exit(r.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1))"

# Use shell form to allow variable substitution for PORT
CMD serve -s build -l ${PORT}
