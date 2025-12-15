# Frontend Dockerfile for QueryDiscovery
# Builds React app for production

# ============================================================================
# Stage 1: Build
# ============================================================================
FROM node:18-bookworm AS builder

WORKDIR /app

# Install yarn
RUN npm install -g yarn

# Copy package.json only
COPY package.json ./

# Install dependencies with yarn (handles platform deps better)
RUN yarn install --legacy-peer-deps 2>/dev/null || yarn install

# Copy source code
COPY . .

# Build arguments for environment variables
ARG REACT_APP_API_URL
ARG REACT_APP_WS_URL
ARG REACT_APP_ENABLE_DEBUG=false

ENV REACT_APP_API_URL=${REACT_APP_API_URL}
ENV REACT_APP_WS_URL=${REACT_APP_WS_URL}
ENV REACT_APP_ENABLE_DEBUG=${REACT_APP_ENABLE_DEBUG}

# Build the production app
RUN yarn build

# ============================================================================
# Stage 2: Production
# ============================================================================
FROM node:18-bookworm-slim

WORKDIR /app

# Install serve
RUN npm install -g serve

# Copy built assets
COPY --from=builder /app/build ./build

# Railway sets PORT env var
ENV PORT=3000
EXPOSE ${PORT}

CMD ["sh", "-c", "serve -s build -l ${PORT}"]
