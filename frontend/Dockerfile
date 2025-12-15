# Frontend Dockerfile - Serves pre-built static files
# No npm install or build needed - files are pre-built locally

FROM node:18-alpine

WORKDIR /app

# Install serve globally
RUN npm install -g serve

# Copy pre-built static files
COPY build/ ./build/

# Railway sets PORT env var
ENV PORT=3000
EXPOSE ${PORT}

# Serve static files
CMD ["sh", "-c", "serve -s build -l ${PORT}"]
