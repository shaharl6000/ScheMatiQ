# Frontend - Static file server using nginx
# No Node.js, no npm, no build - just serve pre-built files

FROM nginx:alpine

# Copy pre-built static files to nginx html directory
COPY build/ /usr/share/nginx/html/

# Create nginx config template that uses PORT env var
RUN echo 'server { \
    listen ${PORT}; \
    root /usr/share/nginx/html; \
    index index.html; \
    location / { \
        try_files $uri $uri/ /index.html; \
    } \
}' > /etc/nginx/templates/default.conf.template

# Default port
ENV PORT=3000

EXPOSE 3000

# nginx:alpine uses envsubst to process templates automatically
CMD ["nginx", "-g", "daemon off;"]
