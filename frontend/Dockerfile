# Frontend - Static file server using nginx
# No Node.js, no npm, no build - just serve pre-built files

FROM nginx:alpine

# Copy pre-built static files to nginx html directory
COPY build/ /usr/share/nginx/html/

# Copy custom nginx config for SPA routing
RUN echo 'server { \
    listen 3000; \
    root /usr/share/nginx/html; \
    index index.html; \
    location / { \
        try_files $uri $uri/ /index.html; \
    } \
}' > /etc/nginx/conf.d/default.conf

# Railway uses PORT env var - nginx needs to listen on it
# We'll use a startup script to handle this
RUN echo '#!/bin/sh' > /start.sh && \
    echo 'sed -i "s/listen 3000/listen ${PORT:-3000}/" /etc/nginx/conf.d/default.conf' >> /start.sh && \
    echo 'nginx -g "daemon off;"' >> /start.sh && \
    chmod +x /start.sh

EXPOSE 3000

CMD ["/start.sh"]
