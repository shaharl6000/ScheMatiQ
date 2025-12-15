# Frontend - Static file server using nginx
FROM nginx:alpine

# Copy pre-built static files
COPY build/ /usr/share/nginx/html/

# Create a simple startup script that configures nginx with Railway's PORT
RUN printf '#!/bin/sh\n\
PORT="${PORT:-3000}"\n\
cat > /etc/nginx/conf.d/default.conf << EOF\n\
server {\n\
    listen $PORT;\n\
    root /usr/share/nginx/html;\n\
    index index.html;\n\
    location / {\n\
        try_files \\$uri \\$uri/ /index.html;\n\
    }\n\
}\n\
EOF\n\
nginx -g "daemon off;"\n' > /start.sh && chmod +x /start.sh

CMD ["/bin/sh", "/start.sh"]
