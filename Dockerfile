# ============================================================
# Stage 1: Build Next.js dashboard → static files
# ============================================================
FROM node:20-alpine AS dashboard-build

WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm ci

COPY dashboard/ .
# Empty API_BASE → relative URLs → same origin via nginx
ENV NEXT_PUBLIC_API_URL=""
RUN npm run build
# output: "export" generates /app/dashboard/out/

# ============================================================
# Stage 2: Python backend
# ============================================================
FROM python:3.13-slim AS backend

RUN apt-get update && \
    apt-get install -y --no-install-recommends nginx && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# App source
COPY config/ config/
COPY processors/ processors/
COPY database/ database/
COPY reports/ reports/
COPY api/ api/

# Static dashboard from stage 1
COPY --from=dashboard-build /app/dashboard/out /app/static

# Nginx config
COPY deploy/nginx.conf /etc/nginx/nginx.conf

# Data directory (SQLite)
RUN mkdir -p /app/data

# Startup script
COPY deploy/start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 80

CMD ["/app/start.sh"]
