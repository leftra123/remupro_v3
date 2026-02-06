#!/bin/sh
set -e

echo "=== RemuPro v3 Starting ==="

# Start FastAPI in background
echo "Starting FastAPI on :8000..."
uvicorn api.main:app \
  --host 127.0.0.1 \
  --port 8000 \
  --workers 2 \
  --log-level info &

# Wait for API to be ready
echo "Waiting for API..."
for i in $(seq 1 30); do
  if nc -z 127.0.0.1 8000 2>/dev/null; then
    echo "API ready!"
    break
  fi
  sleep 1
done

# Start nginx in foreground
echo "Starting nginx on :80..."
nginx -g "daemon off;"
