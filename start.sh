#!/bin/bash
# Render start script — once alembic migration, sonra gunicorn.
# Migration fail olursa devam et (uygulamayi acmak gerek, manuel fix daha sonra).
set -u

echo "[start.sh] Running alembic upgrade head..."
alembic upgrade head || echo "[start.sh] WARNING: alembic upgrade failed, continuing with gunicorn..."

echo "[start.sh] Starting gunicorn on port $PORT..."
exec gunicorn app.main:app \
  -w 1 \
  -k uvicorn.workers.UvicornWorker \
  --bind "0.0.0.0:$PORT" \
  --timeout 300
