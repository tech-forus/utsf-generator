#!/bin/bash
set -e
export PYTHONPATH=/app/src
exec gunicorn app:app \
  --chdir /app/src/web \
  --bind 0.0.0.0:${PORT:-8080} \
  --threads 4 \
  --timeout 300 \
  --log-level debug \
  --access-logfile - \
  --error-logfile -
