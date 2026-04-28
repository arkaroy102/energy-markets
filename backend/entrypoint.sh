#!/bin/sh
set -e

echo "Running alembic upgrade head..."
alembic upgrade head
echo "Migrations complete. Starting uvicorn..."
exec uvicorn runner:app --host 0.0.0.0 --port ${PORT:-8000}
