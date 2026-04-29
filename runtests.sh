#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 [--backend] [--ingestion] [pytest args...]"
    echo ""
    echo "  --backend     Run backend tests only"
    echo "  --ingestion   Run ingestion tests only"
    echo "  (no flags)    Run both suites"
    exit 1
}

# Load variables from .env
if [ ! -f .env ]; then
    echo "Error: .env file not found. Run from the project root." >&2
    exit 1
fi
set -a
source .env
set +a

run_backend=true
run_ingestion=true

# Parse suite flags, collect remaining args to pass to pytest
pytest_args=()
for arg in "$@"; do
    case "$arg" in
        --backend)   run_ingestion=false ;;
        --ingestion) run_backend=false ;;
        --help|-h)   usage ;;
        *)           pytest_args+=("$arg") ;;
    esac
done

if $run_backend; then
    echo "=== Starting database ==="
    docker compose up db -d --wait
    trap 'docker compose stop db' EXIT

    echo "=== Backend tests ==="
    (cd backend && python -m pytest tests/ -v "${pytest_args[@]+"${pytest_args[@]}"}")
fi

if $run_ingestion; then
    [ "$run_backend" = true ] && echo ""
    echo "=== Ingestion tests ==="
    (cd ingestion && python -m pytest tests/ -v "${pytest_args[@]+"${pytest_args[@]}"}")
fi
