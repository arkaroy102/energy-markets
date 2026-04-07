# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a real-time ERCOT electricity price dashboard. The git repo root is `frontend/`, but the broader project at the parent directory (`EnergyMarkets/`) consists of three services orchestrated by Docker Compose:

- **frontend** – React + TypeScript + Vite SPA (this repo)
- **backend** – FastAPI Python server (`../backend/`)
- **ingestion** – Python polling service for ERCOT API (`../ingestion/`)

## Frontend Commands

```bash
npm run dev       # Start dev server on port 5173
npm run build     # Type-check (tsc) then build to dist/
npm run lint      # Run ESLint
npm run preview   # Preview production build
```

## Running the Full Stack

From `EnergyMarkets/` (parent directory):

```bash
docker compose up       # Start all services (db, redis, backend, ingestion)
docker compose down     # Stop services
```

The frontend dev server proxies `/api/*` requests to `http://localhost:8000` (configurable via `VITE_BACKEND_URL` env var). The backend must be running for the frontend to fetch data.

## Architecture

### Frontend (`src/`)

- `src/main.tsx` – React app entry point
- `src/App.tsx` – Single-component app: polls `/api/latest-zone-prices` every 5 seconds using a `while (!cancelled)` loop pattern (not `setInterval`); renders ERCOT zone price table with a live clock
- `src/api/client.ts` – Fetch wrapper for `GET /api/latest-zone-prices`
- `src/types/market.ts` – `ZonePrice` type matching the backend response shape

Vite proxies `/api` to the backend in dev mode (`vite.config.ts`). No routing library is used—the app is a single page.

### Backend (`../backend/`)

FastAPI app in `runner.py` with SQLAlchemy ORM. Two tables:

- `node_table` (`Node`) – electrical bus nodes with `grid` (ERCOT/NYISO/CAISO enum), `node_name`, `settlement_load_zone`, and other metadata
- `node_price_table` (`NodePrice`) – time-series LMP prices per node, unique on `(node_id, timestamp_utc)`

The key endpoint served to the frontend is `GET /api/latest-zone-prices`: it computes the latest price per node, then aggregates by `settlement_load_zone` (avg LMP, min/max timestamps, node count). Results are cached in Redis for 5 minutes (`latest_zone_prices` key), invalidated on each price batch write.

### Ingestion (`../ingestion/`)

Producer-consumer threading pattern (`fetcher` thread → bounded `queue.Queue` → `writer` thread):

- `fetcher`: polls ERCOT public API for LMP electrical bus data, paginates through results, tracks `maxtime` to avoid re-fetching already-stored data
- `writer`: dequeues batches, upserts new nodes via `/locations/batch`, then posts prices via `/prices/batch`
- `ercot_client.py`: HTTP client wrapping backend REST calls; reads `BACKEND_URL` from env (default `http://localhost:8000`)
- ERCOT API credentials and subscription key are hardcoded in `ercot_api.py`
