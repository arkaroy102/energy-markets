# EnergyMarkets

A real-time electricity pricing dashboard supporting multiple independent system operators (ISOs). Ingestion services poll each grid's public API, store LMP prices per electrical bus node, and the frontend displays average prices aggregated by settlement load zone — updated live every 5 seconds.

**Grids currently supported:** ERCOT (Texas), NYISO (New York)

![Zone price table showing live ERCOT and NYISO average LMP by settlement zone](assets/energy-app-sshot-zone-avg-lmp.png)

![LMP timeseries chart with multi-series overlay across ERCOT and NYISO nodes](assets/energy-app-sshot-timeseries.png)

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────┐
│ingestion-   │     │              │     │          │     │          │
│ercot        ├────►│   backend    ├────►│ postgres │     │  redis   │
│ingestion-   │     │  (FastAPI)   │     │          │     │  cache   │
│nyiso        ├────►│              │◄────┤          │◄────┤          │
└─────────────┘     └──────┬───────┘     └──────────┘     └──────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  frontend   │
                    │ (React/Vite)│
                    └─────────────┘
```

| Service | Description |
|---------|-------------|
| `db` | PostgreSQL — stores bus nodes and time-series LMP prices |
| `redis` | Caches the latest zone price aggregation per grid |
| `backend` | FastAPI — REST API consumed by the frontend and ingestion services |
| `ingestion-ercot` | Polls ERCOT's public API every ~5 minutes (SCED interval) |
| `ingestion-nyiso` | Polls NYISO's public API for real-time zonal prices |
| `frontend` | React + Vite SPA — run locally with `npm run dev` |

The ingestion services use a producer-consumer threading pattern: a fetcher thread polls each grid API and enqueues raw batches; a writer thread dequeues them and posts to the backend via REST. The backend aggregates prices by zone and caches the result in Redis, invalidating the cache on each new write.

---

## Running Locally

**Prerequisites:** Docker Desktop, Node.js 18+

### 1. Configure credentials

Create a `.env` file in the project root. This is only required if you want ERCOT data — NYISO requires no credentials.

```bash
# ERCOT credentials (optional — only needed for ERCOT ingestion)
ERCOT_USERNAME=your_ercot_email@example.com
ERCOT_PASSWORD=your_ercot_password
ERCOT_SUBSCRIPTION_KEY=your_subscription_key

# Test database (optional — only needed to run backend tests)
TEST_DATABASE_URL=postgresql://postgres:mypassword@localhost:5432/energymarkets_test
```

ERCOT API credentials are free — register at the [ERCOT API Portal](https://developer.ercot.com). If you skip this, NYISO ingestion still runs and the dashboard will populate with New York data.

### 2. Start the backend stack

```bash
docker compose up
```

This starts PostgreSQL, Redis, the FastAPI backend, and both ingestion services. The database schema is created automatically on first start. To run only specific ingestion services:

```bash
# NYISO only (no credentials needed)
docker compose up db redis backend ingestion-nyiso

# ERCOT only
docker compose up db redis backend ingestion-ercot
```

### 3. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

The app runs at `http://localhost:5173`. The Vite dev server proxies `/api/*` to the backend at `http://localhost:8000`.

---

## What to expect on first run

The database starts empty. Ingestion begins immediately on startup:

- **NYISO**: first prices appear within ~1 minute
- **ERCOT**: SCED intervals run every 5 minutes; first prices appear within one cycle
- The frontend zone price table will show "No data" until the first write completes
- Node metadata is upserted automatically — no manual seeding required

There is currently no historical data import. The dashboard shows live prices from the time the stack first started.

---

## Testing

The backend has integration tests that run against a real Postgres database. See [`backend/tests/README.md`](backend/tests/README.md) for setup and usage.

```bash
# One-time: create the test database (assumes Docker stack is running)
docker exec energymarkets-db-1 psql -U postgres -c "CREATE DATABASE energymarkets_test;"

# Run tests (TEST_DATABASE_URL sourced from .env)
cd backend
source ../.env && python -m pytest tests/test_crud.py -v
```
