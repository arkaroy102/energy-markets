# EnergyMarkets

A real-time electricity pricing dashboard. Currently supports ERCOT (Electric Reliability Council of Texas). The dashboard polls ERCOT's public API every 5 minutes (aligned to SCED intervals), stores LMP prices per electrical bus node, and displays average prices aggregated by settlement load zone.

## Services

| Service | Description |
|---------|-------------|
| `db` | PostgreSQL — stores nodes and time-series LMP prices |
| `redis` | Cache for the latest zone price aggregation |
| `backend` | FastAPI — REST API consumed by both the frontend and ingestion |
| `ingestion` | Python service — polls ERCOT API and writes prices to the backend |
| `frontend` | React + Vite SPA — run locally with `npm`, not via Docker |

## Running the Stack

**1. Start backend services**

```bash
docker compose up db redis backend ingestion
```

**2. Start the frontend locally**

```bash
cd frontend
npm install
npm run dev
```

The frontend runs on `http://localhost:5173` and proxies `/api/*` requests to the backend at `http://localhost:8000`.

## Configuration

Create a `.env` file in the project root (gitignored). It is loaded automatically by the `ingestion` service.

```bash
ERCOT_USERNAME=your_ercot_email@example.com
ERCOT_PASSWORD=your_ercot_password
ERCOT_SUBSCRIPTION_KEY=your_subscription_key
```

ERCOT API credentials are obtained by registering at the [ERCOT API Portal](https://developer.ercot.com).

## First-time data seeding

This project depends on live ERCOT data that accumulates over time. A new environment will start with an empty database — the ingestion service will begin backfilling from the current time, but there is no historical data to import.

What to expect on first run:
- The ingestion service fetches the current SCED interval on startup, so prices appear within a few minutes
- The frontend zone price table will be empty until the first ingestion write completes
- Full zone coverage (all settlement load zones populated) takes one SCED cycle (~5 minutes)
- Node metadata (`node_table`) is upserted on every ingestion run, so it populates automatically — no manual seeding required

There is currently no snapshot or seed file for historical prices.

## Testing

See [`backend/tests/README.md`](backend/tests/README.md).
