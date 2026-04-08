# EnergyMarkets

Real-time ERCOT electricity price dashboard.

## Services

- **frontend** – React + TypeScript + Vite SPA
- **backend** – FastAPI Python server
- **ingestion** – Python polling service for ERCOT API

## Running the Stack

```bash
docker compose up
```

## Configuration

Create a `.env` file in the project root (gitignored). It is loaded automatically by the `ingestion` service via Docker Compose.

| Variable | Description |
|----------|-------------|
| `ERCOT_USERNAME` | ERCOT API portal account email |
| `ERCOT_PASSWORD` | ERCOT API portal account password |
| `ERCOT_SUBSCRIPTION_KEY` | ERCOT API subscription key (`Ocp-Apim-Subscription-Key` header) |

## Testing

See [`backend/tests/README.md`](backend/tests/README.md).
