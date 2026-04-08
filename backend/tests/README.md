# Backend Integration Tests

Integration tests for the FastAPI backend, covering CRUD operations for locations and prices against a real Postgres database.

## Setup

Tests require a throwaway Postgres database. Create one on the running Docker instance:

```bash
docker exec energymarkets-db-1 psql -U postgres -c "CREATE DATABASE energymarkets_test;"
```

This only needs to be done once. The schema is created automatically when the tests run.

## Running Tests

The Docker Compose stack does not need to be fully running — only the `db` service (port 5432) is required. Redis is not required.

```bash
cd backend
TEST_DATABASE_URL=postgresql://postgres:mypassword@localhost:5432/energymarkets_test \
  python -m pytest tests/test_crud.py -v
```

## Test Design

### Isolation

Each test calls `clear_state()` at the start, which deletes all prices then all locations via the `DELETE /prices` and `DELETE /locations` endpoints. Tests are order-independent and leave no data behind, even if a previous test crashed mid-run.

### Database setup

`conftest.py` sets `DATABASE_URL = TEST_DATABASE_URL` before importing `runner.py`, so the app's SQLAlchemy engine targets the test database for the entire session. `Base.metadata.create_all()` runs on import, keeping the schema in sync with the ORM models automatically.

## Test Coverage

### Locations (`test_crud.py`)

| Test | What it verifies |
|------|-----------------|
| `test_create_single_location` | POST /locations returns correct fields |
| `test_create_location_batch` | POST /locations/batch inserts all rows |
| `test_read_locations_by_grid` | GET /locations filters by grid correctly |
| `test_read_location_by_node_name` | GET /location returns correct node with matching ID |
| `test_read_location_not_found` | Returns 404 for unknown node |
| `test_create_location_idempotent` | Duplicate insert is a no-op; DB has one row |
| `test_delete_locations_removes_all` | DELETE /locations empties the table |

### Prices (`test_crud.py`)

| Test | What it verifies |
|------|-----------------|
| `test_create_single_price` | POST /prices round-trips correctly |
| `test_create_price_batch` | POST /prices/batch inserts all rows |
| `test_read_prices_ordered_desc` | GET /prices/{node_id} returns newest-first |
| `test_read_prices_limit` | limit=1 returns only the latest price |
| `test_price_data_accuracy` | LMP accurate to 1e-6; timestamp survives timezone round-trip |
| `test_price_idempotent_insert` | Duplicate (node_id, timestamp) ignored; first write wins |
| `test_delete_prices_only` | DELETE /prices removes prices, leaves nodes intact |
| `test_delete_locations_cascades_prices` | DELETE /locations also removes dependent prices |
