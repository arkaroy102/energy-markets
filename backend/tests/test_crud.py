"""
CRUD integration tests against a real test database.

Requires TEST_DATABASE_URL env var pointing at a throwaway postgres database.
Each test clears all state on entry so tests are independent and leave no data.
"""

from datetime import datetime, timezone
from fastapi.testclient import TestClient


def clear_state(client: TestClient):
    """Delete all prices and locations; verify the DB is empty."""
    r = client.delete("/prices")
    assert r.status_code == 200
    r = client.delete("/locations")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Location CRUD
# ---------------------------------------------------------------------------

def test_create_single_location(client):
    clear_state(client)

    r = client.post("/locations", json={
        "grid": "ERCOT",
        "node_name": "HB_NORTH",
        "node_type": "ELECTRICAL_BUS",
    })
    assert r.status_code == 200
    body = r.json()
    assert body["grid"] == "ERCOT"
    assert body["node_name"] == "HB_NORTH"
    assert body["node_type"] == "ELECTRICAL_BUS"
    assert isinstance(body["node_id"], int)

    clear_state(client)


def test_create_location_batch(client):
    clear_state(client)

    payload = [
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "ERCOT", "node_name": "HB_SOUTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "ERCOT", "node_name": "HB_WEST",  "node_type": "ELECTRICAL_BUS"},
    ]
    r = client.post("/locations/batch", json=payload)
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 3
    names = {row["node_name"] for row in rows}
    assert names == {"HB_NORTH", "HB_SOUTH", "HB_WEST"}

    clear_state(client)


def test_read_locations_by_grid(client):
    clear_state(client)

    payload = [
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "NYISO", "node_name": "CAPITL",   "node_type": "ELECTRICAL_BUS"},
    ]
    client.post("/locations/batch", json=payload)

    r = client.get("/locations", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["node_name"] == "HB_NORTH"

    r = client.get("/locations", params={"grid": "NYISO"})
    assert r.status_code == 200
    assert r.json()[0]["node_name"] == "CAPITL"

    clear_state(client)


def test_read_location_by_node_name(client):
    clear_state(client)

    created = client.post("/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
    ]).json()
    expected_id = created[0]["node_id"]

    r = client.get("/location", params={"grid": "ERCOT", "node_name": "HB_NORTH"})
    assert r.status_code == 200
    body = r.json()
    assert body["node_id"] == expected_id
    assert body["node_name"] == "HB_NORTH"
    assert body["grid"] == "ERCOT"

    clear_state(client)


def test_read_location_not_found(client):
    clear_state(client)

    r = client.get("/location", params={"grid": "ERCOT", "node_name": "DOES_NOT_EXIST"})
    assert r.status_code == 404

    clear_state(client)


def test_create_location_idempotent(client):
    """Inserting the same node twice returns only one result (on_conflict_do_nothing)."""
    clear_state(client)

    payload = [
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
    ]
    r = client.post("/locations/batch", json=payload)
    assert r.status_code == 200
    # second insert is a no-op; only the first row is returned
    assert len(r.json()) == 1

    r = client.get("/locations", params={"grid": "ERCOT"})
    assert len(r.json()) == 1

    clear_state(client)


def test_delete_locations_removes_all(client):
    clear_state(client)

    client.post("/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "ERCOT", "node_name": "HB_SOUTH", "node_type": "ELECTRICAL_BUS"},
    ])

    r = client.delete("/locations")
    assert r.status_code == 200

    r = client.get("/locations", params={"grid": "ERCOT"})
    assert r.json() == []


# ---------------------------------------------------------------------------
# Price CRUD
# ---------------------------------------------------------------------------

TS1 = "2024-01-01T06:00:00+00:00"
TS2 = "2024-01-01T06:05:00+00:00"
TS3 = "2024-01-01T06:10:00+00:00"


def _create_node(client, node_name="HB_NORTH") -> int:
    rows = client.post("/locations/batch", json=[
        {"grid": "ERCOT", "node_name": node_name, "node_type": "ELECTRICAL_BUS"},
    ]).json()
    return rows[0]["node_id"]


def test_create_single_price(client):
    clear_state(client)

    node_id = _create_node(client)
    r = client.post("/prices", json={
        "node_id": node_id,
        "timestamp_utc": TS1,
        "lmp": 42.5,
    })
    assert r.status_code == 200

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["node_id"] == node_id
    assert rows[0]["lmp"] == 42.5

    clear_state(client)


def test_create_price_batch(client):
    clear_state(client)

    node_id = _create_node(client)
    payload = [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ]
    r = client.post("/prices/batch", json=payload)
    assert r.status_code == 200

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    assert len(rows) == 3

    clear_state(client)


def test_read_prices_ordered_desc(client):
    """GET /prices/{node_id} returns rows newest-first."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ])

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    timestamps = [row["timestamp_utc"] for row in rows]
    assert timestamps == sorted(timestamps, reverse=True)

    clear_state(client)


def test_read_prices_limit(client):
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ])

    r = client.get(f"/prices/{node_id}", params={"limit": 1})
    rows = r.json()
    assert len(rows) == 1
    # default limit=1 returns the most recent price
    assert rows[0]["lmp"] == 30.0

    clear_state(client)


def test_price_data_accuracy(client):
    """Verify lmp and timestamp round-trip exactly."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 123.456},
    ])

    r = client.get(f"/prices/{node_id}", params={"limit": 1})
    row = r.json()[0]
    assert row["node_id"] == node_id
    assert abs(row["lmp"] - 123.456) < 1e-6

    # timestamp must survive the round-trip (DB stores TIMESTAMPTZ)
    stored_ts = datetime.fromisoformat(row["timestamp_utc"])
    original_ts = datetime.fromisoformat(TS1)
    assert stored_ts == original_ts

    clear_state(client)


def test_price_idempotent_insert(client):
    """Duplicate (node_id, timestamp_utc) is silently ignored."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 99.0},  # duplicate ts
    ])

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["lmp"] == 10.0  # first write wins

    clear_state(client)


def test_delete_prices_only(client):
    """DELETE /prices removes prices but leaves locations intact."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
    ])

    r = client.delete("/prices")
    assert r.status_code == 200

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    assert r.json() == []

    # node still exists
    r = client.get("/locations", params={"grid": "ERCOT"})
    assert len(r.json()) == 1

    clear_state(client)


def test_delete_locations_cascades_prices(client):
    """DELETE /locations also removes dependent prices."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
    ])

    r = client.delete("/locations")
    assert r.status_code == 200

    r = client.get("/locations", params={"grid": "ERCOT"})
    assert r.json() == []

    r = client.get(f"/prices/{node_id}", params={"limit": 10})
    assert r.json() == []


# ---------------------------------------------------------------------------
# Timeseries endpoint
# ---------------------------------------------------------------------------

# Timestamps spread across two calendar days (UTC)
TS_DAY1_A = "2024-03-01T00:05:00+00:00"
TS_DAY1_B = "2024-03-01T00:10:00+00:00"
TS_DAY1_C = "2024-03-01T00:15:00+00:00"
TS_DAY2_A = "2024-03-02T00:05:00+00:00"


def test_timeseries_returns_correct_day(client):
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_B, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_C, "lmp": 30.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY2_A, "lmp": 99.0},
    ])

    r = client.get("/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 3
    lmps = [row["lmp"] for row in rows]
    assert lmps == [10.0, 20.0, 30.0]

    clear_state(client)


def test_timeseries_ordered_ascending(client):
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS_DAY1_C, "lmp": 30.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_B, "lmp": 20.0},
    ])

    r = client.get("/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    rows = r.json()
    timestamps = [row["timestamp_utc"] for row in rows]
    assert timestamps == sorted(timestamps)

    clear_state(client)


def test_timeseries_empty_for_date_with_no_data(client):
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
    ])

    r = client.get("/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-02"
    })
    assert r.status_code == 200
    assert r.json() == []

    clear_state(client)


def test_timeseries_node_not_found(client):
    clear_state(client)

    r = client.get("/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "DOES_NOT_EXIST", "date": "2024-03-01"
    })
    assert r.status_code == 404

    clear_state(client)


def test_timeseries_data_accuracy(client):
    """LMP and timestamp round-trip exactly through the timeseries endpoint."""
    clear_state(client)

    node_id = _create_node(client)
    client.post("/prices/batch", json=[
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 47.123},
    ])

    r = client.get("/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    row = r.json()[0]
    assert abs(row["lmp"] - 47.123) < 1e-6

    stored_ts = datetime.fromisoformat(row["timestamp_utc"])
    original_ts = datetime.fromisoformat(TS_DAY1_A)
    assert stored_ts == original_ts

    clear_state(client)
