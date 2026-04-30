"""
CRUD integration tests against a real test database.

Requires TEST_DATABASE_URL env var pointing at a throwaway postgres database.
Each test clears all state on entry so tests are independent and leave no data.

Internal endpoints: /internal/locations/*, /internal/prices/*
Public API endpoints: /api/locations, /api/prices/timeseries, /api/prices/zone-summary
"""

from datetime import datetime, timezone
from fastapi.testclient import TestClient


def clear_state(client: TestClient):
    """Delete all prices and locations."""
    r = client.delete("/internal/prices")
    assert r.status_code == 200
    r = client.delete("/internal/locations")
    assert r.status_code == 200


def post_prices(client: TestClient, payload: list, grid: str = "ERCOT"):
    return client.post("/internal/prices/batch", params={"grid": grid}, json=payload)


# ---------------------------------------------------------------------------
# Location CRUD
# ---------------------------------------------------------------------------

def test_create_single_location(client):
    clear_state(client)

    r = client.post("/internal/locations", json={
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
    r = client.post("/internal/locations/batch", json=payload)
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
    client.post("/internal/locations/batch", json=payload)

    r = client.get("/internal/locations", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["node_name"] == "HB_NORTH"

    r = client.get("/internal/locations", params={"grid": "NYISO"})
    assert r.status_code == 200
    assert r.json()[0]["node_name"] == "CAPITL"

    clear_state(client)


def test_create_location_idempotent(client):
    """Calling the batch endpoint twice with the same node results in one node in the DB."""
    clear_state(client)

    payload = [{"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"}]
    client.post("/internal/locations/batch", json=payload)
    client.post("/internal/locations/batch", json=payload)

    r = client.get("/internal/locations", params={"grid": "ERCOT"})
    assert len(r.json()) == 1

    clear_state(client)


def test_create_location_upserts_latlon(client):
    """Second insert with lat/lon updates coordinates on existing node."""
    clear_state(client)

    r = client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
    ])
    assert r.status_code == 200

    r = client.get("/api/locations", params={"grid": "ERCOT"})
    assert r.status_code == 200
    node = next(n for n in r.json() if n["node_name"] == "HB_NORTH")
    assert node["latitude"] is None
    assert node["longitude"] is None

    r = client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS",
         "latitude": 31.5, "longitude": -97.1},
    ])
    assert r.status_code == 200

    r = client.get("/api/locations", params={"grid": "ERCOT"})
    assert r.status_code == 200
    node = next(n for n in r.json() if n["node_name"] == "HB_NORTH")
    assert abs(node["latitude"] - 31.5) < 1e-6
    assert abs(node["longitude"] - (-97.1)) < 1e-6

    clear_state(client)


def test_create_location_upsert_does_not_overwrite_latlon_with_null(client):
    """Second insert without lat/lon preserves existing coordinates."""
    clear_state(client)

    r = client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS",
         "latitude": 31.5, "longitude": -97.1},
    ])
    assert r.status_code == 200

    r = client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
    ])
    assert r.status_code == 200

    r = client.get("/api/locations", params={"grid": "ERCOT"})
    assert r.status_code == 200
    node = next(n for n in r.json() if n["node_name"] == "HB_NORTH")
    assert abs(node["latitude"] - 31.5) < 1e-6
    assert abs(node["longitude"] - (-97.1)) < 1e-6

    clear_state(client)


def test_delete_locations_removes_all(client):
    clear_state(client)

    client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": "HB_NORTH", "node_type": "ELECTRICAL_BUS"},
        {"grid": "ERCOT", "node_name": "HB_SOUTH", "node_type": "ELECTRICAL_BUS"},
    ])

    r = client.delete("/internal/locations")
    assert r.status_code == 200

    r = client.get("/internal/locations", params={"grid": "ERCOT"})
    assert r.json() == []


# ---------------------------------------------------------------------------
# Price CRUD
# ---------------------------------------------------------------------------

TS1 = "2024-01-01T06:00:00+00:00"
TS2 = "2024-01-01T06:05:00+00:00"
TS3 = "2024-01-01T06:10:00+00:00"


def _create_node(client, node_name="HB_NORTH") -> int:
    rows = client.post("/internal/locations/batch", json=[
        {"grid": "ERCOT", "node_name": node_name, "node_type": "ELECTRICAL_BUS"},
    ]).json()
    return rows[0]["node_id"]


def test_create_single_price(client):
    clear_state(client)

    node_id = _create_node(client)
    r = client.post("/internal/prices", params={"grid": "ERCOT"}, json={
        "node_id": node_id,
        "timestamp_utc": TS1,
        "lmp": 42.5,
    })
    assert r.status_code == 200

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
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
    r = post_prices(client, payload)
    assert r.status_code == 200

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    assert len(rows) == 3

    clear_state(client)


def test_read_prices_ordered_desc(client):
    """GET /internal/prices/{node_id} returns rows newest-first."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ])

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    timestamps = [row["timestamp_utc"] for row in rows]
    assert timestamps == sorted(timestamps, reverse=True)

    clear_state(client)


def test_read_prices_limit(client):
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ])

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 1})
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["lmp"] == 30.0

    clear_state(client)


def test_price_data_accuracy(client):
    """Verify lmp and timestamp round-trip exactly."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 123.456},
    ])

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 1})
    row = r.json()[0]
    assert row["node_id"] == node_id
    assert abs(row["lmp"] - 123.456) < 1e-6

    stored_ts = datetime.fromisoformat(row["timestamp_utc"])
    original_ts = datetime.fromisoformat(TS1)
    assert stored_ts == original_ts

    clear_state(client)


def test_price_idempotent_insert(client):
    """Duplicate (node_id, timestamp_utc) is silently ignored."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 99.0},
    ])

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["lmp"] == 10.0

    clear_state(client)


def test_delete_prices_only(client):
    """DELETE /internal/prices removes prices but leaves locations intact."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
    ])

    r = client.delete("/internal/prices")
    assert r.status_code == 200

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
    assert r.json() == []

    r = client.get("/internal/locations", params={"grid": "ERCOT"})
    assert len(r.json()) == 1

    clear_state(client)


def test_delete_locations_cascades_prices(client):
    """DELETE /internal/locations also removes dependent prices."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
    ])

    r = client.delete("/internal/locations")
    assert r.status_code == 200

    r = client.get("/internal/locations", params={"grid": "ERCOT"})
    assert r.json() == []

    r = client.get(f"/internal/prices/{node_id}", params={"limit": 10})
    assert r.json() == []


# ---------------------------------------------------------------------------
# Latest timestamp endpoint
# ---------------------------------------------------------------------------

def test_latest_timestamp_empty_db(client):
    clear_state(client)

    r = client.get("/internal/prices/latest-timestamp", params={"grid": "ERCOT"})
    assert r.status_code == 200
    assert r.json()["timestamp_utc"] is None

    clear_state(client)


def test_latest_timestamp_returns_max_for_grid(client):
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},
    ])

    r = client.get("/internal/prices/latest-timestamp", params={"grid": "ERCOT"})
    assert r.status_code == 200
    stored = datetime.fromisoformat(r.json()["timestamp_utc"])
    assert stored == datetime.fromisoformat(TS3)

    clear_state(client)


def test_latest_timestamp_isolates_by_grid(client):
    """ERCOT latest-timestamp must not be influenced by prices from other grids."""
    clear_state(client)

    ercot_id = _create_node(client, "HB_NORTH")
    nyiso_rows = client.post("/internal/locations/batch", json=[
        {"grid": "NYISO", "node_name": "CAPITL", "node_type": "ELECTRICAL_BUS"},
    ]).json()
    nyiso_id = nyiso_rows[0]["node_id"]

    post_prices(client, [
        {"node_id": ercot_id, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": nyiso_id, "timestamp_utc": TS3, "lmp": 99.0},  # later, different grid
    ])

    r = client.get("/internal/prices/latest-timestamp", params={"grid": "ERCOT"})
    assert r.status_code == 200
    stored = datetime.fromisoformat(r.json()["timestamp_utc"])
    assert stored == datetime.fromisoformat(TS1)  # must not see TS3 from NYISO

    clear_state(client)


def test_latest_timestamp_missing_grid_param(client):
    r = client.get("/internal/prices/latest-timestamp")
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Timeseries endpoint (public API)
# ---------------------------------------------------------------------------

TS_DAY1_A = "2024-03-01T00:05:00+00:00"
TS_DAY1_B = "2024-03-01T00:10:00+00:00"
TS_DAY1_C = "2024-03-01T00:15:00+00:00"
TS_DAY2_A = "2024-03-02T00:05:00+00:00"


def test_timeseries_returns_correct_day(client):
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_B, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_C, "lmp": 30.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY2_A, "lmp": 99.0},
    ])

    r = client.get("/api/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 3
    assert [row["lmp"] for row in rows] == [10.0, 20.0, 30.0]

    clear_state(client)


def test_timeseries_ordered_ascending(client):
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS_DAY1_C, "lmp": 30.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
        {"node_id": node_id, "timestamp_utc": TS_DAY1_B, "lmp": 20.0},
    ])

    r = client.get("/api/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    timestamps = [row["timestamp_utc"] for row in r.json()]
    assert timestamps == sorted(timestamps)

    clear_state(client)


def test_timeseries_empty_for_date_with_no_data(client):
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 10.0},
    ])

    r = client.get("/api/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-02"
    })
    assert r.status_code == 200
    assert r.json() == []

    clear_state(client)


def test_timeseries_node_not_found(client):
    clear_state(client)

    r = client.get("/api/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "DOES_NOT_EXIST", "date": "2024-03-01"
    })
    assert r.status_code == 404

    clear_state(client)


def test_timeseries_data_accuracy(client):
    """LMP and timestamp round-trip exactly through the timeseries endpoint."""
    clear_state(client)

    node_id = _create_node(client)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS_DAY1_A, "lmp": 47.123},
    ])

    r = client.get("/api/prices/timeseries", params={
        "grid": "ERCOT", "node_name": "HB_NORTH", "date": "2024-03-01"
    })
    row = r.json()[0]
    assert abs(row["lmp"] - 47.123) < 1e-6

    stored_ts = datetime.fromisoformat(row["timestamp_utc"])
    original_ts = datetime.fromisoformat(TS_DAY1_A)
    assert stored_ts == original_ts

    clear_state(client)


# ---------------------------------------------------------------------------
# Map nodes endpoint (public API)
# ---------------------------------------------------------------------------

def _create_node_full(client, node_name, *, zone=None, lat=None, lon=None, grid="ERCOT") -> int:
    rows = client.post("/internal/locations/batch", json=[{
        "grid": grid,
        "node_name": node_name,
        "node_type": "ELECTRICAL_BUS",
        "settlement_load_zone": zone,
        "latitude": lat,
        "longitude": lon,
    }]).json()
    return rows[0]["node_id"]


def test_map_nodes_basic(client):
    """Returns geocoded nodes with correct LMP and zone averages."""
    clear_state(client)

    # NORTH zone: two geocoded nodes, lmp 10 and 30 → zone avg 20
    north_a = _create_node_full(client, "NORTH_A", zone="NORTH", lat=31.0, lon=-97.0)
    north_b = _create_node_full(client, "NORTH_B", zone="NORTH", lat=31.5, lon=-97.5)
    # SOUTH zone: one geocoded node, lmp 50 → zone avg 50
    south_a = _create_node_full(client, "SOUTH_A", zone="SOUTH", lat=29.0, lon=-96.0)

    post_prices(client, [
        {"node_id": north_a, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": north_b, "timestamp_utc": TS1, "lmp": 30.0},
        {"node_id": south_a, "timestamp_utc": TS1, "lmp": 50.0},
    ])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 3

    by_name = {row["node_name"]: row for row in rows}

    assert abs(by_name["NORTH_A"]["lmp"] - 10.0) < 1e-6
    assert abs(by_name["NORTH_B"]["lmp"] - 30.0) < 1e-6
    assert abs(by_name["SOUTH_A"]["lmp"] - 50.0) < 1e-6

    # Both NORTH nodes see the same zone avg
    assert abs(by_name["NORTH_A"]["zone_avg_lmp"] - 20.0) < 1e-6
    assert abs(by_name["NORTH_B"]["zone_avg_lmp"] - 20.0) < 1e-6
    # SOUTH zone avg equals the single node's price
    assert abs(by_name["SOUTH_A"]["zone_avg_lmp"] - 50.0) < 1e-6

    # Coordinates preserved
    assert abs(by_name["NORTH_A"]["latitude"] - 31.0) < 1e-6
    assert abs(by_name["NORTH_A"]["longitude"] - (-97.0)) < 1e-6

    clear_state(client)


def test_map_nodes_uses_latest_timestamp(client):
    """When a node has prices at multiple timestamps, only the most recent LMP is used."""
    clear_state(client)

    node_id = _create_node_full(client, "NODE_A", zone="NORTH", lat=31.0, lon=-97.0)
    post_prices(client, [
        {"node_id": node_id, "timestamp_utc": TS1, "lmp": 10.0},  # oldest
        {"node_id": node_id, "timestamp_utc": TS2, "lmp": 20.0},
        {"node_id": node_id, "timestamp_utc": TS3, "lmp": 30.0},  # latest
    ])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert abs(rows[0]["lmp"] - 30.0) < 1e-6  # TS3's value, not TS1 or TS2

    clear_state(client)


def test_map_nodes_excludes_ungeocoded_nodes(client):
    """Nodes without lat/lon do not appear in the result."""
    clear_state(client)

    geo_a  = _create_node_full(client, "GEO_A",   zone="NORTH", lat=31.0, lon=-97.0)
    geo_b  = _create_node_full(client, "GEO_B",   zone="NORTH", lat=32.0, lon=-98.0)
    ungeo_c = _create_node_full(client, "UNGEO_C", zone="NORTH")  # no lat/lon
    ungeo_d = _create_node_full(client, "UNGEO_D", zone="NORTH")  # no lat/lon

    post_prices(client, [
        {"node_id": geo_a,   "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": geo_b,   "timestamp_utc": TS1, "lmp": 20.0},
        {"node_id": ungeo_c, "timestamp_utc": TS1, "lmp": 30.0},
        {"node_id": ungeo_d, "timestamp_utc": TS1, "lmp": 40.0},
    ])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 2
    assert {row["node_name"] for row in rows} == {"GEO_A", "GEO_B"}

    clear_state(client)


def test_map_nodes_zone_avg_includes_ungeocoded_nodes(client):
    """Zone average is computed from all nodes in the zone, not just geocoded ones."""
    clear_state(client)

    # NORTH zone: 1 geocoded (lmp=10) + 2 non-geocoded (lmp=20, 30)
    # zone avg must be (10+20+30)/3 = 20, not just 10
    geocoded = _create_node_full(client, "GEO_A",   zone="NORTH", lat=31.0, lon=-97.0)
    ungeo_b  = _create_node_full(client, "UNGEO_B", zone="NORTH")
    ungeo_c  = _create_node_full(client, "UNGEO_C", zone="NORTH")

    post_prices(client, [
        {"node_id": geocoded, "timestamp_utc": TS1, "lmp": 10.0},
        {"node_id": ungeo_b,  "timestamp_utc": TS1, "lmp": 20.0},
        {"node_id": ungeo_c,  "timestamp_utc": TS1, "lmp": 30.0},
    ])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1  # only the geocoded node returned
    assert abs(rows[0]["zone_avg_lmp"] - 20.0) < 1e-6  # avg of all 3 nodes in zone

    clear_state(client)


def test_map_nodes_null_zone_yields_null_zone_avg(client):
    """A geocoded node with no settlement_load_zone returns zone_avg_lmp=null."""
    clear_state(client)

    node_id = _create_node_full(client, "NODE_A", zone=None, lat=31.0, lon=-97.0)
    post_prices(client, [{"node_id": node_id, "timestamp_utc": TS1, "lmp": 50.0}])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert abs(rows[0]["lmp"] - 50.0) < 1e-6
    assert rows[0]["zone_avg_lmp"] is None

    clear_state(client)


def test_map_nodes_no_prices_yields_null_lmp(client):
    """A geocoded node with no prices at all appears with lmp=null and zone_avg_lmp=null."""
    clear_state(client)

    _create_node_full(client, "NODE_A", zone="NORTH", lat=31.0, lon=-97.0)
    # no prices inserted

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["lmp"] is None
    assert rows[0]["zone_avg_lmp"] is None

    clear_state(client)


def test_map_nodes_empty_when_no_geocoded_nodes(client):
    """Returns empty list when no nodes in the grid have coordinates."""
    clear_state(client)

    ungeo = _create_node_full(client, "NODE_A", zone="NORTH")  # no lat/lon
    post_prices(client, [{"node_id": ungeo, "timestamp_utc": TS1, "lmp": 10.0}])

    r = client.get("/api/prices/map-nodes", params={"grid": "ERCOT"})
    assert r.status_code == 200
    assert r.json() == []

    clear_state(client)
