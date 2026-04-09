import requests
import os

BACKEND_URL = os.getenv('BACKEND_URL', default="http://localhost:8000")


def get_locations(grid: str):
    r = requests.get(
        f"{BACKEND_URL}/internal/locations",
        params={"grid": grid},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def get_latest_timestamp(grid: str):
    r = requests.get(
        f"{BACKEND_URL}/internal/prices/latest-timestamp",
        params={"grid": grid},
        timeout=30,
    )
    r.raise_for_status()
    resp_body = r.json()
    return resp_body["timestamp_utc"] if "timestamp_utc" in resp_body else None


def put_locations(node_names: list[str], grid: str):
    if not node_names:
        return []
    payload = [
        {"grid": grid, "node_name": node_name, "node_type": "ELECTRICAL_BUS"}
        for node_name in node_names
    ]
    r = requests.post(
        f"{BACKEND_URL}/internal/locations/batch",
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    results = r.json()
    if len(results) < len(payload):
        print(f"Only inserted {len(results)} instead of {len(payload)}")
    return results


def put_prices(payload: list[dict]):
    if not payload:
        return None
    r = requests.post(
        f"{BACKEND_URL}/internal/prices/batch",
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
