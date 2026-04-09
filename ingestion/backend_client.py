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


def put_locations(locations: list[dict]):
    if not locations:
        return []
    r = requests.post(
        f"{BACKEND_URL}/internal/locations/batch",
        json=locations,
        timeout=60,
    )
    r.raise_for_status()
    results = r.json()
    if len(results) < len(locations):
        print(f"Only inserted {len(results)} instead of {len(locations)}")
    return results


def put_prices(payload: list[dict], grid: str):
    if not payload:
        return None
    r = requests.post(
        f"{BACKEND_URL}/internal/prices/batch",
        params={"grid": grid},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
