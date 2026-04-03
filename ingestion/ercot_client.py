import requests, os

BACKEND_URL = os.getenv('BACKEND_URL', default="http://localhost:8000")

def get_locations():
    params = {
        "grid": "ERCOT"
    }

    r = requests.get(
        f"{BACKEND_URL}/locations",
        params=params,
        timeout=30
    )

    r.raise_for_status()
    #print(f"Loaded {len(r.json())} rows")
    return r.json()

def get_latest_timestamp():
    r = requests.get(
        f"{BACKEND_URL}/latest-price-timestamp",
        timeout=30
    )

    r.raise_for_status()
    resp_body = r.json()
    return resp_body["timestamp_utc"] if "timestamp_utc" in resp_body else None

def get_location_by_name(node_name : str):
    params = {
        "grid": "ERCOT",
        "node_name": node_name
    }

    r = requests.get(
        f"{BACKEND_URL}/location",
        params=params,
        timeout=30
    )

    if r.ok:
        return r.json()
    else:
        return None


def put_locations(node_names : list[str]):
    if len(node_names) == 0:
        return []
    payload = [{
        "grid": "ERCOT",
        "node_name": node_name,
        "node_type": "ELECTRICAL_BUS",
    }
               for node_name in node_names
               ]

    r = requests.post(
        f"{BACKEND_URL}/locations/batch",
        json=payload,
        timeout=30
    )

    r.raise_for_status()
    results = r.json()
    if len(results) < len(payload):
        print(f"Only inserted {len(results)} instead of {len(payload)}")
    return results

def put_prices(payload : list[dict]):
    if len(payload) == 0:
        return None
    r = requests.post(
        f"{BACKEND_URL}/prices/batch",
        json=payload,
        timeout=30
    )

    r.raise_for_status()
    results = r.json()
    if len(results) < len(payload):
        print(f"Only inserted {len(results)} instead of {len(payload)}")
    return results
