import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ingestion import find_new_buses, build_price_payload
from grid_client import PriceRecord

ct = ZoneInfo("America/Chicago")


def make_record(node_name: str, ts_str: str, lmp: float) -> PriceRecord:
    """Create a PriceRecord with a UTC timestamp, simulating ERCOTClient output."""
    return PriceRecord(
        node_name=node_name,
        timestamp_utc=datetime.fromisoformat(ts_str).replace(tzinfo=ct).astimezone(timezone.utc),
        lmp=lmp,
    )


# --- find_new_buses ---

def test_find_new_buses_returns_only_unknown():
    records = [
        make_record('BUS_A', '2026-04-04T14:10:20', 10.0),
        make_record('BUS_B', '2026-04-04T14:10:20', 11.0),
        make_record('BUS_C', '2026-04-04T14:10:20', 13.0),
    ]
    location_id_dict = {'BUS_B': 42}

    assert find_new_buses(records, location_id_dict) == {'BUS_A', 'BUS_C'}


def test_find_new_buses_deduplicates():
    records = [
        make_record('BUS_A', '2026-04-04T14:10:20', 10.0),
        make_record('BUS_A', '2026-04-04T14:15:20', 11.0),
    ]

    assert find_new_buses(records, location_id_dict={}) == {'BUS_A'}


def test_find_new_buses_all_known():
    records = [
        make_record('BUS_A', '2026-04-04T14:10:20', 10.0),
        make_record('BUS_B', '2026-04-04T14:10:20', 11.0),
    ]
    location_id_dict = {'BUS_A': 1, 'BUS_B': 2}

    assert find_new_buses(records, location_id_dict) == set()


def test_find_new_buses_empty_records():
    assert find_new_buses([], location_id_dict={'BUS_A': 1}) == set()


# --- build_price_payload ---

def test_build_price_payload_emits_utc():
    # April 4 2026 14:10:20 CDT (UTC-5) → 19:10:20 UTC
    records = [make_record('BUS_A', '2026-04-04T14:10:20', 14.8)]
    location_id_dict = {'BUS_A': 1}

    payload = build_price_payload(records, location_id_dict)

    assert len(payload) == 1
    assert payload[0]['timestamp_utc'] == '2026-04-04T19:10:20+00:00'


def test_build_price_payload_winter_emits_utc():
    # January 4 2026 14:10:20 CST (UTC-6) → 20:10:20 UTC
    records = [make_record('BUS_A', '2026-01-04T14:10:20', 10.0)]
    location_id_dict = {'BUS_A': 1}

    payload = build_price_payload(records, location_id_dict)

    assert payload[0]['timestamp_utc'] == '2026-01-04T20:10:20+00:00'


def test_build_price_payload_maps_node_id_and_lmp():
    records = [
        make_record('BUS_A', '2026-04-04T14:10:20', 14.8),
        make_record('BUS_B', '2026-04-04T14:10:20', -27.01),
    ]
    location_id_dict = {'BUS_A': 1, 'BUS_B': 2}

    payload = build_price_payload(records, location_id_dict)

    by_node = {p['node_id']: p for p in payload}
    assert by_node[1]['lmp'] == pytest.approx(14.8)
    assert by_node[2]['lmp'] == pytest.approx(-27.01)


def test_build_price_payload_empty():
    assert build_price_payload([], {}) == []
