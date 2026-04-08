import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ercot_ingestion import find_new_buses, build_price_payload, parse_initial_maxtime, compute_maxtime

ct = ZoneInfo("America/Chicago")


def test_find_new_buses_returns_only_unknown():
    rows = [
        ['2026-04-04T14:10:20', None, 'BUS_A', 10.0],
        ['2026-04-04T14:10:20', None, 'BUS_B', 11.0],
        ['2026-04-04T14:10:20', None, 'BUS_C', 13.0],
    ]
    location_id_dict = {'BUS_B': 42}

    new_buses = find_new_buses(rows, location_id_dict)

    assert new_buses == {'BUS_A', 'BUS_C'}


def test_find_new_buses_deduplicates():
    rows = [
        ['2026-04-04T14:10:20', None, 'BUS_A', 10.0],
        ['2026-04-04T14:15:20', None, 'BUS_A', 11.0],  # same bus, second timestamp
    ]

    new_buses = find_new_buses(rows, location_id_dict={})

    assert new_buses == {'BUS_A'}


def test_find_new_buses_all_known():
    rows = [
        ['2026-04-04T14:10:20', None, 'BUS_A', 10.0],
        ['2026-04-04T14:10:20', None, 'BUS_B', 11.0],
    ]
    location_id_dict = {'BUS_A': 1, 'BUS_B': 2}

    new_buses = find_new_buses(rows, location_id_dict)

    assert new_buses == set()


def test_build_price_payload_attaches_ct_timezone():
    # April 4 2026 is CDT (UTC-5), naive ERCOT timestamp gets -05:00 offset attached
    rows = [
        ['2026-04-04T14:10:20', None, 'BUS_A', 14.8],
    ]
    location_id_dict = {'BUS_A': 1}

    payload = build_price_payload(rows, location_id_dict, ct)

    assert len(payload) == 1
    assert payload[0]['timestamp_utc'] == '2026-04-04T14:10:20-05:00'


def test_build_price_payload_maps_node_id_and_lmp():
    rows = [
        ['2026-04-04T14:10:20', None, 'BUS_A', 14.8],
        ['2026-04-04T14:10:20', None, 'BUS_B', -27.01],
    ]
    location_id_dict = {'BUS_A': 1, 'BUS_B': 2}

    payload = build_price_payload(rows, location_id_dict, ct)

    by_node = {p['node_id']: p for p in payload}
    assert by_node[1]['lmp'] == pytest.approx(14.8)
    assert by_node[2]['lmp'] == pytest.approx(-27.01)


def test_build_price_payload_winter_timestamp_attaches_cst_timezone():
    # January 4 2026 is CST (UTC-6), naive ERCOT timestamp gets -06:00 offset attached
    rows = [
        ['2026-01-04T14:10:20', None, 'BUS_A', 10.0],
    ]
    location_id_dict = {'BUS_A': 1}

    payload = build_price_payload(rows, location_id_dict, ct)

    assert payload[0]['timestamp_utc'] == '2026-01-04T14:10:20-06:00'


# --- parse_initial_maxtime ---

def test_parse_initial_maxtime_none_returns_datetime_min():
    result = parse_initial_maxtime(None, ct)

    assert result == datetime.min.replace(tzinfo=ct)


def test_parse_initial_maxtime_utc_string_converts_to_ct():
    # 2026-04-04 19:10:20 UTC = 14:10:20 CDT (UTC-5)
    result = parse_initial_maxtime('2026-04-04T19:10:20', ct)

    assert result.tzinfo is not None
    assert result.astimezone(timezone.utc) == datetime(2026, 4, 4, 19, 10, 20, tzinfo=timezone.utc)


# --- compute_maxtime ---

def test_compute_maxtime_returns_latest():
    current = datetime(2026, 4, 4, 13, 0, 0, tzinfo=ct)
    rows = [
        ['2026-04-04T14:00:00', None, 'BUS_A', 10.0],
        ['2026-04-04T14:05:00', None, 'BUS_B', 11.0],
        ['2026-04-04T14:03:00', None, 'BUS_C', 12.0],
    ]

    result = compute_maxtime(rows, current, ct)

    assert result == datetime(2026, 4, 4, 14, 5, 0, tzinfo=ct)


def test_compute_maxtime_empty_rows_returns_current():
    current = datetime(2026, 4, 4, 13, 0, 0, tzinfo=ct)

    result = compute_maxtime([], current, ct)

    assert result == current


def test_compute_maxtime_dst_boundary():
    # 2026 DST spring forward: March 8 at 2:00 AM CST -> 3:00 AM CDT
    # 01:59 CST (UTC-6) = 07:59 UTC; 03:01 CDT (UTC-5) = 08:01 UTC
    current = datetime(2026, 3, 8, 0, 0, 0, tzinfo=ct)
    rows = [
        ['2026-03-08T01:59:00', None, 'BUS_A', 10.0],  # CST
        ['2026-03-08T03:01:00', None, 'BUS_B', 11.0],  # CDT
    ]

    result = compute_maxtime(rows, current, ct)

    assert result.astimezone(timezone.utc) == datetime(2026, 3, 8, 8, 1, 0, tzinfo=timezone.utc)
