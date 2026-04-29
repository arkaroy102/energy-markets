import pytest

from ercot_client import ERCOTClient

pytestmark = pytest.mark.integration


def test_initial_locations_returns_buses():
    client = ERCOTClient()
    locations = client.initial_locations()

    assert len(locations) > 10000


def test_initial_locations_geocoded_count():
    client = ERCOTClient()
    locations = client.initial_locations()

    geocoded = [loc for loc in locations if loc["latitude"] is not None and loc["longitude"] is not None]
    print(f"\nGeocoded: {len(geocoded)} / {len(locations)}")
    assert len(geocoded) >= 100


def test_initial_locations_schema():
    client = ERCOTClient()
    locations = client.initial_locations()

    loc = locations[0]
    assert "grid" in loc
    assert "node_name" in loc
    assert "node_type" in loc
    assert "settlement_load_zone" in loc
    assert "latitude" in loc
    assert "longitude" in loc
    assert loc["grid"] == "ERCOT"
    assert loc["node_type"] == "ELECTRICAL_BUS"
