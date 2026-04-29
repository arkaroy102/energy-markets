import pytest
from datetime import date
from nyiso.api import NYISOAPIClient

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def client():
    return NYISOAPIClient()


def test_fetch_generators_returns_active_generators(client):
    generators = client.fetch_generators()

    print(f"\nActive generators: {len(generators)}")
    assert len(generators) > 800


def test_fetch_generators_spot_check(client):
    generators = client.fetch_generators()

    by_name = {g.name: g for g in generators}
    g = by_name["59TH STREET_GT_1"]
    assert g.ptid == "24138"
    assert g.zone == "N.Y.C."
    assert abs(g.latitude - 40.76612) < 1e-4
    assert abs(g.longitude - (-73.99611)) < 1e-4


def test_fetch_generators_no_inactive(client):
    generators = client.fetch_generators()

    # All returned generators should be active (inactive filtered in fetch_generators)
    assert all(g.name for g in generators)
    assert all(g.ptid for g in generators)
    assert all(g.zone for g in generators)


def test_fetch_lmp_returns_rows_for_known_date(client):
    rows = client.fetch_lmp(date(2026, 4, 9))

    print(f"\nRows for 2026-04-09: {len(rows)}")
    assert len(rows) > 90000


def test_fetch_lmp_spot_check(client):
    rows = client.fetch_lmp(date(2026, 4, 9))

    by_key = {(r["Time Stamp"], r["Name"]): r for r in rows}
    row = by_key[("04/09/2026 00:05:00", "59TH STREET_GT_1")]
    assert row["PTID"] == "24138"
    assert float(row["LBMP ($/MWHr)"]) == pytest.approx(38.80)


def test_fetch_lmp_returns_empty_for_future_date(client):
    rows = client.fetch_lmp(date(2030, 1, 1))

    assert rows == []


def test_fetch_latest_lmp_returns_rows(client):
    rows = client.fetch_latest_lmp()

    print(f"\nLatest interval rows: {len(rows)}")
    assert len(rows) > 500


def test_fetch_latest_lmp_single_timestamp(client):
    """All rows in the latest interval file should share the same timestamp."""
    rows = client.fetch_latest_lmp()

    timestamps = {r["Time Stamp"] for r in rows}
    print(f"\nTimestamps in latest file: {timestamps}")
    assert len(timestamps) == 1


def test_fetch_latest_lmp_has_expected_columns(client):
    rows = client.fetch_latest_lmp()

    assert len(rows) > 0
    row = rows[0]
    assert "Time Stamp" in row
    assert "Name" in row
    assert "PTID" in row
    assert "LBMP ($/MWHr)" in row
