import os
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from ercot_api import TokenManager, ErcotClient

pytestmark = pytest.mark.integration

ct = ZoneInfo("America/Chicago")


def test_token_manager():
    tm = TokenManager(os.environ["ERCOT_USERNAME"], os.environ["ERCOT_PASSWORD"])
    token = tm.get_token()

    print(f"\nToken (first 20 chars): {token[:20]}...")
    print(f"Token length: {len(token)}")

    assert token is not None
    assert len(token) > 0


def test_ercot_client_fetch():
    client = ErcotClient()

    start = datetime(2026, 4, 4, 14, 0, 0, tzinfo=ct)
    end   = datetime(2026, 4, 4, 14, 15, 0, tzinfo=ct)

    total_records = 0
    all_pages = []
    for data in client.iter_pages(start, end):
        total_records = data['_meta']['totalRecords']
        all_pages.append(data)

    print(f"\nTime range: {start} -> {end}")
    print(f"Total records: {total_records}")
    print(f"Pages fetched: {len(all_pages)}")

    assert total_records == 57522, "Total records did not match expectation"
    assert len(all_pages) == 6, "Num pages did not match expectation"

    # Build lookup: (timestamp, bus) -> lmp, order-independent
    rows_by_key = {
        (row[0], row[2]): row[3]
        for page in all_pages
        for row in page['data']
    }

    # Spot-check specific records observed from a prior run
    assert rows_by_key[('2026-04-04T14:10:20', 'EB21192')] == pytest.approx(14.8)
    assert rows_by_key[('2026-04-04T14:10:20', 'EB21282')] == pytest.approx(16.59)
    assert rows_by_key[('2026-04-04T14:10:20', 'EB21292')] == pytest.approx(16.59)
    assert rows_by_key[('2026-04-04T14:10:20', 'EB21297')] == pytest.approx(16.59)
    assert rows_by_key[('2026-04-04T14:10:20', 'EB2132')]  == pytest.approx(-27.01)
