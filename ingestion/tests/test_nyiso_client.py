import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from nyiso.client import NYISOClient
from grid_client import PriceRecord

_et = ZoneInfo("America/New_York")
_utc = timezone.utc

# Fixed "now" used across tests to keep assertions deterministic
_NOW = datetime(2026, 4, 8, 14, 0, 0, tzinfo=_utc)


def make_client() -> NYISOClient:
    """Build a NYISOClient with mocked API — no real HTTP."""
    with patch("nyiso.client.NYISOAPIClient") as MockAPI:
        MockAPI.return_value.fetch_generators.return_value = []
        client = NYISOClient()
    return client


def make_row(ts_et: str, name: str = "GEN_A", lmp: float = 30.0) -> dict:
    return {
        "Time Stamp": ts_et,
        "Name": name,
        "PTID": "12345",
        "LBMP ($/MWHr)": str(lmp),
        "Marginal Cost Losses ($/MWHr)": "0.0",
        "Marginal Cost Congestion ($/MWHr)": "0.0",
    }


# ---------------------------------------------------------------------------
# iter_pages mode selection
# ---------------------------------------------------------------------------

def test_iter_pages_uses_live_mode_when_window_small():
    client = make_client()
    start = _NOW - timedelta(minutes=5)
    end = _NOW

    client._api.fetch_latest_lmp = MagicMock(return_value=[])

    with patch("nyiso.client.datetime") as mock_dt:
        mock_dt.now.return_value = _NOW
        mock_dt.strptime = datetime.strptime
        list(client.iter_pages(start, end))

    client._api.fetch_latest_lmp.assert_called_once()
    client._api.fetch_lmp.assert_not_called()


def test_iter_pages_uses_catchup_mode_when_start_is_old():
    client = make_client()
    start = _NOW - timedelta(days=2)
    end = _NOW

    client._api.fetch_lmp = MagicMock(return_value=[])

    with patch("nyiso.client.datetime") as mock_dt:
        mock_dt.now.return_value = _NOW
        mock_dt.strptime = datetime.strptime
        list(client.iter_pages(start, end))

    client._api.fetch_lmp.assert_called()
    client._api.fetch_latest_lmp.assert_not_called()


def test_iter_pages_uses_catchup_when_end_is_old():
    """Large window but end is also far from now — catch-up, not live."""
    client = make_client()
    start = _NOW - timedelta(hours=2)
    end = _NOW - timedelta(hours=1)  # end is 1 hour ago

    client._api.fetch_lmp = MagicMock(return_value=[])

    with patch("nyiso.client.datetime") as mock_dt:
        mock_dt.now.return_value = _NOW
        mock_dt.strptime = datetime.strptime
        list(client.iter_pages(start, end))

    client._api.fetch_lmp.assert_called()
    client._api.fetch_latest_lmp.assert_not_called()


# ---------------------------------------------------------------------------
# _parse_rows
# ---------------------------------------------------------------------------

def test_parse_rows_converts_et_to_utc():
    client = make_client()
    # 2026-04-09 09:30 ET (EDT = UTC-4) → 13:30 UTC
    rows = [make_row("04/09/2026 09:30:00", lmp=45.0)]
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    records = client._parse_rows(rows, after=after)

    assert len(records) == 1
    assert records[0].timestamp_utc == datetime(2026, 4, 9, 13, 30, 0, tzinfo=_utc)
    assert records[0].lmp == pytest.approx(45.0)


def test_parse_rows_filters_before_after():
    client = make_client()
    after = datetime(2026, 4, 9, 13, 30, 0, tzinfo=_utc)
    rows = [
        make_row("04/09/2026 09:30:00", lmp=10.0),  # == after (UTC), excluded
        make_row("04/09/2026 09:35:00", lmp=20.0),  # after, included
    ]

    records = client._parse_rows(rows, after=after)

    assert len(records) == 1
    assert records[0].lmp == pytest.approx(20.0)


def test_parse_rows_filters_after_before():
    client = make_client()
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)
    before = datetime(2026, 4, 9, 13, 30, 0, tzinfo=_utc)
    rows = [
        make_row("04/09/2026 09:25:00", lmp=10.0),  # 13:25 UTC, included
        make_row("04/09/2026 09:35:00", lmp=20.0),  # 13:35 UTC, excluded
    ]

    records = client._parse_rows(rows, after=after, before=before)

    assert len(records) == 1
    assert records[0].lmp == pytest.approx(10.0)


def test_parse_rows_updates_last_interval_ts():
    client = make_client()
    assert client._last_interval_ts is None

    rows = [
        make_row("04/09/2026 09:25:00", lmp=10.0),
        make_row("04/09/2026 09:30:00", lmp=20.0),
    ]
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    client._parse_rows(rows, after=after)

    assert client._last_interval_ts == datetime(2026, 4, 9, 13, 30, 0, tzinfo=_utc)


def test_parse_rows_does_not_update_last_interval_ts_when_empty():
    client = make_client()
    client._last_interval_ts = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    client._parse_rows([], after=datetime(2026, 4, 9, 12, 0, 0, tzinfo=_utc))

    assert client._last_interval_ts == datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)


# ---------------------------------------------------------------------------
# _fetch_live gap detection
# ---------------------------------------------------------------------------

def test_fetch_live_warns_on_gap(caplog):
    client = make_client()
    client._last_interval_ts = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    # New interval is 11 minutes later — gap exceeds one SCED interval
    client._api.fetch_latest_lmp = MagicMock(return_value=[
        make_row("04/09/2026 09:11:00", lmp=30.0),  # 13:11 UTC
    ])
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    import logging
    with caplog.at_level(logging.WARNING, logger="nyiso.client"):
        client._fetch_live(after=after)

    assert any("gap detected" in r.message for r in caplog.records)


def test_fetch_live_no_warning_on_normal_interval(caplog):
    client = make_client()
    client._last_interval_ts = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    # Normal 5-minute advance
    client._api.fetch_latest_lmp = MagicMock(return_value=[
        make_row("04/09/2026 09:05:00", lmp=30.0),  # 13:05 UTC
    ])
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    import logging
    with caplog.at_level(logging.WARNING, logger="nyiso.client"):
        client._fetch_live(after=after)

    assert not any("gap detected" in r.message for r in caplog.records)


def test_fetch_live_no_warning_when_no_prev_ts(caplog):
    """First live poll — no previous timestamp to compare against."""
    client = make_client()
    assert client._last_interval_ts is None

    client._api.fetch_latest_lmp = MagicMock(return_value=[
        make_row("04/09/2026 09:05:00", lmp=30.0),
    ])
    after = datetime(2026, 4, 9, 13, 0, 0, tzinfo=_utc)

    import logging
    with caplog.at_level(logging.WARNING, logger="nyiso.client"):
        client._fetch_live(after=after)

    assert not any("gap detected" in r.message for r in caplog.records)
