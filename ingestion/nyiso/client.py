import logging
from datetime import datetime, timezone, timedelta
from typing import Iterator
from zoneinfo import ZoneInfo

from grid_client import GridClient, PriceRecord
from nyiso.api import NYISOAPIClient

logger = logging.getLogger(__name__)

_et = ZoneInfo("America/New_York")
_TS_FMT = "%m/%d/%Y %H:%M:%S"


class NYISOClient(GridClient):
    _SCED_INTERVAL = timedelta(minutes=5)

    def __init__(self):
        self._api = NYISOAPIClient()
        self._generators = self._api.fetch_generators()
        self._last_interval_ts: datetime | None = None
        logger.info(f"Loaded {len(self._generators)} NYISO generators")

    def grid(self) -> str:
        return "NYISO"

    def node_type(self) -> str:
        return "GENERATOR"

    def initial_locations(self) -> list[dict]:
        return [
            {
                "grid": "NYISO",
                "node_name": g.name,
                "node_type": self.node_type(),
                "external_id": g.ptid,
                "settlement_load_zone": g.zone,
                "latitude": g.latitude,
                "longitude": g.longitude,
            }
            for g in self._generators
        ]

    # Switch from catch-up (daily CSV) to live (latest interval endpoint)
    # once maxtime is within two SCED intervals of now.
    _LIVE_THRESHOLD = timedelta(minutes=10)
    _CATCHUP_CHUNK_SIZE = 10000

    def iter_pages(self, start: datetime, end: datetime) -> Iterator[list[PriceRecord]]:
        now = datetime.now(timezone.utc)
        live_mode = (now - end <= self._LIVE_THRESHOLD) and (end - start <= self._LIVE_THRESHOLD)
        logger.info(f"NYISO iter_pages: {'live' if live_mode else 'catch-up'} mode | start={start} end={end} now={now}")
        if live_mode:
            yield self._fetch_live(start)
        else:
            yield from self._catchup_pages(start, end)

    def _catchup_pages(self, start: datetime, end: datetime) -> Iterator[list[PriceRecord]]:
        current_date = start.astimezone(_et).date()
        end_date = end.astimezone(_et).date()

        while current_date <= end_date:
            rows = self._api.fetch_lmp(current_date)
            records = self._parse_rows(rows, after=start, before=end)
            logger.info(f"NYISO catch-up {current_date}: {len(records)} records in window")
            for i in range(0, len(records), self._CATCHUP_CHUNK_SIZE):
                yield records[i:i + self._CATCHUP_CHUNK_SIZE]
            current_date += timedelta(days=1)

    def _fetch_live(self, after: datetime) -> list[PriceRecord]:
        prev_ts = self._last_interval_ts
        rows = self._api.fetch_latest_lmp()
        records = self._parse_rows(rows, after=after)

        if records and prev_ts is not None:
            new_ts = self._last_interval_ts  # updated by _parse_rows
            gap = new_ts - prev_ts
            if gap > self._SCED_INTERVAL:
                logger.warning(
                    f"NYISO interval gap detected: last={prev_ts}, "
                    f"new={new_ts}, gap={gap}"
                )

        logger.info(f"NYISO live: {len(records)} new records")
        return records

    def _parse_rows(self, rows: list[dict], after: datetime, before: datetime | None = None) -> list[PriceRecord]:
        records = []
        for row in rows:
            ts = (
                datetime.strptime(row["Time Stamp"], _TS_FMT)
                .replace(tzinfo=_et)
                .astimezone(timezone.utc)
            )
            if ts <= after:
                continue
            if before is not None and ts > before:
                continue
            records.append(PriceRecord(
                node_name=row["Name"],
                timestamp_utc=ts,
                lmp=float(row["LBMP ($/MWHr)"]),
            ))

        if records:
            self._last_interval_ts = max(r.timestamp_utc for r in records)

        return records
