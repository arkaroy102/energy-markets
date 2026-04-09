import logging
from datetime import datetime, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

from grid_client import GridClient, PriceRecord
from ercot_api import ErcotClient as ErcotAPIClient

logger = logging.getLogger(__name__)

# ERCOT timestamps are published in Central Time
_ct = ZoneInfo("America/Chicago")


class ERCOTClient(GridClient):
    def __init__(self):
        self._api = ErcotAPIClient()

    def grid(self) -> str:
        return "ERCOT"

    def iter_pages(self, start: datetime, end: datetime) -> Iterator[list[PriceRecord]]:
        for data in self._api.iter_pages(start, end):
            meta = data["_meta"]
            logger.info(
                f"Page {meta['currentPage']}/{meta['totalPages']}, "
                f"records: {meta['totalRecords']}"
            )
            rows = data.get("data", [])
            yield [
                PriceRecord(
                    node_name=row[2],
                    timestamp_utc=datetime.fromisoformat(row[0])
                        .replace(tzinfo=_ct)
                        .astimezone(timezone.utc),
                    lmp=row[3],
                )
                for row in rows
            ]
