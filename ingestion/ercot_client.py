import csv
import io
import logging
import zipfile
from datetime import datetime, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

import requests

from grid_client import GridClient, PriceRecord
from ercot_api import ErcotClient as ErcotAPIClient

logger = logging.getLogger(__name__)

# ERCOT timestamps are published in Central Time
_ct = ZoneInfo("America/Chicago")

_SP_LIST_DOC_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=10008"
_SP_LIST_DOWNLOAD_URL = "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"

# Column names in the NP4-160-SG Settlement_Points CSV
_COL_BUS = "ELECTRICAL_BUS"
_COL_ZONE = "SETTLEMENT_LOAD_ZONE"


def _fetch_bus_to_zone() -> dict[str, str]:
    """Fetch the latest NP4-160-SG file and return a bus_name -> load_zone mapping."""
    resp = requests.get(_SP_LIST_DOC_URL, timeout=30)
    resp.raise_for_status()
    docs = resp.json().get("ListDocsByRptTypeRes", {}).get("DocumentList", [])
    if not docs:
        raise RuntimeError("No documents found in NP4-160-SG listing")

    latest_doc_id = docs[0]["Document"]["DocID"]
    logger.info(f"Fetching NP4-160-SG doc ID: {latest_doc_id}")

    download_url = _SP_LIST_DOWNLOAD_URL.format(doc_id=latest_doc_id)
    resp = requests.get(download_url, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = next(n for n in zf.namelist() if "Settlement_Points" in n and n.endswith(".csv"))
        with zf.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            columns = reader.fieldnames
            logger.info(f"NP4-160-SG columns: {columns}")

            mapping = {}
            for row in reader:
                bus = row.get(_COL_BUS, "").strip()
                zone = row.get(_COL_ZONE, "").strip()
                if bus and zone:
                    mapping[bus] = zone

    logger.info(f"Loaded {len(mapping)} bus-to-zone mappings from NP4-160-SG")
    return mapping


class ERCOTClient(GridClient):
    def __init__(self):
        self._api = ErcotAPIClient()

    def grid(self) -> str:
        return "ERCOT"

    def node_type(self) -> str:
        return "ELECTRICAL_BUS"

    def initial_locations(self) -> list[dict]:
        try:
            bus_to_zone = _fetch_bus_to_zone()
        except Exception as e:
            logger.warning(f"Failed to fetch NP4-160-SG bus-to-zone mapping: {e}")
            return []

        return [
            {
                "grid": "ERCOT",
                "node_name": bus,
                "node_type": self.node_type(),
                "settlement_load_zone": zone,
            }
            for bus, zone in bus_to_zone.items()
        ]

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
