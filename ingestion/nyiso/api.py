import csv
import io
import logging
import requests
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

GENERATOR_CSV_URL = "http://mis.nyiso.com/public/csv/generator/generator.csv"
REALTIME_LMP_URL = "http://mis.nyiso.com/public/csv/realtime/{date}realtime_gen.csv"
REALTIME_LATEST_URL = "http://mis.nyiso.com/public/realtime/realtime_gen_lbmp.csv"


@dataclass
class GeneratorRecord:
    name: str
    ptid: str
    zone: str
    latitude: float | None
    longitude: float | None


class NYISOAPIClient:
    def __init__(self):
        self._session = requests.Session()

    def fetch_generators(self) -> list[GeneratorRecord]:
        resp = self._session.get(GENERATOR_CSV_URL, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        generators = []
        for row in reader:
            if row.get("Active", "N").strip() != "Y":
                continue
            generators.append(GeneratorRecord(
                name=row["Generator Name"],
                ptid=row["Generator PTID"],
                zone=row["Zone"],
                latitude=float(row["Latitude"]) if row["Latitude"].strip() else None,
                longitude=float(row["Longitude"]) if row["Longitude"].strip() else None,
            ))
        return generators

    def fetch_lmp(self, target_date: date) -> list[dict]:
        url = REALTIME_LMP_URL.format(date=target_date.strftime("%Y%m%d"))
        resp = self._session.get(url, timeout=30)
        if resp.status_code == 404:
            logger.info(f"No NYISO LMP data for {target_date}")
            return []
        resp.raise_for_status()
        return list(csv.DictReader(io.StringIO(resp.text)))

    def fetch_latest_lmp(self) -> list[dict]:
        resp = self._session.get(REALTIME_LATEST_URL, timeout=30)
        resp.raise_for_status()
        return list(csv.DictReader(io.StringIO(resp.text)))
