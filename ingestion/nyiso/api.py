import csv
import io
import logging
import requests
import zipfile
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

GENERATOR_CSV_URL = "http://mis.nyiso.com/public/csv/generator/generator.csv"
REALTIME_LMP_URL = "http://mis.nyiso.com/public/csv/realtime/{date}realtime_gen.csv"
REALTIME_MONTHLY_URL = "http://mis.nyiso.com/public/csv/realtime/{year}{month:02d}01realtime_gen_csv.zip"
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
        self._daily_cache: dict[date, list[dict]] = {}

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
        if target_date in self._daily_cache:
            return self._daily_cache.pop(target_date)

        url = REALTIME_LMP_URL.format(date=target_date.strftime("%Y%m%d"))
        resp = self._session.get(url, timeout=30)
        if resp.status_code == 200:
            return list(csv.DictReader(io.StringIO(resp.text)))

        if resp.status_code == 404:
            self._fetch_monthly_zip(target_date)
            return self._daily_cache.pop(target_date, [])

        resp.raise_for_status()
        return []

    def _fetch_monthly_zip(self, target_date: date) -> None:
        url = REALTIME_MONTHLY_URL.format(year=target_date.year, month=target_date.month)
        logger.info(f"Fetching NYISO monthly zip for {target_date.year}-{target_date.month:02d}")
        resp = self._session.get(url, timeout=120)
        if resp.status_code == 404:
            logger.warning(f"No NYISO monthly zip for {target_date.year}-{target_date.month:02d}")
            return
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if not name.endswith("realtime_gen.csv"):
                    continue
                day_str = name.replace("realtime_gen.csv", "")
                try:
                    day = date(int(day_str[:4]), int(day_str[4:6]), int(day_str[6:8]))
                except ValueError:
                    continue
                with zf.open(name) as f:
                    rows = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))
                self._daily_cache[day] = rows
        logger.info(f"Cached {len(self._daily_cache)} days from monthly zip")

    def fetch_latest_lmp(self) -> list[dict]:
        resp = self._session.get(REALTIME_LATEST_URL, timeout=30)
        resp.raise_for_status()
        return list(csv.DictReader(io.StringIO(resp.text)))
