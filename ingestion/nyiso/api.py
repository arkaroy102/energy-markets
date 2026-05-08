import csv
import io
import logging
import time
import requests
import zipfile
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

GENERATOR_CSV_URL = "http://mis.nyiso.com/public/csv/generator/generator.csv"
REALTIME_LMP_URL = "http://mis.nyiso.com/public/csv/realtime/{date}realtime_gen.csv"
REALTIME_MONTHLY_URL = "http://mis.nyiso.com/public/csv/realtime/{year}{month:02d}01realtime_gen_csv.zip"
REALTIME_LATEST_URL = "http://mis.nyiso.com/public/realtime/realtime_gen_lbmp.csv"

_NETWORK_ERRORS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
)


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

    def _get(self, url: str, timeout: int = 30, max_retries: int = 5) -> bytes | None:
        """GET with retry logic for transient network and HTTP errors.

        - 404: returns None (data not yet published)
        - 429: retries with backoff
        - 5xx: retries with backoff
        - Network errors: recreates session and retries with backoff
        - Success: returns response body as bytes
        """
        backoff = 1.0
        for attempt in range(max_retries + 1):
            try:
                resp = self._session.get(url, timeout=timeout)

                if resp.status_code == 404:
                    return None
                elif resp.status_code == 429:
                    logger.warning(f"NYISO rate limited, sleeping {backoff}s (attempt {attempt})")
                elif resp.status_code >= 500:
                    logger.warning(f"NYISO server error {resp.status_code} on {url} (attempt {attempt})")
                else:
                    resp.raise_for_status()
                    return resp.content

            except _NETWORK_ERRORS as e:
                logger.warning(f"NYISO network error on {url}: {e}, retrying (attempt {attempt})")
                self._session.close()
                self._session = requests.Session()

            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2

        raise RuntimeError(f"NYISO _get failed after {max_retries} retries: {url}")

    def fetch_generators(self) -> list[GeneratorRecord]:
        data = self._get(GENERATOR_CSV_URL)
        if data is None:
            return []
        reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
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
        data = self._get(url)
        if data is not None:
            return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))

        self._fetch_monthly_zip(target_date)
        return self._daily_cache.pop(target_date, [])

    def _fetch_monthly_zip(self, target_date: date) -> None:
        url = REALTIME_MONTHLY_URL.format(year=target_date.year, month=target_date.month)
        logger.info(f"Fetching NYISO monthly zip for {target_date.year}-{target_date.month:02d}")
        data = self._get(url, timeout=120)
        if data is None:
            logger.warning(f"No NYISO monthly zip for {target_date.year}-{target_date.month:02d}")
            return

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
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
        data = self._get(REALTIME_LATEST_URL)
        if data is None:
            return []
        return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
