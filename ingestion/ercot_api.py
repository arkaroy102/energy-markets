import logging
import requests
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

USERNAME = "arka.roy102@gmail.com"
PASSWORD = "fac@ajk!wnv_heu9NAU"
SUBSCRIPTION_KEY = "c23173b7e2094b1fab1fff5cc0bbd25f"

SCOPE = "openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access"
CLIENT_ID = "fec253ea-0d06-4272-a5e6-b478baeecd70"

ERCOT_LMP_URL = "https://api.ercot.com/api/public-reports/np6-787-cd/lmp_electrical_bus"
AUTH_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token\
?username={username}\
&password={password}\
&grant_type=password\
&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access\
&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70\
&response_type=id_token"

class TokenManager():
    def __init__(self, username, password):
        self.auth_url = AUTH_URL.format(username=username, password=password)
        self._access_token = None
        self.ttl = 3600 # seconds
        self._lock = threading.Lock()

    def get_token(self):
        with self._lock:
            if self._access_token is None or time.time() >= self._expires_at:
                self._fetch_token()
            return self._access_token

    def _fetch_token(self):
        logger.debug("Fetching token")
        auth_resp = requests.post(self.auth_url)
        auth_resp.raise_for_status()

        data = auth_resp.json()

        self._expires_at = time.time() + float(data['expires_in'])
        self._access_token = data["access_token"]

    def force_refresh(self):
        with self._lock:
            self._fetch_token()
            return self._access_token


class ErcotClient:
    def __init__(self):
        self._session = requests.Session()
        self._token_manager = TokenManager(USERNAME, PASSWORD)

    def iter_pages(self, start: datetime, end: datetime, timeout=60, max_retries=5, batch_size=10000):
        page = 1
        total_pages = 1
        while page <= total_pages:
            data = self._fetch_page(start, end, page, batch_size, timeout, max_retries)
            total_pages = data["_meta"]["totalPages"]
            yield data
            page += 1

    def _fetch_page(self, start: datetime, end: datetime, page: int, batch_size: int, timeout: int, max_retries: int):
        backoff = 1.0
        params = {
            "SCEDTimestampFrom": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "SCEDTimestampTo": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "size": batch_size,
            "page": page,
        }
        logger.debug(f"Fetching page with params: {params}")

        headers = {
            "Authorization": f"Bearer {self._token_manager.get_token()}",
            "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
        }
        for attempt in range(max_retries + 1):
            try:
                resp = self._session.get(ERCOT_LMP_URL, headers=headers, params=params, timeout=timeout)
                if resp.status_code < 400:
                    return resp.json()
                elif resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after is not None:
                        sleep_s = float(retry_after)
                    else:
                        sleep_s = backoff
                        logger.warning(f"Rate limited, sleeping for {sleep_s} seconds")
                elif resp.status_code == 401:
                    logger.warning("Token expired, refreshing")
                    self._token_manager.force_refresh()
                    headers["Authorization"] = f"Bearer {self._token_manager.get_token()}"
                    continue
                else:
                    resp.raise_for_status()

                if attempt == max_retries:
                    raise RuntimeError(f"Rate limited after {max_retries} retries: {resp.text}")
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                logger.warning(f"Request error {e}, closing session and retrying")
                self._session.close()
                self._session = requests.Session()
                continue
            time.sleep(sleep_s)
            backoff *= 2
        raise RuntimeError(f"Reached max retries")
