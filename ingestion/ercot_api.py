import requests
import queue
import threading
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from ercot_client import put_locations, get_locations, get_location_by_name, put_prices, get_latest_timestamp

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
        print("Fetching token")
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

    def get_ercot_data(self, params : dict, timeout=60, max_retries=5):
        backoff = 1.0
        print(params)

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
                        print(f"Rate limited, sleeping for {sleep_s} seconds")
                elif resp.status_code == 401:
                    print(f"Token expired, refreshing")
                    self._token_manager.force_refresh()
                    headers["Authorization"] = f"Bearer {self._token_manager.get_token()}"
                    continue
                else:
                    resp.raise_for_status()

                if attempt == max_retries:
                    raise RuntimeError(f"Rate limited after {max_retries} retries: {resp.text}")
            except(requests.exceptions.SSLError,
                   requests.exceptions.ConnectionError,
                   requests.exceptions.Timeout) as e:
                print(f"Request error {e}, closing session and retrying")
                self._session.close()
                self._session = requests.Session()
                continue
            time.sleep(sleep_s)
            backoff *= 2
        return None

ct = ZoneInfo("America/Chicago")
poll_period = 2 # seconds

location_id_dict = {}
for row in get_locations():
    try:
        location_id_dict[row['node_name']] = row['node_id']
    except:
        print(f"Could not add row: {row}")

metrics = {}
metrics["total"] = {"count" : 0, "total" : 0, "last" : 0}
metrics["ercot_api"] = {"count" : 0, "total" : 0, "last" : 0}
metrics["write_price"] = {"count" : 0, "total" : 0, "last" : 0}
metrics["serialize_prices"] = {"count" : 0, "total" : 0, "last" : 0}

q = queue.Queue(maxsize=100)  # bounded queue

def fetcher():

    eclient = ErcotClient()
    batch_id = 0
    # Spin in a loop and poll API

    latest_db_timestamp = get_latest_timestamp()
    print(latest_db_timestamp)
    maxtime = datetime.fromisoformat(latest_db_timestamp).replace(tzinfo=timezone.utc).astimezone(ct) if latest_db_timestamp != None else datetime.min.replace(tzinfo=ct)
    print(maxtime)
    while True:
        t0 = time.perf_counter()

        now = datetime.now(ct)
        start = (max(now - timedelta(days=5), maxtime + timedelta(seconds=1))).strftime("%Y-%m-%dT%H:%M:%S")
        end   = (now).strftime("%Y-%m-%dT%H:%M:%S")

        page_num = 1
        total_pages = 1
        params = {
            "SCEDTimestampFrom": start,
            "SCEDTimestampTo": end,
            "page": page_num,
            "size": 10000,
        }

        while page_num <= total_pages:
            params["page"] = page_num

            t0_0 = time.perf_counter()
            data = eclient.get_ercot_data(params)
            assert data != None, "Failed to get ercot data"
            t0_1 = time.perf_counter()

            if page_num == 1:
                total_pages = data['_meta']['totalPages']

            print(f"Page: {data['_meta']['currentPage']} out of {total_pages}, numrecords: {data['_meta']['totalRecords']}")
            if 'data' in data and data['_meta']['totalRecords'] > 0:
                data['starttime'] = t0
                data['ercot_api_time'] = (t0_1 - t0_0)
                data['batch_id'] = batch_id
                data['batch_done'] = data['_meta']['currentPage'] == data['_meta']['totalPages']
                q.put(data)             # blocks if queue is full
            else:
                print(f"No results fetched")

            maxtime = max([datetime.fromisoformat(row[0]).replace(tzinfo=ct) for row in data['data']] + [maxtime])
            page_num += 1
        print(f"Received maxtime: {maxtime}")
        time.sleep(poll_period)
        batch_id += 1

def writer():
    while True:
        data = q.get()         # blocks if queue is empty
        batch_id = data['batch_id']
        batch_start_time = data['starttime']
        curr_ercot_api_time = data['ercot_api_time']

        print(f"consuming batch: {batch_id}, with {len(data['data'])} records")
        t0_1 = time.perf_counter()
        new_busses = set()
        for row in data['data']:
            assert len(row) == 4, f"Unexpected row: {row}"
            electrical_bus = row[2]
            if electrical_bus not in location_id_dict and electrical_bus not in new_busses:
                new_busses.add(electrical_bus)
                print(f"New bus found: {electrical_bus}")

        # Batch insert new_busses
        for row in put_locations(list(new_busses)):
            try:
                location_id_dict[row['node_name']] = row['node_id']
            except:
                assert False, f"Could not add row: {row}"
        print(f"Node map size: {len(location_id_dict)}")

        payload = [{"node_id" : location_id_dict[row[2]],
                    "timestamp_utc" : datetime.fromisoformat(row[0]).replace(tzinfo=ct).astimezone(timezone.utc).isoformat(),
                    "lmp" : row[3]} for row in data['data']]
        t0_2 = time.perf_counter()
        put_prices(payload)
        t0_3 = time.perf_counter()
        q.task_done()

        t1 = time.perf_counter()

        metrics["serialize_prices"]["count"] += 1
        metrics["serialize_prices"]["total"] += (t0_2 - t0_1)
        metrics["serialize_prices"]["last"] = (t0_2 - t0_1)

        metrics["write_price"]["count"] += 1
        metrics["write_price"]["total"] += (t0_3 - t0_2)
        metrics["write_price"]["last"] = (t0_3 - t0_2)

        metrics["ercot_api"]["count"] += 1
        metrics["ercot_api"]["total"] += curr_ercot_api_time
        metrics["ercot_api"]["last"] = curr_ercot_api_time

        if data['batch_done']:
            metrics["total"]["count"] += 1
            metrics["total"]["total"] += (t1 - batch_start_time)
            metrics["total"]["last"] = (t1 - batch_start_time)

            for key in metrics:
                avg = metrics[key]["total"] / metrics[key]["count"]
                print(f"avg {key}: {avg}, last: {metrics[key]["last"]}, callcount: {metrics[key]["count"]}")


t1 = threading.Thread(target=fetcher)
t2 = threading.Thread(target=writer)

t1.start()
t2.start()

t1.join()
t2.join()
#q.join()  # waits until all tasks are done
