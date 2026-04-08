import logging
import os
import queue
import threading
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

from ercot_api import ErcotClient
from ercot_client import put_locations, get_locations, put_prices, get_latest_timestamp

ct = ZoneInfo("America/Chicago")
poll_period = 2 # seconds
num_workers = 4  # 2 for steady state (2 pages per SCED interval), 4 for catch-up mode
metrics_log_interval = 10

location_id_dict = {}
location_dict_lock = threading.Lock()
metrics_lock = threading.Lock()

metrics = {}
metrics["ercot_api"] = {"count" : 0, "total" : 0, "last" : 0}
metrics["write_price"] = {"count" : 0, "total" : 0, "last" : 0}
metrics["serialize_prices"] = {"count" : 0, "total" : 0, "last" : 0}

q = queue.Queue(maxsize=100)  # bounded queue


def find_new_buses(rows: list, location_id_dict: dict) -> set:
    new_buses = set()
    for row in rows:
        assert len(row) == 4, f"Unexpected row: {row}"
        bus = row[2]
        if bus not in location_id_dict and bus not in new_buses:
            new_buses.add(bus)
    return new_buses


def parse_initial_maxtime(latest_db_timestamp: str | None, tz) -> datetime:
    if latest_db_timestamp is None:
        return datetime.min.replace(tzinfo=tz)
    return datetime.fromisoformat(latest_db_timestamp).replace(tzinfo=timezone.utc).astimezone(tz)


def compute_maxtime(rows: list, current_maxtime: datetime, tz) -> datetime:
    return max(
        (datetime.fromisoformat(row[0]).replace(tzinfo=tz) for row in rows),
        default=current_maxtime,
    )


def build_price_payload(rows: list, location_id_dict: dict, tz) -> list:
    return [
        {
            "node_id": location_id_dict[row[2]],
            "timestamp_utc": datetime.fromisoformat(row[0]).replace(tzinfo=tz).isoformat(),
            "lmp": row[3],
        }
        for row in rows
    ]


def fetcher():
    eclient = ErcotClient()
    batch_id = 0

    latest_db_timestamp = get_latest_timestamp()
    maxtime = parse_initial_maxtime(latest_db_timestamp, ct)
    logger.info(f"Initial maxtime: {maxtime}")
    while True:
        t0 = time.perf_counter()

        now = datetime.now(ct)
        start = max(now - timedelta(days=5), maxtime + timedelta(seconds=1))
        end   = now
        t0_page = time.perf_counter()
        for data in eclient.iter_pages(start, end):
            t0_1 = time.perf_counter()
            ercot_api_time = t0_1 - t0_page

            logger.info(f"Page: {data['_meta']['currentPage']} out of {data['_meta']['totalPages']}, numrecords: {data['_meta']['totalRecords']}")
            if 'data' in data and data['_meta']['totalRecords'] > 0:
                data['starttime'] = t0
                data['ercot_api_time'] = ercot_api_time
                data['batch_id'] = batch_id
                data['batch_done'] = data['_meta']['currentPage'] == data['_meta']['totalPages']
                q.put(data)             # blocks if queue is full
                logger.info(f"Queue size after put: {q.qsize()}")
            else:
                logger.info("No results fetched")

            maxtime = compute_maxtime(data['data'], maxtime, ct)
            t0_page = time.perf_counter()
        logger.info(f"Received maxtime: {maxtime}")
        time.sleep(poll_period)
        batch_id += 1

def writer():
    while True:
        data = q.get()         # blocks if queue is empty
        logger.info(f"Queue size after get: {q.qsize()}")
        batch_id = data['batch_id']
        batch_start_time = data['starttime']
        curr_ercot_api_time = data['ercot_api_time']

        logger.info(f"Consuming batch {batch_id} with {len(data['data'])} records")
        t0_1 = time.perf_counter()

        # Serialize location resolution across workers to avoid races on
        # location_id_dict when multiple workers encounter the same new bus
        with location_dict_lock:
            new_busses = find_new_buses(data['data'], location_id_dict)
            for bus in new_busses:
                logger.info(f"New bus found: {bus}")
            for row in put_locations(list(new_busses)):
                try:
                    location_id_dict[row['node_name']] = row['node_id']
                except Exception as e:
                    raise RuntimeError(f"Could not add row: {row}") from e
            logger.info(f"Node map size: {len(location_id_dict)}")
            location_snapshot = dict(location_id_dict)  # snapshot under lock before releasing

        payload = build_price_payload(data['data'], location_snapshot, ct)

        t0_2 = time.perf_counter()
        put_prices(payload)
        t0_3 = time.perf_counter()
        q.task_done()

        t1 = time.perf_counter()

        with metrics_lock:
            metrics["serialize_prices"]["count"] += 1
            metrics["serialize_prices"]["total"] += (t0_2 - t0_1)
            metrics["serialize_prices"]["last"] = (t0_2 - t0_1)

            metrics["write_price"]["count"] += 1
            metrics["write_price"]["total"] += (t0_3 - t0_2)
            metrics["write_price"]["last"] = (t0_3 - t0_2)

            metrics["ercot_api"]["count"] += 1
            metrics["ercot_api"]["total"] += curr_ercot_api_time
            metrics["ercot_api"]["last"] = curr_ercot_api_time

            if metrics["write_price"]["count"] % metrics_log_interval == 0:
                for key in metrics:
                    avg = metrics[key]["total"] / metrics[key]["count"]
                    logger.info(f"metrics [{key}] avg={avg:.4f}s last={metrics[key]['last']:.4f}s count={metrics[key]['count']}")


def _thread_excepthook(args):
    logger.critical(
        f"Unhandled exception in thread {args.thread.name}, exiting",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    os._exit(1)


if __name__ == '__main__':
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(threadName)-12s %(levelname)-8s %(message)s",
    )
    threading.excepthook = _thread_excepthook

    for row in get_locations():
        try:
            location_id_dict[row['node_name']] = row['node_id']
        except:
            logger.warning(f"Could not add row: {row}")

    fetcher_thread = threading.Thread(target=fetcher, name="fetcher")
    worker_threads = [threading.Thread(target=writer, name=f"writer-{i}") for i in range(num_workers)]

    fetcher_thread.start()
    for w in worker_threads:
        w.start()

    fetcher_thread.join()
    for w in worker_threads:
        w.join()
