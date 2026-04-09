import logging
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

import backend_client
from grid_client import GridClient, PriceRecord

poll_period = 2  # seconds
num_workers = 4  # 2 for steady state (2 pages per SCED interval), 4 for catch-up mode
metrics_log_interval = 10

location_id_dict = {}
location_dict_lock = threading.Lock()
metrics_lock = threading.Lock()

metrics = {}
metrics["grid_api"] = {"count": 0, "total": 0, "last": 0}
metrics["write_price"] = {"count": 0, "total": 0, "last": 0}
metrics["serialize_prices"] = {"count": 0, "total": 0, "last": 0}

q = queue.Queue(maxsize=100)  # bounded queue


def find_new_buses(records: list[PriceRecord], location_id_dict: dict) -> set:
    return {r.node_name for r in records if r.node_name not in location_id_dict}


def build_price_payload(records: list[PriceRecord], location_id_dict: dict) -> list:
    return [
        {
            "node_id": location_id_dict[r.node_name],
            "timestamp_utc": r.timestamp_utc.isoformat(),
            "lmp": r.lmp,
        }
        for r in records
    ]


def fetcher(client: GridClient, max_lookback_days: int):
    batch_id = 0

    latest_db_timestamp = backend_client.get_latest_timestamp(client.grid())
    floor = datetime.now(timezone.utc) - timedelta(days=max_lookback_days)
    if latest_db_timestamp is None:
        maxtime = floor
    else:
        parsed = datetime.fromisoformat(latest_db_timestamp)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        maxtime = max(parsed, floor)
    logger.info(f"Initial maxtime: {maxtime}")

    while True:
        t0 = time.perf_counter()
        now = datetime.now(timezone.utc)

        t0_page = time.perf_counter()
        for records in client.iter_pages(start=maxtime + timedelta(seconds=1), end=now):
            t0_1 = time.perf_counter()
            grid_api_time = t0_1 - t0_page

            if records:
                item = {
                    "records": records,
                    "starttime": t0,
                    "grid_api_time": grid_api_time,
                    "batch_id": batch_id,
                }
                q.put(item)  # blocks if queue is full
                logger.info(f"Queue size after put: {q.qsize()}")
            else:
                logger.info("No results fetched")

            maxtime = max(
                (r.timestamp_utc for r in records),
                default=maxtime,
            )
            t0_page = time.perf_counter()

        logger.info(f"Received maxtime: {maxtime}")
        time.sleep(poll_period)
        batch_id += 1


def writer(client: GridClient):
    while True:
        data = q.get()  # blocks if queue is empty
        logger.info(f"Queue size after get: {q.qsize()}")
        batch_id = data["batch_id"]
        batch_start_time = data["starttime"]
        curr_grid_api_time = data["grid_api_time"]
        records: list[PriceRecord] = data["records"]

        logger.info(f"Consuming batch {batch_id} with {len(records)} records")
        t0_1 = time.perf_counter()

        # Serialize location resolution across workers to avoid races on
        # location_id_dict when multiple workers encounter the same new bus
        with location_dict_lock:
            new_buses = find_new_buses(records, location_id_dict)
            for bus in new_buses:
                logger.info(f"New bus found: {bus}")
            for row in backend_client.put_locations(list(new_buses), client.grid()):
                try:
                    location_id_dict[row["node_name"]] = row["node_id"]
                except Exception as e:
                    raise RuntimeError(f"Could not add row: {row}") from e
            logger.info(f"Node map size: {len(location_id_dict)}")
            location_snapshot = dict(location_id_dict)  # snapshot under lock before releasing

        payload = build_price_payload(records, location_snapshot)

        t0_2 = time.perf_counter()
        backend_client.put_prices(payload)
        t0_3 = time.perf_counter()
        q.task_done()

        with metrics_lock:
            metrics["serialize_prices"]["count"] += 1
            metrics["serialize_prices"]["total"] += t0_2 - t0_1
            metrics["serialize_prices"]["last"] = t0_2 - t0_1

            metrics["write_price"]["count"] += 1
            metrics["write_price"]["total"] += t0_3 - t0_2
            metrics["write_price"]["last"] = t0_3 - t0_2

            metrics["grid_api"]["count"] += 1
            metrics["grid_api"]["total"] += curr_grid_api_time
            metrics["grid_api"]["last"] = curr_grid_api_time

            if metrics["write_price"]["count"] % metrics_log_interval == 0:
                for key in metrics:
                    avg = metrics[key]["total"] / metrics[key]["count"]
                    logger.info(
                        f"metrics [{key}] avg={avg:.4f}s "
                        f"last={metrics[key]['last']:.4f}s "
                        f"count={metrics[key]['count']}"
                    )


def _thread_excepthook(args):
    logger.critical(
        f"Unhandled exception in thread {args.thread.name}, exiting",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    os._exit(1)


if __name__ == "__main__":
    import argparse
    from ercot_client import ERCOTClient

    CLIENTS = {
        "ERCOT": ERCOTClient,
    }

    parser = argparse.ArgumentParser(description="Grid price ingestion service")
    parser.add_argument(
        "--grid",
        required=True,
        choices=list(CLIENTS),
        help="Grid to ingest from",
    )
    parser.add_argument(
        "--max-lookback",
        type=int,
        default=10,
        metavar="DAYS",
        help="Maximum days to look back on startup (default: 10)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(threadName)-12s %(levelname)-8s %(message)s",
    )
    threading.excepthook = _thread_excepthook

    client = CLIENTS[args.grid]()

    for row in backend_client.get_locations(client.grid()):
        try:
            location_id_dict[row["node_name"]] = row["node_id"]
        except Exception:
            logger.warning(f"Could not add row: {row}")

    fetcher_thread = threading.Thread(
        target=fetcher, args=(client, args.max_lookback), name="fetcher"
    )
    worker_threads = [
        threading.Thread(target=writer, args=(client,), name=f"writer-{i}")
        for i in range(num_workers)
    ]

    fetcher_thread.start()
    for w in worker_threads:
        w.start()

    fetcher_thread.join()
    for w in worker_threads:
        w.join()
