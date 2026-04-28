import logging
import os
import time

from fastapi import FastAPI, Request

from routers.internal import internal_router
from routers.internal.locations import router as internal_locations_router
from routers.internal.prices import router as internal_prices_router
from routers.api import api_router
from routers.api.locations import router as api_locations_router
from routers.api.prices import router as api_prices_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI()

internal_router.include_router(internal_locations_router)
internal_router.include_router(internal_prices_router)
app.include_router(internal_router)

api_router.include_router(api_locations_router)
api_router.include_router(api_prices_router)
app.include_router(api_router)


@app.get("/health")
def health():
    return {"status": "ok"}


metrics = {}

@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    key = f"{request.method} {request.url.path}"
    if key not in metrics:
        metrics[key] = {"count": 0, "total": 0.0}

    metrics[key]["count"] += 1
    metrics[key]["total"] += duration
    avg = metrics[key]["total"] / metrics[key]["count"]

    logger.info(f"{key} avg={avg:.4f}s last={duration:.4f}s count={metrics[key]['count']}")

    cache_status = getattr(request.state, "cache_status", None)
    if cache_status:
        grid = request.query_params.get("grid", "unknown")
        cache_key = f"cache:{key}:{grid}"
        if cache_key not in metrics:
            metrics[cache_key] = {"hit": 0, "miss": 0, "error": 0}
        metrics[cache_key][cache_status] += 1
        total_cache = sum(metrics[cache_key].values())
        hit_rate = metrics[cache_key]["hit"] / total_cache
        error_rate = metrics[cache_key]["error"] / total_cache
        logger.info(f"Cache [{grid}] hit_rate={hit_rate:.4f} error_rate={error_rate:.4f} total={total_cache}")

    response.headers["X-Process-Time"] = str(duration)
    return response
