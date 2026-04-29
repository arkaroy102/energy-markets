import os
import redis

REDIS_URL = os.getenv("REDIS_URL", default="redis://localhost:6379/0")

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True
)


def zone_price_cache_key(grid: str) -> str:
    return f"latest_zone_prices:{grid}"

def map_nodes_cache_key(grid: str) -> str:
    return f"map_nodes:{grid}"
