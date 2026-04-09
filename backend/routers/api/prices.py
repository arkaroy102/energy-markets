import json
from datetime import datetime, timezone, date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from db import get_db
from models import Node, NodePrice, GridEnum
from schemas import ZonePriceResponse, TimeseriesPoint
from redis_client import redis_client, zone_price_cache_key

import logging
logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS_LATEST_ZONE_PRICES = 300  # 5 minutes

router = APIRouter(prefix="/prices", tags=["api-prices"])


@router.get("/timeseries", response_model=list[TimeseriesPoint])
def get_price_timeseries(
    grid: GridEnum,
    node_name: str,
    date: date = Query(...),
    db: Session = Depends(get_db),
):
    node = (
        db.query(Node)
        .filter(Node.grid == grid, Node.node_name == node_name)
        .first()
    )
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found in grid {grid}")

    day_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    rows = (
        db.query(NodePrice)
        .filter(
            NodePrice.node_id == node.node_id,
            NodePrice.timestamp_utc >= day_start,
            NodePrice.timestamp_utc < day_end,
        )
        .order_by(NodePrice.timestamp_utc.asc())
        .all()
    )
    return [{"timestamp_utc": row.timestamp_utc.replace(tzinfo=timezone.utc), "lmp": row.lmp} for row in rows]


@router.get("/zone-summary", response_model=list[ZonePriceResponse])
def get_latest_zone_prices(grid: GridEnum, request: Request, db: Session = Depends(get_db)):
    cache_key = zone_price_cache_key(grid.value)
    try:
        cached = redis_client.get(cache_key)
        if cached:
            request.state.cache_status = "hit"
            return json.loads(cached)
        else:
            request.state.cache_status = "miss"
    except Exception as exc:
        request.state.cache_status = "error"
        logger.warning(f"Redis exception: {exc}")

    latest_per_node = (
        db.query(
            NodePrice.node_id.label("node_id"),
            func.max(NodePrice.timestamp_utc).label("latest_timestamp_utc")
        )
        .group_by(NodePrice.node_id)
        .subquery()
    )

    rows = (
        db.query(
            Node.settlement_load_zone.label("settlement_load_zone"),
            func.avg(NodePrice.lmp).label("avg_lmp"),
            func.min(NodePrice.timestamp_utc).label("min_timestamp_utc"),
            func.max(NodePrice.timestamp_utc).label("max_timestamp_utc"),
            func.count(NodePrice.node_id).label("num_nodes"),
        )
        .join(
            latest_per_node,
            and_(
                NodePrice.node_id == latest_per_node.c.node_id,
                NodePrice.timestamp_utc == latest_per_node.c.latest_timestamp_utc,
            ),
        )
        .join(Node, NodePrice.node_id == Node.node_id)
        .filter(Node.grid == grid, Node.settlement_load_zone.isnot(None))
        .group_by(Node.settlement_load_zone)
        .order_by(Node.settlement_load_zone)
        .all()
    )

    result = [
        {
            "settlement_load_zone": row.settlement_load_zone,
            "avg_lmp": float(row.avg_lmp) if row.avg_lmp is not None else None,
            "min_timestamp_utc": row.min_timestamp_utc.replace(tzinfo=timezone.utc).isoformat(),
            "max_timestamp_utc": row.max_timestamp_utc.replace(tzinfo=timezone.utc).isoformat(),
            "num_nodes": row.num_nodes,
        }
        for row in rows
    ]

    try:
        redis_client.setex(
            cache_key,
            CACHE_TTL_SECONDS_LATEST_ZONE_PRICES,
            json.dumps(result),
        )
    except Exception as exc:
        logger.warning(f"Redis write failed: {exc}")

    return result
