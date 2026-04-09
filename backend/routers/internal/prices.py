from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func

from db import get_db
from models import GridEnum, Node, NodePrice
from schemas import PriceCreate, PriceResponse, LatestTimestampResponse
from redis_client import redis_client, zone_price_cache_key

import logging
logger = logging.getLogger(__name__)

_MAX_BATCH_SIZE = 10_000

router = APIRouter(prefix="/prices", tags=["internal-prices"])

# --- Write helper ---
# Uses SQLAlchemy Core for bulk insert — see routers/internal/__init__.py for rationale.
def insert_prices(db: Session, prices: list[PriceCreate], grid: GridEnum):
    stmt = insert(NodePrice).values([
        {"node_id": p.node_id, "timestamp_utc": p.timestamp_utc, "lmp": p.lmp}
        for p in prices
    ])
    stmt = stmt.on_conflict_do_nothing(index_elements=["node_id", "timestamp_utc"])
    db.execute(stmt)
    db.commit()

    try:
        redis_client.delete(zone_price_cache_key(grid.value))
    except Exception as exc:
        logger.warning(f"Redis cache invalidation failed: {exc}")


# Fixed-path routes must be defined before /{node_id} to avoid shadowing.
@router.get("/latest", response_model=list[PriceResponse])
def get_latest_prices(db: Session = Depends(get_db)):
    rows = (
        db.query(NodePrice)
        .distinct(NodePrice.node_id)
        .order_by(NodePrice.node_id, NodePrice.timestamp_utc.desc())
        .all()
    )
    return [{"node_id": row.node_id, "timestamp_utc": row.timestamp_utc, "lmp": row.lmp} for row in rows]

@router.get("/latest-timestamp", response_model=LatestTimestampResponse)
def get_latest_price_timestamp(grid: GridEnum = Query(...), db: Session = Depends(get_db)):
    latest_timestamp = (
        db.query(func.max(NodePrice.timestamp_utc))
        .join(Node, NodePrice.node_id == Node.node_id)
        .filter(Node.grid == grid)
        .scalar()
    )
    return {"timestamp_utc": latest_timestamp}

@router.get("/{node_id}", response_model=list[PriceResponse])
def get_prices(node_id: int, limit: int = Query(1, ge=1), db: Session = Depends(get_db)):
    rows = (
        db.query(NodePrice)
        .filter(NodePrice.node_id == node_id)
        .order_by(NodePrice.timestamp_utc.desc())
        .limit(limit)
        .all()
    )
    return [{"node_id": row.node_id, "timestamp_utc": row.timestamp_utc, "lmp": row.lmp} for row in rows]

@router.post("")
def create_price(price: PriceCreate, grid: GridEnum = Query(...), db: Session = Depends(get_db)):
    try:
        insert_prices(db, [price], grid)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {}

@router.post("/batch")
def create_prices(prices: list[PriceCreate], grid: GridEnum = Query(...), db: Session = Depends(get_db)):
    if len(prices) > _MAX_BATCH_SIZE:
        raise HTTPException(status_code=422, detail=f"Batch size {len(prices)} exceeds maximum {_MAX_BATCH_SIZE}")
    try:
        insert_prices(db, prices, grid)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {}

@router.delete("")
def delete_all_prices(db: Session = Depends(get_db)):
    db.query(NodePrice).delete()
    db.commit()
    return {}
