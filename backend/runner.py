from datetime import datetime, timezone
import logging
import os
import time, json

from fastapi import Request
from fastapi import FastAPI, Depends, Query
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from sqlalchemy.dialects.postgresql import insert

from pydantic import BaseModel

from db import engine, SessionLocal
from models import Base, Node, NodePrice, GridEnum, NodeTypeEnum

from redis_client import redis_client

logger = logging.getLogger(__name__)

CACHE_KEY_LATEST_ZONE_PRICES = "latest_zone_prices"
CACHE_TTL_SECONDS_LATEST_ZONE_PRICES = 300 # 5 minutes

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(message)s",
)

app = FastAPI()

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class LocationCreate(BaseModel):
    grid: GridEnum
    node_name: str
    node_type: NodeTypeEnum

class LocationResponse(BaseModel):
    node_id: int
    grid: GridEnum
    node_name: str
    node_type: NodeTypeEnum

class LocationSummary(BaseModel):
    node_id: int
    node_name: str

@app.post("/locations", response_model=LocationResponse | None)
def create_location(location: LocationCreate, db: Session = Depends(get_db)):
    rows = insert_locations(db, [location])
    if not rows:
        return {}
    return {
        "node_id": rows[0].node_id,
        "grid": rows[0].grid,
        "node_name": rows[0].node_name,
        "node_type": rows[0].node_type,
    }

@app.post("/locations/batch", response_model=list[LocationResponse])
def create_locations(
    locations: list[LocationCreate],
    db: Session = Depends(get_db),
):
    rows = insert_locations(db, locations)
    return [{
        "node_id": row.node_id,
        "grid": row.grid,
        "node_name": row.node_name,
        "node_type": row.node_type,
    } for row in rows]

@app.get("/locations", response_model=list[LocationSummary])
def get_locations(
    grid: GridEnum,
    db: Session = Depends(get_db),
):
    rows = (
        db.query(Node)
        .filter(Node.grid == grid)
        .all()
    )

    return [
        {
            "node_id": row.node_id,
            "node_name": row.node_name,
        }
        for row in rows
    ]

def insert_locations(db: Session, locations: list[LocationCreate]):
    stmt = insert(Node).values([
        {
            "grid" : p.grid,
            "node_name" : p.node_name,
            "node_type" : p.node_type,
        }
        for p in locations
    ])

    stmt = stmt.on_conflict_do_nothing(
        index_elements=["grid", "node_name"]
    ).returning(Node.node_id, Node.grid, Node.node_name, Node.node_type)

    result = db.execute(stmt)
    written_rows = result.fetchall()
    db.commit()
    return written_rows

@app.get("/location", response_model=LocationResponse)
def get_location_by_node_name(
    grid: GridEnum,
    node_name: str,
    db: Session = Depends(get_db),
):
    row = (
        db.query(Node)
        .filter(
            Node.grid == grid,
            Node.node_name == node_name,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Node name {node_name} not found")

    return {
        "node_id": row.node_id,
        "grid": row.grid,
        "node_name": row.node_name,
        "node_type": row.node_type,
    }

class PriceCreate(BaseModel):
    node_id: int
    timestamp_utc: datetime
    lmp: float

class PriceResponse(BaseModel):
    node_id: int
    timestamp_utc: datetime
    lmp: float

class ZonePriceResponse(BaseModel):
    settlement_load_zone: str
    avg_lmp: float | None
    min_timestamp_utc: datetime
    max_timestamp_utc: datetime
    num_nodes: int

class LatestTimestampResponse(BaseModel):
    timestamp_utc: datetime | None

def insert_prices(db: Session, prices: list[PriceCreate]):
    stmt = insert(NodePrice).values([
        {
            "node_id" : p.node_id,
            "timestamp_utc" : p.timestamp_utc,
            "lmp" : p.lmp,
        }
        for p in prices
    ])

    stmt = stmt.on_conflict_do_nothing(
        index_elements=["node_id", "timestamp_utc"]
    )

    try:
        result = db.execute(stmt)
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc))

    try:
        redis_client.delete(CACHE_KEY_LATEST_ZONE_PRICES)
    except Exception as exc:
        logger.warning(f"Redis delete error: {exc}")

@app.post("/prices")
def create_price(price: PriceCreate, db: Session = Depends(get_db)):
    insert_prices(db, [price])
    return {}

@app.post("/prices/batch")
def create_prices(
    prices: list[PriceCreate],
    db: Session = Depends(get_db),
):
    insert_prices(db, prices)
    return {}

@app.delete("/prices")
def delete_all_prices(db: Session = Depends(get_db)):
    db.query(NodePrice).delete()
    db.commit()
    return {}

@app.delete("/locations")
def delete_all_locations(db: Session = Depends(get_db)):
    db.query(NodePrice).delete()  # prices first: FK constraint
    db.query(Node).delete()
    db.commit()
    return {}

@app.get("/prices/{node_id}", response_model=list[PriceResponse])
def get_prices(
    node_id: int,
    limit: int = Query(1, ge=1),  # default = 1
    db: Session = Depends(get_db)
):
    rows = (
        db.query(NodePrice)
        .filter(NodePrice.node_id == node_id)
        .order_by(NodePrice.timestamp_utc.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "node_id": row.node_id,
            "timestamp_utc": row.timestamp_utc,
            "lmp": row.lmp,
        }
        for row in rows
    ]

@app.get("/latest-prices", response_model=list[PriceResponse])
def get_latest_prices(db: Session = Depends(get_db)):
    rows = (
        db.query(NodePrice)
        .distinct(NodePrice.node_id)
        .order_by(NodePrice.node_id, NodePrice.timestamp_utc.desc())
        .all()
    )

    return [
        {
            "node_id": row.node_id,
            "timestamp_utc": row.timestamp_utc,
            "lmp": row.lmp,
        }
        for row in rows
    ]

@app.get("/latest-price-timestamp", response_model=LatestTimestampResponse)
def get_latest_price_timestamp(db: Session = Depends(get_db)):
    latest_timestamp = db.query(func.max(NodePrice.timestamp_utc)).scalar()
    return { "timestamp_utc": latest_timestamp }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/latest-zone-prices", response_model=list[ZonePriceResponse])
def get_latest_zone_prices(request: Request, db: Session = Depends(get_db)):
    try:
        cached = redis_client.get(CACHE_KEY_LATEST_ZONE_PRICES)
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
        .filter(Node.settlement_load_zone.isnot(None))
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
        redis_client.setex(CACHE_KEY_LATEST_ZONE_PRICES,
                           CACHE_TTL_SECONDS_LATEST_ZONE_PRICES,
                           json.dumps(result))
    except Exception as exc:
        logger.warning(f"Redis write failed {exc}")

    return result


metrics = {}

@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = (time.perf_counter() - start)


    key = f"{request.method} {request.url.path}"

    if key not in metrics:
        metrics[key] = {"count": 0, "total": 0.0, "cache_hit": 0.0, "cache_miss": 0.0, "cache_error": 0.0}

    metrics[key]["count"] += 1
    metrics[key]["total"] += duration

    avg = metrics[key]["total"] / metrics[key]["count"]


    logger.info(f"{request.method} {request.url.path} took {duration:.4f}s")
    logger.info(f"{key} avg={avg:.4f}s last={duration:.4f}s callcount={metrics[key]["count"]}")

    cache_status = getattr(request.state, "cache_status", None)
    if cache_status:
        if cache_status == "hit":
            metrics[key]["cache_hit"] += 1
        elif cache_status == "miss":
            metrics[key]["cache_miss"] += 1
        elif cache_status == "error":
            metrics[key]["cache_error"] += 1

        hit_rate = metrics[key]["cache_hit"]/metrics[key]["count"]
        error_rate = metrics[key]["cache_error"]/metrics[key]["count"]

        logger.info(f"Cache hit rate: {hit_rate:.4f}, error_rate: {error_rate:.4f}")

    response.headers["X-Process-Time"] = str(duration)
    return response
