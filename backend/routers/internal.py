from fastapi import APIRouter, Depends, HTTPException
from fastapi import Query
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from db import get_db
from models import Node, NodePrice, GridEnum
from schemas import (
    LocationCreate, LocationResponse, LocationSummary,
    PriceCreate, PriceResponse, LatestTimestampResponse,
)
from sqlalchemy import func

# --- Internal auth dependency ---
# No-op today — Docker Compose network provides isolation.
# Future: validate X-Internal-Token header against an env var,
# or use cloud IAM/mTLS depending on the deployment platform.
def verify_internal_caller():
    pass

router = APIRouter(
    prefix="/internal",
    dependencies=[Depends(verify_internal_caller)],
)

# --- Write helpers ---
# Writes use SQLAlchemy Core (insert().values()) rather than the ORM for two reasons:
# 1. Bulk inserts: Core produces a single SQL statement regardless of batch size;
#    ORM db.add() in a loop issues N round trips.
# 2. Upsert syntax: on_conflict_do_nothing with returning() is only available via Core.
# Reads use the ORM (db.query()) where the result sets are small and the filter
# syntax is cleaner. This is an intentional split, not an inconsistency.

def insert_locations(db: Session, locations: list[LocationCreate]):
    stmt = insert(Node).values([
        {
            "grid": p.grid,
            "node_name": p.node_name,
            "node_type": p.node_type,
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


def insert_prices(db: Session, prices: list[PriceCreate]):
    stmt = insert(NodePrice).values([
        {
            "node_id": p.node_id,
            "timestamp_utc": p.timestamp_utc,
            "lmp": p.lmp,
        }
        for p in prices
    ])
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["node_id", "timestamp_utc"]
    )
    result = db.execute(stmt)
    db.commit()


# --- Location routes ---

@router.post("/locations", response_model=LocationResponse | None)
def create_location(location: LocationCreate, db: Session = Depends(get_db)):
    rows = insert_locations(db, [location])
    if not rows:
        return None
    return {
        "node_id": rows[0].node_id,
        "grid": rows[0].grid,
        "node_name": rows[0].node_name,
        "node_type": rows[0].node_type,
    }

@router.post("/locations/batch", response_model=list[LocationResponse])
def create_locations(locations: list[LocationCreate], db: Session = Depends(get_db)):
    rows = insert_locations(db, locations)
    return [{
        "node_id": row.node_id,
        "grid": row.grid,
        "node_name": row.node_name,
        "node_type": row.node_type,
    } for row in rows]

@router.get("/locations", response_model=list[LocationSummary])
def get_locations(grid: GridEnum, db: Session = Depends(get_db)):
    rows = db.query(Node).filter(Node.grid == grid).all()
    return [{"node_id": row.node_id, "node_name": row.node_name} for row in rows]

@router.get("/location", response_model=LocationResponse)
def get_location_by_node_name(grid: GridEnum, node_name: str, db: Session = Depends(get_db)):
    row = (
        db.query(Node)
        .filter(Node.grid == grid, Node.node_name == node_name)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    return {
        "node_id": row.node_id,
        "grid": row.grid,
        "node_name": row.node_name,
        "node_type": row.node_type,
    }


# --- Price routes ---

@router.post("/prices")
def create_price(price: PriceCreate, db: Session = Depends(get_db)):
    try:
        insert_prices(db, [price])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {}

@router.post("/prices/batch")
def create_prices(prices: list[PriceCreate], db: Session = Depends(get_db)):
    try:
        insert_prices(db, prices)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {}

@router.get("/prices/{node_id}", response_model=list[PriceResponse])
def get_prices(
    node_id: int,
    limit: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(NodePrice)
        .filter(NodePrice.node_id == node_id)
        .order_by(NodePrice.timestamp_utc.desc())
        .limit(limit)
        .all()
    )
    return [{"node_id": row.node_id, "timestamp_utc": row.timestamp_utc, "lmp": row.lmp} for row in rows]

@router.get("/latest-prices", response_model=list[PriceResponse])
def get_latest_prices(db: Session = Depends(get_db)):
    rows = (
        db.query(NodePrice)
        .distinct(NodePrice.node_id)
        .order_by(NodePrice.node_id, NodePrice.timestamp_utc.desc())
        .all()
    )
    return [{"node_id": row.node_id, "timestamp_utc": row.timestamp_utc, "lmp": row.lmp} for row in rows]

@router.get("/latest-price-timestamp", response_model=LatestTimestampResponse)
def get_latest_price_timestamp(db: Session = Depends(get_db)):
    latest_timestamp = db.query(func.max(NodePrice.timestamp_utc)).scalar()
    return {"timestamp_utc": latest_timestamp}


# --- Delete routes (test teardown only) ---

@router.delete("/prices")
def delete_all_prices(db: Session = Depends(get_db)):
    db.query(NodePrice).delete()
    db.commit()
    return {}

@router.delete("/locations")
def delete_all_locations(db: Session = Depends(get_db)):
    db.query(NodePrice).delete()  # prices first: FK constraint
    db.query(Node).delete()
    db.commit()
    return {}
