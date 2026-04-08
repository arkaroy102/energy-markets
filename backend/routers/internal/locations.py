from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from db import get_db
from models import Node, GridEnum
from schemas import LocationCreate, LocationResponse, LocationSummary

router = APIRouter(prefix="/locations", tags=["internal-locations"])

# --- Write helper ---
# Uses SQLAlchemy Core for bulk upsert with returning() — not available via ORM.
# See routers/internal/__init__.py for the broader ORM/Core split rationale.
def insert_locations(db: Session, locations: list[LocationCreate]):
    stmt = insert(Node).values([
        {"grid": p.grid, "node_name": p.node_name, "node_type": p.node_type}
        for p in locations
    ])
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["grid", "node_name"]
    ).returning(Node.node_id, Node.grid, Node.node_name, Node.node_type)

    result = db.execute(stmt)
    written_rows = result.fetchall()
    db.commit()
    return written_rows


@router.post("", response_model=LocationResponse | None)
def create_location(location: LocationCreate, db: Session = Depends(get_db)):
    rows = insert_locations(db, [location])
    if not rows:
        return None
    return {"node_id": rows[0].node_id, "grid": rows[0].grid, "node_name": rows[0].node_name, "node_type": rows[0].node_type}

@router.post("/batch", response_model=list[LocationResponse])
def create_locations(locations: list[LocationCreate], db: Session = Depends(get_db)):
    rows = insert_locations(db, locations)
    return [{"node_id": r.node_id, "grid": r.grid, "node_name": r.node_name, "node_type": r.node_type} for r in rows]

@router.get("", response_model=list[LocationSummary])
def get_locations(grid: GridEnum, db: Session = Depends(get_db)):
    rows = db.query(Node).filter(Node.grid == grid).all()
    return [{"node_id": row.node_id, "node_name": row.node_name} for row in rows]

@router.delete("")
def delete_all_locations(db: Session = Depends(get_db)):
    from models import NodePrice
    db.query(NodePrice).delete()  # prices first: FK constraint
    db.query(Node).delete()
    db.commit()
    return {}
