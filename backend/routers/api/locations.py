from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from db import get_db
from models import Node, GridEnum
from schemas import LocationSummary

router = APIRouter(prefix="/locations", tags=["api-locations"])


@router.get("", response_model=list[LocationSummary])
def get_locations(grid: GridEnum, db: Session = Depends(get_db)):
    rows = db.query(Node).filter(Node.grid == grid).all()
    return [{"node_id": row.node_id, "node_name": row.node_name, "latitude": row.latitude, "longitude": row.longitude} for row in rows]
