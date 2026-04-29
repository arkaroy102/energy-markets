from datetime import datetime
from pydantic import BaseModel
from models import GridEnum, NodeTypeEnum


class LocationCreate(BaseModel):
    grid: GridEnum
    node_name: str
    node_type: NodeTypeEnum
    external_id: str | None = None
    settlement_load_zone: str | None = None
    latitude: float | None = None
    longitude: float | None = None

class LocationResponse(BaseModel):
    node_id: int
    grid: GridEnum
    node_name: str
    node_type: NodeTypeEnum
    external_id: str | None = None
    settlement_load_zone: str | None = None
    latitude: float | None = None
    longitude: float | None = None

class LocationSummary(BaseModel):
    node_id: int
    node_name: str
    latitude: float | None = None
    longitude: float | None = None

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

class TimeseriesPoint(BaseModel):
    timestamp_utc: datetime
    lmp: float
