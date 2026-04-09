from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator


@dataclass
class PriceRecord:
    node_name: str
    timestamp_utc: datetime  # always timezone-aware UTC
    lmp: float


class GridClient(ABC):
    @abstractmethod
    def grid(self) -> str:
        """Return the grid identifier string: ERCOT, NYISO, CAISO"""
        ...

    @abstractmethod
    def node_type(self) -> str:
        """Return the node type string for this grid: ELECTRICAL_BUS, GENERATOR, etc."""
        ...

    def initial_locations(self) -> list[dict]:
        """
        Return location payloads to upsert at startup.

        Override for grids with a known node list (e.g. NYISO generator CSV).
        Default is empty — buses are discovered lazily during ingestion (e.g. ERCOT).
        """
        return []

    @abstractmethod
    def iter_pages(self, start: datetime, end: datetime) -> Iterator[list[PriceRecord]]:
        """
        Yield pages of normalized price records for the given time window.

        start/end are timezone-aware datetimes. Each yielded list may be empty
        if the API returned no records for that page. The caller is responsible
        for tracking maxtime and passing start/end on each poll cycle.
        """
        ...
