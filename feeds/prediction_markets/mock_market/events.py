from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class OrderBookState:
    """Full snapshot, sent once on subscribe."""

    bids: dict[float, int]
    asks: dict[float, int]
    timestamp: datetime


@dataclass(frozen=True)
class OrderBookUpdate:
    """Diff. A level with size 0 means that level is gone; {} means no change."""

    updated_bids: dict[float, int]
    updated_asks: dict[float, int]
    timestamp: datetime


@dataclass(frozen=True)
class NewTargetNumberUpdate:
    target_number: int
    window_end: datetime  # when this market closes; consumers need it to price
    timestamp: datetime