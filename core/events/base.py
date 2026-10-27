# core/events/base.py

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar


PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True, slots=True)
class EventEnvelope(Generic[PayloadT]):
    timestamp: datetime
    payload: PayloadT
    instance_id: str | None = None

    def __str__(self) -> str:
        return f"Event for instance {self.instance_id}: {self.payload} at {self.timestamp}"