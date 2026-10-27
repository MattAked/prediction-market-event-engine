from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto

from ...core.events.execution import ExecutionEvent
from ...core.models.orders import OrderSide
from ...core.outputs import ExecutionIntent, PlaceOrderIntent
from ..models import ExecutionState

ZERO = Decimal('0')


@dataclass(slots=True)
class ExecutionRecord:
    intent: PlaceOrderIntent
    requested_at: datetime
    state: ExecutionState = ExecutionState.PENDING_SUBMISSION
    accepted_quantity: Decimal | None = None
    accepted_price: Decimal | None = None
    venue_order_id: str | None = None
    accepted_at: datetime | None = None
    cancelled_at: datetime | None = None
    expires_at: datetime | None = None
    filled_quantity: Decimal = ZERO
    total_fill_cost: Decimal = ZERO
    fees: Decimal = ZERO
    rejection_reason: str | None = None

    @property
    def remaining_quantity(self) -> Decimal:
        quantity = self.accepted_quantity if self.accepted_quantity is not None else self.intent.quantity
        return quantity - self.filled_quantity


@dataclass(frozen=True, slots=True)
class ExecutionIntentArrived:
    intent: ExecutionIntent


@dataclass(frozen=True, slots=True)
class OrderExpiryDue:
    intent_id: str


@dataclass(frozen=True, slots=True)
class ScheduledExecutionEvent:
    event: ExecutionEvent | ExecutionIntentArrived | OrderExpiryDue
    delay: float  # seconds relative to the current replay timestamp

    def __post_init__(self) -> None:
        from math import isfinite
        if not isfinite(self.delay) or self.delay < 0:
            raise ValueError('Event delay must be finite and nonnegative')


@dataclass(frozen=True, slots=True)
class VenueOrderAccepted:
    accepted_quantity: Decimal
    accepted_price: Decimal | None
    venue_order_id: str


@dataclass(frozen=True, slots=True)
class VenueOrderRejected:
    reason: str


@dataclass(frozen=True, slots=True)
class CancellationValidation:
    rejection_reason: str | None = None


class BookSide(Enum):
    BID = auto()
    ASK = auto()


class BookUpdateKind(Enum):
    SNAPSHOT = auto()
    ABSOLUTE = auto()
    DELTA = auto()


@dataclass(frozen=True, slots=True)
class BookLevel:
    side: BookSide
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class BookUpdate:
    instrument_key: str
    kind: BookUpdateKind
    levels: tuple[BookLevel, ...]


@dataclass(frozen=True, slots=True)
class FillRecord:
    intent_id: str
    venue_order_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    price: Decimal
    fee: Decimal
    matched_at: datetime
    notification_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    fills: tuple[FillRecord, ...]
    positions: dict[str, Decimal]
    cash_flows: dict[str, Decimal]
    total_fees: Decimal
    open_order_ids: tuple[str, ...]
    pending_request_ids: tuple[str, ...] = ()
    shutdown_started_at: datetime | None = None
    cleanup_deadline: datetime | None = None
    finalised_at: datetime | None = None
