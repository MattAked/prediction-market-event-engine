# core/events/execution.py

from dataclasses import dataclass
from decimal import Decimal

from ..models.orders import OrderSide


class ExecutionEvent:
    pass


@dataclass(frozen=True, slots=True)
class OrderSubmitted(ExecutionEvent):
    intent_id: str
    venue_order_id: str
    instrument_id: str
    side: OrderSide
    accepted_quantity: Decimal
    accepted_price: Decimal | None


@dataclass(frozen=True, slots=True)
class OrderSubmissionRejected(ExecutionEvent):
    intent_id: str
    instrument_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class OrderSubmissionFailed(ExecutionEvent):
    intent_id: str
    instrument_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class OrderPartiallyFilled(ExecutionEvent):
    intent_id: str
    venue_order_id: str
    instrument_id: str
    fill_quantity: Decimal
    fill_price: Decimal
    remaining_quantity: Decimal


@dataclass(frozen=True, slots=True)
class OrderFilled(ExecutionEvent):
    intent_id: str
    venue_order_id: str
    instrument_id: str
    fill_quantity: Decimal
    fill_price: Decimal
    avg_fill_price: Decimal  # Quantity-weighted average over the entire order


@dataclass(frozen=True, slots=True)
class OrderFailed(ExecutionEvent):
    intent_id: str
    venue_order_id: str
    instrument_id: str
    filled_quantity: Decimal
    remaining_quantity: Decimal
    reason: str


@dataclass(frozen=True, slots=True)
class OrderCancelled(ExecutionEvent):
    intent_id: str
    venue_order_id: str
    instrument_id: str
    filled_quantity: Decimal
    remaining_quantity: Decimal


@dataclass(frozen=True, slots=True)
class OrderCancellationRejected(ExecutionEvent):
    """The venue (or executor) refused the cancel, but the order may remain live."""

    intent_id: str
    target_intent_id: str
    reason: str
    instrument_id: str | None = None


@dataclass(frozen=True, slots=True)
class OrderCancellationFailed(ExecutionEvent):
    """The cancel request errored, but the order may remain live."""

    intent_id: str
    target_intent_id: str
    reason: str
    instrument_id: str | None = None
