# executors/models.py

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Generic, Protocol, TypeVar

from ...core.outputs import CancelOrderIntent, PlaceOrderIntent
from ..models import ExecutionState


class VenueSubmission(Protocol):
    @property
    def venue_order_id(self) -> str:
        ...


SubmissionT = TypeVar(
    "SubmissionT",
    bound=VenueSubmission,
)

@dataclass(slots=True)
class ExecutionRecord(Generic[SubmissionT]):
    intent: PlaceOrderIntent

    accepted_quantity: Decimal | None = None
    accepted_price: Decimal | None = None
    venue_submission: SubmissionT | None = None

    state: ExecutionState = ExecutionState.PENDING_SUBMISSION
    cancellation_intent: CancelOrderIntent | None = None

    filled_quantity: Decimal = Decimal("0")
    total_fill_cost: Decimal = Decimal("0")

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class VenueExecutionResult:
    pass


@dataclass
class VenueOrderResult(
    VenueExecutionResult,
    Generic[SubmissionT],
):
    venue_submission: SubmissionT


@dataclass
class VenueOrderAccepted(VenueOrderResult[SubmissionT]):
    accepted_quantity: Decimal
    accepted_price: Decimal | None


@dataclass
class VenueOrderRejected(VenueExecutionResult):
    reason: str


@dataclass
class VenueOrderFill(VenueOrderResult[SubmissionT]):
    fill_quantity: Decimal
    fill_price: Decimal


@dataclass
class VenueOrderFailed(VenueOrderResult[SubmissionT]):
    reason: str


@dataclass
class VenueOrderCancelled(VenueOrderResult[SubmissionT]):
    pass


@dataclass
class VenueCancellationAccepted(VenueExecutionResult):
    pass


@dataclass
class VenueOrderCancellationRejected(VenueExecutionResult):
    reason: str


@dataclass
class VenueOrderCancellationFailed(VenueExecutionResult):
    reason: str


type PlaceOrderResult[SubmissionT: VenueSubmission] = (
    VenueOrderAccepted[SubmissionT]
    | VenueOrderRejected
)


type CancelOrderResult = (
    VenueCancellationAccepted
    | VenueOrderCancellationRejected
    | VenueOrderCancellationFailed
)


type MonitorResult[SubmissionT: VenueSubmission] = (
    VenueOrderFill[SubmissionT]
    | VenueOrderCancelled[SubmissionT]
    | VenueOrderFailed[SubmissionT]
)
