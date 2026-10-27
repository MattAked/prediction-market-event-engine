from typing import Generic, TypeVar, Protocol
from datetime import datetime
from collections.abc import Iterable
from decimal import Decimal

from ...core.events import EventEnvelope
from ...core.outputs import CancelOrderIntent, PlaceOrderIntent, ExecutionIntent
from .models import (
    BookUpdate, CancellationValidation, ExecutionRecord,
    VenueOrderAccepted, VenueOrderRejected, ScheduledExecutionEvent,
)

InstrumentT = TypeVar('InstrumentT')


class VenueAdapter(Generic[InstrumentT]):
    """Subclass contract for offline venue interpretation; no network calls.

    normalise_event may update adapter rules and return None for non-book events.
    Placement validation runs at simulated venue processing time. Backtest
    matching and accounting are executor responsibilities.
    """

    def normalise_event(self, event: EventEnvelope) -> BookUpdate | None:
        raise NotImplementedError

    def instrument_key(self, instrument: InstrumentT) -> str:
        raise NotImplementedError

    def validate_place_order(
        self, instrument: InstrumentT, order: PlaceOrderIntent,
    ) -> VenueOrderAccepted | VenueOrderRejected:
        raise NotImplementedError

    def validate_cancel_order(
        self, instrument: InstrumentT, order: CancelOrderIntent,
        record: ExecutionRecord,
    ) -> CancellationValidation:
        return CancellationValidation()

    def calculate_fee(
        self, instrument: InstrumentT, order: PlaceOrderIntent,
        quantity: Decimal, price: Decimal, *, is_maker: bool,
    ) -> Decimal:
        return Decimal('0')

    @property
    def processing_latency(self) -> float:
        return 0.0


class BacktestExecutorProtocol(Protocol):
    """Engine-facing contract. The engine supplies time and schedules delays."""

    def handle_intent(
        self, intent: ExecutionIntent, timestamp: datetime | None = None,
    ) -> ScheduledExecutionEvent: ...

    def handle_event(
        self, event: EventEnvelope,
    ) -> Iterable[ScheduledExecutionEvent]: ...

    def begin_shutdown(
        self, timestamp: datetime, *, grace_period: float = 2.0,
        cancel_open_orders: bool = True,
    ) -> Iterable[ScheduledExecutionEvent]: ...

    async def finalise(self) -> None: ...
