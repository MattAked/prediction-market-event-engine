from ...core.models.orders import TimeInForce
# executors/protocols.py

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from datetime import datetime
from typing import Generic, TypeVar, Any

from .models import (
    CancelOrderResult,
    MonitorResult,
    PlaceOrderResult,
    SubmissionT,
)

from ...core.events import EventEnvelope


EventT = TypeVar("EventT")


class QueueEventSink(Generic[EventT]):
    def __init__(
            self, 
            instance_id: str,
            queue: asyncio.Queue[EventEnvelope[Any]]
    ) -> None:
        self._instance_id = instance_id
        self._queue = queue
        self._closed = False

    async def emit(self, event: EventT) -> None:
        await self._queue.put(
            EventEnvelope(
                timestamp=datetime.now(),
                payload=event,
                instance_id=self._instance_id,
            )
        )

    def close(self) -> None:
        self._closed = True


InstrumentT = TypeVar("InstrumentT")


class VenueAdapter(Generic[InstrumentT, SubmissionT]):
    async def place_market_buy(
        self,
        instrument: InstrumentT,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[SubmissionT]:
        ...

    async def place_market_sell(
        self,
        instrument: InstrumentT,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[SubmissionT]:
        ...

    async def place_limit_buy(
        self,
        instrument: InstrumentT,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[SubmissionT]:
        ...

    async def place_limit_sell(
        self,
        instrument: InstrumentT,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[SubmissionT]:
        ...

    async def cancel_order(
        self,
        submission: SubmissionT,
    ) -> CancelOrderResult:
        ...

    def monitor_order(
        self,
        submission: SubmissionT,
    ) -> AsyncIterator[MonitorResult[SubmissionT]]:
        ...
