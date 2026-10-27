from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from math import isfinite
from typing import Generic, Mapping

from ...core.events import EventEnvelope
from ...core.events.execution import (
    ExecutionEvent, OrderCancelled, OrderCancellationRejected, OrderFilled,
    OrderPartiallyFilled, OrderSubmitted, OrderSubmissionRejected,
)
from ...core.models.orders import OrderSide, TimeInForce
from ...core.outputs import ExecutionIntent, PlaceOrderIntent, CancelOrderIntent
from ..models import ExecutionState
from .models import (
    BookUpdate, ExecutionIntentArrived, ExecutionRecord, ExecutionSummary,
    FillRecord, OrderExpiryDue, ScheduledExecutionEvent, VenueOrderAccepted,
    VenueOrderRejected, ZERO,
)
from .order_book import OrderBook
from .protocols import InstrumentT, VenueAdapter


class BacktestExecutor(Generic[InstrumentT]):
    """Serial venue simulation. State changes at venue time; notifications are delayed.

    No real-time sleeps or network calls. Unexpected adapter/data errors propagate
    to the backtest runner rather than masquerading as simulated venue failures.
    """

    def __init__(
        self, venue_latency: float, adapter: VenueAdapter[InstrumentT],
        instruments: Mapping[str, InstrumentT],
    ) -> None:
        self._check_latency(venue_latency)
        self.venue_latency = venue_latency
        self.adapter = adapter
        self.instruments = dict(instruments)
        self.instrument_ids_by_key: dict[str, str] = {}
        for internal_id, instrument in self.instruments.items():
            key = adapter.instrument_key(instrument)
            if key in self.instrument_ids_by_key:
                raise ValueError(f'Duplicate venue instrument key: {key}')
            self.instrument_ids_by_key[key] = internal_id
        self.books = {instrument_id: OrderBook() for instrument_id in self.instruments}
        self.execution_records: dict[str, ExecutionRecord] = {}
        self._requests: dict[str, ExecutionIntent] = {}
        self._processed_requests: set[str] = set()
        self._current_time: datetime | None = None
        self._closed = False
        self._closing = False
        self._cancel_on_shutdown = False
        self._shutdown_sequence = 0
        self._shutdown_started_at: datetime | None = None
        self._cleanup_deadline: datetime | None = None
        self._finalised_at: datetime | None = None
        self.fills: list[FillRecord] = []
        self.positions: dict[str, Decimal] = {key: ZERO for key in self.instruments}
        self.cash_flows: dict[str, Decimal] = {key: ZERO for key in self.instruments}

    @staticmethod
    def _check_latency(value: float) -> None:
        if not isfinite(value) or value < 0:
            raise ValueError('Latency must be finite and nonnegative')

    @property
    def outbound_latency(self) -> float:
        return self.venue_latency

    @property
    def inbound_latency(self) -> float:
        return self.venue_latency

    def _advance_time(self, timestamp: datetime) -> None:
        if self._current_time is not None and timestamp < self._current_time:
            raise ValueError('Executor events must be timestamp ordered')
        self._current_time = timestamp

    def handle_intent(
        self, intent: ExecutionIntent, timestamp: datetime | None = None,
    ) -> ScheduledExecutionEvent:
        if self._closed or self._closing:
            raise RuntimeError('Executor is shutting down or closed')
        return self._schedule_intent(intent, timestamp)

    def _schedule_intent(
        self, intent: ExecutionIntent, timestamp: datetime | None,
    ) -> ScheduledExecutionEvent:
        if not isinstance(intent, (PlaceOrderIntent, CancelOrderIntent)):
            raise TypeError('Unknown execution intent')
        if intent.intent_id in self._requests:
            raise ValueError(f'Duplicate intent ID: {intent.intent_id}')
        if timestamp is None:
            timestamp = self._current_time
        if timestamp is None:
            raise RuntimeError('Provide a timestamp before the first replay event')
        self._advance_time(timestamp)
        processing_latency = self.adapter.processing_latency
        self._check_latency(processing_latency)
        delay = self.outbound_latency + processing_latency
        self._requests[intent.intent_id] = intent
        if isinstance(intent, PlaceOrderIntent):
            self.execution_records[intent.intent_id] = ExecutionRecord(intent, timestamp)
        # A pending cancel does not deactivate its target. It may fill in flight.
        return ScheduledExecutionEvent(ExecutionIntentArrived(intent), delay)

    def handle_event(self, event: EventEnvelope) -> list[ScheduledExecutionEvent]:
        if self._closed:
            raise RuntimeError('Executor is shut down')
        self._advance_time(event.timestamp)
        payload = event.payload
        if isinstance(payload, ExecutionIntentArrived):
            return self.handle_intent_arrived(payload, event.timestamp)
        if isinstance(payload, OrderExpiryDue):
            return self._expire_order(payload.intent_id, event.timestamp)
        if isinstance(payload, ExecutionEvent):
            # These are notifications of transitions already applied at venue time.
            return []
        update = self.adapter.normalise_event(event)
        if update is None:
            return []
        if not isinstance(update, BookUpdate):
            raise TypeError('Adapter must return BookUpdate or None')
        instrument_id = self.apply_book_update(update)
        if instrument_id is None:
            return []
        return self._match_open_orders(instrument_id, event.timestamp)

    def handle_intent_arrived(
        self, event: ExecutionIntentArrived, timestamp: datetime,
    ) -> list[ScheduledExecutionEvent]:
        self._advance_time(timestamp)
        intent = event.intent
        if self._requests.get(intent.intent_id) != intent:
            raise ValueError('Arrival does not correspond to a registered request')
        if intent.intent_id in self._processed_requests:
            return []
        self._processed_requests.add(intent.intent_id)
        if isinstance(intent, PlaceOrderIntent):
            return self._submit_order(intent, timestamp)
        return self._cancel_order(intent, timestamp)

    def _notify(self, event: ExecutionEvent) -> ScheduledExecutionEvent:
        return ScheduledExecutionEvent(event, self.inbound_latency)

    def _reject_order(self, record: ExecutionRecord, reason: str) -> list[ScheduledExecutionEvent]:
        record.state = ExecutionState.REJECTED
        record.rejection_reason = reason
        return [self._notify(OrderSubmissionRejected(
            record.intent.intent_id, record.intent.instrument_id, reason,
        ))]

    def _submit_order(self, order: PlaceOrderIntent, timestamp: datetime) -> list[ScheduledExecutionEvent]:
        record = self.execution_records[order.intent_id]
        if record.state is not ExecutionState.PENDING_SUBMISSION:
            return []
        if order.instrument_id not in self.instruments:
            return self._reject_order(record, 'Unknown instrument')
        if not isfinite(order.limit_expiry) or order.limit_expiry < 0:
            return self._reject_order(record, 'Invalid expiry')
        if not order.quantity.is_finite() or order.quantity <= ZERO:
            return self._reject_order(record, 'Invalid quantity')
        result = self.adapter.validate_place_order(self.instruments[order.instrument_id], order)
        if isinstance(result, VenueOrderRejected):
            return self._reject_order(record, result.reason)
        if not isinstance(result, VenueOrderAccepted):
            raise TypeError('Invalid adapter placement result')
        # v1 does not silently amend requested quantities or prices.
        if result.accepted_quantity != order.quantity or result.accepted_price != order.limit_price:
            raise ValueError('Adapter must accept the requested quantity and limit price unchanged')
        if not result.venue_order_id or any(
            r.venue_order_id == result.venue_order_id for r in self.execution_records.values()
        ):
            raise ValueError('Adapter returned an empty or duplicate venue order ID')
        book = self.books[order.instrument_id]
        if order.time_in_force is not TimeInForce.GTC and not book.initialised:
            return self._reject_order(record, 'No initial book snapshot')
        record.accepted_quantity = result.accepted_quantity
        record.accepted_price = result.accepted_price
        record.venue_order_id = result.venue_order_id
        record.accepted_at = timestamp
        record.state = ExecutionState.SUBMITTED
        events = [self._notify(OrderSubmitted(
            order.intent_id, result.venue_order_id, order.instrument_id,
            order.side, result.accepted_quantity, result.accepted_price,
        ))]
        # v1 expiry is seconds from acceptance; zero means no expiry.
        if order.limit_expiry > 0:
            record.expires_at = timestamp + timedelta(seconds=order.limit_expiry)
            events.append(ScheduledExecutionEvent(OrderExpiryDue(order.intent_id), order.limit_expiry))
        events.extend(self._match_order(record, timestamp, is_maker=False))
        # An in-flight placement may become accepted during cleanup. Once its
        # acceptance is known, request cancellation using normal venue latency.
        if self._closing and self._cancel_on_shutdown and record.state is ExecutionState.SUBMITTED:
            events.extend(self._request_shutdown_cancel(record, timestamp))
        return events

    def _cancel_order(self, intent: CancelOrderIntent, timestamp: datetime) -> list[ScheduledExecutionEvent]:
        record = self.execution_records.get(intent.target_intent_id)
        reason = None
        if record is None:
            reason = 'Unknown target order'
        elif record.state is not ExecutionState.SUBMITTED:
            reason = f'Target order is not active: {record.state.value}'
        else:
            validation = self.adapter.validate_cancel_order(
                self.instruments[record.intent.instrument_id], intent, record,
            )
            reason = validation.rejection_reason
        if reason is not None:
            return [self._notify(OrderCancellationRejected(
                intent.intent_id, intent.target_intent_id, reason,
                record.intent.instrument_id if record else None,
            ))]
        assert record is not None
        return [self._cancel_record(record, timestamp)]

    def _cancel_record(self, record: ExecutionRecord, timestamp: datetime) -> ScheduledExecutionEvent:
        record.state = ExecutionState.CANCELLED
        record.cancelled_at = timestamp
        assert record.venue_order_id is not None
        return self._notify(OrderCancelled(
            record.intent.intent_id, record.venue_order_id, record.intent.instrument_id,
            record.filled_quantity, record.remaining_quantity,
        ))

    def _expire_order(self, intent_id: str, timestamp: datetime) -> list[ScheduledExecutionEvent]:
        record = self.execution_records.get(intent_id)
        if record is not None and record.state is ExecutionState.SUBMITTED:
            if record.expires_at is not None and timestamp >= record.expires_at:
                return [self._cancel_record(record, timestamp)]
        return []

    def apply_book_update(self, update: BookUpdate) -> str | None:
        instrument_id = self.instrument_ids_by_key.get(update.instrument_key)
        if instrument_id is not None:
            self.books[instrument_id].apply(update)
        return instrument_id

    def _match_open_orders(self, instrument_id: str, timestamp: datetime) -> list[ScheduledExecutionEvent]:
        records = [r for r in self.execution_records.values()
                   if r.intent.instrument_id == instrument_id and r.state is ExecutionState.SUBMITTED]
        # Opposite sides consume separate depth. Within each side: price, then
        # acceptance time; dict insertion order resolves equal-time ties.
        def priority(record: ExecutionRecord):
            price = record.accepted_price or ZERO
            return (record.intent.side.value, -price if record.intent.side is OrderSide.BUY else price,
                    record.accepted_at)
        events = []
        for record in sorted(records, key=priority):
            if record.expires_at is not None and timestamp >= record.expires_at:
                events.append(self._cancel_record(record, timestamp))
            else:
                events.extend(self._match_order(record, timestamp, is_maker=True))
        return events

    def _match_order(
        self, record: ExecutionRecord, timestamp: datetime, *, is_maker: bool,
    ) -> list[ScheduledExecutionEvent]:
        order = record.intent
        book = self.books[order.instrument_id]
        plan = book.plan(order.side, record.remaining_quantity, record.accepted_price) if book.initialised else []
        if order.time_in_force is TimeInForce.FOK:
            if sum((quantity for _, quantity in plan), ZERO) != record.remaining_quantity:
                # FOK failure is accepted then cancelled, without consuming depth.
                return [self._cancel_record(record, timestamp)]
        # Compute and validate all fees before committing any fill in this plan.
        priced_plan = []
        for price, quantity in plan:
            fee = self.adapter.calculate_fee(
                self.instruments[order.instrument_id], order, quantity, price, is_maker=is_maker,
            )
            if not fee.is_finite():
                raise ValueError('Adapter returned a nonfinite fee')
            priced_plan.append((price, quantity, fee))
        events = []
        assert record.venue_order_id is not None
        for price, quantity, fee in priced_plan:
            book.consume(order.side, price, quantity)
            record.filled_quantity += quantity
            record.total_fill_cost += quantity * price
            record.fees += fee
            sign = Decimal('1') if order.side is OrderSide.BUY else Decimal('-1')
            self.positions[order.instrument_id] += sign * quantity
            self.cash_flows[order.instrument_id] -= sign * quantity * price + fee
            self.fills.append(FillRecord(
                order.intent_id, record.venue_order_id, order.instrument_id, order.side,
                quantity, price, fee, timestamp, timestamp + timedelta(seconds=self.inbound_latency),
            ))
            if record.remaining_quantity == ZERO:
                record.state = ExecutionState.FILLED
                event = OrderFilled(order.intent_id, record.venue_order_id, order.instrument_id, quantity, price, record.total_fill_cost / record.filled_quantity)
            else:
                event = OrderPartiallyFilled(
                    order.intent_id, record.venue_order_id, order.instrument_id,
                    quantity, price, record.remaining_quantity,
                )
            events.append(self._notify(event))
        if order.time_in_force is TimeInForce.IOC and record.state is ExecutionState.SUBMITTED:
            events.append(self._cancel_record(record, timestamp))
        return events

    def get_summary(self) -> ExecutionSummary:
        return ExecutionSummary(
            tuple(self.fills), dict(self.positions), dict(self.cash_flows),
            sum((fill.fee for fill in self.fills), ZERO),
            tuple(key for key, r in self.execution_records.items()
                  if r.state in (ExecutionState.SUBMITTED, ExecutionState.PENDING_SUBMISSION)),
            tuple(key for key in self._requests if key not in self._processed_requests),
            self._shutdown_started_at, self._cleanup_deadline, self._finalised_at,
        )

    def _request_shutdown_cancel(
        self, record: ExecutionRecord, timestamp: datetime,
    ) -> list[ScheduledExecutionEvent]:
        # Preserve an already in-flight explicit cancellation.
        if any(isinstance(request, CancelOrderIntent)
               and request.target_intent_id == record.intent.intent_id
               and key not in self._processed_requests
               for key, request in self._requests.items()):
            return []
        while True:
            self._shutdown_sequence += 1
            request_id = f'__shutdown_cancel_{self._shutdown_sequence}'
            if request_id not in self._requests:
                break
        return [self._schedule_intent(
            CancelOrderIntent(request_id, record.intent.intent_id), timestamp,
        )]

    def begin_shutdown(
        self, timestamp: datetime, *, grace_period: float = 2.0,
        cancel_open_orders: bool = True,
    ) -> list[ScheduledExecutionEvent]:
        """Request venue cancellations; engine replays returned work during grace."""
        if self._closed:
            raise RuntimeError('Executor is closed')
        if self._closing:
            return []
        self._check_latency(grace_period)
        self._advance_time(timestamp)
        self._closing = True
        self._cancel_on_shutdown = cancel_open_orders
        self._shutdown_started_at = timestamp
        self._cleanup_deadline = timestamp + timedelta(seconds=grace_period)
        events = []
        if cancel_open_orders:
            for record in self.execution_records.values():
                if record.state is ExecutionState.SUBMITTED:
                    events.extend(self._request_shutdown_cancel(record, timestamp))
        return events

    async def finalise(self) -> None:
        """Close local resources without inventing fills or cancellation outcomes.

        Safe on normal exhaustion, code failure and task cancellation alike.
        Remaining records/requests stay inspectable as unresolved simulation state.
        """
        if self._closed:
            return
        self._finalised_at = self._current_time
        self._closed = True
