# executors/live_executor.py

import asyncio
from decimal import Decimal
from collections.abc import Coroutine, Mapping
from typing import Any, Generic

from ...core.events.execution import (
    ExecutionEvent,
    OrderCancellationFailed,
    OrderCancellationRejected,
    OrderCancelled,
    OrderFailed,
    OrderFilled,
    OrderPartiallyFilled,
    OrderSubmissionFailed,
    OrderSubmissionRejected,
    OrderSubmitted,
)
from ...core.models.orders import OrderSide
from ...core.outputs.execution import (
    CancelOrderIntent,
    ExecutionIntent,
    PlaceOrderIntent,
)
from .models import (
    ExecutionRecord,
    ExecutionState,
    SubmissionT,
    VenueCancellationAccepted,
    VenueOrderAccepted,
    VenueOrderCancellationFailed,
    VenueOrderCancellationRejected,
    VenueOrderCancelled,
    VenueOrderFailed,
    VenueOrderFill,
    VenueOrderRejected,
)
from .protocols import InstrumentT, QueueEventSink, VenueAdapter


class LiveExecutor(Generic[InstrumentT, SubmissionT]):
    def __init__(
        self,
        adapter: VenueAdapter[InstrumentT, SubmissionT],
        instruments: Mapping[str, InstrumentT],
    ) -> None:
        self.adapter = adapter
        self.instruments = instruments

        self.tasks: set[asyncio.Task[Any]] = set()
        self.execution_records: dict[str, ExecutionRecord[SubmissionT]] = {}

        self._event_sink: QueueEventSink[ExecutionEvent] | None = None
        self._shutting_down = False
        self._used_place_intent_ids: set[str] = set()

    def set_event_sink(
        self,
        event_sink: QueueEventSink[ExecutionEvent],
    ) -> None:
        if self._event_sink is not None:
            raise RuntimeError("Event sink has already been configured")

        self._event_sink = event_sink

    async def _emit(self, event: ExecutionEvent) -> None:
        if self._event_sink is None:
            raise RuntimeError("Event sink has not been configured")

        await self._event_sink.emit(event)

    def _create_task(
        self,
        coroutine: Coroutine[Any, Any, Any],
    ) -> None:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def handle_intent(self, intent: ExecutionIntent) -> None:
        if self._shutting_down:
            raise RuntimeError("Executor is shutting down")

        match intent:
            case PlaceOrderIntent():
                self._create_task(self.handle_place_order(intent))

            case CancelOrderIntent():
                self._create_task(self.handle_cancel_order(intent))

    async def handle_place_order(self, order: PlaceOrderIntent) -> None:
        # No await before reserving the ID: concurrently queued placements cannot
        # replace each other's records, including IDs of completed orders.
        if order.intent_id in self._used_place_intent_ids or order.intent_id in self.execution_records:
            await self._emit(OrderSubmissionRejected(
                intent_id=order.intent_id,
                instrument_id=order.instrument_id,
                reason="Duplicate placement intent ID",
            ))
            return
        self._used_place_intent_ids.add(order.intent_id)

        venue_instrument = self.instruments.get(order.instrument_id)
        if venue_instrument is None:
            raise RuntimeError(
                "Venue instrument not found so unable to place order"
            )

        record = ExecutionRecord[SubmissionT](intent=order)
        self.execution_records[order.intent_id] = record

        try:
            if order.side == OrderSide.BUY:
                if order.limit_price is not None:
                    result = await self.adapter.place_limit_buy(
                        venue_instrument,
                        order.quantity,
                        order.limit_price,
                        order.limit_expiry,
                        time_in_force=order.time_in_force,
                    )
                else:
                    result = await self.adapter.place_market_buy(
                        venue_instrument,
                        order.quantity,
                        time_in_force=order.time_in_force,
                    )
            else:
                if order.limit_price is not None:
                    result = await self.adapter.place_limit_sell(
                        venue_instrument,
                        order.quantity,
                        order.limit_price,
                        order.limit_expiry,
                        time_in_force=order.time_in_force,
                    )
                else:
                    result = await self.adapter.place_market_sell(
                        venue_instrument,
                        order.quantity,
                        time_in_force=order.time_in_force,
                    )

        except Exception as exc:
            async with record.lock:
                record.state = ExecutionState.FAILED
                cancellation_intent = record.cancellation_intent

            await self._emit(
                OrderSubmissionFailed(
                    intent_id=order.intent_id,
                    instrument_id=order.instrument_id,
                    reason=str(exc),
                )
            )

            if cancellation_intent is not None:
                await self._emit(
                    OrderCancellationRejected(
                        intent_id=cancellation_intent.intent_id,
                        target_intent_id=cancellation_intent.target_intent_id,
                        instrument_id=order.instrument_id,
                        reason="Target order failed before venue acceptance",
                    )
                )

            self.execution_records.pop(order.intent_id, None)
            return

        if isinstance(result, VenueOrderRejected):
            async with record.lock:
                record.state = ExecutionState.REJECTED
                cancellation_intent = record.cancellation_intent

            await self._emit(
                OrderSubmissionRejected(
                    intent_id=order.intent_id,
                    instrument_id=order.instrument_id,
                    reason=result.reason,
                )
            )

            if cancellation_intent is not None:
                await self._emit(
                    OrderCancellationRejected(
                        intent_id=cancellation_intent.intent_id,
                        target_intent_id=cancellation_intent.target_intent_id,
                        instrument_id=order.instrument_id,
                        reason="Target order was rejected before venue acceptance",
                    )
                )

            self.execution_records.pop(order.intent_id, None)
            return

        if not isinstance(result, VenueOrderAccepted):
            record.state = ExecutionState.FAILED
            await self._emit(OrderSubmissionFailed(
                intent_id=order.intent_id,
                instrument_id=order.instrument_id,
                reason=f"Unexpected placement result: {type(result).__name__}",
            ))
            if record.cancellation_intent is not None:
                cancel = record.cancellation_intent
                await self._emit(OrderCancellationRejected(
                    intent_id=cancel.intent_id, target_intent_id=cancel.target_intent_id,
                    instrument_id=order.instrument_id,
                    reason="Target order failed before venue acceptance",
                ))
            self.execution_records.pop(order.intent_id, None)
            return

        async with record.lock:
            record.accepted_quantity = result.accepted_quantity
            record.accepted_price = result.accepted_price
            record.venue_submission = result.venue_submission

            cancellation_intent = record.cancellation_intent
            shutdown_cancellation = (
                self._shutting_down and cancellation_intent is None
            )

            if cancellation_intent is not None or shutdown_cancellation:
                record.state = ExecutionState.CANCELLING
            else:
                record.state = ExecutionState.SUBMITTED

        await self._emit(
            OrderSubmitted(
                intent_id=order.intent_id,
                venue_order_id=result.venue_submission.venue_order_id,
                instrument_id=order.instrument_id,
                side=order.side,
                accepted_quantity=result.accepted_quantity,
                accepted_price=result.accepted_price,
            )
        )

        if cancellation_intent is not None:
            self._create_task(
                self._cancel_record(
                    record,
                    cancellation_intent,
                )
            )
        elif shutdown_cancellation:
            self._create_task(self._cancel_for_shutdown(record))

        try:
            async for event in self.adapter.monitor_order(
                result.venue_submission
            ):
                event_type: str | None = None
                terminal = False

                fill_quantity = None
                fill_price = None
                filled_quantity = None
                remaining_quantity = None
                average_fill_price = None
                terminal_reason = None
                cancellation_event_id = None
                venue_order_id = event.venue_submission.venue_order_id

                async with record.lock:
                    accepted_quantity = record.accepted_quantity
                    if accepted_quantity is None:
                        raise RuntimeError(
                            "Accepted quantity is unavailable for submitted order"
                        )

                    if isinstance(event, VenueOrderFill):
                        if not event.fill_quantity.is_finite() or event.fill_quantity <= 0:
                            raise ValueError("Invalid venue fill quantity")
                        if not event.fill_price.is_finite() or event.fill_price < 0:
                            raise ValueError("Invalid venue fill price")
                        if event.fill_quantity > accepted_quantity - record.filled_quantity:
                            raise ValueError("Venue overfill exceeds remaining quantity")
                        record.filled_quantity += event.fill_quantity
                        record.total_fill_cost += (
                            event.fill_quantity * event.fill_price
                        )

                        remaining_quantity = (
                            accepted_quantity - record.filled_quantity
                        )

                        if remaining_quantity == 0:
                            record.state = ExecutionState.FILLED
                            terminal = True
                            event_type = "filled"
                            fill_quantity = event.fill_quantity
                            fill_price = event.fill_price
                            average_fill_price = (
                                record.total_fill_cost
                                / record.filled_quantity
                            )
                        else:
                            event_type = "partial_fill"
                            fill_quantity = event.fill_quantity
                            fill_price = event.fill_price

                    elif isinstance(event, VenueOrderCancelled):
                        record.state = ExecutionState.CANCELLED
                        terminal = True
                        event_type = "cancelled"

                        filled_quantity = record.filled_quantity
                        remaining_quantity = (
                            accepted_quantity - record.filled_quantity
                        )

                        cancellation_event_id = order.intent_id

                    elif isinstance(event, VenueOrderFailed):
                        record.state = ExecutionState.FAILED
                        terminal = True
                        event_type = "failed"

                        filled_quantity = record.filled_quantity
                        remaining_quantity = (
                            accepted_quantity - record.filled_quantity
                        )
                        terminal_reason = event.reason

                if event_type == "filled":
                    assert fill_quantity is not None
                    assert fill_price is not None
                    assert average_fill_price is not None

                    await self._emit(
                        OrderFilled(
                            intent_id=order.intent_id,
                            venue_order_id=venue_order_id,
                            instrument_id=order.instrument_id,
                            fill_quantity=fill_quantity,
                            fill_price=fill_price,
                            avg_fill_price=average_fill_price,
                        )
                    )

                elif event_type == "partial_fill":
                    assert fill_quantity is not None
                    assert fill_price is not None
                    assert remaining_quantity is not None

                    await self._emit(
                        OrderPartiallyFilled(
                            intent_id=order.intent_id,
                            venue_order_id=venue_order_id,
                            instrument_id=order.instrument_id,
                            fill_quantity=fill_quantity,
                            fill_price=fill_price,
                            remaining_quantity=remaining_quantity,
                        )
                    )

                elif event_type == "cancelled":
                    assert cancellation_event_id is not None
                    assert filled_quantity is not None
                    assert remaining_quantity is not None

                    await self._emit(
                        OrderCancelled(
                            intent_id=cancellation_event_id,
                            venue_order_id=venue_order_id,
                            instrument_id=order.instrument_id,
                            filled_quantity=filled_quantity,
                            remaining_quantity=remaining_quantity,
                        )
                    )

                elif event_type == "failed":
                    assert filled_quantity is not None
                    assert remaining_quantity is not None
                    assert terminal_reason is not None

                    await self._emit(
                        OrderFailed(
                            intent_id=order.intent_id,
                            venue_order_id=venue_order_id,
                            instrument_id=order.instrument_id,
                            filled_quantity=filled_quantity,
                            remaining_quantity=remaining_quantity,
                            reason=terminal_reason,
                        )
                    )

                if terminal:
                    self.execution_records.pop(order.intent_id, None)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._fail_monitor(record, f"Order monitor failed: {exc}")
            return

        await self._fail_monitor(record, "Order monitor ended without a terminal result")

    async def _fail_monitor(self, record: ExecutionRecord[SubmissionT], reason: str) -> None:
        """Report local uncertainty without inventing or clamping venue fills."""
        async with record.lock:
            record.state = ExecutionState.FAILED
            accepted = record.accepted_quantity or record.intent.quantity
            remaining = max(accepted - record.filled_quantity, Decimal("0"))
            submission = record.venue_submission
            if submission is None:
                raise RuntimeError("Monitor failure has no venue submission")
        try:
            await self._emit(OrderFailed(
                intent_id=record.intent.intent_id,
                venue_order_id=submission.venue_order_id,
                instrument_id=record.intent.instrument_id,
                filled_quantity=record.filled_quantity,
                remaining_quantity=remaining,
                reason=reason,
            ))
        finally:
            self.execution_records.pop(record.intent.intent_id, None)

    async def handle_cancel_order(
        self,
        order: CancelOrderIntent,
    ) -> None:
        record = self.execution_records.get(order.target_intent_id)

        if record is None:
            await self._emit(
                OrderCancellationRejected(
                    intent_id=order.intent_id,
                    target_intent_id=order.target_intent_id,
                    reason="Target order not found",
                )
            )
            return

        rejection_reason: str | None = None
        should_cancel = False

        async with record.lock:
            if record.cancellation_intent is not None:
                rejection_reason = (
                    "Target order already has a cancellation request"
                )

            elif record.state == ExecutionState.PENDING_SUBMISSION:
                record.cancellation_intent = order
                record.state = ExecutionState.CANCELLATION_PENDING

            elif record.state in {
                ExecutionState.CANCELLED,
                ExecutionState.FILLED,
                ExecutionState.REJECTED,
                ExecutionState.FAILED,
            }:
                rejection_reason = (
                    f"Target order is already {record.state.value}"
                )

            else:
                if record.venue_submission is None:
                    raise RuntimeError(
                        "Submitted order has no venue submission"
                    )

                record.cancellation_intent = order
                record.state = ExecutionState.CANCELLING
                should_cancel = True

        if rejection_reason is not None:
            await self._emit(
                OrderCancellationRejected(
                    intent_id=order.intent_id,
                    target_intent_id=order.target_intent_id,
                    instrument_id=record.intent.instrument_id,
                    reason=rejection_reason,
                )
            )
            return

        if should_cancel:
            await self._cancel_record(record, order)

    async def _cancel_record(
        self,
        record: ExecutionRecord[SubmissionT],
        cancel_intent: CancelOrderIntent,
    ) -> None:
        submission = record.venue_submission

        if submission is None:
            raise RuntimeError(
                "Cannot cancel order before venue acceptance"
            )

        try:
            result = await self.adapter.cancel_order(submission)
        except Exception as exc:
            async with record.lock:
                if record.state == ExecutionState.CANCELLING:
                    record.state = ExecutionState.SUBMITTED

                if record.cancellation_intent is cancel_intent:
                    record.cancellation_intent = None

            await self._emit(
                OrderCancellationFailed(
                    intent_id=cancel_intent.intent_id,
                    target_intent_id=cancel_intent.target_intent_id,
                    instrument_id=record.intent.instrument_id,
                    reason=str(exc),
                )
            )
            return

        if isinstance(result, VenueCancellationAccepted):
            return

        if isinstance(result, VenueOrderCancellationRejected):
            async with record.lock:
                if record.state == ExecutionState.CANCELLING:
                    record.state = ExecutionState.SUBMITTED

                if record.cancellation_intent is cancel_intent:
                    record.cancellation_intent = None

            await self._emit(
                OrderCancellationRejected(
                    intent_id=cancel_intent.intent_id,
                    target_intent_id=cancel_intent.target_intent_id,
                    instrument_id=record.intent.instrument_id,
                    reason=result.reason,
                )
            )
            return

        if isinstance(result, VenueOrderCancellationFailed):
            async with record.lock:
                if record.state == ExecutionState.CANCELLING:
                    record.state = ExecutionState.SUBMITTED

                if record.cancellation_intent is cancel_intent:
                    record.cancellation_intent = None

            await self._emit(
                OrderCancellationFailed(
                    intent_id=cancel_intent.intent_id,
                    target_intent_id=cancel_intent.target_intent_id,
                    instrument_id=record.intent.instrument_id,
                    reason=result.reason,
                )
            )

    async def _cancel_for_shutdown(
        self,
        record: ExecutionRecord[SubmissionT],
    ) -> None:
        submission = record.venue_submission
        if submission is None:
            return

        try:
            await self.adapter.cancel_order(submission)
        except Exception:
            # Shutdown cancellation is best-effort. The executor is going away,
            # so there is no useful recovery action here.
            return

    async def shutdown(
        self,
        grace_period: float = 2.0,
        cancel_open_orders: bool = True
    ) -> None:
        if self._shutting_down:
            return

        self._shutting_down = True

        records = list(self.execution_records.values())
        if cancel_open_orders:
            for record in records:
                should_cancel = False

                async with record.lock:
                    if (
                        record.venue_submission is not None
                        and record.state == ExecutionState.SUBMITTED
                        and record.cancellation_intent is None
                    ):
                        record.state = ExecutionState.CANCELLING
                        should_cancel = True

                if should_cancel:
                    self._create_task(self._cancel_for_shutdown(record))

        active_tasks = list(self.tasks)
        if active_tasks:
            _, pending = await asyncio.wait(
                active_tasks,
                timeout=grace_period,
            )

            # Include tasks that may have been created while waiting, such as a
            # shutdown cancellation spawned after an in-flight order is accepted.
            pending = set(pending) | {
                task for task in self.tasks if not task.done()
            }

            for task in pending:
                task.cancel()

            if pending:
                await asyncio.gather(
                    *pending,
                    return_exceptions=True,
                )

        self.tasks.clear()
