from __future__ import annotations
from PredictionMarketEventSniper.core.models.orders import TimeInForce

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.execution import (
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
from PredictionMarketEventSniper.core.models.orders import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from PredictionMarketEventSniper.core.outputs.execution import (
    CancelOrderIntent,
    PlaceOrderIntent,
)
from PredictionMarketEventSniper.executors.live.live_executor import LiveExecutor
from PredictionMarketEventSniper.executors.live.models import (
    CancelOrderResult,
    ExecutionRecord,
    MonitorResult,
    PlaceOrderResult,
    VenueCancellationAccepted,
    VenueOrderAccepted,
    VenueOrderCancellationFailed,
    VenueOrderCancellationRejected,
    VenueOrderCancelled,
    VenueOrderFailed,
    VenueOrderFill,
    VenueOrderRejected,
)
from PredictionMarketEventSniper.executors.models import ExecutionState
from PredictionMarketEventSniper.executors.live.protocols import (
    QueueEventSink,
    VenueAdapter,
)


@dataclass(frozen=True, slots=True)
class FakeSubmission:
    venue_order_id: str


class FakeAdapter(VenueAdapter[str, FakeSubmission]):
    """Deterministic in-memory venue for LiveExecutor unit tests.

    Tests control placement acknowledgement, fills, cancellation acknowledgement,
    rejection/failure results, and adapter exceptions independently. This lets us
    reproduce lifecycle races without sleeps or real sockets.
    """

    def __init__(
        self,
        fills: list[tuple[Decimal, Decimal]] | None = None,
        *,
        pause_after_fills: int | None = None,
        cancel_after_fills: bool = False,
        pause_on_placement: bool = False,
        pause_on_cancel: bool = False,
        placement_rejection_reason: str | None = None,
        placement_exception: Exception | None = None,
        cancel_result: CancelOrderResult | None = None,
        cancel_exception: Exception | None = None,
        monitor_failure_reason: str | None = None,
        unexpected_place_result: object | None = None,
        venue_order_id: str = "venue-order-1",
    ) -> None:
        self.fills = fills or []
        self.pause_after_fills = pause_after_fills
        self.cancel_after_fills = cancel_after_fills
        self.pause_on_placement = pause_on_placement
        self.pause_on_cancel = pause_on_cancel
        self.placement_rejection_reason = placement_rejection_reason
        self.placement_exception = placement_exception
        self.cancel_result = cancel_result
        self.cancel_exception = cancel_exception
        self.monitor_failure_reason = monitor_failure_reason
        self.unexpected_place_result = unexpected_place_result
        self.venue_order_id = venue_order_id

        self.resume_fills = asyncio.Event()
        self.resume_placement = asyncio.Event()
        self.resume_cancel = asyncio.Event()

        self.placement_started = asyncio.Event()
        self.cancel_started = asyncio.Event()
        self.cancel_accepted = asyncio.Event()

        self.cancelled = False
        self.cancel_call_count = 0
        self.place_calls: list[
            tuple[str, str, Decimal, Decimal | None, float | None]
        ] = []

    async def _place(
        self,
        method: str,
        instrument: str,
        quantity: Decimal,
        price: Decimal | None,
        expiry: float | None,
    ) -> PlaceOrderResult[FakeSubmission]:
        self.place_calls.append(
            (method, instrument, quantity, price, expiry)
        )
        self.cancelled = False
        self.placement_started.set()

        if self.pause_on_placement:
            await self.resume_placement.wait()

        if self.placement_exception is not None:
            raise self.placement_exception

        if self.unexpected_place_result is not None:
            return self.unexpected_place_result  # type: ignore[return-value]

        if self.placement_rejection_reason is not None:
            return VenueOrderRejected(
                reason=self.placement_rejection_reason
            )

        return VenueOrderAccepted(
            venue_submission=FakeSubmission(
                venue_order_id=self.venue_order_id
            ),
            accepted_quantity=quantity,
            accepted_price=price,
        )

    async def place_limit_buy(
        self,
        instrument: str,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[FakeSubmission]:
        return await self._place(
            "place_limit_buy",
            instrument,
            quantity,
            price,
            expiry,
        )

    async def place_limit_sell(
        self,
        instrument: str,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[FakeSubmission]:
        return await self._place(
            "place_limit_sell",
            instrument,
            quantity,
            price,
            expiry,
        )

    async def place_market_buy(
        self,
        instrument: str,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[FakeSubmission]:
        return await self._place(
            "place_market_buy",
            instrument,
            quantity,
            None,
            None,
        )

    async def place_market_sell(
        self,
        instrument: str,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[FakeSubmission]:
        return await self._place(
            "place_market_sell",
            instrument,
            quantity,
            None,
            None,
        )

    async def monitor_order(
        self,
        submission: FakeSubmission,
    ) -> AsyncIterator[MonitorResult[FakeSubmission]]:
        # 0 means stop before the first fill.
        if self.pause_after_fills == 0:
            await self.resume_fills.wait()

            if self.cancelled:
                yield VenueOrderCancelled(
                    venue_submission=submission
                )
                return

        for fill_count, (quantity, price) in enumerate(
            self.fills,
            start=1,
        ):
            if self.cancelled:
                yield VenueOrderCancelled(
                    venue_submission=submission
                )
                return

            yield VenueOrderFill(
                venue_submission=submission,
                fill_quantity=quantity,
                fill_price=price,
            )

            if self.pause_after_fills == fill_count:
                await self.resume_fills.wait()

                if self.cancelled:
                    yield VenueOrderCancelled(
                        venue_submission=submission
                    )
                    return

        if self.monitor_failure_reason is not None:
            yield VenueOrderFailed(
                venue_submission=submission,
                reason=self.monitor_failure_reason,
            )
            return

        if self.cancel_after_fills or self.cancelled:
            yield VenueOrderCancelled(
                venue_submission=submission,
            )

    async def cancel_order(
        self,
        submission: FakeSubmission,
    ) -> CancelOrderResult:
        self.cancel_call_count += 1
        self.cancel_started.set()

        if self.pause_on_cancel:
            await self.resume_cancel.wait()

        if self.cancel_exception is not None:
            raise self.cancel_exception

        if self.cancel_result is not None:
            if isinstance(self.cancel_result, VenueCancellationAccepted):
                self.cancelled = True
                self.cancel_accepted.set()
            return self.cancel_result

        if submission.venue_order_id == self.venue_order_id:
            self.cancelled = True
            self.cancel_accepted.set()
            return VenueCancellationAccepted()

        return VenueOrderCancellationRejected(
            reason="venue order id doesn't match open order"
        )


def make_order(
    *,
    intent_id: str = "buy-1",
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.LIMIT,
    quantity: Decimal = Decimal("10"),
    price: Decimal = Decimal("0.60"),
) -> PlaceOrderIntent:
    if order_type is OrderType.MARKET:
        return PlaceOrderIntent(
            intent_id=intent_id,
            instrument_id="mock",
            side=side,
            quantity=quantity,
            order_type=order_type,
            time_in_force=TimeInForce.IOC,
        )

    return PlaceOrderIntent(
        intent_id=intent_id,
        instrument_id="mock",
        side=side,
        quantity=quantity,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        limit_price=price,
        limit_expiry=30.0,
    )


def make_buy_order() -> PlaceOrderIntent:
    return make_order()


def make_cancel_order(
    *,
    intent_id: str = "cancel-1",
    target_intent_id: str = "buy-1",
) -> CancelOrderIntent:
    return CancelOrderIntent(
        intent_id=intent_id,
        target_intent_id=target_intent_id,
    )


def make_executor(
    adapter: FakeAdapter,
) -> tuple[
    LiveExecutor[str, FakeSubmission],
    asyncio.Queue[EventEnvelope[ExecutionEvent]],
]:
    executor = LiveExecutor(
        adapter=adapter,
        instruments={"mock": "mock"},
    )

    event_queue: asyncio.Queue[
        EventEnvelope[ExecutionEvent]
    ] = asyncio.Queue()

    executor.set_event_sink(
        QueueEventSink(
            instance_id="test-strategy",
            queue=event_queue,
        )
    )

    return executor, event_queue


async def next_execution_event(
    queue: asyncio.Queue[EventEnvelope[ExecutionEvent]],
) -> ExecutionEvent:
    envelope = await asyncio.wait_for(
        queue.get(),
        timeout=1.0,
    )
    return envelope.payload


async def assert_no_execution_event(
    queue: asyncio.Queue[EventEnvelope[ExecutionEvent]],
    *,
    timeout: float = 0.05,
) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get(), timeout=timeout)


async def wait_for_executor_tasks_to_finish(
    executor: LiveExecutor[Any, Any],
) -> None:
    async def _wait() -> None:
        while executor.tasks:
            await asyncio.sleep(0)

    await asyncio.wait_for(_wait(), timeout=1.0)


# ---------------------------------------------------------------------------
# Placement / fill lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_fill_updates_execution_record() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("4"), Decimal("0.55"))],
        pause_after_fills=1,
        cancel_after_fills=True,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )

    submitted = await next_execution_event(event_queue)
    partial_fill = await next_execution_event(event_queue)

    assert isinstance(submitted, OrderSubmitted)
    assert isinstance(partial_fill, OrderPartiallyFilled)
    assert partial_fill.fill_quantity == Decimal("4")
    assert partial_fill.fill_price == Decimal("0.55")
    assert partial_fill.remaining_quantity == Decimal("6")

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.SUBMITTED
    assert record.accepted_quantity == Decimal("10")
    assert record.filled_quantity == Decimal("4")
    assert record.total_fill_cost == Decimal("2.20")

    adapter.resume_fills.set()
    await order_task

    cancelled = await next_execution_event(event_queue)
    assert isinstance(cancelled, OrderCancelled)
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_multiple_fills_produce_final_weighted_average() -> None:
    adapter = FakeAdapter(
        fills=[
            (Decimal("4"), Decimal("0.50")),
            (Decimal("6"), Decimal("0.70")),
        ],
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    submitted = await next_execution_event(event_queue)
    partial_fill = await next_execution_event(event_queue)
    filled = await next_execution_event(event_queue)

    assert isinstance(submitted, OrderSubmitted)
    assert isinstance(partial_fill, OrderPartiallyFilled)
    assert partial_fill.fill_quantity == Decimal("4")
    assert partial_fill.remaining_quantity == Decimal("6")

    assert isinstance(filled, OrderFilled)
    assert filled.fill_quantity == Decimal("6")
    assert filled.fill_price == Decimal("0.70")
    assert filled.avg_fill_price == Decimal("0.62")
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_single_full_fill_emits_only_terminal_fill() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("10"), Decimal("0.55"))]
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    submitted = await next_execution_event(event_queue)
    filled = await next_execution_event(event_queue)

    assert isinstance(submitted, OrderSubmitted)
    assert isinstance(filled, OrderFilled)
    assert filled.fill_quantity == Decimal("10")
    assert filled.fill_price == Decimal("0.55")
    await assert_no_execution_event(event_queue)
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_monitor_failure_after_partial_fill_emits_order_failed() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("4"), Decimal("0.50"))],
        monitor_failure_reason="venue stream became unrecoverable",
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    assert isinstance(
        await next_execution_event(event_queue),
        OrderPartiallyFilled,
    )

    failed = await next_execution_event(event_queue)
    assert isinstance(failed, OrderFailed)
    assert failed.filled_quantity == Decimal("4")
    assert failed.remaining_quantity == Decimal("6")
    assert failed.reason == "venue stream became unrecoverable"
    assert "buy-1" not in executor.execution_records


# ---------------------------------------------------------------------------
# Placement rejection / failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_venue_rejects_placement() -> None:
    adapter = FakeAdapter(
        placement_rejection_reason="venue rejected order"
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    event = await next_execution_event(event_queue)
    assert isinstance(event, OrderSubmissionRejected)
    assert event.intent_id == "buy-1"
    assert event.reason == "venue rejected order"
    assert "buy-1" not in executor.execution_records
    await assert_no_execution_event(event_queue)


@pytest.mark.asyncio
async def test_placement_exception_emits_submission_failed() -> None:
    adapter = FakeAdapter(
        placement_exception=RuntimeError("venue unavailable")
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    event = await next_execution_event(event_queue)
    assert isinstance(event, OrderSubmissionFailed)
    assert event.intent_id == "buy-1"
    assert event.reason == "venue unavailable"
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_pending_cancel_then_placement_rejected() -> None:
    adapter = FakeAdapter(
        pause_on_placement=True,
        placement_rejection_reason="bad order",
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    await adapter.placement_started.wait()

    await executor.handle_cancel_order(make_cancel_order())
    assert (
        executor.execution_records["buy-1"].state
        == ExecutionState.CANCELLATION_PENDING
    )

    adapter.resume_placement.set()
    await order_task

    rejected = await next_execution_event(event_queue)
    cancel_rejected = await next_execution_event(event_queue)

    assert isinstance(rejected, OrderSubmissionRejected)
    assert isinstance(cancel_rejected, OrderCancellationRejected)
    assert cancel_rejected.intent_id == "cancel-1"
    assert cancel_rejected.target_intent_id == "buy-1"
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_pending_cancel_then_placement_exception() -> None:
    adapter = FakeAdapter(
        pause_on_placement=True,
        placement_exception=RuntimeError("connection lost"),
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    await adapter.placement_started.wait()

    await executor.handle_cancel_order(make_cancel_order())
    adapter.resume_placement.set()
    await order_task

    failed = await next_execution_event(event_queue)
    cancel_rejected = await next_execution_event(event_queue)

    assert isinstance(failed, OrderSubmissionFailed)
    assert isinstance(cancel_rejected, OrderCancellationRejected)
    assert cancel_rejected.intent_id == "cancel-1"
    assert cancel_rejected.target_intent_id == "buy-1"
    assert "buy-1" not in executor.execution_records


# ---------------------------------------------------------------------------
# Cancellation lifecycle / races
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancellation_mid_fills() -> None:
    adapter = FakeAdapter(
        fills=[
            (Decimal("4"), Decimal("0.50")),
            (Decimal("6"), Decimal("0.70")),
        ],
        pause_after_fills=1,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    partial_fill = await next_execution_event(event_queue)
    assert isinstance(partial_fill, OrderPartiallyFilled)

    await executor.handle_cancel_order(make_cancel_order())
    adapter.resume_fills.set()

    cancelled = await next_execution_event(event_queue)
    assert isinstance(cancelled, OrderCancelled)
    assert cancelled.filled_quantity == Decimal("4")
    assert cancelled.remaining_quantity == Decimal("6")

    await order_task
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_cancellation_prior_to_fills() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("10"), Decimal("0.50"))],
        pause_after_fills=0,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await executor.handle_cancel_order(make_cancel_order())
    adapter.resume_fills.set()

    cancelled = await next_execution_event(event_queue)
    assert isinstance(cancelled, OrderCancelled)
    assert cancelled.filled_quantity == Decimal("0")
    assert cancelled.remaining_quantity == Decimal("10")

    await order_task
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_cancellation_prior_to_confirmation_no_fills() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("10"), Decimal("0.50"))],
        pause_on_placement=True,
        pause_after_fills=0,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    await adapter.placement_started.wait()

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.PENDING_SUBMISSION

    cancel = make_cancel_order()
    await executor.handle_cancel_order(cancel)

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.CANCELLATION_PENDING
    assert record.cancellation_intent is cancel

    adapter.resume_placement.set()
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await asyncio.wait_for(adapter.cancel_started.wait(), timeout=1.0)
    assert adapter.cancelled is True

    adapter.resume_fills.set()
    cancelled = await next_execution_event(event_queue)

    assert isinstance(cancelled, OrderCancelled)
    assert cancelled.filled_quantity == Decimal("0")
    assert cancelled.remaining_quantity == Decimal("10")

    await order_task
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_cancellation_prior_to_confirmation_with_fill_while_cancel_in_flight() -> None:
    adapter = FakeAdapter(
        fills=[
            (Decimal("3"), Decimal("0.50")),
            (Decimal("4"), Decimal("0.60")),
        ],
        pause_on_placement=True,
        pause_on_cancel=True,
        pause_after_fills=1,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    await adapter.placement_started.wait()

    await executor.handle_cancel_order(make_cancel_order())
    assert (
        executor.execution_records["buy-1"].state
        == ExecutionState.CANCELLATION_PENDING
    )

    adapter.resume_placement.set()
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await asyncio.wait_for(adapter.cancel_started.wait(), timeout=1.0)
    assert adapter.cancelled is False

    partial = await next_execution_event(event_queue)
    assert isinstance(partial, OrderPartiallyFilled)
    assert partial.fill_quantity == Decimal("3")
    assert partial.remaining_quantity == Decimal("7")

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.CANCELLING
    assert record.filled_quantity == Decimal("3")

    adapter.resume_cancel.set()
    await asyncio.wait_for(adapter.cancel_accepted.wait(), timeout=1.0)
    adapter.resume_fills.set()

    cancelled = await next_execution_event(event_queue)
    assert isinstance(cancelled, OrderCancelled)
    assert cancelled.filled_quantity == Decimal("3")
    assert cancelled.remaining_quantity == Decimal("7")

    await order_task
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_full_fill_wins_race_against_cancellation() -> None:
    adapter = FakeAdapter(
        fills=[
            (Decimal("3"), Decimal("0.50")),
            (Decimal("7"), Decimal("0.60")),
        ],
        pause_after_fills=1,
        pause_on_cancel=True,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    assert isinstance(
        await next_execution_event(event_queue),
        OrderPartiallyFilled,
    )

    cancel_task = asyncio.create_task(
        executor.handle_cancel_order(make_cancel_order())
    )
    await adapter.cancel_started.wait()
    assert adapter.cancelled is False

    # Let monitoring receive the final fill while venue cancellation is still
    # in flight.
    adapter.resume_fills.set()

    filled = await next_execution_event(event_queue)
    assert isinstance(filled, OrderFilled)
    assert filled.fill_quantity == Decimal("7")
    assert filled.fill_price == Decimal("0.60")
    assert filled.avg_fill_price == Decimal("0.57")
    await order_task
    assert "buy-1" not in executor.execution_records

    # Venue cancellation eventually completes, but must not create a second
    # terminal execution event.
    adapter.resume_cancel.set()
    await cancel_task
    await assert_no_execution_event(event_queue)


@pytest.mark.asyncio
async def test_unknown_cancellation_target_is_rejected_without_adapter_call() -> None:
    adapter = FakeAdapter()
    executor, event_queue = make_executor(adapter)

    await executor.handle_cancel_order(
        make_cancel_order(target_intent_id="does-not-exist")
    )

    event = await next_execution_event(event_queue)
    assert isinstance(event, OrderCancellationRejected)
    assert event.target_intent_id == "does-not-exist"
    assert adapter.cancel_call_count == 0


@pytest.mark.asyncio
async def test_second_cancellation_is_rejected_while_first_is_in_flight() -> None:
    adapter = FakeAdapter(
        pause_after_fills=0,
        pause_on_cancel=True,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    first_cancel_task = asyncio.create_task(
        executor.handle_cancel_order(
            make_cancel_order(intent_id="cancel-1")
        )
    )
    await adapter.cancel_started.wait()

    await executor.handle_cancel_order(
        make_cancel_order(intent_id="cancel-2")
    )

    rejected = await next_execution_event(event_queue)
    assert isinstance(rejected, OrderCancellationRejected)
    assert rejected.intent_id == "cancel-2"
    assert adapter.cancel_call_count == 1

    adapter.resume_cancel.set()
    await first_cancel_task
    adapter.resume_fills.set()

    assert isinstance(await next_execution_event(event_queue), OrderCancelled)
    await order_task


@pytest.mark.asyncio
async def test_venue_cancellation_rejection_restores_submitted_state() -> None:
    adapter = FakeAdapter(
        pause_after_fills=0,
        cancel_result=VenueOrderCancellationRejected(
            reason="too late to cancel"
        ),
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await executor.handle_cancel_order(make_cancel_order())

    rejected = await next_execution_event(event_queue)
    assert isinstance(rejected, OrderCancellationRejected)
    assert rejected.reason == "too late to cancel"

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.SUBMITTED
    assert record.cancellation_intent is None

    # End the order cleanly for the test.
    adapter.cancel_after_fills = True
    adapter.resume_fills.set()
    await order_task
    assert isinstance(await next_execution_event(event_queue), OrderCancelled)


@pytest.mark.asyncio
async def test_venue_cancellation_failure_restores_submitted_state() -> None:
    adapter = FakeAdapter(
        pause_after_fills=0,
        cancel_result=VenueOrderCancellationFailed(
            reason="cancel endpoint failed"
        ),
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await executor.handle_cancel_order(make_cancel_order())

    failed = await next_execution_event(event_queue)
    assert isinstance(failed, OrderCancellationFailed)
    assert failed.reason == "cancel endpoint failed"

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.SUBMITTED
    assert record.cancellation_intent is None

    adapter.cancel_after_fills = True
    adapter.resume_fills.set()
    await order_task
    assert isinstance(await next_execution_event(event_queue), OrderCancelled)


@pytest.mark.asyncio
async def test_cancellation_exception_restores_submitted_state() -> None:
    adapter = FakeAdapter(
        pause_after_fills=0,
        cancel_exception=RuntimeError("cancel request crashed"),
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await executor.handle_cancel_order(make_cancel_order())

    failed = await next_execution_event(event_queue)
    assert isinstance(failed, OrderCancellationFailed)
    assert failed.reason == "cancel request crashed"

    record = executor.execution_records["buy-1"]
    assert record.state == ExecutionState.SUBMITTED
    assert record.cancellation_intent is None

    adapter.cancel_after_fills = True
    adapter.resume_fills.set()
    await order_task
    assert isinstance(await next_execution_event(event_queue), OrderCancelled)


# ---------------------------------------------------------------------------
# Intent routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "order_type", "expected_method"),
    [
        (OrderSide.BUY, OrderType.LIMIT, "place_limit_buy"),
        (OrderSide.SELL, OrderType.LIMIT, "place_limit_sell"),
        (OrderSide.BUY, OrderType.MARKET, "place_market_buy"),
        (OrderSide.SELL, OrderType.MARKET, "place_market_sell"),
    ],
)
async def test_order_routes_to_correct_adapter_method(
    side: OrderSide,
    order_type: OrderType,
    expected_method: str,
) -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("10"), Decimal("0.50"))]
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(
        make_order(side=side, order_type=order_type)
    )

    assert adapter.place_calls[0][0] == expected_method
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    assert isinstance(await next_execution_event(event_queue), OrderFilled)


@pytest.mark.asyncio
async def test_missing_instrument_raises_before_adapter_call() -> None:
    adapter = FakeAdapter()
    executor = LiveExecutor(
        adapter=adapter,
        instruments={},
    )

    with pytest.raises(
        RuntimeError,
        match="Venue instrument not found",
    ):
        await executor.handle_place_order(make_buy_order())

    assert adapter.place_calls == []
    assert executor.execution_records == {}


# ---------------------------------------------------------------------------
# Shutdown / task lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_requests_cancellation_for_submitted_order() -> None:
    adapter = FakeAdapter(
        pause_after_fills=0,
    )
    executor, event_queue = make_executor(adapter)

    order_task = asyncio.create_task(
        executor.handle_place_order(make_buy_order())
    )
    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)

    await executor.shutdown(grace_period=0.1)

    await asyncio.wait_for(adapter.cancel_started.wait(), timeout=1.0)
    assert adapter.cancel_call_count == 1

    # Directly-created order_task is not owned by executor.tasks, so finish its
    # monitor explicitly.
    adapter.resume_fills.set()
    await order_task
    assert isinstance(await next_execution_event(event_queue), OrderCancelled)


@pytest.mark.asyncio
async def test_shutdown_during_pending_placement_cancels_after_acceptance() -> None:
    adapter = FakeAdapter(
        pause_on_placement=True,
        pause_after_fills=0,
    )
    executor, event_queue = make_executor(adapter)

    executor.handle_intent(make_buy_order())
    await adapter.placement_started.wait()

    shutdown_task = asyncio.create_task(
        executor.shutdown(grace_period=1.0)
    )

    # Let the pending venue placement finish after shutdown has begun.
    await asyncio.sleep(0)
    adapter.resume_placement.set()

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    await asyncio.wait_for(adapter.cancel_started.wait(), timeout=1.0)

    adapter.resume_fills.set()
    assert isinstance(await next_execution_event(event_queue), OrderCancelled)

    await shutdown_task
    assert not executor.tasks


@pytest.mark.asyncio
async def test_handle_intent_rejects_new_work_after_shutdown() -> None:
    adapter = FakeAdapter()
    executor, _ = make_executor(adapter)

    await executor.shutdown(grace_period=0)

    with pytest.raises(RuntimeError, match="Executor is shutting down"):
        executor.handle_intent(make_buy_order())


@pytest.mark.asyncio
async def test_handle_intent_task_is_removed_after_terminal_fill() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("10"), Decimal("0.50"))]
    )
    executor, event_queue = make_executor(adapter)

    executor.handle_intent(make_buy_order())

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    assert isinstance(await next_execution_event(event_queue), OrderFilled)

    await wait_for_executor_tasks_to_finish(executor)
    assert not executor.tasks
    assert "buy-1" not in executor.execution_records


# ---------------------------------------------------------------------------
# Contract-hardening tests.
#
# These enforce duplicate ID, invalid fill and monitor failure behaviour.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_place_intent_id_is_rejected_before_adapter_call() -> None:
    adapter = FakeAdapter(
        placement_exception=RuntimeError(
            "adapter should not be called for duplicate intent id"
        )
    )
    executor, event_queue = make_executor(adapter)

    existing_order = make_buy_order()
    executor.execution_records[existing_order.intent_id] = ExecutionRecord(
        intent=existing_order
    )

    duplicate = make_order(intent_id="buy-1")
    await executor.handle_place_order(duplicate)

    event = await next_execution_event(event_queue)
    assert isinstance(event, OrderSubmissionRejected)
    assert "duplicate" in event.reason.lower()
    assert adapter.place_calls == []
    assert executor.execution_records["buy-1"].intent is existing_order


@pytest.mark.asyncio
async def test_overfill_fails_order_instead_of_emitting_negative_remaining() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("11"), Decimal("0.50"))]
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    event = await next_execution_event(event_queue)

    assert isinstance(event, OrderFailed)
    assert "overfill" in event.reason.lower()
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_unexpected_place_result_emits_submission_failed() -> None:
    adapter = FakeAdapter(
        unexpected_place_result=object(),
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    event = event_queue.get_nowait().payload
    assert isinstance(event, OrderSubmissionFailed)
    assert "unexpected" in event.reason.lower()
    assert "buy-1" not in executor.execution_records


@pytest.mark.asyncio
async def test_monitor_ending_without_terminal_result_fails_and_cleans_record() -> None:
    adapter = FakeAdapter(
        fills=[(Decimal("4"), Decimal("0.50"))]
    )
    executor, event_queue = make_executor(adapter)

    await executor.handle_place_order(make_buy_order())

    assert isinstance(await next_execution_event(event_queue), OrderSubmitted)
    assert isinstance(
        await next_execution_event(event_queue),
        OrderPartiallyFilled,
    )

    assert "buy-1" not in executor.execution_records
    failed = event_queue.get_nowait().payload
    assert isinstance(failed, OrderFailed)
    assert "terminal" in failed.reason.lower()


@pytest.mark.asyncio
async def test_duplicate_completed_id_is_not_reused() -> None:
    adapter = FakeAdapter(fills=[(Decimal("10"), Decimal("0.5"))])
    executor, queue = make_executor(adapter)
    await executor.handle_place_order(make_buy_order())
    assert isinstance(await next_execution_event(queue), OrderSubmitted)
    assert isinstance(await next_execution_event(queue), OrderFilled)
    previous_calls = len(adapter.place_calls)
    await executor.handle_place_order(make_buy_order())
    assert isinstance(await next_execution_event(queue), OrderSubmissionRejected)
    assert len(adapter.place_calls) == previous_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity,price", [
    ("0", "0.5"), ("-1", "0.5"), ("NaN", "0.5"),
    ("Infinity", "0.5"), ("1", "NaN"), ("1", "Infinity"),
    ("1", "-0.1"), ("7", "0.5"),
])
async def test_invalid_fill_preserves_previously_confirmed_totals(quantity: str, price: str) -> None:
    adapter = FakeAdapter(fills=[
        (Decimal("4"), Decimal("0.4")), (Decimal(quantity), Decimal(price)),
    ])
    executor, queue = make_executor(adapter)
    await executor.handle_place_order(make_buy_order())
    assert isinstance(await next_execution_event(queue), OrderSubmitted)
    assert isinstance(await next_execution_event(queue), OrderPartiallyFilled)
    failed = await next_execution_event(queue)
    assert isinstance(failed, OrderFailed)
    assert failed.filled_quantity == Decimal("4")
    assert failed.remaining_quantity == Decimal("6")
    assert "buy-1" not in executor.execution_records
    assert queue.empty()


@pytest.mark.asyncio
async def test_monitor_exception_reports_known_fills_and_cleans_record() -> None:
    class BrokenMonitor(FakeAdapter):
        async def monitor_order(self, submission):
            yield VenueOrderFill(submission, Decimal("4"), Decimal("0.5"))
            raise RuntimeError("monitor disconnected")

    executor, queue = make_executor(BrokenMonitor())
    await executor.handle_place_order(make_buy_order())
    assert isinstance(await next_execution_event(queue), OrderSubmitted)
    assert isinstance(await next_execution_event(queue), OrderPartiallyFilled)
    failed = await next_execution_event(queue)
    assert isinstance(failed, OrderFailed)
    assert failed.filled_quantity == Decimal("4")
    assert failed.remaining_quantity == Decimal("6")
    assert "monitor disconnected" in failed.reason
    assert "buy-1" not in executor.execution_records
