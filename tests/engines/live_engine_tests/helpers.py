from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from time import monotonic
from unittest.mock import AsyncMock, Mock

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.timer import TimerExpired
from PredictionMarketEventSniper.core.models.orders import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from PredictionMarketEventSniper.core.outputs import (
    ShutdownPolicy,
    StopStrategy,
    StrategyOutput,
)
from PredictionMarketEventSniper.core.outputs.execution import PlaceOrderIntent
from PredictionMarketEventSniper.core.outputs.timer import (
    CancelTimer,
    ScheduleTimer,
)
from PredictionMarketEventSniper.engine.live_engine import LiveEngine
from PredictionMarketEventSniper.engine.models import (
    StrategyExitReason,
    StrategyHandle,
    StrategyTermination,
)
from PredictionMarketEventSniper.executors.live.live_executor import LiveExecutor
from PredictionMarketEventSniper.executors.live.protocols import VenueAdapter
from PredictionMarketEventSniper.strategy.base import (
    StrategyInstance,
    StrategyLogic,
)


# ---------------------------------------------------------------------------
# Test doubles / events
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FakeSubmission:
    venue_order_id: str


class FakeAdapter(VenueAdapter[str, FakeSubmission]):
    pass


class FakeEvent:
    pass


class FakeTerminalEvent(FakeEvent):
    pass


class FakeLeaveOpenTerminalEvent(FakeEvent):
    pass


class FakeGracefulTerminalEvent(FakeEvent):
    pass


class FakeStartTimer(FakeEvent):
    pass


class FakeCancelTimer(FakeEvent):
    pass


class FakeExecutionIntentEvent(FakeEvent):
    pass


class FakeOutputsAroundStopEvent(FakeEvent):
    pass


class FakeFailureEvent(FakeEvent):
    pass


class FakeHeavyFailureEvent(FakeEvent):
    pass


class FakeHeavyEvent(FakeEvent):
    def __init__(self, event_id: int = 1):
        self.event_id = event_id


class FakeBlockingHeavyEvent(FakeEvent):
    pass


def make_order_intent(intent_id: str = "test-order") -> PlaceOrderIntent:
    return PlaceOrderIntent(
        intent_id=intent_id,
        instrument_id="TEST",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
    )


class FakeLogic(StrategyLogic):
    def __init__(
        self,
        timer_length: float = 1,
    ) -> None:
        self.timer_length = timer_length

        self.time_start: float | None = None
        self.timer_duration: float | None = None
        self.timer_expired_event = asyncio.Event()
        self.timer_expiry_count = 0

        self.handle_thread_id: int | None = None

        self.event_received_event = asyncio.Event()
        self.received_event: EventEnvelope | None = None

        # Used to deterministically test serial heavy-event processing.
        self.first_event_started = threading.Event()
        self.release_first_event = threading.Event()
        self.second_event_started = threading.Event()
        self.processed_event_ids: list[int] = []

        # Used to exercise worker failure paths.
        self.failure_exception = RuntimeError("strategy handler crashed")
        self.heavy_failure_exception = RuntimeError("heavy strategy handler crashed")

        # Used to inspect termination behavior.
        self.terminations: list[StrategyTermination] = []
        self.termination_called = threading.Event()
        self.raise_on_termination = False
        self.termination_exception = RuntimeError("termination handler crashed")
        self.termination_outputs: list[StrategyOutput] = []

        # Used to expose termination-vs-heavy-handler races.
        self.blocking_heavy_started = threading.Event()
        self.release_blocking_heavy = threading.Event()
        self.blocking_heavy_finished = threading.Event()

    def is_heavy(self, payload: object) -> bool:
        return isinstance(
            payload,
            (
                FakeHeavyEvent,
                FakeHeavyFailureEvent,
                FakeBlockingHeavyEvent,
            ),
        )

    def handle_event(
        self,
        event: EventEnvelope,
        state: object,
        params: object,
    ) -> list[StrategyOutput]:
        outputs: list[StrategyOutput] = []
        payload = event.payload

        self.handle_thread_id = threading.get_ident()

        # Only the exact base FakeEvent is used by the routing test. Heavy
        # subclasses must not set asyncio.Event from a worker thread.
        if type(payload) is FakeEvent:
            self.received_event = event
            self.event_received_event.set()

        if isinstance(payload, FakeHeavyEvent):
            if payload.event_id == 1:
                self.first_event_started.set()
                self.release_first_event.wait()
            elif payload.event_id == 2:
                self.second_event_started.set()

            self.processed_event_ids.append(payload.event_id)

        if isinstance(payload, FakeBlockingHeavyEvent):
            self.blocking_heavy_started.set()
            self.release_blocking_heavy.wait()
            self.blocking_heavy_finished.set()

        if isinstance(payload, FakeFailureEvent):
            raise self.failure_exception

        if isinstance(payload, FakeHeavyFailureEvent):
            raise self.heavy_failure_exception

        if isinstance(payload, FakeStartTimer):
            outputs.append(
                ScheduleTimer(
                    timer_id="test_timer",
                    delay=timedelta(seconds=self.timer_length),
                )
            )
            self.time_start = monotonic()

        elif isinstance(payload, FakeTerminalEvent):
            outputs.append(StopStrategy())

        elif isinstance(payload, FakeLeaveOpenTerminalEvent):
            outputs.append(
                StopStrategy(
                    policy=ShutdownPolicy.LEAVE_OPEN_ORDERS,
                )
            )

        elif isinstance(payload, FakeGracefulTerminalEvent):
            outputs.append(
                StopStrategy(
                    policy=ShutdownPolicy.LEAVE_OPEN_ORDERS,
                    grace_period=3.5,
                )
            )

        elif isinstance(payload, FakeExecutionIntentEvent):
            outputs.append(make_order_intent())

        elif isinstance(payload, FakeOutputsAroundStopEvent):
            outputs.extend(
                [
                    make_order_intent("before-stop"),
                    StopStrategy(),
                    make_order_intent("after-stop"),
                ]
            )

        elif isinstance(payload, FakeCancelTimer):
            outputs.append(
                CancelTimer(
                    timer_id="test_timer",
                )
            )

        elif isinstance(payload, TimerExpired):
            self.timer_expiry_count += 1
            self.timer_expired_event.set()

            if self.time_start is not None:
                self.timer_duration = monotonic() - self.time_start

        return outputs

    def handle_termination(
        self,
        termination: StrategyTermination,
        state: object,
        params: object,
    ) -> list[StrategyOutput]:
        self.terminations.append(termination)
        self.termination_called.set()

        if self.raise_on_termination:
            raise self.termination_exception

        return list(self.termination_outputs)


class FakeStrategy(StrategyInstance):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def register_fake_strategy(
    engine: LiveEngine,
    *,
    instance_id: str = "test_strategy",
    timer_length: float = 1,
) -> tuple[StrategyHandle, FakeLogic, LiveExecutor]:
    executor = LiveExecutor(
        adapter=FakeAdapter(),
        instruments={"TEST": "TEST"},
    )

    logic = FakeLogic(timer_length=timer_length)

    strat_instance = FakeStrategy(
        instance_id=instance_id,
        params={},
        state={},
        logic=logic,
        executor=executor,
    )

    handle = engine.register_strategy(
        instance_id,
        strat_instance,
    )

    return handle, logic, executor


def get_engine_with_strategy(
    timer_length: float = 1,
) -> tuple[
    LiveEngine,
    StrategyHandle,
    str,
    FakeLogic,
    LiveExecutor,
]:
    engine = LiveEngine()
    instance_id = "test_strategy"

    handle, logic, executor = register_fake_strategy(
        engine,
        instance_id=instance_id,
        timer_length=timer_length,
    )

    return engine, handle, instance_id, logic, executor


async def put_event(
    handle: StrategyHandle,
    instance_id: str,
    payload: object,
) -> None:
    await handle.queue.put(
        EventEnvelope(
            timestamp=datetime.now(),
            payload=payload,
            instance_id=instance_id,
        )
    )


async def wait_for_thread_event(
    event: threading.Event,
    *,
    timeout: float = 1.0,
) -> None:
    await asyncio.wait_for(
        asyncio.to_thread(event.wait),
        timeout=timeout,
    )
