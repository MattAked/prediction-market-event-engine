"""Serial, deterministic replay for one strategy instance per run."""
from __future__ import annotations

import heapq
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import count
from math import isfinite

from ..core.events import EventEnvelope, TimerExpired
from ..core.outputs import (
    CancelTimer, ExecutionIntent, ScheduleTimer, ShutdownPolicy,
    StopStrategy, StrategyOutput,
)
from ..executors.backtest.protocols import BacktestExecutorProtocol
from ..executors.backtest.models import (
    ExecutionIntentArrived, OrderExpiryDue, ScheduledExecutionEvent,
)
from ..strategy.base import StrategyInstance
from .models import StrategyExit, StrategyExitReason

@dataclass(order=True, slots=True)
class ReplayItem:
    timestamp: datetime
    sequence: int
    event: EventEnvelope = field(compare=False)
    cancelled: bool = field(default=False, compare=False)


async def _iterate(events: Iterable[EventEnvelope] | AsyncIterable[EventEnvelope]) -> AsyncGenerator[EventEnvelope, None]:
    if isinstance(events, AsyncIterable):
        iterator = aiter(events)
        try:
            async for event in iterator:
                yield event
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
    else:
        iterator = iter(events)
        try:
            for event in iterator:
                yield event
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


class BacktestEngine:
    """Merge ordered history with synthetic events, without real-time waits.

    Equal-time synthetic events precede the next historical event, and use
    insertion order among themselves. History preserves source order. History
    exhaustion drains pending synthetic events, then finalises unchanged.
    A strategy stop enters bounded executor-only cleanup. Errors propagate;
    strategy termination hooks are never called by this backtest engine.
    Use separate engine objects for concurrent runs. Executor accounting is
    available directly from the instance; the return value is lifecycle only.
    """

    def __init__(self) -> None:
        self._queue: list[ReplayItem] = []
        self._timers: dict[str, ReplayItem] = {}
        self._sequence = count()
        self._current_time: datetime | None = None
        self._running = False
        self._next_external: EventEnvelope | None = None
        self._history_exhausted = False

    @property
    def current_time(self) -> datetime | None:
        """Last processed simulated timestamp, or None before the first event."""
        return self._current_time

    async def backtest_instance(
        self,
        strategy_instance: StrategyInstance[BacktestExecutorProtocol],
        events: Iterable[EventEnvelope] | AsyncIterable[EventEnvelope],
    ) -> StrategyExit:
        if self._running:
            raise RuntimeError("This BacktestEngine already has an active run")

        self._reset()
        self._running = True
        source = self._historical_events(events, strategy_instance.instance_id)
        failure: BaseException | None = None
        try:
            stop = await self._replay(strategy_instance, source)
            if stop is not None:
                await self._cleanup_executor(strategy_instance, source, stop)
            return StrategyExit(strategy_instance.instance_id, StrategyExitReason.COMPLETED)
        except BaseException as exc:
            # Errors and cancellation never become an ordinary partial result.
            failure = exc
            raise
        finally:
            errors: list[BaseException] = []
            try:
                await strategy_instance.executor.finalise()
            except BaseException as exc:
                errors.append(exc)
            try:
                await source.aclose()
            except BaseException as exc:
                errors.append(exc)
            self._queue.clear()
            self._timers.clear()
            self._next_external = None
            self._running = False
            if errors:
                if failure is not None:
                    # Preserve the original code/data error, not a cleanup error.
                    for error in errors:
                        failure.add_note(f"Backtest resource finalisation also failed: {error!r}")
                elif len(errors) == 1:
                    raise errors[0]
                else:
                    raise BaseExceptionGroup("Backtest resource finalisation failed", errors)

    def _reset(self) -> None:
        self._queue.clear()
        self._timers.clear()
        self._sequence = count()
        self._current_time = None
        self._next_external = None
        self._history_exhausted = False

    async def _historical_events(
        self,
        events: Iterable[EventEnvelope] | AsyncIterable[EventEnvelope],
        instance_id: str,
    ) -> AsyncGenerator[EventEnvelope, None]:
        """Adapt and validate history; source exceptions propagate unchanged."""
        source = _iterate(events)
        previous_time: datetime | None = None
        try:
            async for event in source:
                if event.instance_id not in (None, instance_id):
                    raise ValueError("Historical event belongs to another instance")
                if previous_time is not None and event.timestamp < previous_time:
                    raise ValueError("Historical events must be timestamp ordered")
                previous_time = event.timestamp
                yield event
        finally:
            await source.aclose()

    async def _replay(
        self,
        instance: StrategyInstance[BacktestExecutorProtocol],
        history: AsyncIterator[EventEnvelope],
        *, executor_only: bool = False, until: datetime | None = None,
    ) -> StopStrategy | None:
        """Use one merger for normal replay and the bounded cleanup phase.

        Keep the external lookahead on the engine: a stop caused by an internal
        event must not discard the historical item it was compared against.
        """
        while True:
            if self._next_external is None and not self._history_exhausted:
                self._next_external = await anext(history, None)
                self._history_exhausted = self._next_external is None
            while self._queue and self._queue[0].cancelled:
                heapq.heappop(self._queue)

            external = self._next_external
            if self._queue and (external is None or self._queue[0].timestamp <= external.timestamp):
                timestamp = self._queue[0].timestamp
                if until is not None and timestamp > until:
                    return None
                event = self._pop_internal_event()
            elif external is not None:
                if until is not None and external.timestamp > until:
                    return None
                event = external
                self._next_external = None
            else:
                return None

            if executor_only:
                self._process_executor_event(instance, event)
            else:
                stop = await self._process_event(instance, event)
                if stop is not None:
                    return stop

    async def _cleanup_executor(
        self, instance: StrategyInstance[BacktestExecutorProtocol],
        history: AsyncIterator[EventEnvelope], stop: StopStrategy,
    ) -> None:
        grace = 2.0 if stop.grace_period is None else stop.grace_period
        if not isfinite(grace) or grace < 0:
            raise ValueError("Shutdown grace period must be finite and nonnegative")
        timestamp = self._current_time
        if timestamp is None:
            raise RuntimeError("Strategy stop has no replay timestamp")
        # Strategy timers have no role once its normal callbacks are disabled.
        for timer_id in list(self._timers):
            self._cancel_timer(timer_id)
        for generated in instance.executor.begin_shutdown(
            timestamp, grace_period=grace,
            cancel_open_orders=stop.policy is ShutdownPolicy.CANCEL_OPEN_ORDERS,
        ):
            self._schedule_execution_event(instance.instance_id, generated)
        await self._replay(
            instance, history, executor_only=True,
            until=timestamp + timedelta(seconds=grace),
        )

    def _pop_internal_event(self) -> EventEnvelope:
        item = heapq.heappop(self._queue)
        payload = item.event.payload
        if isinstance(payload, TimerExpired):
            if self._timers.get(payload.timer_id) is item:
                self._timers.pop(payload.timer_id)
        return item.event

    async def _process_event(
        self,
        instance: StrategyInstance[BacktestExecutorProtocol],
        event: EventEnvelope,
    ) -> StopStrategy | None:
        self._current_time = event.timestamp

        self._process_executor_event(instance, event)

        # Internal requests change venue state, but are not strategy notifications.
        if isinstance(event.payload, (ExecutionIntentArrived, OrderExpiryDue)):
            return None

        for output in instance.handle_event(event):
            if isinstance(output, StopStrategy):
                return output
            await self._process_output(instance, output)
        return None

    def _process_executor_event(
        self, instance: StrategyInstance[BacktestExecutorProtocol], event: EventEnvelope,
    ) -> None:
        self._current_time = event.timestamp
        if isinstance(event.payload, TimerExpired):
            return
        for generated in instance.executor.handle_event(event):
            self._schedule_execution_event(instance.instance_id, generated)

    def _schedule(self, instance_id: str, payload: object, delay: timedelta) -> ReplayItem:
        if delay < timedelta(0):
            raise ValueError("Synthetic event delay cannot be negative")
        if self._current_time is None:
            raise RuntimeError("Cannot schedule before simulated time is established")
        event = EventEnvelope(self._current_time + delay, payload, instance_id)
        item = ReplayItem(event.timestamp, next(self._sequence), event)
        heapq.heappush(self._queue, item)
        return item

    def _schedule_execution_event(
        self, instance_id: str, generated: ScheduledExecutionEvent,
    ) -> None:
        self._schedule(
            instance_id, generated.event, timedelta(seconds=generated.delay),
        )

    async def _process_output(
        self, instance: StrategyInstance[BacktestExecutorProtocol], output: StrategyOutput,
    ) -> None:
        if isinstance(output, ExecutionIntent):
            generated = instance.executor.handle_intent(output, timestamp=self._current_time)
            self._schedule_execution_event(instance.instance_id, generated)
        elif isinstance(output, ScheduleTimer):
            if output.delay < timedelta(0):
                raise ValueError("Timer delay cannot be negative")
            if output.timer_id in self._timers:
                raise ValueError(f"Timer ID already active: {output.timer_id}")
            self._timers[output.timer_id] = self._schedule(
                instance.instance_id, TimerExpired(output.timer_id), output.delay,
            )
        elif isinstance(output, CancelTimer):
            self._cancel_timer(output.timer_id)

    def _cancel_timer(self, timer_id: str) -> None:
        item = self._timers.pop(timer_id, None)
        if item is not None:
            item.cancelled = True

