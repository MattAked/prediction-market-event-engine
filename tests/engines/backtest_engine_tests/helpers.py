from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.execution import ExecutionEvent
from PredictionMarketEventSniper.core.outputs import StrategyOutput, ExecutionIntent, CancelOrderIntent
from PredictionMarketEventSniper.engine.backtest_engine import BacktestEngine
from PredictionMarketEventSniper.engine.models import StrategyTermination
from PredictionMarketEventSniper.executors.backtest.models import ExecutionIntentArrived, ScheduledExecutionEvent
from PredictionMarketEventSniper.executors.backtest.protocols import BacktestExecutorProtocol
from PredictionMarketEventSniper.strategy.base import StrategyInstance

T = datetime(2026, 1, 1)


@dataclass(frozen=True)
class ProbeEvent(ExecutionEvent):
    tag: str


def envelope(payload: object, at: float = 0.0, instance_id: str | None = 'match') -> EventEnvelope[object]:
    return EventEnvelope(T + timedelta(seconds=at), payload, instance_id)


def request(id: str = 'cancel') -> CancelOrderIntent:
    return CancelOrderIntent(id, 'target')


class ExecutorSpy:
    """Schedules controlled events without copying the engine's merge algorithm."""
    def __init__(self) -> None:
        self.events: list[EventEnvelope[object]] = []
        self.intents: list[tuple[ExecutionIntent, datetime | None]] = []
        self.shutdown_calls: list[tuple[datetime, float, bool]] = []
        self.finalise_calls = 0
        self.closed = False
        self.intent_delay = 0.0
        self.on_event: Callable[[EventEnvelope[object]], list[ScheduledExecutionEvent]] | None = None
        self.shutdown_events: list[ScheduledExecutionEvent] = []
        self.event_error: Exception | None = None
        self.intent_error: Exception | None = None
        self.shutdown_error: Exception | None = None
        self.finalise_error: Exception | None = None

    def handle_event(self, event: EventEnvelope[object]) -> list[ScheduledExecutionEvent]:
        self.events.append(event)
        if self.event_error is not None:
            raise self.event_error
        return [] if self.on_event is None else self.on_event(event)

    def handle_intent(self, intent: ExecutionIntent, timestamp: datetime | None = None) -> ScheduledExecutionEvent:
        self.intents.append((intent, timestamp))
        if self.intent_error is not None:
            raise self.intent_error
        return ScheduledExecutionEvent(ExecutionIntentArrived(intent), self.intent_delay)

    def begin_shutdown(self, timestamp: datetime, *, grace_period: float = 2.0,
                       cancel_open_orders: bool = True) -> list[ScheduledExecutionEvent]:
        self.shutdown_calls.append((timestamp, grace_period, cancel_open_orders))
        if self.shutdown_error is not None:
            raise self.shutdown_error
        return list(self.shutdown_events)

    async def finalise(self) -> None:
        self.finalise_calls += 1
        self.closed = True
        if self.finalise_error is not None:
            raise self.finalise_error


class LogicSpy:
    def __init__(self) -> None:
        self.events: list[EventEnvelope[object]] = []
        self.on_event: Callable[[EventEnvelope[object]], list[StrategyOutput]] | None = None
        self.error: Exception | None = None
        self.termination_calls = 0
        self.heavy_calls = 0

    def handle_event(self, event: EventEnvelope[object], state: object, params: object) -> list[StrategyOutput]:
        self.events.append(event)
        if self.error is not None:
            raise self.error
        return [] if self.on_event is None else self.on_event(event)

    def handle_termination(self, termination: StrategyTermination, state: object, params: object) -> list[StrategyOutput]:
        self.termination_calls += 1
        raise AssertionError('Backtests must never call strategy termination')

    def is_heavy(self, payload: object) -> bool:
        self.heavy_calls += 1
        raise AssertionError('Backtests run strategy callbacks serially without heavy routing')


@dataclass
class Harness:
    engine: BacktestEngine
    executor: ExecutorSpy
    logic: LogicSpy
    instance: StrategyInstance[BacktestExecutorProtocol]


def make_run() -> Harness:
    engine, executor, logic = BacktestEngine(), ExecutorSpy(), LogicSpy()
    instance = StrategyInstance[BacktestExecutorProtocol]('match', {}, {}, logic, executor)
    return Harness(engine, executor, logic, instance)


def payloads(events: Iterable[EventEnvelope[object]]) -> list[object]:
    return [event.payload for event in events]


class TrackingSource:
    """Explicitly closeable async history; records reads without wall-clock waits."""
    def __init__(self, events: Iterable[EventEnvelope[object]]) -> None:
        self.items = list(events)
        self.index = 0
        self.reads = 0
        self.close_calls = 0
        self.close_error: Exception | None = None

    def __aiter__(self) -> TrackingSource:
        return self

    async def __anext__(self) -> EventEnvelope[object]:
        self.reads += 1
        if self.index == len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
