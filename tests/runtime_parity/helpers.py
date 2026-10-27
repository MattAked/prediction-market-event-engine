"""Independent venue script and identical strategy for real engine/executor runs."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal as D

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.execution import (
    ExecutionEvent, OrderSubmitted, OrderFilled, OrderPartiallyFilled,
    OrderCancelled, OrderSubmissionRejected,
)
from PredictionMarketEventSniper.core.models.orders import OrderSide, OrderType, TimeInForce
from PredictionMarketEventSniper.core.outputs import PlaceOrderIntent, CancelOrderIntent, StopStrategy, ShutdownPolicy
from PredictionMarketEventSniper.engine.live_engine import LiveEngine
from PredictionMarketEventSniper.engine.backtest_engine import BacktestEngine
from PredictionMarketEventSniper.strategy.base import StrategyInstance
from PredictionMarketEventSniper.executors.live.live_executor import LiveExecutor
from PredictionMarketEventSniper.executors.live.protocols import VenueAdapter as LiveAdapter
from PredictionMarketEventSniper.executors.live.models import (
    VenueOrderAccepted, VenueOrderRejected, VenueOrderFill,
    VenueOrderCancelled, VenueCancellationAccepted,
)
from PredictionMarketEventSniper.executors.backtest.backtest_executor import BacktestExecutor
from PredictionMarketEventSniper.executors.backtest.protocols import VenueAdapter as BacktestAdapter, BacktestExecutorProtocol
from PredictionMarketEventSniper.executors.backtest.models import (
    BookUpdate, BookLevel, BookSide, BookUpdateKind,
    VenueOrderAccepted as SimAccepted, VenueOrderRejected as SimRejected,
)

TIMEOUT = 3  # Failure bound only, never a mechanism for ordering a scenario.


@dataclass(frozen=True)
class Begin:
    pass


@dataclass(frozen=True)
class Scenario:
    name: str
    side: OrderSide = OrderSide.BUY
    price: D | None = None
    tif: TimeInForce = TimeInForce.IOC
    levels: tuple[tuple[str, str], ...] = (('.4', '5'),)
    # Scripted independently of the book/matcher: quantity, price.
    fills: tuple[tuple[str, str], ...] = (('5', '.4'),)
    later_levels: tuple[tuple[str, str], ...] | None = None
    reject: bool = False
    auto_cancel: bool = False
    cancel_after: type[ExecutionEvent] | None = None
    stop_on_submission: bool = False
    policy: ShutdownPolicy = ShutdownPolicy.CANCEL_OPEN_ORDERS

    def intent(self):
        return PlaceOrderIntent('order', 'home', self.side, D('5'),
                                OrderType.MARKET if self.price is None else OrderType.LIMIT,
                                self.tif, 0, self.price)

    def book(self, later: bool = False):
        levels = self.later_levels if later else self.levels
        side = BookSide.ASK if self.side is OrderSide.BUY else BookSide.BID
        return BookUpdate('token', BookUpdateKind.SNAPSHOT,
                          tuple(BookLevel(side, D(p), D(q)) for p, q in levels or ()))


@dataclass(frozen=True)
class Submission:
    venue_order_id: str = 'venue-order'


class ScriptedLiveAdapter(LiveAdapter[str, Submission]):
    """No matching code: reports exactly the independent scenario script."""
    def __init__(self, scenario: Scenario):
        self.scenario = scenario
        self.release_later = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.calls = []
        self.cancel_calls = 0
        self.submission = Submission()

    async def _place(self, method, instrument, quantity, price=None, expiry=None):
        self.calls.append((method, instrument, quantity, price))
        if self.scenario.reject:
            return VenueOrderRejected('scripted rejection')
        return VenueOrderAccepted(self.submission, quantity, price)

    async def place_market_buy(self, instrument, quantity, *, time_in_force):
        assert time_in_force is self.scenario.tif
        return await self._place('market_buy', instrument, quantity)

    async def place_market_sell(self, instrument, quantity, *, time_in_force):
        assert time_in_force is self.scenario.tif
        return await self._place('market_sell', instrument, quantity)

    async def place_limit_buy(self, instrument, quantity, price, expiry, *, time_in_force):
        assert time_in_force is self.scenario.tif
        return await self._place('limit_buy', instrument, quantity, price, expiry)

    async def place_limit_sell(self, instrument, quantity, price, expiry, *, time_in_force):
        assert time_in_force is self.scenario.tif
        return await self._place('limit_sell', instrument, quantity, price, expiry)

    async def monitor_order(self, submission):
        if self.scenario.later_levels is not None:
            await self.release_later.wait()
        for quantity, price in self.scenario.fills:
            yield VenueOrderFill(submission, D(quantity), D(price))
        if self.scenario.auto_cancel:
            yield VenueOrderCancelled(submission)
        else:
            # Remain monitorable until a user/shutdown cancel. A full fill causes
            # the executor to close this generator before reaching the wait.
            await self.cancelled.wait()
            yield VenueOrderCancelled(submission)

    async def cancel_order(self, submission):
        self.cancel_calls += 1
        self.cancelled.set()
        return VenueCancellationAccepted()


class ScriptedBacktestAdapter(BacktestAdapter[str]):
    def __init__(self, scenario):
        self.scenario = scenario
        self.calls = []

    def instrument_key(self, instrument):
        return instrument

    def normalise_event(self, event):
        return event.payload if isinstance(event.payload, BookUpdate) else None

    def validate_place_order(self, instrument, order):
        method = ('market_' if order.limit_price is None else 'limit_') + order.side.value
        self.calls.append((method, instrument, order.quantity, order.limit_price))
        if self.scenario.reject:
            return SimRejected('scripted rejection')
        return SimAccepted(order.quantity, order.limit_price, 'venue-order')


@dataclass
class StrategyState:
    # Compare semantic payloads directly: no rewriting fill quantities or IDs.
    trace: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    terminations: list = field(default_factory=list)
    event_count: int = 0
    filled_quantity: D = D('0')
    fill_cost: D = D('0')
    avg_fill_price: D | None = None


class ParityLogic:
    def __init__(self, scenario):
        self.scenario = scenario
        self.observed = asyncio.Queue()

    def is_heavy(self, payload):
        return False

    def handle_event(self, event, state, params):
        payload = event.payload
        state.event_count += 1
        state.trace.append(payload)
        if isinstance(payload, (OrderPartiallyFilled, OrderFilled)):
            state.filled_quantity += payload.fill_quantity
            state.fill_cost += payload.fill_quantity * payload.fill_price
            if isinstance(payload, OrderFilled):
                state.avg_fill_price = payload.avg_fill_price
        self.observed.put_nowait(payload)
        outputs = []
        if isinstance(payload, Begin):
            outputs.append(self.scenario.intent())
        elif self.scenario.cancel_after and isinstance(payload, self.scenario.cancel_after):
            outputs.append(CancelOrderIntent('cancel', 'order'))
        if isinstance(payload, (OrderFilled, OrderCancelled, OrderSubmissionRejected)):
            outputs.append(StopStrategy(policy=self.scenario.policy, grace_period=0))
        elif isinstance(payload, OrderSubmitted) and self.scenario.stop_on_submission:
            outputs.append(StopStrategy(policy=self.scenario.policy, grace_period=0))
        state.outputs.extend(outputs)
        return outputs

    def handle_termination(self, termination, state, params):
        state.terminations.append(termination.reason)
        return []


async def observed(logic, payload_type):
    async with asyncio.timeout(TIMEOUT):
        while True:
            payload = await logic.observed.get()
            if isinstance(payload, payload_type):
                return payload


async def run_live(scenario):
    adapter = ScriptedLiveAdapter(scenario)
    executor = LiveExecutor(adapter, {'home': 'token'})
    engine = LiveEngine()
    state, logic = StrategyState(), ParityLogic(scenario)
    instance = StrategyInstance('match', {}, state, logic, executor)
    handle = engine.register_strategy('match', instance)
    try:
        await handle.queue.put(EventEnvelope(datetime.now(), scenario.book(), 'match'))
        await handle.queue.put(EventEnvelope(datetime.now(), Begin(), 'match'))
        if scenario.later_levels is not None:
            await observed(logic, OrderSubmitted)
            await handle.queue.put(EventEnvelope(datetime.now(), scenario.book(True), 'match'))
            # Ensure strategy saw the historical update before allowing the
            # scripted fill to enter the executor/engine queue.
            await asyncio.wait_for(handle.queue.join(), TIMEOUT)
            adapter.release_later.set()
        result = await asyncio.wait_for(asyncio.shield(handle.done), TIMEOUT)
        assert handle.queue.empty()
        assert not executor.tasks
        return state, result, adapter.calls, adapter.cancel_calls, executor
    finally:
        adapter.release_later.set()
        adapter.cancelled.set()
        await engine.shutdown()


async def run_backtest(scenario, streamed=False):
    adapter = ScriptedBacktestAdapter(scenario)
    executor = BacktestExecutor(0, adapter, {'home': 'token'})
    engine = BacktestEngine()
    state, logic = StrategyState(), ParityLogic(scenario)
    instance = StrategyInstance[BacktestExecutorProtocol]('match', {}, state, logic, executor)
    start = datetime(2026, 1, 1)
    history = [EventEnvelope(start, scenario.book(), 'match'), EventEnvelope(start, Begin(), 'match')]
    if scenario.later_levels is not None:
        history.append(EventEnvelope(start + timedelta(seconds=1), scenario.book(True), 'match'))
    async def source():
        for event in history:
            yield event
    result = await engine.backtest_instance(instance, source() if streamed else history)
    return state, result, adapter.calls, executor
