"""Focused shutdown regressions; broader backtest engine tests can extend these."""
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal as D

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope, TimerExpired
from PredictionMarketEventSniper.core.events.execution import OrderSubmitted
from PredictionMarketEventSniper.core.models.orders import OrderSide, OrderType, TimeInForce
from PredictionMarketEventSniper.core.outputs import PlaceOrderIntent, ScheduleTimer, StopStrategy, ShutdownPolicy, StrategyOutput
from PredictionMarketEventSniper.engine.backtest_engine import BacktestEngine
from PredictionMarketEventSniper.engine.models import StrategyExitReason
from PredictionMarketEventSniper.executors.backtest.backtest_executor import BacktestExecutor
from PredictionMarketEventSniper.executors.backtest.protocols import BacktestExecutorProtocol
from PredictionMarketEventSniper.executors.backtest.models import BookUpdate, BookLevel, BookSide, BookUpdateKind
from PredictionMarketEventSniper.executors.backtest.venue_adapters.testing_mock import MockBacktestVenueAdapter
from PredictionMarketEventSniper.executors.models import ExecutionState
from PredictionMarketEventSniper.strategy.base import StrategyInstance

T = datetime(2026,1,1)


def envelope(payload, at: float = 0.0):
    return EventEnvelope(T+timedelta(seconds=at),payload,'match')


def book(price: str = '.7'):
    return BookUpdate('token',BookUpdateKind.SNAPSHOT,(BookLevel(BookSide.ASK,D(price),D('5')),))


class Logic:
    def __init__(self, grace: float = 1.0, policy=ShutdownPolicy.CANCEL_OPEN_ORDERS,
                 stop_on='submitted', timer=False):
        self.grace=grace
        self.policy=policy
        self.stop_on=stop_on
        self.timer=timer
        self.seen=[]

    def handle_event(self,event,state,params) -> list[StrategyOutput]:
        self.seen.append(event.payload)
        if event.payload=='begin':
            outputs: list[StrategyOutput] = [PlaceOrderIntent('order','home',OrderSide.BUY,D('5'),OrderType.LIMIT,TimeInForce.GTC,0,D('.5'))]
            if self.timer:
                outputs.append(ScheduleTimer('strategy-timer',timedelta(seconds=.6)))
            if self.stop_on=='begin':
                outputs.append(StopStrategy(self.policy,self.grace))
            return outputs
        if isinstance(event.payload,OrderSubmitted) and self.stop_on=='submitted':
            return [StopStrategy(self.policy,self.grace)]
        return []

    def handle_termination(self,*args,**kwargs):
        raise AssertionError('Backtest must not call strategy termination hook')

    def is_heavy(self,payload):
        return False


def setup(logic):
    executor=BacktestExecutor(.1,MockBacktestVenueAdapter(.2),{'home':'token'})
    instance=StrategyInstance[BacktestExecutorProtocol]('match',{}, {},logic,executor)
    return BacktestEngine(),executor,instance


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed',[False,True])
async def test_cleanup_preserves_lookahead_and_fills_before_cancel_without_callbacks(streamed):
    logic=Logic(timer=True)
    engine,executor,instance=setup(logic)
    history=[envelope(book()),envelope('begin'),envelope(book('.4'),.5)]
    async def source():
        for event in history:
            yield event
    result=await engine.backtest_instance(instance,source() if streamed else history)
    assert result.reason is StrategyExitReason.COMPLETED
    assert len(logic.seen)==3  # initial book, begin, acceptance; no cleanup callbacks
    assert not any(isinstance(event,TimerExpired) for event in logic.seen)
    assert executor.fills[0].matched_at==T+timedelta(seconds=.5)
    assert executor.positions['home']==D('5')
    assert executor.execution_records['order'].state is ExecutionState.FILLED
    assert executor.get_summary().pending_request_ids==()


@pytest.mark.asyncio
async def test_cleanup_cancels_at_venue_time_not_stop_time():
    logic=Logic()
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin')])
    record=executor.execution_records['order']
    assert record.state is ExecutionState.CANCELLED
    assert record.cancelled_at==T+timedelta(seconds=.7)
    assert executor.get_summary().cleanup_deadline==T+timedelta(seconds=1.4)
    assert not executor.fills


@pytest.mark.asyncio
async def test_deadline_retains_unresolved_requests_instead_of_inventing_cancel():
    logic=Logic(grace=.05)
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin'),envelope(book('.4'),.5)])
    summary=executor.get_summary()
    assert summary.open_order_ids==('order',)
    assert len(summary.pending_request_ids)==1
    assert executor.execution_records['order'].cancelled_at is None
    assert not summary.fills
    assert engine.current_time==T+timedelta(seconds=.4)


@pytest.mark.asyncio
@pytest.mark.parametrize('grace',[0.0,.3])
async def test_cancel_processed_at_inclusive_deadline(grace):
    logic=Logic(grace=grace)
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin')])
    assert executor.execution_records['order'].state is (
        ExecutionState.SUBMITTED if grace==0 else ExecutionState.CANCELLED)


@pytest.mark.asyncio
async def test_leave_open_orders_can_fill_during_grace():
    logic=Logic(policy=ShutdownPolicy.LEAVE_OPEN_ORDERS)
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin'),envelope(book('.4'),.5)])
    assert executor.positions['home']==D('5')
    assert executor.get_summary().pending_request_ids==()
    assert len(logic.seen)==3


@pytest.mark.asyncio
@pytest.mark.parametrize('grace',[.1,1.0])
async def test_pending_submission_during_shutdown(grace):
    logic=Logic(grace=grace,stop_on='begin')
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin')])
    record=executor.execution_records['order']
    assert record.state is (ExecutionState.PENDING_SUBMISSION if grace==.1 else ExecutionState.CANCELLED)
    assert len(logic.seen)==2
    assert not executor.fills


@pytest.mark.asyncio
async def test_natural_exhaustion_preserves_open_order_without_shutdown_trading():
    logic=Logic(stop_on='never')
    engine,executor,instance=setup(logic)
    await engine.backtest_instance(instance,[envelope(book()),envelope('begin')])
    assert executor.get_summary().open_order_ids==('order',)
    assert executor.get_summary().shutdown_started_at is None
    assert executor.get_summary().pending_request_ids==()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind',['strategy','source','executor'])
async def test_errors_propagate_and_finalise_without_strategy_cleanup(kind):
    logic=Logic(stop_on='never')
    engine,executor,instance=setup(logic)
    error=RuntimeError('original failure')
    if kind=='strategy':
        def fail(*args,**kwargs): raise error
        logic.handle_event=fail
    elif kind=='executor':
        def fail(*args,**kwargs): raise error
        executor.adapter.normalise_event=fail
    async def source():
        if kind=='source':
            raise error
        yield envelope(book())
    with pytest.raises(RuntimeError) as caught:
        await engine.backtest_instance(instance,source())
    assert caught.value is error
    assert executor._closed
    assert executor.get_summary().shutdown_started_at is None
    assert not engine._running
    # Engine reuse after error is supported with a fresh instance/executor.
    _,fresh_executor,fresh_instance=setup(Logic())
    assert (await engine.backtest_instance(fresh_instance,[])).reason is StrategyExitReason.COMPLETED
    assert fresh_executor._closed


@pytest.mark.asyncio
async def test_cancellation_propagates_and_finalises():
    engine,executor,instance=setup(Logic())
    entered=asyncio.Event()
    async def source():
        entered.set()
        await asyncio.Event().wait()
        yield envelope(book())
    task=asyncio.create_task(engine.backtest_instance(instance,source()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert executor._closed
    assert not engine._running


@pytest.mark.asyncio
async def test_cleanup_error_does_not_hide_original_error():
    engine,executor,instance=setup(Logic())
    original=RuntimeError('source error')
    async def source():
        raise original
        yield envelope(book())
    async def broken_finalise():
        raise ValueError('finalise failed')
    executor.finalise=broken_finalise
    with pytest.raises(RuntimeError) as caught:
        await engine.backtest_instance(instance,source())
    assert caught.value is original
    assert any('finalise failed' in note for note in original.__notes__)
