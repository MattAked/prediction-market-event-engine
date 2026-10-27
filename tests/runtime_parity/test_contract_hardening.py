"""Regressions for adapter instructions, real mock strategy and timer identity."""
from datetime import datetime, timedelta
from decimal import Decimal as D
from unittest.mock import AsyncMock
import asyncio

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.execution import OrderFilled, OrderPartiallyFilled
from PredictionMarketEventSniper.core.models.orders import OrderSide, TimeInForce
from PredictionMarketEventSniper.core.outputs import ScheduleTimer, CancelTimer, StrategyOutput
from PredictionMarketEventSniper.engine.models import StrategyTermination
from PredictionMarketEventSniper.engine.live_engine import LiveEngine
from PredictionMarketEventSniper.engine.backtest_engine import BacktestEngine
from PredictionMarketEventSniper.executors.live.live_executor import LiveExecutor
from PredictionMarketEventSniper.executors.live.protocols import QueueEventSink
from PredictionMarketEventSniper.strategies.mock_scenario.strategy import PredictedNumberStrategy
from PredictionMarketEventSniper.strategies.mock_scenario.state import PredictedNumberState, PredictedNumberParams
from PredictionMarketEventSniper.strategy.base import StrategyInstance
from PredictionMarketEventSniper.executors.backtest.backtest_executor import BacktestExecutor
from PredictionMarketEventSniper.executors.backtest.protocols import BacktestExecutorProtocol
from PredictionMarketEventSniper.executors.backtest.venue_adapters.testing_mock import MockBacktestVenueAdapter
from .helpers import Scenario, ScriptedLiveAdapter


class NoOpLogic:
    """Valid strategy implementation for tests that only dispatch timer outputs."""

    def handle_event(
        self, event: EventEnvelope, state: object, params: object,
    ) -> list[StrategyOutput]:
        return []

    def handle_termination(
        self, termination: StrategyTermination, state: object, params: object,
    ) -> list[StrategyOutput]:
        return []

    def is_heavy(self, payload: object) -> bool:
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize('side', list(OrderSide))
@pytest.mark.parametrize('price,tif', [
    (None,TimeInForce.IOC),(None,TimeInForce.FOK),
    (D('.5'),TimeInForce.IOC),(D('.5'),TimeInForce.FOK),(D('.5'),TimeInForce.GTC),
])
async def test_exact_tif_reaches_live_placement_adapter(side,price,tif):
    scenario=Scenario('tif',side=side,price=price,tif=tif)
    adapter=ScriptedLiveAdapter(scenario)
    method_name=('place_market_' if price is None else 'place_limit_')+side.value
    call=AsyncMock(wraps=getattr(adapter,method_name))
    setattr(adapter,method_name,call)
    executor=LiveExecutor(adapter,{'home':'token'})
    executor.set_event_sink(QueueEventSink('match',asyncio.Queue()))
    await executor.handle_place_order(scenario.intent())
    assert call.await_count==1
    awaited = call.await_args
    assert awaited is not None
    assert awaited.kwargs=={'time_in_force':tif}
    assert awaited.args[:2]==('token',D('5'))
    await executor.shutdown(grace_period=0)


def test_existing_mock_strategy_accumulates_final_incremental_fill():
    logic=PredictedNumberStrategy()
    state=PredictedNumberState(active_order_intent_id='order',active_order_side=OrderSide.BUY)
    params=PredictedNumberParams()
    now=datetime(2026,1,1)
    logic.handle_event(EventEnvelope(now,OrderPartiallyFilled('order','venue','mock',D('3'),D('.4'),D('3')),'match'),state,params)
    assert state.position_quantity==D('3')
    logic.handle_event(EventEnvelope(now,OrderFilled('order','venue','mock',D('3'),D('.5'),D('.45')),'match'),state,params)
    assert state.position_quantity==D('6')
    assert state.active_order_intent_id is None


@pytest.mark.asyncio
async def test_live_duplicate_timer_rejected_cancel_and_expiry_allow_reuse():
    engine=LiveEngine()
    executor = LiveExecutor(ScriptedLiveAdapter(Scenario('timer')), {'home': 'token'})
    instance = StrategyInstance('match', {}, {}, NoOpLogic(), executor)
    timer=ScheduleTimer('id',timedelta(hours=1))
    try:
        await engine._process_strat_output(instance,timer)
        original=engine.timer_tasks[('match','id')]
        with pytest.raises(ValueError,match='already active'):
            await engine._process_strat_output(instance,timer)
        assert engine.timer_tasks[('match','id')] is original
        await engine._process_strat_output(instance,CancelTimer('id'))
        assert ('match','id') not in engine.timer_tasks
        await engine._process_strat_output(instance,ScheduleTimer('id',timedelta(0)))
        expiring=engine.timer_tasks[('match','id')]
        await expiring
        assert ('match','id') not in engine.timer_tasks
        await engine._process_strat_output(instance,timer)
    finally:
        tasks=list(engine.timer_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await engine.shutdown()


@pytest.mark.asyncio
async def test_backtest_duplicate_timer_rejected_cancel_and_expiry_allow_reuse():
    engine=BacktestEngine()
    engine._current_time=datetime(2026,1,1)
    executor = BacktestExecutor(0, MockBacktestVenueAdapter(), {'home': 'token'})
    instance = StrategyInstance[BacktestExecutorProtocol](
        'match', {}, {}, NoOpLogic(), executor,
    )
    timer=ScheduleTimer('id',timedelta(hours=1))
    await engine._process_output(instance,timer)
    original=engine._timers['id']
    with pytest.raises(ValueError,match='already active'):
        await engine._process_output(instance,timer)
    assert engine._timers['id'] is original
    await engine._process_output(instance,CancelTimer('id'))
    assert original.cancelled
    await engine._process_output(instance,ScheduleTimer('id',timedelta(0)))
    assert engine._pop_internal_event().payload.timer_id=='id'
    assert 'id' not in engine._timers
    await engine._process_output(instance,timer)
