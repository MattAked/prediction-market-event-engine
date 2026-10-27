from decimal import Decimal as D

import pytest

from PredictionMarketEventSniper.core.events.execution import (
    OrderSubmitted, OrderFilled, OrderPartiallyFilled, OrderCancelled, OrderSubmissionRejected,
)
from PredictionMarketEventSniper.core.models.orders import OrderSide, TimeInForce
from PredictionMarketEventSniper.core.outputs import ShutdownPolicy
from PredictionMarketEventSniper.engine.models import StrategyExitReason
from PredictionMarketEventSniper.executors.models import ExecutionState
from .helpers import Scenario, Begin, run_live, run_backtest


CASES = [
    Scenario('market_buy'),
    Scenario('market_sell', side=OrderSide.SELL, levels=(('.6','5'),), fills=(('5','.6'),)),
    Scenario('limit_buy', price=D('.5'), tif=TimeInForce.GTC),
    Scenario('limit_sell', side=OrderSide.SELL, price=D('.5'), tif=TimeInForce.GTC,
             levels=(('.6','5'),), fills=(('5','.6'),)),
    Scenario('ioc_partial_cancel', levels=(('.4','2'),), fills=(('2','.4'),),auto_cancel=True),
    Scenario('ioc_empty_cancel', levels=(),fills=(),auto_cancel=True),
    Scenario('fok_insufficient',tif=TimeInForce.FOK,levels=(('.4','2'),),fills=(),auto_cancel=True),
    Scenario('fok_full',tif=TimeInForce.FOK),
    Scenario('resting_limit',price=D('.5'),tif=TimeInForce.GTC,levels=(('.7','5'),),later_levels=(('.4','5'),)),
    Scenario('rejection',reject=True,fills=()),
    Scenario('stop_cancel_open',price=D('.5'),tif=TimeInForce.GTC,levels=(),fills=(),stop_on_submission=True),
    Scenario('stop_leave_open',price=D('.5'),tif=TimeInForce.GTC,levels=(),fills=(),stop_on_submission=True,
             policy=ShutdownPolicy.LEAVE_OPEN_ORDERS),
]


def expected_trace(scenario):
    trace=[scenario.book(),Begin()]
    order=scenario.intent()
    if scenario.reject:
        return trace+[OrderSubmissionRejected('order','home','scripted rejection')]
    trace.append(OrderSubmitted('order','venue-order','home',scenario.side,D('5'),scenario.price))
    if scenario.stop_on_submission:
        return trace
    if scenario.later_levels is not None:
        trace.append(scenario.book(True))
    total=D('0')
    cost=D('0')
    for quantity,price in scenario.fills:
        total+=D(quantity)
        cost+=D(quantity)*D(price)
        if total==D('5'):
            trace.append(OrderFilled('order','venue-order','home',D(quantity),D(price),cost/total))
        else:
            trace.append(OrderPartiallyFilled('order','venue-order','home',D(quantity),D(price),D('5')-total))
    if scenario.auto_cancel or scenario.cancel_after:
        cancellation_id='order'
        trace.append(OrderCancelled(cancellation_id,'venue-order','home',total,D('5')-total))
    return trace


async def assert_parity(scenario, streamed):
    live,live_exit,live_calls,cancels,live_executor=await run_live(scenario)
    back,back_exit,back_calls,back_executor=await run_backtest(scenario,streamed)
    expected=expected_trace(scenario)
    # Each mode must independently satisfy the scenario, not just agree.
    assert live.trace==expected, 'Live trace differs from the scenario oracle'
    assert back.trace == expected, 'Backtest trace differs from the scenario oracle'
    assert live.trace==back.trace
    assert live.filled_quantity == back.filled_quantity
    assert live.fill_cost == back.fill_cost
    assert live.avg_fill_price == back.avg_fill_price
    if not scenario.stop_on_submission:
        expected_quantity = sum((D(q) for q, _ in scenario.fills), D('0'))
        expected_cost = sum((D(q) * D(p) for q, p in scenario.fills), D('0'))
        assert live.filled_quantity == expected_quantity
        assert live.fill_cost == expected_cost
        if expected_quantity == D('5'):
            assert live.avg_fill_price == expected_cost / expected_quantity
    assert live.outputs==back.outputs
    assert live.event_count==back.event_count
    assert live.terminations == [StrategyExitReason.COMPLETED]
    assert back.terminations == []  # Backtests intentionally skip termination hooks.
    assert live_exit==back_exit
    assert live_exit.exception is None
    assert live_calls==back_calls
    if scenario.stop_on_submission:
        leave=scenario.policy is ShutdownPolicy.LEAVE_OPEN_ORDERS
        assert cancels==(0 if leave else 1)
        assert back_executor.get_summary().open_order_ids==(('order',) if leave else ())
        assert back_executor.execution_records['order'].state is (
            ExecutionState.SUBMITTED if leave else ExecutionState.CANCELLED)
    else:
        assert back_executor.get_summary().open_order_ids==()
    return live,back


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed',[False,True],ids=['list','async-source'])
@pytest.mark.parametrize('scenario',CASES,ids=lambda s:s.name)
async def test_pipeline_parity(scenario,streamed):
    await assert_parity(scenario,streamed)


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed',[False,True],ids=['list','async-source'])
async def test_multilevel_final_fill_contract(streamed):
    scenario=Scenario('multilevel',levels=(('.4','2'),('.5','3')),fills=(('2','.4'),('3','.5')))
    await assert_parity(scenario,streamed)


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed',[False,True],ids=['list','async-source'])
@pytest.mark.parametrize('partial',[False,True],ids=['unfilled','partially-filled'])
async def test_explicit_cancel_identifier_contract(streamed,partial):
    scenario=Scenario('explicit_cancel',price=D('.5'),tif=TimeInForce.GTC,
                      levels=(('.4','2'),) if partial else (),
                      fills=(('2','.4'),) if partial else (),
                      cancel_after=OrderPartiallyFilled if partial else OrderSubmitted)
    await assert_parity(scenario,streamed)
