from datetime import timedelta

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope, TimerExpired
from PredictionMarketEventSniper.core.outputs import ScheduleTimer, StopStrategy, ShutdownPolicy
from PredictionMarketEventSniper.executors.backtest.models import ExecutionIntentArrived, OrderExpiryDue, ScheduledExecutionEvent
from .helpers import T, ProbeEvent, envelope, make_run, payloads, request


@pytest.mark.asyncio
async def test_original_historical_envelope_reaches_both_callbacks_executor_first():
    h = make_run()
    original = envelope({'arbitrary': 'payload'})
    def strategy(event: EventEnvelope[object]):
        assert h.executor.events[-1] is event
        return []
    h.logic.on_event = strategy
    await h.engine.backtest_instance(h.instance, [original])
    assert h.executor.events == h.logic.events == [original]
    assert h.logic.events[0] is original
    assert h.logic.heavy_calls == 0


@pytest.mark.asyncio
async def test_intent_is_dispatched_once_at_engine_time_and_arrival_is_executor_only():
    h = make_run()
    intent = request()
    h.executor.intent_delay = .25
    h.logic.on_event = lambda event: [intent] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start', 7)])
    assert h.executor.intents == [(intent, T + timedelta(seconds=7))]
    assert payloads(h.logic.events) == ['start']
    assert payloads(h.executor.events) == ['start', ExecutionIntentArrived(intent)]
    arrival = h.executor.events[-1]
    assert arrival.timestamp == T + timedelta(seconds=7.25)
    assert arrival.instance_id == 'match'


@pytest.mark.asyncio
@pytest.mark.parametrize('internal', [ExecutionIntentArrived(request()), OrderExpiryDue('order')])
async def test_internal_executor_events_never_reach_strategy(internal):
    h = make_run()
    h.executor.on_event = lambda event: [ScheduledExecutionEvent(internal, 1)] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert payloads(h.logic.events) == ['start']
    assert payloads(h.executor.events) == ['start', internal]


@pytest.mark.asyncio
async def test_strategy_timer_is_not_forwarded_to_executor():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('only-strategy', timedelta(seconds=1))] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert payloads(h.executor.events) == ['start']
    assert payloads(h.logic.events) == ['start', TimerExpired('only-strategy')]
    assert h.logic.events[-1].instance_id == 'match'


@pytest.mark.asyncio
async def test_execution_notification_reaches_executor_then_strategy_unchanged():
    h = make_run()
    notification = ProbeEvent('notification')
    h.executor.on_event = lambda event: [ScheduledExecutionEvent(notification, .1)] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert h.executor.events[-1] is h.logic.events[-1]
    assert h.logic.events[-1].payload is notification


@pytest.mark.asyncio
async def test_stop_ignores_later_outputs_and_cleanup_never_calls_strategy():
    h = make_run()
    before, after = request('before'), request('after')
    h.logic.on_event = lambda event: [before, StopStrategy(grace_period=1), after, ScheduleTimer('ignored', timedelta(0))]
    h.executor.shutdown_events = [ScheduledExecutionEvent(ProbeEvent('cleanup'), .5)]
    await h.engine.backtest_instance(h.instance, [envelope('stop'), envelope('historical-cleanup', .25)])
    assert [intent for intent, _ in h.executor.intents] == [before]
    assert payloads(h.logic.events) == ['stop']
    assert payloads(h.executor.events) == ['stop', ExecutionIntentArrived(before), 'historical-cleanup', ProbeEvent('cleanup')]
    assert h.logic.termination_calls == 0
    assert h.executor.finalise_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('policy,cancel', [(ShutdownPolicy.CANCEL_OPEN_ORDERS, True), (ShutdownPolicy.LEAVE_OPEN_ORDERS, False)])
@pytest.mark.parametrize('grace,expected', [(None, 2.0), (0.0, 0.0), (.5, .5)])
async def test_stop_passes_timestamp_grace_and_policy_to_executor(policy, cancel, grace, expected):
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(policy, grace)]
    await h.engine.backtest_instance(h.instance, [envelope('stop', 3)])
    assert h.executor.shutdown_calls == [(T + timedelta(seconds=3), expected, cancel)]
