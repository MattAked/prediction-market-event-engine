from datetime import timedelta

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope, TimerExpired
from PredictionMarketEventSniper.core.outputs import ScheduleTimer, StrategyOutput
from PredictionMarketEventSniper.executors.backtest.models import ScheduledExecutionEvent
from .helpers import T, ProbeEvent, envelope, make_run, payloads, TrackingSource


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed', [False, True])
async def test_merges_reentrant_synthetic_events_and_equal_time_history(streamed: bool):
    h = make_run()
    def generated(event: EventEnvelope[object]) -> list[ScheduledExecutionEvent]:
        if event.payload == 'start':
            return [ScheduledExecutionEvent(ProbeEvent('late'), 3),
                    ScheduledExecutionEvent(ProbeEvent('early'), 1),
                    ScheduledExecutionEvent(ProbeEvent('tie'), 2)]
        if event.payload == ProbeEvent('early'):
            return [ScheduledExecutionEvent(ProbeEvent('child'), .5)]
        return []
    h.executor.on_event = generated
    history = [envelope('start'), envelope('history-tie', 2), envelope('end', 4)]
    await h.engine.backtest_instance(h.instance, TrackingSource(history) if streamed else history)
    assert payloads(h.logic.events) == [
        'start', ProbeEvent('early'), ProbeEvent('child'), ProbeEvent('tie'),
        'history-tie', ProbeEvent('late'), 'end',
    ]
    assert [e.timestamp for e in h.logic.events] == [T + timedelta(seconds=t) for t in (0, 1, 1.5, 2, 2, 3, 4)]


@pytest.mark.asyncio
async def test_equal_time_internal_events_keep_insertion_order_including_children():
    h = make_run()
    def generated(event: EventEnvelope[object]) -> list[ScheduledExecutionEvent]:
        if event.payload == 'start':
            return [ScheduledExecutionEvent(ProbeEvent('a'), 0), ScheduledExecutionEvent(ProbeEvent('b'), 0)]
        if event.payload == ProbeEvent('a'):
            return [ScheduledExecutionEvent(ProbeEvent('child'), 0)]
        return []
    h.executor.on_event = generated
    await h.engine.backtest_instance(h.instance, [envelope('start'), envelope('external')])
    assert payloads(h.logic.events) == ['start', ProbeEvent('a'), ProbeEvent('b'), ProbeEvent('child'), 'external']


@pytest.mark.asyncio
async def test_equal_time_history_preserves_source_order_without_payload_comparison():
    h = make_run()
    first, second = object(), object()
    await h.engine.backtest_instance(h.instance, [envelope(first), envelope(second)])
    assert payloads(h.logic.events) == [first, second]


@pytest.mark.asyncio
async def test_heap_ties_never_compare_unorderable_payloads():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('z', timedelta(seconds=1)), ScheduleTimer('a', timedelta(seconds=1))] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert payloads(h.logic.events) == ['start', TimerExpired('z'), TimerExpired('a')]


@pytest.mark.asyncio
async def test_executor_outputs_precede_strategy_outputs_in_same_timestamp_heap():
    h = make_run()
    h.executor.on_event = lambda event: [ScheduledExecutionEvent(ProbeEvent('executor'), 0)] if event.payload == 'start' else []
    h.logic.on_event = lambda event: [ScheduleTimer('strategy', timedelta(0))] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start'), envelope('external')])
    assert payloads(h.logic.events) == ['start', ProbeEvent('executor'), TimerExpired('strategy'), 'external']


@pytest.mark.asyncio
async def test_remaining_internal_events_drain_after_history_exhaustion():
    h = make_run()
    h.executor.on_event = lambda event: [ScheduledExecutionEvent(ProbeEvent('after-history'), 3600)] if event.payload == 'start' else []
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert payloads(h.logic.events) == ['start', ProbeEvent('after-history')]
    assert h.engine.current_time == T + timedelta(hours=1)


@pytest.mark.asyncio
async def test_engine_clock_matches_callback_timestamp_and_processing_consumes_no_time():
    h = make_run()
    observed = []
    def strategy(event: EventEnvelope[object]) -> list[StrategyOutput]:
        observed.append(h.engine.current_time)
        return [ScheduleTimer('delay', timedelta(seconds=5))] if event.payload == 'start' else []
    h.logic.on_event = strategy
    await h.engine.backtest_instance(h.instance, [envelope('start', 10)])
    assert observed == [T + timedelta(seconds=10), T + timedelta(seconds=15)]
