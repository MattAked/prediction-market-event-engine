"""Engine-only cleanup routing checks complement real-executor shutdown tests."""
from datetime import timedelta

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.outputs import StopStrategy
from PredictionMarketEventSniper.executors.backtest.models import ScheduledExecutionEvent
from .helpers import T, ProbeEvent, envelope, make_run, payloads, TrackingSource


@pytest.mark.asyncio
async def test_cleanup_merges_history_and_new_executor_events_at_inclusive_deadline():
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(grace_period=1)]
    h.executor.shutdown_events = [ScheduledExecutionEvent(ProbeEvent('internal-first'), .5)]
    def generated(event: EventEnvelope[object]) -> list[ScheduledExecutionEvent]:
        if event.payload == ProbeEvent('internal-first'):
            return [ScheduledExecutionEvent(ProbeEvent('at-deadline'), .5),
                    ScheduledExecutionEvent(ProbeEvent('past-deadline'), .500001)]
        return []
    h.executor.on_event = generated
    history = [envelope('stop'), envelope('history-at-half', .5),
               envelope('history-at-deadline', 1), envelope('too-late', 1.1)]
    await h.engine.backtest_instance(h.instance, history)
    assert payloads(h.logic.events) == ['stop']
    assert payloads(h.executor.events) == [
        'stop', ProbeEvent('internal-first'), 'history-at-half',
        ProbeEvent('at-deadline'), 'history-at-deadline',
    ]
    assert h.engine.current_time == T + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_zero_grace_processes_due_now_but_not_future_cleanup_events():
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(grace_period=0)]
    h.executor.shutdown_events = [ScheduledExecutionEvent(ProbeEvent('now'), 0),
                                  ScheduledExecutionEvent(ProbeEvent('future'), .000001)]
    await h.engine.backtest_instance(h.instance, [envelope('stop')])
    assert payloads(h.executor.events) == ['stop', ProbeEvent('now')]
    assert payloads(h.logic.events) == ['stop']
    assert h.engine.current_time == T


@pytest.mark.asyncio
async def test_stop_from_internal_event_keeps_prefetched_historical_item():
    h = make_run()
    h.executor.on_event = lambda event: [ScheduledExecutionEvent(ProbeEvent('stop-signal'), 1)] if event.payload == 'start' else []
    h.logic.on_event = lambda event: [StopStrategy(grace_period=2)] if event.payload == ProbeEvent('stop-signal') else []
    source = TrackingSource([envelope('start'), envelope('prefetched', 2), envelope('beyond', 4)])
    await h.engine.backtest_instance(h.instance, source)
    assert payloads(h.logic.events) == ['start', ProbeEvent('stop-signal')]
    assert payloads(h.executor.events) == ['start', ProbeEvent('stop-signal'), 'prefetched']
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_executor_error_during_cleanup_propagates_and_finalises():
    h = make_run()
    original = RuntimeError('cleanup processing failed')
    h.logic.on_event = lambda event: [StopStrategy(grace_period=1)]
    def generated(event: EventEnvelope[object]) -> list[ScheduledExecutionEvent]:
        if event.payload == 'cleanup-history':
            raise original
        return []
    h.executor.on_event = generated
    source = TrackingSource([envelope('stop'), envelope('cleanup-history', .5)])
    with pytest.raises(RuntimeError) as caught:
        await h.engine.backtest_instance(h.instance, source)
    assert caught.value is original
    assert h.executor.finalise_calls == 1
    assert source.close_calls == 1
    assert h.logic.termination_calls == 0


@pytest.mark.asyncio
async def test_source_error_during_cleanup_is_not_a_successful_partial_result():
    h = make_run()
    original = OSError('historical read failed during grace')
    h.logic.on_event = lambda event: [StopStrategy(grace_period=1)]
    async def source():
        yield envelope('stop')
        raise original
    with pytest.raises(OSError) as caught:
        await h.engine.backtest_instance(h.instance, source())
    assert caught.value is original
    assert h.executor.finalise_calls == 1
    assert payloads(h.logic.events) == ['stop']
