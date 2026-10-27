import asyncio
from datetime import timedelta

import pytest

from PredictionMarketEventSniper.core.outputs import StopStrategy, ScheduleTimer
from PredictionMarketEventSniper.engine.models import StrategyExitReason
from .helpers import T, envelope, make_run, TrackingSource, payloads


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed', [False, True])
async def test_empty_history_finalises_without_callbacks_or_shutdown(streamed):
    h = make_run()
    result = await h.engine.backtest_instance(h.instance, TrackingSource([]) if streamed else [])
    assert result.instance_id == 'match'
    assert result.reason is StrategyExitReason.COMPLETED
    assert result.exception is None
    assert not h.logic.events and not h.executor.events
    assert not h.executor.shutdown_calls
    assert h.executor.finalise_calls == 1
    assert h.engine.current_time is None


@pytest.mark.asyncio
@pytest.mark.parametrize('instance_id', [None, 'match'])
async def test_matching_or_absent_external_instance_id_is_preserved(instance_id):
    h = make_run()
    original = envelope('external', instance_id=instance_id)
    await h.engine.backtest_instance(h.instance, [original])
    assert h.logic.events[0] is original
    assert h.logic.events[0].instance_id == instance_id


@pytest.mark.asyncio
async def test_wrong_instance_is_rejected_before_either_callback():
    h = make_run()
    with pytest.raises(ValueError, match='another instance'):
        await h.engine.backtest_instance(h.instance, [envelope('wrong', instance_id='other')])
    assert not h.logic.events and not h.executor.events
    assert h.executor.closed


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed', [False, True])
async def test_decreasing_external_timestamps_raise(streamed):
    h = make_run()
    items = [envelope('first', 2), envelope('backwards', 1)]
    with pytest.raises(ValueError, match='timestamp ordered'):
        await h.engine.backtest_instance(h.instance, TrackingSource(items) if streamed else items)
    assert payloads(h.logic.events) == ['first']
    assert h.executor.closed


@pytest.mark.asyncio
async def test_async_source_is_only_read_one_event_ahead():
    h = make_run()
    source = TrackingSource(envelope(str(i), i) for i in range(20))
    h.logic.on_event = lambda event: [StopStrategy(grace_period=0)]
    await h.engine.backtest_instance(h.instance, source)
    assert payloads(h.logic.events) == ['0']
    assert source.reads == 2  # First processed; one lookahead establishes deadline.
    assert source.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('stop', [False, True])
async def test_sync_generator_is_closed_on_exhaustion_or_early_stop(stop):
    h = make_run()
    closed = []
    def source():
        try:
            yield envelope('first')
            yield envelope('beyond-deadline', 10)
            yield envelope('not-read', 20)
        finally:
            closed.append(True)
    if stop:
        h.logic.on_event = lambda event: [StopStrategy(grace_period=0)]
    await h.engine.backtest_instance(h.instance, source())
    assert closed == [True]


@pytest.mark.asyncio
async def test_reuse_resets_clock_heap_and_cancelled_timer_state():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('id', timedelta(hours=1)), StopStrategy(grace_period=0)]
    await h.engine.backtest_instance(h.instance, [envelope('stop', 100)])
    fresh = make_run()
    await h.engine.backtest_instance(fresh.instance, [envelope('new', 0)])
    assert payloads(fresh.logic.events) == ['new']
    assert h.engine.current_time == T
    assert fresh.executor.finalise_calls == 1


@pytest.mark.asyncio
async def test_concurrent_run_rejected_without_touching_active_or_second_executor():
    h, other = make_run(), make_run()
    entered, release = asyncio.Event(), asyncio.Event()
    async def source():
        entered.set()
        await release.wait()
        yield envelope('first')
    task = asyncio.create_task(h.engine.backtest_instance(h.instance, source()))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(RuntimeError, match='active run'):
            await h.engine.backtest_instance(other.instance, [])
        assert other.executor.finalise_calls == 0
        assert h.executor.finalise_calls == 0
        release.set()
        result = await asyncio.wait_for(task, 1)
        assert result.reason is StrategyExitReason.COMPLETED
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_separate_engine_objects_can_run_concurrently():
    a, b = make_run(), make_run()
    results = await asyncio.gather(
        a.engine.backtest_instance(a.instance, [envelope('a')]),
        b.engine.backtest_instance(b.instance, [envelope('b')]),
    )
    assert all(result.reason is StrategyExitReason.COMPLETED for result in results)
    assert payloads(a.logic.events) == ['a']
    assert payloads(b.logic.events) == ['b']
