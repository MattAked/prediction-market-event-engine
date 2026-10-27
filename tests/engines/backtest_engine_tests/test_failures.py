import asyncio

import pytest

from PredictionMarketEventSniper.core.outputs import StopStrategy
from .helpers import envelope, make_run, request, TrackingSource


@pytest.mark.asyncio
@pytest.mark.parametrize('where', ['strategy', 'executor-event', 'intent', 'shutdown', 'finalise'])
async def test_errors_propagate_original_exception_and_always_finalise(where):
    h = make_run()
    original = RuntimeError(where)
    if where == 'strategy':
        h.logic.error = original
    elif where == 'executor-event':
        h.executor.event_error = original
    elif where == 'intent':
        h.logic.on_event = lambda event: [request()]
        h.executor.intent_error = original
    elif where == 'shutdown':
        h.logic.on_event = lambda event: [StopStrategy()]
        h.executor.shutdown_error = original
    else:
        h.executor.finalise_error = original
    source = TrackingSource([envelope('start')])
    with pytest.raises(RuntimeError) as caught:
        await h.engine.backtest_instance(h.instance, source)
    assert caught.value is original
    assert h.executor.finalise_calls == 1
    assert source.close_calls == 1
    assert h.logic.termination_calls == 0
    assert not h.engine._running


@pytest.mark.asyncio
@pytest.mark.parametrize('streamed', [False, True])
async def test_source_error_is_not_wrapped_or_returned_as_exit(streamed):
    h = make_run()
    original = LookupError('storage read failed')
    def sync_source():
        yield envelope('first')
        raise original
    async def async_source():
        yield envelope('first')
        raise original
    with pytest.raises(LookupError) as caught:
        await h.engine.backtest_instance(h.instance, async_source() if streamed else sync_source())
    assert caught.value is original
    assert h.executor.finalise_calls == 1
    assert not h.executor.shutdown_calls


@pytest.mark.asyncio
@pytest.mark.parametrize('grace', [-1.0, float('nan'), float('inf')])
async def test_invalid_grace_period_raises_before_shutdown_initiation(grace):
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(grace_period=grace)]
    with pytest.raises(ValueError, match='grace period'):
        await h.engine.backtest_instance(h.instance, [envelope('stop')])
    assert not h.executor.shutdown_calls
    assert h.executor.finalise_calls == 1


@pytest.mark.asyncio
async def test_primary_error_kept_when_finalisation_and_source_close_also_fail():
    h = make_run()
    original = RuntimeError('strategy failed')
    h.logic.error = original
    h.executor.finalise_error = ValueError('executor close failed')
    source = TrackingSource([envelope('start')])
    source.close_error = OSError('source close failed')
    with pytest.raises(RuntimeError) as caught:
        await h.engine.backtest_instance(h.instance, source)
    assert caught.value is original
    assert any('executor close failed' in note for note in original.__notes__)
    assert any('source close failed' in note for note in original.__notes__)
    assert source.close_calls == 1
    assert not h.engine._running


@pytest.mark.asyncio
async def test_source_close_error_without_primary_error_propagates():
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(grace_period=0)]
    source = TrackingSource([envelope('stop'), envelope('past-cutoff', 10)])
    original = OSError('close failed')
    source.close_error = original
    with pytest.raises(OSError) as caught:
        await h.engine.backtest_instance(h.instance, source)
    assert caught.value is original
    assert h.executor.finalise_calls == 1


@pytest.mark.asyncio
async def test_two_finalisation_errors_form_group_instead_of_losing_one():
    h = make_run()
    h.logic.on_event = lambda event: [StopStrategy(grace_period=0)]
    finalise_error = ValueError('executor close')
    close_error = OSError('source close')
    h.executor.finalise_error = finalise_error
    source = TrackingSource([envelope('stop'), envelope('past-cutoff', 10)])
    source.close_error = close_error
    with pytest.raises(BaseExceptionGroup) as caught:
        await h.engine.backtest_instance(h.instance, source)
    assert caught.value.exceptions == (finalise_error, close_error)


@pytest.mark.asyncio
async def test_cancellation_closes_source_and_finalises_without_shutdown_requests():
    h = make_run()
    entered, closed = asyncio.Event(), asyncio.Event()
    async def source():
        try:
            entered.set()
            await asyncio.Event().wait()
            yield envelope('never')
        finally:
            closed.set()
    task = asyncio.create_task(h.engine.backtest_instance(h.instance, source()))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert closed.is_set()
        assert h.executor.finalise_calls == 1
        assert not h.executor.shutdown_calls
        assert not h.engine._running
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
