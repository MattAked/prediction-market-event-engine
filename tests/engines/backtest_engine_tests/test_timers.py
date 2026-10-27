from datetime import timedelta

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope, TimerExpired
from PredictionMarketEventSniper.core.outputs import ScheduleTimer, CancelTimer, StopStrategy, StrategyOutput
from .helpers import T, envelope, make_run, payloads


@pytest.mark.asyncio
async def test_cancelled_timer_does_not_fire_or_advance_clock():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('cancelled', timedelta(hours=10)), CancelTimer('cancelled')]
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert payloads(h.logic.events) == ['start']
    assert h.engine.current_time == T


@pytest.mark.asyncio
async def test_cancel_unknown_timer_is_noop():
    h = make_run()
    h.logic.on_event = lambda event: [CancelTimer('missing')]
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert h.executor.finalise_calls == 1


@pytest.mark.asyncio
async def test_cancel_then_reuse_same_id_only_new_expiry_fires():
    h = make_run()
    def strategy(event: EventEnvelope[object]) -> list[StrategyOutput]:
        if event.payload == 'start':
            return [ScheduleTimer('id', timedelta(seconds=10))]
        if event.payload == 'replace':
            return [CancelTimer('id'), ScheduleTimer('id', timedelta(seconds=1))]
        return []
    h.logic.on_event = strategy
    await h.engine.backtest_instance(h.instance, [envelope('start'), envelope('replace', 1)])
    assert payloads(h.logic.events) == ['start', 'replace', TimerExpired('id')]
    assert h.engine.current_time == T + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_expired_id_can_be_scheduled_from_its_expiry_callback():
    h = make_run()
    expiries = 0
    def strategy(event: EventEnvelope[object]) -> list[StrategyOutput]:
        nonlocal expiries
        if event.payload == 'start':
            return [ScheduleTimer('id', timedelta(seconds=1))]
        if isinstance(event.payload, TimerExpired):
            expiries += 1
            if expiries < 3:
                return [ScheduleTimer('id', timedelta(seconds=1))]
        return []
    h.logic.on_event = strategy
    await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert expiries == 3
    assert h.engine.current_time == T + timedelta(seconds=3)


@pytest.mark.asyncio
async def test_duplicate_active_id_raises_and_finalises():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('id', timedelta(seconds=10)), ScheduleTimer('id', timedelta(seconds=1))]
    with pytest.raises(ValueError, match='already active'):
        await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert h.executor.finalise_calls == 1
    assert h.logic.termination_calls == 0


@pytest.mark.asyncio
async def test_negative_timer_delay_raises_without_success_result():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('negative', timedelta(seconds=-1))]
    with pytest.raises(ValueError, match='negative'):
        await h.engine.backtest_instance(h.instance, [envelope('start')])
    assert h.executor.closed


@pytest.mark.asyncio
async def test_zero_delay_timer_runs_after_current_output_list_before_next_history():
    h = make_run()
    calls = []
    def strategy(event: EventEnvelope[object]) -> list[StrategyOutput]:
        calls.append(event.payload)
        return [ScheduleTimer('zero', timedelta(0))] if event.payload == 'start' else []
    h.logic.on_event = strategy
    await h.engine.backtest_instance(h.instance, [envelope('start'), envelope('next')])
    assert calls == ['start', TimerExpired('zero'), 'next']


@pytest.mark.asyncio
async def test_pending_strategy_timers_are_discarded_when_cleanup_starts():
    h = make_run()
    h.logic.on_event = lambda event: [ScheduleTimer('would-fire', timedelta(seconds=1)), StopStrategy(grace_period=2)]
    await h.engine.backtest_instance(h.instance, [envelope('stop'), envelope('cleanup-history', 1.5)])
    assert payloads(h.logic.events) == ['stop']
    assert payloads(h.executor.events) == ['stop', 'cleanup-history']
    assert h.engine.current_time == T + timedelta(seconds=1.5)
