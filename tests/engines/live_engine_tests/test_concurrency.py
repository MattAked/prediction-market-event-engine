import asyncio

import pytest

from PredictionMarketEventSniper.engine.models import StrategyExitReason

from .helpers import (
    FakeBlockingHeavyEvent,
    get_engine_with_strategy,
    put_event,
    wait_for_thread_event,
)

# ---------------------------------------------------------------------------
# Concurrency hardening
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_external_stop_waits_for_in_flight_heavy_handler_before_termination() -> None:
    """External stop requests must wait for the current handler to reach a safe boundary.

    stop_strategy() no longer performs termination directly. It records a pending
    termination request on the StrategyHandle, sets the stopping event, and waits
    for the instance worker to complete the lifecycle.

    If the worker is already processing a heavy event in the thread pool, that
    handler is allowed to finish normally. The worker must not begin another event,
    and only after the current handler completes should it consume the pending
    termination request and run handle_termination().

    This preserves serial access to strategy state and guarantees that external
    shutdown cannot race handle_termination() against an in-flight handle_event().
    """
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await put_event(handle, instance_id, FakeBlockingHeavyEvent())
    await wait_for_thread_event(logic.blocking_heavy_started)

    stop_task = asyncio.create_task(
        engine.stop_strategy(instance_id)
    )

    try:
        # Give stop_strategy a chance to begin. The termination hook must not
        # run while handle_event is still blocked in the executor thread.
        await asyncio.sleep(0)
        assert not logic.termination_called.is_set()

        logic.release_blocking_heavy.set()

        await asyncio.wait_for(stop_task, timeout=1.0)
        await wait_for_thread_event(logic.blocking_heavy_finished)

        assert logic.termination_called.is_set()
        assert handle.done.result().reason is StrategyExitReason.SHUTDOWN

    finally:
        logic.release_blocking_heavy.set()

        if not stop_task.done():
            await asyncio.gather(stop_task, return_exceptions=True)

        await engine.shutdown()
