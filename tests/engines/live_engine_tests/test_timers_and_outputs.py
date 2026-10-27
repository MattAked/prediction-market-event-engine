import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import PredictionMarketEventSniper.engine.live_engine as live_engine_module

from PredictionMarketEventSniper.core.outputs.execution import PlaceOrderIntent
from PredictionMarketEventSniper.engine.models import StrategyExitReason

from .helpers import (
    FakeCancelTimer,
    FakeExecutionIntentEvent,
    FakeOutputsAroundStopEvent,
    FakeStartTimer,
    get_engine_with_strategy,
    put_event,
)

# ---------------------------------------------------------------------------
# Timer lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timer_task_creation() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy(
        timer_length=0.1
    )

    await put_event(handle, instance_id, FakeStartTimer())
    await handle.queue.join()

    assert (instance_id, "test_timer") in engine.timer_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_cancel_timer() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy(
        timer_length=0.1
    )

    await put_event(handle, instance_id, FakeStartTimer())
    await handle.queue.join()

    timer_task_key = (instance_id, "test_timer")
    assert timer_task_key in engine.timer_tasks

    await put_event(handle, instance_id, FakeCancelTimer())
    await handle.queue.join()

    assert timer_task_key not in engine.timer_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_timer_expiry_reaches_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    timer_length = 0.1
    sleep_started = asyncio.Event()
    release_timer = asyncio.Event()
    requested_delays: list[float] = []

    class ControlledAsyncio:
        # Replace only the engine module's asyncio reference. Patching
        # asyncio.sleep itself would also alter other tests and framework code.
        def __getattr__(self, name: str):
            return getattr(asyncio, name)

        async def sleep(self, delay: float) -> None:
            requested_delays.append(delay)
            sleep_started.set()
            await release_timer.wait()

    monkeypatch.setattr(live_engine_module, "asyncio", ControlledAsyncio())
    engine, handle, instance_id, logic, executor = get_engine_with_strategy(
        timer_length=timer_length,
    )
    try:
        await put_event(handle, instance_id, FakeStartTimer())
        await asyncio.wait_for(handle.queue.join(), timeout=1.0)
        await asyncio.wait_for(sleep_started.wait(), timeout=1.0)

        assert requested_delays == [timer_length]
        assert logic.timer_expiry_count == 0
        assert not logic.timer_expired_event.is_set()

        release_timer.set()
        await asyncio.wait_for(logic.timer_expired_event.wait(), timeout=1.0)
        await asyncio.wait_for(handle.queue.join(), timeout=1.0)

        assert logic.timer_expiry_count == 1
        assert (instance_id, "test_timer") not in engine.timer_tasks
    finally:
        release_timer.set()
        await engine.shutdown()


@pytest.mark.asyncio
async def test_active_timer_is_cancelled_during_strategy_termination() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy(
        timer_length=60.0
    )

    await put_event(handle, instance_id, FakeStartTimer())
    await handle.queue.join()

    timer_key = (instance_id, "test_timer")
    timer_task = engine.timer_tasks[timer_key]

    await engine.stop_strategy(instance_id)

    assert timer_key not in engine.timer_tasks
    assert timer_task.done()
    assert logic.timer_expiry_count == 0
    assert handle.queue.empty()

    await engine.shutdown()


# ---------------------------------------------------------------------------
# Strategy outputs / executor dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execution_intent_is_dispatched_to_executor_exactly_once() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    handle_intent_mock = Mock()
    executor.handle_intent = handle_intent_mock

    await put_event(handle, instance_id, FakeExecutionIntentEvent())
    await handle.queue.join()

    handle_intent_mock.assert_called_once()
    call = handle_intent_mock.call_args
    assert call is not None
    dispatched_intent = call.args[0]

    assert isinstance(dispatched_intent, PlaceOrderIntent)
    assert dispatched_intent.intent_id == "test-order"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_outputs_before_stop_are_processed_and_outputs_after_stop_are_ignored() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    handle_intent_mock = Mock()
    executor.handle_intent = handle_intent_mock
    executor.shutdown = AsyncMock()

    await put_event(handle, instance_id, FakeOutputsAroundStopEvent())

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert exit_result.reason is StrategyExitReason.COMPLETED
    assert handle_intent_mock.call_count == 1
    call = handle_intent_mock.call_args
    assert call is not None
    assert call.args[0].intent_id == "before-stop"

    await engine.shutdown()


