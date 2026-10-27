import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from PredictionMarketEventSniper.core.outputs import StopStrategy
from PredictionMarketEventSniper.core.outputs.execution import PlaceOrderIntent
from PredictionMarketEventSniper.engine.live_engine import LiveEngine
from PredictionMarketEventSniper.engine.models import StrategyExitReason

from .helpers import (
    FakeEvent,
    FakeFailureEvent,
    FakeGracefulTerminalEvent,
    FakeHeavyFailureEvent,
    FakeLeaveOpenTerminalEvent,
    FakeTerminalEvent,
    get_engine_with_strategy,
    make_order_intent,
    put_event,
    register_fake_strategy,
)

# ---------------------------------------------------------------------------
# StopStrategy / normal completion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_strategy_termination() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await put_event(handle, instance_id, FakeTerminalEvent())

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert handle.stopping.is_set()
    assert handle.done.done()

    assert exit_result.instance_id == instance_id
    assert exit_result.reason is StrategyExitReason.COMPLETED
    assert exit_result.exception is None

    assert len(logic.terminations) == 1
    assert logic.terminations[0].reason is StrategyExitReason.COMPLETED
    assert logic.terminations[0].exception is None

    assert executor._shutting_down is True

    assert instance_id not in engine.register
    assert instance_id not in engine.handles
    assert instance_id not in engine.event_sinks
    assert instance_id not in engine.instance_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_stop_strategy_default_policy_cancels_open_orders() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    executor.shutdown = AsyncMock()

    await put_event(handle, instance_id, FakeTerminalEvent())
    await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    executor.shutdown.assert_awaited_once_with(
        cancel_open_orders=True,
    )

    await engine.shutdown()


@pytest.mark.asyncio
async def test_stop_strategy_leave_open_orders_propagates_to_executor() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    executor.shutdown = AsyncMock()

    await put_event(handle, instance_id, FakeLeaveOpenTerminalEvent())

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert exit_result.reason is StrategyExitReason.COMPLETED

    executor.shutdown.assert_awaited_once_with(
        cancel_open_orders=False,
    )

    await engine.shutdown()


@pytest.mark.asyncio
async def test_stop_strategy_custom_grace_period_propagates_to_executor() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    executor.shutdown = AsyncMock()

    await put_event(handle, instance_id, FakeGracefulTerminalEvent())
    await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    executor.shutdown.assert_awaited_once_with(
        grace_period=3.5,
        cancel_open_orders=False,
    )

    await engine.shutdown()


# ---------------------------------------------------------------------------
# Worker failure lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_heavy_worker_failure_terminates_instance_as_failed() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await put_event(handle, instance_id, FakeFailureEvent())

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert handle.stopping.is_set()
    assert exit_result.reason is StrategyExitReason.FAILED
    assert exit_result.exception is logic.failure_exception

    assert len(logic.terminations) == 1
    assert logic.terminations[0].reason is StrategyExitReason.FAILED
    assert logic.terminations[0].exception is logic.failure_exception

    assert executor._shutting_down is True

    assert instance_id not in engine.register
    assert instance_id not in engine.handles
    assert instance_id not in engine.event_sinks
    assert instance_id not in engine.instance_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_heavy_worker_failure_terminates_instance_as_failed() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await put_event(handle, instance_id, FakeHeavyFailureEvent())

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert exit_result.reason is StrategyExitReason.FAILED
    assert exit_result.exception is logic.heavy_failure_exception

    assert len(logic.terminations) == 1
    assert logic.terminations[0].exception is logic.heavy_failure_exception

    await engine.shutdown()


@pytest.mark.asyncio
async def test_worker_failure_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    with caplog.at_level(
        logging.ERROR,
        logger="PredictionMarketEventSniper.engine.live_engine",
    ):
        await put_event(handle, instance_id, FakeFailureEvent())
        await asyncio.wait_for(handle.done, timeout=1.0)

    assert any(
        "Strategy instance worker failed" in record.getMessage()
        and instance_id in record.getMessage()
        for record in caplog.records
    )

    await engine.shutdown()


@pytest.mark.asyncio
async def test_termination_handler_failure_does_not_prevent_forced_teardown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    logic.raise_on_termination = True

    with caplog.at_level(
        logging.ERROR,
        logger="PredictionMarketEventSniper.engine.live_engine",
    ):
        await put_event(handle, instance_id, FakeFailureEvent())
        exit_result = await asyncio.wait_for(handle.done, timeout=1.0)

    await handle.queue.join()

    # The original worker failure remains the lifecycle result even though the
    # best-effort termination hook also failed.
    assert exit_result.reason is StrategyExitReason.FAILED
    assert exit_result.exception is logic.failure_exception
    assert executor._shutting_down is True

    assert instance_id not in engine.register
    assert instance_id not in engine.handles
    assert instance_id not in engine.event_sinks
    assert instance_id not in engine.instance_tasks

    assert any(
        "Strategy termination handler failed" in record.getMessage()
        for record in caplog.records
    )

    await engine.shutdown()


@pytest.mark.asyncio
async def test_termination_outputs_are_processed_before_executor_shutdown() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    call_order: list[str] = []

    cleanup_intent = make_order_intent("cleanup-order")
    logic.termination_outputs = [cleanup_intent]

    def handle_intent(intent: PlaceOrderIntent) -> None:
        assert intent is cleanup_intent
        call_order.append("intent")

    async def shutdown(**kwargs: object) -> None:
        call_order.append("shutdown")

    executor.handle_intent = Mock(side_effect=handle_intent)
    executor.shutdown = AsyncMock(side_effect=shutdown)

    await put_event(handle, instance_id, FakeFailureEvent())
    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert exit_result.reason is StrategyExitReason.FAILED
    assert call_order == ["intent", "shutdown"]

    await engine.shutdown()


@pytest.mark.asyncio
async def test_stop_strategy_returned_from_termination_hook_is_ignored() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    logic.termination_outputs = [StopStrategy()]
    executor.shutdown = AsyncMock()

    await put_event(handle, instance_id, FakeFailureEvent())
    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)
    await handle.queue.join()

    assert exit_result.reason is StrategyExitReason.FAILED
    assert len(logic.terminations) == 1
    executor.shutdown.assert_awaited_once_with(
        cancel_open_orders=True,
    )

    await engine.shutdown()


# ---------------------------------------------------------------------------
# Public stop / engine-wide shutdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_external_stop_strategy_uses_shutdown_reason() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await engine.stop_strategy(instance_id)

    exit_result = await asyncio.wait_for(handle.done, timeout=1.0)

    assert handle.stopping.is_set()
    assert exit_result.reason is StrategyExitReason.SHUTDOWN
    assert exit_result.exception is None

    assert len(logic.terminations) == 1
    assert logic.terminations[0].reason is StrategyExitReason.SHUTDOWN

    assert executor._shutting_down is True
    assert instance_id not in engine.register
    assert instance_id not in engine.handles
    assert instance_id not in engine.instance_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_stop_strategy_is_harmless_after_instance_has_terminated() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await engine.stop_strategy(instance_id)
    first_exit = await asyncio.wait_for(handle.done, timeout=1.0)

    # The public API should be idempotent for callers cleaning up defensively.
    await engine.stop_strategy(instance_id)

    assert handle.done.result() is first_exit
    assert len(logic.terminations) == 1

    await engine.shutdown()


@pytest.mark.asyncio
async def test_engine_shutdown_terminates_all_registered_instances() -> None:
    engine = LiveEngine()

    handle_a, logic_a, executor_a = register_fake_strategy(
        engine,
        instance_id="strategy-a",
    )
    handle_b, logic_b, executor_b = register_fake_strategy(
        engine,
        instance_id="strategy-b",
    )

    await engine.shutdown()

    exit_a = handle_a.done.result()
    exit_b = handle_b.done.result()

    assert exit_a.reason is StrategyExitReason.SHUTDOWN
    assert exit_b.reason is StrategyExitReason.SHUTDOWN

    assert [termination.reason for termination in logic_a.terminations] == [
        StrategyExitReason.SHUTDOWN
    ]
    assert [termination.reason for termination in logic_b.terminations] == [
        StrategyExitReason.SHUTDOWN
    ]

    assert executor_a._shutting_down is True
    assert executor_b._shutting_down is True

    assert engine.register == {}
    assert engine.handles == {}
    assert engine.event_sinks == {}
    assert engine.instance_tasks == {}
    assert engine.timer_tasks == {}


@pytest.mark.asyncio
async def test_one_instance_failure_does_not_stop_another_instance() -> None:
    engine = LiveEngine()

    failed_handle, failed_logic, _ = register_fake_strategy(
        engine,
        instance_id="failed-strategy",
    )
    healthy_handle, healthy_logic, _ = register_fake_strategy(
        engine,
        instance_id="healthy-strategy",
    )

    await put_event(
        failed_handle,
        "failed-strategy",
        FakeFailureEvent(),
    )

    failed_exit = await asyncio.wait_for(
        failed_handle.done,
        timeout=1.0,
    )

    assert failed_exit.reason is StrategyExitReason.FAILED

    # The other instance should still own a live worker and accept events.
    assert "healthy-strategy" in engine.handles
    assert not healthy_handle.stopping.is_set()
    assert not engine.instance_tasks["healthy-strategy"].done()

    await put_event(
        healthy_handle,
        "healthy-strategy",
        FakeEvent(),
    )
    await healthy_handle.queue.join()

    assert healthy_logic.event_received_event.is_set()

    await engine.shutdown()


