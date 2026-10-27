import asyncio
import threading
from datetime import datetime

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.engine.live_engine import LiveEngine

from .helpers import (
    FakeEvent,
    FakeHeavyEvent,
    get_engine_with_strategy,
    put_event,
    register_fake_strategy,
    wait_for_thread_event,
)

# ---------------------------------------------------------------------------
# Registration / routing / worker execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_instance_registration() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    assert instance_id in engine.register

    assert instance_id in engine.handles
    assert engine.handles[instance_id] is handle

    assert instance_id in engine.instance_tasks

    worker_task = engine.instance_tasks[instance_id]

    assert isinstance(worker_task, asyncio.Task)
    assert not worker_task.done()

    assert executor._event_sink is not None
    assert executor._event_sink._queue is handle.queue

    await engine.shutdown()


@pytest.mark.asyncio
async def test_duplicate_instance_registration_is_rejected() -> None:
    """An instance ID must identify exactly one live runtime.

    Registering the same ID twice would otherwise overwrite engine registries
    while leaving the original worker alive and unreachable.
    """
    engine = LiveEngine()

    first_handle, _, _ = register_fake_strategy(
        engine,
        instance_id="duplicate",
    )
    first_worker = engine.instance_tasks["duplicate"]

    try:
        with pytest.raises((ValueError, RuntimeError)):
            register_fake_strategy(
                engine,
                instance_id="duplicate",
            )
    finally:
        # Defensive cleanup if the current implementation incorrectly accepts
        # the duplicate and overwrites the first worker in its registry.
        if not first_worker.done() and engine.instance_tasks.get("duplicate") is not first_worker:
            first_worker.cancel()
            await asyncio.gather(first_worker, return_exceptions=True)

        await engine.shutdown()

        # The original handle may have been orphaned by a buggy duplicate
        # registration. Cancel its worker above rather than waiting on done.
        _ = first_handle


@pytest.mark.asyncio
async def test_engine_to_strategy_event_routing() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    event = EventEnvelope(
        timestamp=datetime.now(),
        payload=FakeEvent(),
        instance_id=instance_id,
    )

    await handle.queue.put(event)
    await handle.queue.join()

    assert logic.event_received_event.is_set()
    assert logic.received_event is event

    await engine.shutdown()


@pytest.mark.asyncio
async def test_heavy_event_runs_off_event_loop_thread() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    event_loop_thread_id = threading.get_ident()

    await put_event(handle, instance_id, FakeHeavyEvent())

    await wait_for_thread_event(logic.first_event_started)
    logic.release_first_event.set()

    await handle.queue.join()

    assert logic.handle_thread_id is not None
    assert logic.handle_thread_id != event_loop_thread_id

    await engine.shutdown()


@pytest.mark.asyncio
async def test_non_heavy_event_runs_on_main_event_loop() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()
    event_loop_thread_id = threading.get_ident()

    await put_event(handle, instance_id, FakeEvent())
    await handle.queue.join()

    assert logic.handle_thread_id is not None
    assert logic.handle_thread_id == event_loop_thread_id

    await engine.shutdown()


@pytest.mark.asyncio
async def test_serial_strategy_processing() -> None:
    engine, handle, instance_id, logic, executor = get_engine_with_strategy()

    await put_event(handle, instance_id, FakeHeavyEvent(event_id=1))
    await put_event(handle, instance_id, FakeHeavyEvent(event_id=2))

    # Event 1 has entered strategy logic and is deliberately blocked there.
    await wait_for_thread_event(logic.first_event_started)

    # Event 2 is already queued but must not enter strategy logic until event 1
    # has completed.
    assert not logic.second_event_started.is_set()

    logic.release_first_event.set()
    await handle.queue.join()

    assert logic.second_event_started.is_set()
    assert logic.processed_event_ids == [1, 2]

    await engine.shutdown()


