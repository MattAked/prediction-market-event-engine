import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
from typing import Any
import logging

from ..core.exceptions.base import StrategyAlreadyRegisteredError

from ..strategy.base import StrategyInstance
from ..core.outputs import (
    StrategyOutput,
    StopStrategy,
    ShutdownPolicy,
    ExecutionIntent,
    ScheduleTimer,
    CancelTimer,
)
from ..core.events import EventEnvelope, TimerExpired
from ..executors.live.live_executor import LiveExecutor
from ..executors.live.protocols import QueueEventSink
from .models import (
    StrategyHandle,
    StrategyExit,
    StrategyExitReason,
    StrategyTermination,
    StrategyTerminationRequest,
)


logger = logging.getLogger(__name__)


class LiveEngine:
    def __init__(self):
        self.register: dict[str, StrategyInstance[LiveExecutor]] = {}

        # Mapping of strategy instance IDs to their queue/lifecycle handle.
        self.handles: dict[str, StrategyHandle] = {}

        self.event_sinks: dict[str, QueueEventSink[Any]] = {}
        self.instance_tasks: dict[str, asyncio.Task] = {}
        self.timer_tasks: dict[tuple[str, str], asyncio.Task] = {}

        self._pool = ThreadPoolExecutor(max_workers=os.cpu_count())

    def register_strategy(
        self,
        instance_id: str,
        strategy_instance: StrategyInstance[LiveExecutor],
    ) -> StrategyHandle:
        if instance_id in self.handles:
            raise StrategyAlreadyRegisteredError(instance_id)

        queue = asyncio.Queue()
        sink = QueueEventSink(instance_id, queue)

        done_future: asyncio.Future[StrategyExit] = asyncio.Future()

        strategy_handle = StrategyHandle(
            queue=queue,
            stopping=asyncio.Event(),
            done=done_future,
        )

        self.handles[instance_id] = strategy_handle
        self.event_sinks[instance_id] = sink
        self.register[instance_id] = strategy_instance

        # Give the executor only the scoped sink.
        strategy_instance.executor.set_event_sink(sink)

        task = asyncio.create_task(
            self._instance_worker(
                instance_id,
                strategy_instance,
                strategy_handle,
            )
        )

        self.instance_tasks[instance_id] = task

        return strategy_handle

    def _request_termination(
        self,
        handle: StrategyHandle,
        *,
        reason: StrategyExitReason,
        exception: BaseException | None = None,
        stop_strategy: StopStrategy | None = None,
    ) -> None:
        """Record the first termination request and wake the instance worker."""
        if handle.termination_request is not None:
            return

        handle.termination_request = StrategyTerminationRequest(
            reason=reason,
            exception=exception,
            stop_strategy=stop_strategy,
        )
        handle.stopping.set()

    async def stop_strategy(
        self,
        instance_id: str,
        reason: StrategyExitReason = StrategyExitReason.SHUTDOWN,
    ) -> None:
        """Request orderly termination and wait until teardown is complete."""
        handle = self.handles.get(instance_id)

        if handle is None:
            return

        self._request_termination(
            handle,
            reason=reason,
        )

        if not handle.done.done():
            await handle.done

    async def shutdown(self) -> None:
        """Terminate remaining instances and close engine-wide resources."""
        for instance_id in list(self.handles):
            await self.stop_strategy(
                instance_id=instance_id,
                reason=StrategyExitReason.SHUTDOWN,
            )

        # ThreadPoolExecutor.shutdown() is blocking, so do not run it on the
        # event-loop thread.
        await asyncio.to_thread(
            self._pool.shutdown,
            wait=True,
        )

    async def _terminate_requested_instance(
        self,
        instance_id: str,
        handle: StrategyHandle,
    ) -> None:
        request = handle.termination_request

        if request is None:
            raise RuntimeError(
                f"Strategy {instance_id!r} has no pending termination request"
            )

        await self._terminate_instance(
            instance_id=instance_id,
            reason=request.reason,
            exception=request.exception,
            stop_strategy=request.stop_strategy,
        )

    @staticmethod
    def _drain_instance_queue(handle: StrategyHandle) -> None:
        """Discard queued events that must not run once termination is requested."""
        while True:
            try:
                handle.queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            else:
                handle.queue.task_done()

    async def _terminate_instance(
        self,
        instance_id: str,
        reason: StrategyExitReason,
        exception: BaseException | None = None,
        stop_strategy: StopStrategy | None = None,
    ) -> None:
        handle = self.handles.get(instance_id)
        instance = self.register.get(instance_id)

        if handle is None or instance is None:
            return

        logger.info(
            "Strategy instance entering termination: "
            "instance_id=%s reason=%s",
            instance_id,
            reason.name,
        )

        #
        # 1. Cancel strategy timers.
        #
        timer_tasks = [
            task
            for (timer_instance_id, _), task in self.timer_tasks.items()
            if timer_instance_id == instance_id
        ]

        for timer_task in timer_tasks:
            timer_task.cancel()

        if timer_tasks:
            await asyncio.gather(
                *timer_tasks,
                return_exceptions=True,
            )

        # Also release timers cancelled before their coroutine first started.
        for key, task in list(self.timer_tasks.items()):
            if key[0] == instance_id and task in timer_tasks:
                self.timer_tasks.pop(key, None)

        #
        # 2. Give strategy one final best-effort cleanup opportunity.
        #
        try:
            termination = StrategyTermination(
                reason=reason,
                exception=exception,
            )
            termination_outputs = instance.handle_termination(
                termination
            )

            for output in termination_outputs:
                # A strategy is already terminating, so another StopStrategy
                # has no meaning here.
                if isinstance(output, StopStrategy):
                    continue

                await self._process_strat_output(
                    instance,
                    output,
                )

        except Exception:
            logger.exception(
                "Strategy termination handler failed: instance_id=%s",
                instance_id,
            )

        #
        # 3. Shut down executor.
        #
        # Non-StopStrategy termination defaults to cancelling venue-side orders.
        cancel_open_orders = True
        grace_period: float | None = None

        if stop_strategy is not None:
            cancel_open_orders = (
                stop_strategy.policy
                is ShutdownPolicy.CANCEL_OPEN_ORDERS
            )

            grace_period = stop_strategy.grace_period

        try:
            if grace_period is None:
                await instance.executor.shutdown(
                    cancel_open_orders=cancel_open_orders,
                )
            else:
                await instance.executor.shutdown(
                    grace_period=grace_period,
                    cancel_open_orders=cancel_open_orders,
                )

        except Exception:
            logger.exception(
                "Executor shutdown failed during strategy termination: "
                "instance_id=%s",
                instance_id,
            )

        # No further strategy events will be processed. Timers and executor
        # shutdown are complete, so discard anything they left queued and keep
        # asyncio.Queue unfinished-task accounting consistent.
        self._drain_instance_queue(handle)

        #
        # 4. Remove engine-owned runtime state.
        #
        self.register.pop(instance_id, None)
        self.event_sinks.pop(instance_id, None)

        # The instance worker is the sole owner of graceful termination, so it
        # removes its own registry entry and returns after this call.
        self.instance_tasks.pop(instance_id, None)

        #
        # 5. Publish final lifecycle result.
        #
        exit_result = StrategyExit(
            instance_id=instance_id,
            reason=reason,
            exception=exception,
        )

        if not handle.done.done():
            handle.done.set_result(exit_result)

        self.handles.pop(instance_id, None)

        logger.info(
            "Strategy instance terminated: "
            "instance_id=%s reason=%s",
            instance_id,
            reason.name,
        )

    async def _wait_for_event_or_stop(
        self,
        handle: StrategyHandle,
        stop_task: asyncio.Task,
    ) -> EventEnvelope | None:
        """Wait for queue input while allowing a stop request to wake the worker."""
        queue_task = asyncio.create_task(handle.queue.get())

        try:
            await asyncio.wait(
                {queue_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Stop wins a race with a newly-arrived event. If queue.get() also
            # completed, discard that item and balance Queue.task_done().
            if handle.stopping.is_set():
                if queue_task.done() and not queue_task.cancelled():
                    queue_task.result()
                    handle.queue.task_done()
                else:
                    queue_task.cancel()
                    await asyncio.gather(
                        queue_task,
                        return_exceptions=True,
                    )

                return None

            return queue_task.result()

        except BaseException:
            if not queue_task.done():
                queue_task.cancel()
                await asyncio.gather(
                    queue_task,
                    return_exceptions=True,
                )
            raise

    async def _instance_worker(
        self,
        instance_id: str,
        strategy_instance: StrategyInstance,
        handle: StrategyHandle,
    ) -> None:
        loop = asyncio.get_running_loop()
        queue = handle.queue

        # Persistent for the lifetime of the worker. The hot path therefore
        # does not create a second stop-wait task for every strategy event.
        stop_task = asyncio.create_task(handle.stopping.wait())

        try:
            while True:
                # Fast path: when events are already queued, avoid creating a
                # queue.get() task or calling asyncio.wait().
                if handle.stopping.is_set():
                    await self._terminate_requested_instance(
                        instance_id,
                        handle,
                    )
                    return

                try:
                    event = queue.get_nowait()
                except asyncio.QueueEmpty:
                    event = await self._wait_for_event_or_stop(
                        handle,
                        stop_task,
                    )

                    if event is None:
                        await self._terminate_requested_instance(
                            instance_id,
                            handle,
                        )
                        return

                # A stop request may have raced with obtaining the next item.
                # Do not begin a new strategy callback once stopping is set.
                if handle.stopping.is_set():
                    queue.task_done()
                    await self._terminate_requested_instance(
                        instance_id,
                        handle,
                    )
                    return

                try:
                    logger.info(event)

                    if strategy_instance.is_heavy(event):
                        strategy_outputs = await loop.run_in_executor(
                            self._pool,
                            strategy_instance.handle_event,
                            event,
                        )
                    else:
                        strategy_outputs = strategy_instance.handle_event(event)

                    for output in strategy_outputs:
                        if isinstance(output, StopStrategy):
                            # First termination request wins. This means an
                            # external shutdown/stream failure requested while
                            # this event was running is not overwritten by a
                            # StopStrategy returned by the event.
                            self._request_termination(
                                handle,
                                reason=StrategyExitReason.COMPLETED,
                                stop_strategy=output,
                            )
                            break

                        await self._process_strat_output(
                            strategy_instance,
                            output,
                        )

                finally:
                    queue.task_done()

                # Finish the event already in flight, but never begin another
                # one after a termination request has been made.
                if handle.stopping.is_set():
                    await self._terminate_requested_instance(
                        instance_id,
                        handle,
                    )
                    return

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.exception(
                "Strategy instance worker failed: instance_id=%s",
                instance_id,
            )

            self._request_termination(
                handle,
                reason=StrategyExitReason.FAILED,
                exception=exc,
            )

            await self._terminate_requested_instance(
                instance_id,
                handle,
            )

        finally:
            if not stop_task.done():
                stop_task.cancel()
                await asyncio.gather(
                    stop_task,
                    return_exceptions=True,
                )

    async def timer(
        self,
        timer: ScheduleTimer,
        instance_id: str,
    ) -> None:
        try:
            await asyncio.sleep(timer.delay.total_seconds())

            handle = self.handles.get(instance_id)

            if handle is not None and not handle.stopping.is_set():
                await handle.queue.put(
                    EventEnvelope(
                        timestamp=datetime.now(),
                        payload=TimerExpired(timer.timer_id),
                        instance_id=instance_id,
                    )
                )

        except asyncio.CancelledError:
            pass

        finally:
            key = (instance_id, timer.timer_id)
            if self.timer_tasks.get(key) is asyncio.current_task():
                self.timer_tasks.pop(key, None)

    async def _process_strat_output(
        self,
        instance: StrategyInstance,
        strategy_output: StrategyOutput,
    ) -> None:
        if isinstance(strategy_output, ExecutionIntent):
            instance.executor.handle_intent(strategy_output)

        elif isinstance(strategy_output, ScheduleTimer):
            key = (instance.instance_id, strategy_output.timer_id)
            if key in self.timer_tasks:
                raise ValueError(f"Timer ID already active: {strategy_output.timer_id}")
            task = asyncio.create_task(
                self.timer(
                    strategy_output,
                    instance.instance_id,
                )
            )

            key = (
                instance.instance_id,
                strategy_output.timer_id,
            )

            self.timer_tasks[key] = task

        elif isinstance(strategy_output, CancelTimer):
            key = (
                instance.instance_id,
                strategy_output.timer_id,
            )

            task = self.timer_tasks.get(key)

            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    # A task cancelled before starting never enters timer's
                    # try/finally, so release its registration here too.
                    pass
                finally:
                    if self.timer_tasks.get(key) is task:
                        self.timer_tasks.pop(key, None)
