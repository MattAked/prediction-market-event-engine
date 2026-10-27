from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TYPE_CHECKING

from ...core.events import EventEnvelope
from ...engine.models import StrategyExitReason
from ..base import StrategyInstance
from ...executors.live.live_executor import LiveExecutor

if TYPE_CHECKING:
    from ...engine.live_engine import LiveEngine
    from ...engine.models import StrategyHandle

# Executor must be generic as it can be either live or backtest executor
# and an instance shouldn't care/know which.
logger = logging.getLogger(__name__)

class EventStream(Protocol):
    def subscribe(self) -> AsyncIterator[object]:
        ...


class LiveStrategyOrchestrator(ABC):
    """Owns strategy-instance discovery/registration and external stream routing."""

    def __init__(
        self,
        engine: LiveEngine,
    ) -> None:
        self.engine = engine

        self.instance_ids: set[str] = set()
        self.instance_handles: dict[str, StrategyHandle] = {}

        # All forwarding tasks owned by this orchestrator.
        self.stream_tasks: set[asyncio.Task[Any]] = set()

        # Forwarding tasks grouped by strategy instance so they can be
        # stopped independently when that instance begins terminating.
        self.stream_tasks_by_instance: dict[
            str,
            set[asyncio.Task[Any]],
        ] = {}

        # One lightweight lifecycle watcher per strategy instance.
        self.lifecycle_tasks: dict[str, asyncio.Task[Any]] = {}

    @abstractmethod
    async def start(self) -> None:
        """Create/register the strategy instances managed by this orchestrator."""
        ...

    async def stop(self) -> None:
        """Orderly shutdown of every strategy instance owned here."""
        handles = list(self.instance_handles.items())

        # The engine owns instance termination. Setting handle.stopping inside
        # that lifecycle wakes our per-instance watcher, which then cancels the
        # relevant external stream tasks.
        for instance_id, handle in handles:
            if handle.stopping.is_set():
                if not handle.done.done():
                    await handle.done
                continue

            await self.engine.stop_strategy(
                instance_id=instance_id,
                reason=StrategyExitReason.SHUTDOWN,
            )

        # Defensive cleanup for any orchestrator task that survived the normal
        # stopping signal path.
        tasks = list(self.stream_tasks) + list(self.lifecycle_tasks.values())

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        self.stream_tasks.clear()
        self.stream_tasks_by_instance.clear()
        self.lifecycle_tasks.clear()
        self.instance_handles.clear()
        self.instance_ids.clear()

    def register_instance(
        self,
        instance: StrategyInstance[LiveExecutor],
    ) -> StrategyHandle:
        handle = self.engine.register_strategy(
            instance.instance_id,
            instance,
        )

        instance_id = instance.instance_id

        self.instance_ids.add(instance_id)
        self.instance_handles[instance_id] = handle
        self.stream_tasks_by_instance[instance_id] = set()

        lifecycle_task = asyncio.create_task(
            self._watch_instance_stopping(
                instance_id=instance_id,
                handle=handle,
            )
        )

        self.lifecycle_tasks[instance_id] = lifecycle_task

        return handle

    def listen_to_stream(
        self,
        stream: EventStream,
        handle: StrategyHandle,
        instance_id: str,
    ) -> None:
        # Don't start new forwarding work for an instance already stopping.
        if handle.stopping.is_set():
            return

        task = asyncio.create_task(
            self._forward_stream(
                stream=stream,
                handle=handle,
                instance_id=instance_id,
            )
        )

        self.stream_tasks.add(task)

        instance_tasks = self.stream_tasks_by_instance.setdefault(
            instance_id,
            set(),
        )
        instance_tasks.add(task)

        task.add_done_callback(
            lambda completed_task: self._discard_stream_task(
                instance_id,
                completed_task,
            )
        )

    def _discard_stream_task(
        self,
        instance_id: str,
        task: asyncio.Task[Any],
    ) -> None:
        self.stream_tasks.discard(task)

        instance_tasks = self.stream_tasks_by_instance.get(instance_id)

        if instance_tasks is not None:
            instance_tasks.discard(task)

    async def _watch_instance_stopping(
        self,
        instance_id: str,
        handle: StrategyHandle,
    ) -> None:
        """
        Wait for the engine to begin terminating this instance, then stop all
        external stream forwarding for it.
        """

        try:
            await handle.stopping.wait()

            tasks = list(
                self.stream_tasks_by_instance.get(
                    instance_id,
                    (),
                )
            )

            for task in tasks:
                task.cancel()

            if tasks:
                await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

        finally:
            self.stream_tasks_by_instance.pop(instance_id, None)
            self.instance_handles.pop(instance_id, None)
            self.instance_ids.discard(instance_id)

            current_task = asyncio.current_task()

            if self.lifecycle_tasks.get(instance_id) is current_task:
                self.lifecycle_tasks.pop(instance_id, None)

    async def _forward_stream(
        self,
        stream: EventStream,
        handle: StrategyHandle,
        instance_id: str,
    ) -> None:
        try:
            async for payload in stream.subscribe():
                # Covers the race where a stream payload arrives at almost the
                # same moment the engine begins terminating the strategy.
                if handle.stopping.is_set():
                    return

                await handle.queue.put(
                    EventEnvelope(
                        timestamp=datetime.now(),
                        payload=payload,
                        instance_id=instance_id,
                    )
                )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Strategy stream failed: instance_id=%s",
                instance_id,
            )

            # This forwarding task is the component reporting the failure.
            # Remove it from the per-instance set before asking the engine to
            # terminate, otherwise handle.stopping would make the watcher
            # cancel this task halfway through stop_strategy().
            current_task = asyncio.current_task()
            if current_task is not None:
                self._discard_stream_task(
                    instance_id,
                    current_task,
                )

            await self.engine.stop_strategy(
                instance_id=instance_id,
                reason=StrategyExitReason.STREAM_FAILED,
            )
