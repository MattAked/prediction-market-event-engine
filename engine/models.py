# engine/models.py

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..core.outputs import StopStrategy


class StrategyExitReason(Enum):
    COMPLETED = auto()
    FAILED = auto()
    STREAM_FAILED = auto()
    SHUTDOWN = auto()


@dataclass(frozen=True, slots=True)
class StrategyExit:
    instance_id: str
    reason: StrategyExitReason
    exception: BaseException | None = None


@dataclass(frozen=True, slots=True)
class StrategyTerminationRequest:
    reason: StrategyExitReason
    exception: BaseException | None = None
    stop_strategy: StopStrategy | None = None


@dataclass(slots=True)
class StrategyHandle:
    queue: asyncio.Queue
    stopping: asyncio.Event
    done: asyncio.Future[StrategyExit]
    termination_request: StrategyTerminationRequest | None = None


@dataclass(frozen=True, slots=True)
class StrategyTermination:
    reason: StrategyExitReason
    exception: BaseException | None = None
