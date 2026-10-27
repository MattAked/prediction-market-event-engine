from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Generic, Protocol, TYPE_CHECKING, TypeVar

from ..core.events import EventEnvelope
from ..core.outputs import StrategyOutput

if TYPE_CHECKING:
    from ..engine.models import StrategyTermination


ExecutorT = TypeVar("ExecutorT")


class StrategyLogic(Protocol):
    def handle_event(
        self,
        event: EventEnvelope,
        state: object,
        params: object,
    ) -> list[StrategyOutput]:
        raise NotImplementedError(
            "handle_event method must be implemented by subclasses"
        )

    def handle_termination(
        self,
        termination: StrategyTermination,
        state: object,
        params: object,
    ) -> list[StrategyOutput]:
        return []

    def is_heavy(self, payload: object) -> bool:
        return False


@dataclass
class StrategyInstance(Generic[ExecutorT]):
    instance_id: str
    params: object
    state: object
    logic: StrategyLogic
    executor: ExecutorT

    def handle_event(
        self,
        event: EventEnvelope,
    ) -> list[StrategyOutput]:
        return self.logic.handle_event(
            event=event,
            state=self.state,
            params=self.params,
        )

    def handle_termination(
        self,
        termination: StrategyTermination,
    ) -> list[StrategyOutput]:
        return self.logic.handle_termination(
            termination=termination,
            state=self.state,
            params=self.params,
        )

    def is_heavy(
        self,
        event: EventEnvelope,
    ) -> bool:
        return self.logic.is_heavy(event.payload)