from dataclasses import dataclass
from enum import Enum, auto


class StrategyOutput:
    """Marker base for actions requested by strategy logic."""


class ShutdownPolicy(Enum):
    CANCEL_OPEN_ORDERS = auto()
    LEAVE_OPEN_ORDERS = auto()


@dataclass(frozen=True, slots=True)
class StopStrategy(StrategyOutput):
    """Request to stop the strategy instance."""

    policy: ShutdownPolicy = ShutdownPolicy.CANCEL_OPEN_ORDERS
    grace_period: float | None = None