from dataclasses import dataclass
from datetime import timedelta

from .base import StrategyOutput


@dataclass(frozen=True, slots=True)
class ScheduleTimer(StrategyOutput):
    timer_id: str
    delay: timedelta


@dataclass(frozen=True, slots=True)
class CancelTimer(StrategyOutput):
    timer_id: str