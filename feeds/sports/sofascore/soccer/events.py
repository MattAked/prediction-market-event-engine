from enum import StrEnum
from dataclasses import dataclass

@dataclass
class MatchClock:
    minute: int
    second: int

class MatchPeriod(StrEnum):
    FIRST_HALF = "first_half"
    HALF_TIME = "half_time"
    SECOND_HALF = "second_half"
    GAME_OVER = "game_over"

@dataclass
class GoalDisallowedEvent:
    timestamp: float
    previous_score: int
    current_score: int
    clock: MatchClock
    period: MatchPeriod