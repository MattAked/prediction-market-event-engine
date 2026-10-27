from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimerExpired:
    timer_id: str