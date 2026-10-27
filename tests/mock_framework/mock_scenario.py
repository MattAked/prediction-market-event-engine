"""A scripted sequence of numbers, shared by both streams.

Deliberately not random and deliberately not stateful. Both streams derive the
current tick from the same `start_time`, so nothing needs to coordinate them and
a run is byte-for-byte reproducible. That's what makes assertions possible:
"at tick 7 the strategy should have seen 25 while the book was still on 27".

Two clocks are anchored here on purpose. Scheduling uses `start_time`
(monotonic, from `loop.time()`) because it can't jump. Anything that leaves the
process or gets compared against an `Event.timestamp` uses `start_wall`. Both
are captured at construction so they refer to the same instant; they drift
apart over hours, which does not matter for a test venue.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

# Walks toward the target, hits it, then wanders off again.
DEFAULT_NUMBERS = [30, 29, 27, 25, 22, 20, 21, 24, 27, 30, 34, 39]

# One target per market window, cycled.
DEFAULT_TARGETS = [22, 40, 31]


@dataclass
class Scenario:
    start_time: float              # monotonic, from loop.time()
    start_wall: datetime = field(default_factory=datetime.now)
    tick_seconds: float = 1.0
    window_ticks: int = 6          # market window length, in ticks
    lag_ticks: int = 1             # how stale the market's view is == snipe window
    numbers: list[int] = field(default_factory=lambda: list(DEFAULT_NUMBERS))
    targets: list[int] = field(default_factory=lambda: list(DEFAULT_TARGETS))

    # --- ticks --------------------------------------------------------------

    def tick_at(self, now: float) -> int:
        return int((now - self.start_time) / self.tick_seconds)

    def number(self, tick: int) -> int:
        return self.numbers[max(tick, 0) % len(self.numbers)]

    def delayed_number(self, tick: int) -> int:
        """What the market can see. Set lag_ticks=0 to kill the edge."""
        return self.number(tick - self.lag_ticks)

    # --- market windows -----------------------------------------------------

    def window(self, tick: int) -> int:
        return max(tick, 0) // self.window_ticks

    def target(self, tick: int) -> int:
        return self.targets[self.window(tick) % len(self.targets)]

    def ticks_left(self, tick: int) -> int:
        return self.window_ticks - (max(tick, 0) % self.window_ticks)

    def _end_tick(self, tick: int) -> int:
        return (self.window(tick) + 1) * self.window_ticks

    def window_end_ts(self, tick: int) -> float:
        """Monotonic. Use for scheduling inside the process."""
        return self.start_time + self._end_tick(tick) * self.tick_seconds

    def window_end(self, tick: int) -> datetime:
        """Wall clock. Use for anything that goes on the wire or gets compared
        against an Event.timestamp."""
        return self.start_wall + timedelta(
            seconds=self._end_tick(tick) * self.tick_seconds
        )

    def timestamp(self, tick: int) -> datetime:
        """Wall-clock time of a tick, for stamping emitted events."""
        return self.start_wall + timedelta(seconds=tick * self.tick_seconds)