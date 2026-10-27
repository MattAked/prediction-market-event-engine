from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from ...tests.mock_framework.mock_scenario import Scenario
from .events import PredictedNumberEvent


class TestingStream:
    """Emits the current scenario number directly from subscribe().

    The mock market prices from Scenario.delayed_number(), so with lag_ticks=1
    this stream sees the number one tick before the market prices it.
    """

    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario

    async def subscribe(self) -> AsyncIterator[PredictedNumberEvent]:
        loop = asyncio.get_running_loop()

        while True:
            now = loop.time()
            tick = self.scenario.tick_at(now)

            yield PredictedNumberEvent(
                number=self.scenario.number(tick),
                timestamp=now,
            )

            next_tick = self.scenario.start_time + (
                (tick + 1) * self.scenario.tick_seconds
            )
            await asyncio.sleep(max(0.0, next_tick - loop.time()))
