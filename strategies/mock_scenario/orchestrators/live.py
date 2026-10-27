from __future__ import annotations

from typing import Any, TYPE_CHECKING

from ....executors.live.live_executor import LiveExecutor
from ....executors.live.venue_adapters.testing_mock import MockAdapter
from ....feeds.prediction_markets.mock_market.stream import MockMarketStream
from ....feeds.testing.stream import TestingStream
from ....tests.mock_framework.mock_scenario import Scenario
from ....strategy.base import StrategyInstance
from ....strategy.orchestrators.live import LiveStrategyOrchestrator
from ..state import PredictedNumberParams, PredictedNumberState
from ..strategy import PredictedNumberStrategy

if TYPE_CHECKING:
    from ....engine.live_engine import LiveEngine


class MockStrategyOrchestrator(LiveStrategyOrchestrator):
    """Builds one predicted-number mock strategy instance and wires its streams."""

    def __init__(
        self,
        engine: LiveEngine,
        scenario: Scenario,
        params: dict[str, Any],
        *,
        instance_id: str = "mock-1",
        instrument_id: str = "mock",
        market_host: str = "127.0.0.1",
        feed_port: int = 8801,
        trading_port: int = 8802,
        api_key: str = "test-key",
        adapter_poll_interval: float = 0.05,
    ) -> None:
        super().__init__(engine)

        self.scenario = scenario
        self.instance_id = instance_id
        self.instrument_id = instrument_id

        param_values = dict(params)
        # The strategy's fair-value model is expressed in scenario ticks, so
        # default this to the same interval driving the mock market.
        param_values.setdefault("tick_seconds", scenario.tick_seconds)
        param_values.setdefault("instrument_id", instrument_id)

        self.params = PredictedNumberParams(**param_values)
        self.state = PredictedNumberState.create(self.params)
        self.logic = PredictedNumberStrategy()

        self.testing_stream = TestingStream(scenario)
        self.market_stream = MockMarketStream(
            host=market_host,
            port=feed_port,
        )

        self.adapter = MockAdapter(
            host=market_host,
            port=trading_port,
            api_key=api_key,
            poll_interval=adapter_poll_interval,
        )

    async def start(self) -> None:
        executor = LiveExecutor(
            adapter=self.adapter,
            instruments={
                self.instrument_id: self.instrument_id,
            },
        )

        instance = StrategyInstance(
            instance_id=self.instance_id,
            params=self.params,
            state=self.state,
            logic=self.logic,
            executor=executor,
        )

        handle = self.register_instance(instance)

        self.listen_to_stream(
            stream=self.testing_stream,
            handle=handle,
            instance_id=self.instance_id,
        )
        self.listen_to_stream(
            stream=self.market_stream,
            handle=handle,
            instance_id=self.instance_id,
        )
