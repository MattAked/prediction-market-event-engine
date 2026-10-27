from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

from .config import StrategyConfig, enabled_strategies, load_config
from .engine.live_engine import LiveEngine
from .strategy.orchestrators.live import LiveStrategyOrchestrator
from .strategies.mock_scenario.orchestrators.live import MockStrategyOrchestrator
from .tests.mock_framework.mock_market.market import MockMarket
from .tests.mock_framework.mock_market.server import MockMarketServer
from .tests.mock_framework.mock_scenario import Scenario

logger = logging.getLogger(__name__)


async def run(config: dict[str, Any]) -> None:
    strategies = enabled_strategies(config)
    unsupported = sorted({s.type for s in strategies if s.type != "mock"})
    if unsupported:
        raise ValueError(f"Unsupported live strategy type(s): {', '.join(unsupported)}")
    if config.get("recording", {}).get("enabled", False):
        raise NotImplementedError("Recording is not integrated yet; set recording.enabled=false")

    engine = LiveEngine()
    orchestrators: list[LiveStrategyOrchestrator] = []
    server_task: asyncio.Task[None] | None = None
    try:
        mock_settings = dict(config.get("mock_market", {}))
        mocks = [s for s in strategies if s.type == "mock"]
        if mocks:
            loop = asyncio.get_running_loop()
            scenario = Scenario(
                start_time=loop.time(),
                tick_seconds=float(mock_settings.get("tick_seconds", 1.0)),
                window_ticks=int(mock_settings.get("window_ticks", 6)),
                lag_ticks=int(mock_settings.get("lag_ticks", 1)),
            )
            market = MockMarket(
                scenario=scenario,
                spread=float(mock_settings.get("spread", 0.02)),
                depth=int(mock_settings.get("depth", 3)),
                level_size=int(mock_settings.get("level_size", 200)),
                fill_delay_ticks=int(mock_settings.get("fill_delay_ticks", 1)),
                fill_size_per_tick=int(mock_settings.get("fill_size_per_tick", 25)),
            )
            server_task = asyncio.create_task(MockMarketServer(
                market=market,
                host=str(mock_settings.get("host", "127.0.0.1")),
                feed_port=int(mock_settings.get("feed_port", 8801)),
                trading_port=int(mock_settings.get("trading_port", 8802)),
                api_key=str(mock_settings.get("api_key", "test-key")),
            ).run())
            await asyncio.sleep(0)
            if server_task.done():
                await server_task

            for item in mocks:
                orchestrator = MockStrategyOrchestrator(
                    engine=engine, scenario=scenario, params=item.params,
                    instance_id=item.instance_id,
                    instrument_id=str(item.options.get("instrument_id", "mock")),
                    market_host=str(mock_settings.get("host", "127.0.0.1")),
                    feed_port=int(mock_settings.get("feed_port", 8801)),
                    trading_port=int(mock_settings.get("trading_port", 8802)),
                    api_key=str(mock_settings.get("api_key", "test-key")),
                    adapter_poll_interval=float(mock_settings.get("adapter_poll_interval", 0.05)),
                )
                orchestrators.append(orchestrator)
                await orchestrator.start()
        logger.info("Started %d live orchestrator(s)", len(orchestrators))
        if orchestrators:
            await asyncio.Event().wait()
    finally:
        try:
            for orchestrator in reversed(orchestrators):
                try:
                    await orchestrator.stop()
                except Exception:
                    logger.exception("Orchestrator stop failed")
        finally:
            try:
                await engine.shutdown()
            finally:
                if server_task is not None:
                    server_task.cancel()
                    await asyncio.gather(server_task, return_exceptions=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live strategy orchestrators")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("live.toml"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s %(message)s")
    try:
        asyncio.run(run(load_config(args.config)))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
