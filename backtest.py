from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

from .config import enabled_strategies, load_config
from .engine.backtest_engine import BacktestEngine


async def run(config: dict[str, Any]) -> None:
    strategies = enabled_strategies(config)
    if not strategies:
        logging.info("No enabled backtest strategies")
        return
    # Each historical run will get its own engine and freshly constructed
    # strategy instance/executor. The backtest orchestrator will later own
    # historical selection, stream merging, and summary collection.
    for item in strategies:
        engine = BacktestEngine()
        _ = engine
        raise NotImplementedError(
            f"Backtest orchestrator / historical reader not implemented "
            f"for {item.type!r} (instance_id={item.instance_id!r})"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run configured historical backtests")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("backtest.toml"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(load_config(args.config)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
