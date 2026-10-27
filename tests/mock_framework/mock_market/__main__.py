"""Run the venue as its own process.

Example:

    python -m testing.mock_market \
        --feed-port 8801 \
        --trading-port 8802 \
        --tick-seconds 0.25 \
        --fill-delay-ticks 1 \
        --fill-size-per-tick 25
"""

import argparse
import asyncio
import logging

from ..mock_scenario import Scenario
from .market import MockMarket
from .server import MockMarketServer


def main() -> int:
    parser = argparse.ArgumentParser(prog="mock_market")
    parser.add_argument("--feed-port", type=int, default=8801)
    parser.add_argument("--trading-port", type=int, default=8802)
    parser.add_argument("--api-key", default="test-key")

    # Existing scenario controls.
    parser.add_argument("--tick-seconds", type=float, default=1.0)
    parser.add_argument("--lag-ticks", type=int, default=1)

    # Trading lifecycle controls.  These do not alter the scenario itself.
    parser.add_argument("--fill-delay-ticks", type=int, default=1)
    parser.add_argument("--fill-size-per-tick", type=int, default=25)

    args = parser.parse_args()

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )

    loop = asyncio.new_event_loop()
    scenario = Scenario(
        start_time=loop.time(),
        tick_seconds=args.tick_seconds,
        lag_ticks=args.lag_ticks,
    )
    market = MockMarket(
        scenario,
        fill_delay_ticks=args.fill_delay_ticks,
        fill_size_per_tick=args.fill_size_per_tick,
    )
    server = MockMarketServer(
        market,
        feed_port=args.feed_port,
        trading_port=args.trading_port,
        api_key=args.api_key,
    )

    try:
        loop.run_until_complete(server.run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
