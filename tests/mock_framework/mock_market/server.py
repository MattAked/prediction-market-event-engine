"""TCP server for the mock venue.

There are still two independent connections:

  feed port    — unauthenticated market-data broadcast
  trading port — authenticated request/response order API

The trading protocol now supports placement, order-state polling, and
cancellation.  All venue state remains server-side.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from . import protocol as p
from .market import MockMarket

log = logging.getLogger("mock_market")


class MockMarketServer:
    def __init__(
        self,
        market: MockMarket,
        feed_port: int = 8801,
        trading_port: int = 8802,
        api_key: str = "test-key",
        host: str = "127.0.0.1",
    ) -> None:
        self.market = market
        self.feed_port = feed_port
        self.trading_port = trading_port
        self.api_key = api_key
        self.host = host
        self._subscribers: set[asyncio.Queue[bytes]] = set()
        self._writers: set[asyncio.StreamWriter] = set()

    # --- feed ---------------------------------------------------------------

    async def _handle_feed(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._subscribers.add(queue)
        self._writers.add(writer)
        peer = writer.get_extra_info("peername")
        log.info("feed connected %s", peer)

        try:
            # Register before snapshotting so an update landing between the
            # snapshot and the stream cannot be lost.
            writer.write(p.dumps(self.market.snapshot_message(datetime.now())))
            await writer.drain()

            while line := await queue.get():
                writer.write(line)
                await writer.drain()

            # b"" is the shutdown sentinel.
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        finally:
            self._subscribers.discard(queue)
            self._writers.discard(writer)
            writer.close()
            log.info("feed disconnected %s", peer)

    def _broadcast(self, message: dict[str, Any]) -> None:
        line = p.dumps(message)
        for queue in self._subscribers:
            queue.put_nowait(line)

    # --- trading ------------------------------------------------------------

    async def _handle_trading(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        self._writers.add(writer)

        try:
            auth_line = await reader.readline()
            if not auth_line:
                return

            auth = p.loads(auth_line)
            if auth.get("type") != p.AUTH or auth.get("key") != self.api_key:
                await self._send(
                    writer,
                    {"type": p.ERROR, "reason": "bad credentials"},
                )
                return

            await self._send(writer, {"type": p.AUTH_OK})
            log.info("trading client authenticated %s", peer)

            while line := await reader.readline():
                request = p.loads(line)
                result = self._handle_trading_request(request)
                await self._send(writer, result)

        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except (TypeError, ValueError, KeyError) as exc:
            log.warning("bad request from %s: %s", peer, exc)
        finally:
            self._writers.discard(writer)
            writer.close()
            log.info("trading client disconnected %s", peer)

    def _handle_trading_request(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        request_type = request.get("type")
        wall_now = datetime.now()

        try:
            if request_type == p.ORDER:
                result, _ = self.market.submit(
                    side=str(request.get("side", "")),
                    limit_price=float(request.get("price", -1)),
                    size=int(request.get("size", 0)),
                    wall_now=wall_now,
                )
                result["client_order_id"] = request.get("client_order_id")
                return result

            if request_type == p.GET_ORDER:
                return self.market.get_order(
                    venue_order_id=str(request.get("venue_order_id", "")),
                    wall_now=wall_now,
                )

            if request_type == p.CANCEL_ORDER:
                return self.market.cancel_order(
                    venue_order_id=str(request.get("venue_order_id", "")),
                    wall_now=wall_now,
                )

        except (TypeError, ValueError) as exc:
            return {
                "type": p.ERROR,
                "reason": f"invalid request: {exc}",
            }

        return {
            "type": p.ERROR,
            "reason": f"unsupported request type: {request_type!r}",
        }

    @staticmethod
    async def _send(
        writer: asyncio.StreamWriter,
        message: dict[str, Any],
    ) -> None:
        writer.write(p.dumps(message))
        await writer.drain()

    # --- lifecycle ----------------------------------------------------------

    async def run(self) -> None:
        feed = await asyncio.start_server(
            self._handle_feed,
            self.host,
            self.feed_port,
        )
        trading = await asyncio.start_server(
            self._handle_trading,
            self.host,
            self.trading_port,
        )

        log.info(
            "listening: feed %s:%d trading %s:%d",
            self.host,
            self.feed_port,
            self.host,
            self.trading_port,
        )

        try:
            await self._tick_loop()
        finally:
            # Wake every blocked handler before waiting for servers to close.
            for queue in self._subscribers:
                queue.put_nowait(b"")

            for writer in list(self._writers):
                writer.close()

            feed.close()
            trading.close()

            await asyncio.gather(
                feed.wait_closed(),
                trading.wait_closed(),
                return_exceptions=True,
            )
            log.info("stopped")

    async def _tick_loop(self) -> None:
        loop = asyncio.get_running_loop()
        interval = self.market.scenario.tick_seconds
        next_tick = loop.time()

        while True:
            next_tick += interval
            await asyncio.sleep(max(0.0, next_tick - loop.time()))

            for message in self.market.tick(next_tick, datetime.now()):
                self._broadcast(message)
