"""Thin socket clients for the mock venue.

The trading client owns no venue state.  Request/response pairs are serialized
with a lock because monitor polling and cancellation can legitimately happen at
the same time and must not race while reading from the same StreamReader.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator
from typing import Any

from . import protocol as p


class FeedClient:
    """Unauthenticated market-data client."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8801) -> None:
        self.host = host
        self.port = port

    async def subscribe(self) -> AsyncIterator[dict[str, Any]]:
        reader, writer = await asyncio.open_connection(self.host, self.port)

        try:
            while line := await reader.readline():
                message = p.loads(line)
                for side in ("bids", "asks"):
                    if side in message:
                        message[side] = p.decode_levels(message[side])
                yield message
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass


class TradingClient:
    """Authenticated order-entry and order-state client."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8802,
        api_key: str = "test-key",
    ) -> None:
        self.host = host
        self.port = port
        self.api_key = api_key

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._ids = itertools.count(1)

        self._connect_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return (
            self._reader is not None
            and self._writer is not None
            and not self._writer.is_closing()
        )

    async def connect(self) -> None:
        async with self._connect_lock:
            if self.connected:
                return

            reader, writer = await asyncio.open_connection(self.host, self.port)

            try:
                writer.write(p.dumps({"type": p.AUTH, "key": self.api_key}))
                await writer.drain()

                reply = p.loads(await reader.readline())
                if reply.get("type") != p.AUTH_OK:
                    raise PermissionError(reply.get("reason", "auth failed"))

            except Exception:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionResetError, BrokenPipeError):
                    pass
                raise

            self._reader = reader
            self._writer = writer

    async def buy(self, price: float, size: int) -> dict[str, Any]:
        return await self._order("buy", price, size)

    async def sell(self, price: float, size: int) -> dict[str, Any]:
        return await self._order("sell", price, size)

    async def get_order(self, venue_order_id: str) -> dict[str, Any]:
        return await self._request(
            {
                "type": p.GET_ORDER,
                "venue_order_id": venue_order_id,
            }
        )

    async def cancel_order(self, venue_order_id: str) -> dict[str, Any]:
        return await self._request(
            {
                "type": p.CANCEL_ORDER,
                "venue_order_id": venue_order_id,
            }
        )

    async def _order(
        self,
        side: str,
        price: float,
        size: int,
    ) -> dict[str, Any]:
        return await self._request(
            {
                "type": p.ORDER,
                "client_order_id": f"c{next(self._ids)}",
                "side": side,
                "price": price,
                "size": size,
            }
        )

    async def _request(self, message: dict[str, Any]) -> dict[str, Any]:
        if not self.connected:
            raise RuntimeError("call connect() first")

        async with self._request_lock:
            # Re-check after waiting for the lock in case another coroutine
            # closed the client while this request was queued.
            if self._writer is None or self._reader is None or self._writer.is_closing():
                raise RuntimeError("trading client is not connected")

            self._writer.write(p.dumps(message))
            await self._writer.drain()

            line = await self._reader.readline()
            if not line:
                raise ConnectionError("mock trading connection closed")

            return p.loads(line)

    async def close(self) -> None:
        async with self._connect_lock:
            writer = self._writer
            self._reader = None
            self._writer = None

            if writer is None:
                return

            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass
