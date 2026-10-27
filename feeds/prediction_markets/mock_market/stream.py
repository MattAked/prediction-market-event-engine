"""Adapter over the mock venue's feed socket.

Its whole job is translation: wire messages in, `events.py` dataclasses out. No
book is kept here — the strategy already reconstructs one from snapshot plus
diffs, and a second copy in the feed would be a second thing that can be wrong.

There's no queue fan-out any more. Each `subscribe()` opens its own connection,
which is both simpler and how a real venue works: the server does the
broadcasting.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncIterator

from ....tests.mock_framework.mock_market import protocol as p
from ....tests.mock_framework.mock_market.client import FeedClient

from .events import NewTargetNumberUpdate, OrderBookState, OrderBookUpdate

log = logging.getLogger(__name__)

MarketEvent = OrderBookState | OrderBookUpdate | NewTargetNumberUpdate


class MockMarketStream:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8801,
        reconnect: bool = True,
        reconnect_delay: float = 1.0,
        max_reconnect_attempts: int | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.reconnect = reconnect
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        """None means retry forever. Set a number in tests, or a venue that
        never comes back turns a failure into a hang."""

    async def subscribe(self) -> AsyncIterator[MarketEvent]:
        attempts = 0
        while True:
            try:
                async for event in self._read_once():
                    attempts = 0  # a successful read resets the budget
                    yield event
            except (OSError, asyncio.IncompleteReadError) as exc:
                log.warning("feed connection failed: %s", exc)
            except asyncio.CancelledError:
                raise

            if not self.reconnect:
                return

            attempts += 1
            if (
                self.max_reconnect_attempts is not None
                and attempts > self.max_reconnect_attempts
            ):
                raise ConnectionError(
                    f"feed unavailable after {attempts - 1} reconnect attempts"
                )

            log.info("reconnecting to feed (attempt %d)", attempts)
            await asyncio.sleep(self.reconnect_delay)

    async def _read_once(self) -> AsyncIterator[MarketEvent]:
        """One connection's worth of events.

        A snapshot carries both the current book and the current target/window,
        so expose both pieces to consumers immediately.  Otherwise a strategy
        connecting during the first market window would not learn the target
        until the first rollover.
        """
        client = FeedClient(host=self.host, port=self.port)
        async for message in client.subscribe():
            kind = message.get("type")

            if kind == p.SNAPSHOT:
                ts = _parse_ts(message["ts"])

                yield OrderBookState(
                    bids=message["bids"],
                    asks=message["asks"],
                    timestamp=ts,
                )
                yield NewTargetNumberUpdate(
                    target_number=message["target_number"],
                    window_end=_parse_ts(message["window_end"]),
                    timestamp=ts,
                )
                continue

            event = self._decode(message)
            if event is not None:
                yield event

    @staticmethod
    def _decode(message: dict[str, Any]) -> MarketEvent | None:
        kind = message.get("type")
        ts = _parse_ts(message["ts"])

        if kind == p.BOOK_UPDATE:
            return OrderBookUpdate(
                updated_bids=message["bids"],
                updated_asks=message["asks"],
                timestamp=ts,
            )

        if kind == p.NEW_TARGET:
            return NewTargetNumberUpdate(
                target_number=message["target_number"],
                window_end=_parse_ts(message["window_end"]),
                timestamp=ts,
            )

        # Unknown types are skipped rather than raising, so the venue can add
        # messages without breaking every consumer.
        log.debug("ignoring unknown message type: %s", kind)
        return None


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)
