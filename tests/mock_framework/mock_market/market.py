"""The mock venue itself.

The market-data scenario is unchanged: prices are still derived from the
scenario's delayed number.  Trading now has an actual lifecycle instead of
immediate-or-cancel semantics:

* ``submit`` creates an accepted live order immediately.
* each market tick may fill a configurable chunk of each live order;
* cumulative order state remains queryable after fills/cancellation;
* cancellation stops any remaining quantity from filling.

There is still no asyncio or socket I/O in this module, so it can be driven
synchronously in unit tests.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..mock_scenario import Scenario
from . import protocol as p
from .book import BUY, SELL, Fill, OrderBook


def price_from(number: int, target: int, ticks_left: int) -> float:
    """Close to target -> near 1.0. Running out of time -> falls back."""
    distance = abs(target - number)
    closeness = max(0.0, 1.0 - distance / 20)
    urgency = min(1.0, ticks_left / max(distance, 1))
    return round(closeness * urgency, 2)


@dataclass(slots=True)
class MockOrder:
    venue_order_id: str
    side: str
    limit_price: float
    requested_size: int
    remaining_size: int
    submitted_tick: int
    submitted_at: datetime

    status: str = p.OPEN
    fills: list[Fill] = field(default_factory=list)
    cancelled_at: datetime | None = None
    filled_at: datetime | None = None

    @property
    def filled_size(self) -> int:
        return self.requested_size - self.remaining_size

    @property
    def average_fill_price(self) -> float | None:
        if not self.fills:
            return None

        total_size = sum(fill.size for fill in self.fills)
        total_cost = sum(fill.price * fill.size for fill in self.fills)
        return round(total_cost / total_size, 4)


class MockMarket:
    def __init__(
        self,
        scenario: Scenario,
        spread: float = 0.02,
        depth: int = 3,
        level_size: int = 200,
        fill_delay_ticks: int = 1,
        fill_size_per_tick: int = 25,
    ) -> None:
        if fill_delay_ticks < 0:
            raise ValueError("fill_delay_ticks must be >= 0")
        if fill_size_per_tick <= 0:
            raise ValueError("fill_size_per_tick must be > 0")

        self.scenario = scenario
        self.spread = spread
        self.depth = depth
        self.level_size = level_size
        self.fill_delay_ticks = fill_delay_ticks
        self.fill_size_per_tick = fill_size_per_tick

        self.book = OrderBook()
        self.orders: dict[str, MockOrder] = {}

        self._order_ids = itertools.count(1)
        self._tick = 0
        self._window: int | None = None
        self.target_number: int = scenario.target(0)
        self.window_end: datetime = scenario.window_end(0)
        self._prime()

    # --- pricing ------------------------------------------------------------

    def _quote(self, tick: int) -> tuple[dict[float, int], dict[float, int]]:
        fair = price_from(
            self.scenario.delayed_number(tick),
            self.scenario.target(tick),
            self.scenario.ticks_left(tick),
        )
        best_bid = round(min(max(fair - self.spread / 2, 0.0), 0.98), 2)
        best_ask = round(min(max(fair + self.spread / 2, 0.01), 1.0), 2)

        bids = {
            round(best_bid - i * 0.01, 2): self.level_size * (i + 1)
            for i in range(self.depth)
            if best_bid - i * 0.01 >= 0
        }
        asks = {
            round(best_ask + i * 0.01, 2): self.level_size * (i + 1)
            for i in range(self.depth)
            if best_ask + i * 0.01 <= 1.0
        }
        return bids, asks

    def _prime(self) -> None:
        bids, asks = self._quote(0)
        self.book.replace(bids, asks)
        self._window = self.scenario.window(0)
        self.window_end = self.scenario.window_end(0)

    # --- driving ------------------------------------------------------------

    def tick(self, now: float, wall_now: datetime) -> list[dict[str, Any]]:
        """Advance the venue one scenario tick and attempt live-order fills."""
        tick = self.scenario.tick_at(now)
        self._tick = tick

        messages: list[dict[str, Any]] = []

        window = self.scenario.window(tick)
        if window != self._window:
            self._window = window
            self.target_number = self.scenario.target(tick)
            self.window_end = self.scenario.window_end(tick)
            messages.append(self.new_target_message(wall_now))

        # Makers refresh the public book each tick, exactly as before.
        bids, asks = self._quote(tick)
        bid_diff, ask_diff = self.book.replace(bids, asks)
        if bid_diff or ask_diff:
            messages.append(self.book_update_message(bid_diff, ask_diff, wall_now))

        # After the fresh quote is available, live taker orders can consume a
        # bounded amount of liquidity.  Their remainder stays live for future
        # ticks, which is what creates deterministic partial fills.
        messages.extend(self._fill_live_orders(tick, wall_now))

        return messages

    def _fill_live_orders(
        self,
        tick: int,
        wall_now: datetime,
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []

        for order in self.orders.values():
            if order.status != p.OPEN:
                continue

            if tick - order.submitted_tick < self.fill_delay_ticks:
                continue

            size_to_try = min(order.remaining_size, self.fill_size_per_tick)
            if size_to_try <= 0:
                continue

            fills, book_diff = self.book.take(
                side=order.side,
                limit_price=order.limit_price,
                size=size_to_try,
            )

            if not fills:
                continue

            order.fills.extend(fills)
            order.remaining_size -= sum(fill.size for fill in fills)

            if order.remaining_size <= 0:
                order.remaining_size = 0
                order.status = p.FILLED
                order.filled_at = wall_now

            if book_diff:
                bid_diff, ask_diff = (
                    ({}, book_diff)
                    if order.side == BUY
                    else (book_diff, {})
                )
                messages.append(
                    self.book_update_message(bid_diff, ask_diff, wall_now)
                )

        return messages

    # --- trading ------------------------------------------------------------

    def submit(
        self,
        side: str,
        limit_price: float,
        size: int,
        wall_now: datetime,
    ) -> tuple[dict[str, Any], None]:
        """Accept a new live limit order without filling it immediately."""
        if side not in (BUY, SELL):
            return self._order_rejection(f"unknown side: {side}"), None
        if size <= 0:
            return self._order_rejection("size must be positive"), None
        if not 0.0 <= limit_price <= 1.0:
            return self._order_rejection("price must be in [0, 1]"), None

        venue_order_id = f"o{next(self._order_ids)}"
        order = MockOrder(
            venue_order_id=venue_order_id,
            side=side,
            limit_price=limit_price,
            requested_size=size,
            remaining_size=size,
            submitted_tick=self._tick,
            submitted_at=wall_now,
        )
        self.orders[venue_order_id] = order

        return {
            "type": p.ORDER_RESULT,
            "status": p.ACCEPTED,
            "venue_order_id": venue_order_id,
            "side": side,
            "requested": size,
            "price": limit_price,
            "ts": wall_now.isoformat(),
        }, None

    def get_order(
        self,
        venue_order_id: str,
        wall_now: datetime,
    ) -> dict[str, Any]:
        """Return cumulative order state for adapter reconciliation."""
        order = self.orders.get(venue_order_id)
        if order is None:
            return {
                "type": p.ERROR,
                "reason": "order not found",
                "venue_order_id": venue_order_id,
            }

        return {
            "type": p.ORDER_STATE,
            "venue_order_id": order.venue_order_id,
            "status": order.status,
            "side": order.side,
            "limit_price": order.limit_price,
            "requested": order.requested_size,
            "filled": order.filled_size,
            "remaining": order.remaining_size,
            "fills": [
                {"price": fill.price, "size": fill.size}
                for fill in order.fills
            ],
            "avg_price": order.average_fill_price,
            "submitted_at": order.submitted_at.isoformat(),
            "ts": wall_now.isoformat(),
        }

    def cancel_order(
        self,
        venue_order_id: str,
        wall_now: datetime,
    ) -> dict[str, Any]:
        """Cancel whatever quantity remains on a live order."""
        order = self.orders.get(venue_order_id)

        if order is None:
            return self._cancel_rejection(
                venue_order_id,
                "order not found",
                wall_now,
            )

        if order.status == p.FILLED:
            return self._cancel_rejection(
                venue_order_id,
                "order already filled",
                wall_now,
            )

        if order.status == p.CANCELLED:
            return self._cancel_rejection(
                venue_order_id,
                "order already cancelled",
                wall_now,
            )

        order.status = p.CANCELLED
        order.cancelled_at = wall_now

        return {
            "type": p.CANCEL_RESULT,
            "status": p.ACCEPTED,
            "venue_order_id": venue_order_id,
            "filled": order.filled_size,
            "remaining": order.remaining_size,
            "ts": wall_now.isoformat(),
        }

    @staticmethod
    def _order_rejection(reason: str) -> dict[str, Any]:
        return {
            "type": p.ORDER_RESULT,
            "status": p.REJECTED,
            "reason": reason,
        }

    @staticmethod
    def _cancel_rejection(
        venue_order_id: str,
        reason: str,
        wall_now: datetime,
    ) -> dict[str, Any]:
        return {
            "type": p.CANCEL_RESULT,
            "status": p.REJECTED,
            "venue_order_id": venue_order_id,
            "reason": reason,
            "ts": wall_now.isoformat(),
        }

    # --- message builders ---------------------------------------------------

    def snapshot_message(self, wall_now: datetime) -> dict[str, Any]:
        return {
            "type": p.SNAPSHOT,
            "bids": p.encode_levels(self.book.bids),
            "asks": p.encode_levels(self.book.asks),
            "target_number": self.target_number,
            "window_end": self.window_end.isoformat(),
            "ts": wall_now.isoformat(),
        }

    def new_target_message(self, wall_now: datetime) -> dict[str, Any]:
        return {
            "type": p.NEW_TARGET,
            "target_number": self.target_number,
            "window_end": self.window_end.isoformat(),
            "ts": wall_now.isoformat(),
        }

    @staticmethod
    def book_update_message(
        bid_diff: dict[float, int],
        ask_diff: dict[float, int],
        wall_now: datetime,
    ) -> dict[str, Any]:
        return {
            "type": p.BOOK_UPDATE,
            "bids": p.encode_levels(bid_diff),
            "asks": p.encode_levels(ask_diff),
            "ts": wall_now.isoformat(),
        }
