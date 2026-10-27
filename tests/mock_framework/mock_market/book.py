"""Order book and fill matching. No asyncio, no I/O — just the book."""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import Levels

BUY = "buy"
SELL = "sell"


@dataclass(frozen=True, slots=True)
class Fill:
    price: float
    size: int


def diff(old: Levels, new: Levels) -> Levels:
    """Return changed levels. Size 0 means the level has disappeared."""
    out = {price: size for price, size in new.items() if old.get(price) != size}
    out.update({price: 0 for price in old if price not in new})
    return out


class OrderBook:
    def __init__(self) -> None:
        self.bids: Levels = {}
        self.asks: Levels = {}

    def replace(self, bids: Levels, asks: Levels) -> tuple[Levels, Levels]:
        """Requote both sides, returning the diffs to broadcast."""
        bid_diff, ask_diff = diff(self.bids, bids), diff(self.asks, asks)
        self.bids, self.asks = dict(bids), dict(asks)
        return bid_diff, ask_diff

    def take(
        self,
        side: str,
        limit_price: float,
        size: int,
    ) -> tuple[list[Fill], Levels]:
        """Consume up to ``size`` against currently marketable liquidity.

        This method only performs one matching attempt.  It deliberately does
        not own order lifecycle state; ``MockMarket`` keeps any unfilled
        quantity live and can call ``take`` again on a later tick.  That gives
        the mock venue deterministic partial fills without making the book
        itself asynchronous.
        """
        book = self.asks if side == BUY else self.bids
        levels = sorted(book) if side == BUY else sorted(book, reverse=True)
        before = dict(book)

        fills: list[Fill] = []
        remaining = size

        for price in levels:
            if remaining <= 0:
                break

            if (side == BUY and price > limit_price) or (
                side == SELL and price < limit_price
            ):
                break

            available = book[price]
            taken = min(remaining, available)
            if taken <= 0:
                continue

            book[price] = available - taken
            remaining -= taken
            fills.append(Fill(price=price, size=taken))

        for price in [price for price, level_size in book.items() if level_size <= 0]:
            del book[price]

        return fills, diff(before, book)

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None
