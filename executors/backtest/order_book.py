from decimal import Decimal

from ...core.models.orders import OrderSide
from .models import BookSide, BookUpdate, BookUpdateKind, ZERO


class OrderBook:
    """Historical depth plus permanent simulated consumption per side/price."""

    def __init__(self) -> None:
        self.initialised = False
        self.levels: dict[BookSide, dict[Decimal, Decimal]] = {
            BookSide.BID: {}, BookSide.ASK: {},
        }
        self.consumed: dict[BookSide, dict[Decimal, Decimal]] = {
            BookSide.BID: {}, BookSide.ASK: {},
        }

    def apply(self, update: BookUpdate) -> None:
        # Validate on a copy so malformed updates cannot leave a half-applied book.
        if not isinstance(update.kind, BookUpdateKind):
            raise ValueError('Unknown book update kind')
        candidate = (
            {BookSide.BID: {}, BookSide.ASK: {}}
            if update.kind is BookUpdateKind.SNAPSHOT
            else {side: dict(levels) for side, levels in self.levels.items()}
        )
        for level in update.levels:
            if not isinstance(level.side, BookSide):
                raise ValueError('Unknown book side')
            if not level.price.is_finite() or level.price < ZERO or not level.quantity.is_finite():
                raise ValueError('Invalid book price or quantity')
            levels = candidate[level.side]
            quantity = level.quantity
            if update.kind is BookUpdateKind.DELTA:
                quantity += levels.get(level.price, ZERO)
            if quantity < ZERO:
                raise ValueError('Book quantity cannot become negative')
            if quantity == ZERO:
                levels.pop(level.price, None)
            else:
                levels[level.price] = quantity
        self.levels = candidate
        # Require a full initial snapshot; subsequent increments maintain it.
        if update.kind is BookUpdateKind.SNAPSHOT:
            self.initialised = True

    def available(self, side: BookSide, price: Decimal) -> Decimal:
        return max(self.levels[side].get(price, ZERO) - self.consumed[side].get(price, ZERO), ZERO)

    def plan(
        self, side: OrderSide, quantity: Decimal, limit_price: Decimal | None,
    ) -> list[tuple[Decimal, Decimal]]:
        book_side = BookSide.ASK if side is OrderSide.BUY else BookSide.BID
        prices = sorted(self.levels[book_side], reverse=side is OrderSide.SELL)
        fills = []
        for price in prices:
            if limit_price is not None:
                if side is OrderSide.BUY and price > limit_price:
                    break
                if side is OrderSide.SELL and price < limit_price:
                    break
            amount = min(quantity, self.available(book_side, price))
            if amount > ZERO:
                fills.append((price, amount))
                quantity -= amount
            if quantity == ZERO:
                break
        return fills

    def consume(self, side: OrderSide, price: Decimal, quantity: Decimal) -> None:
        book_side = BookSide.ASK if side is OrderSide.BUY else BookSide.BID
        if quantity <= ZERO or quantity > self.available(book_side, price):
            raise ValueError('Cannot consume unavailable liquidity')
        consumed = self.consumed[book_side]
        consumed[price] = consumed.get(price, ZERO) + quantity
