from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from ...core.models.orders import OrderSide


@dataclass(frozen=True, slots=True)
class PriceSample:
    """One observation of the top of book, keyed by the event's own timestamp."""

    timestamp: datetime
    best_bid: float | None
    best_ask: float | None


@dataclass(frozen=True, slots=True)
class PredictedNumberParams:
    """Immutable config. Never written to by the logic."""

    min_edge: float = 0.10
    """Minimum fair-vs-ask gap before acting."""

    reaction_window_ms: int = 1_000
    """How far back to look when deciding whether the market has already moved.
    Keep this near one tick; a longer window compares against prices from
    before the information existed."""

    reaction_threshold: float = 0.10
    """Movement *toward* fair over that window above which we assume we're late."""

    max_clip: int = 100
    """Cap on tokens taken per signal."""

    price_buffer_size: int = 64

    tick_seconds: float = 1.0
    """Sampling interval of the signal stream. The model measures time in
    ticks, so it needs this to convert a wall-clock window into steps."""

    instrument_id: str = "mock"
    """Framework instrument id used when emitting execution intents."""

    order_timeout_seconds: float = 1.5
    """How long a live mock order may remain open before requesting cancel."""

    exit_delay_seconds: float = 1.0
    """How long to hold a filled/partially-filled buy before trying to sell."""


@dataclass(slots=True)
class PredictedNumberState:
    """Mutable working state, owned by one StrategyInstance."""

    bids: dict[float, int] = field(default_factory=dict)
    asks: dict[float, int] = field(default_factory=dict)
    target_number: int | None = None
    window_end: datetime | None = None
    price_buffer: deque[PriceSample] = field(default_factory=lambda: deque(maxlen=64))

    book_ready: bool = False
    """False until the first snapshot lands. Diffs before that must be dropped."""

    last_number: int | None = None

    # Simple execution state for the mock strategy.  We deliberately allow only
    # one live order at a time so the test exercises the framework lifecycle
    # without needing a full portfolio/order manager inside the strategy.
    position_quantity: Decimal = Decimal("0")
    active_order_intent_id: str | None = None
    active_order_side: OrderSide | None = None
    active_order_filled_quantity: Decimal = Decimal("0")
    active_cancel_intent_id: str | None = None

    order_timeout_timer_id: str | None = None
    exit_timer_id: str | None = None
    next_id: int = 1

    @classmethod
    def create(cls, params: PredictedNumberParams) -> PredictedNumberState:
        return cls(price_buffer=deque(maxlen=params.price_buffer_size))

    # --- convenience --------------------------------------------------------

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None
