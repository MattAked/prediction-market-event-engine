from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from ...core.events import EventEnvelope, TimerExpired
from ...core.events.execution import (
    OrderCancellationFailed,
    OrderCancellationRejected,
    OrderCancelled,
    OrderFailed,
    OrderFilled,
    OrderPartiallyFilled,
    OrderSubmissionFailed,
    OrderSubmissionRejected,
)
from ...core.models.orders import OrderSide, OrderType, TimeInForce
from ...core.outputs import (
    CancelOrderIntent,
    CancelTimer,
    PlaceOrderIntent,
    ScheduleTimer,
    StrategyOutput,
)
from ...feeds.prediction_markets.mock_market.events import (
    NewTargetNumberUpdate,
    OrderBookState,
    OrderBookUpdate,
)
from ...feeds.testing.events import PredictedNumberEvent
from ...strategy.base import StrategyLogic
from .state import PredictedNumberParams, PredictedNumberState, PriceSample


class PredictedNumberStrategy(StrategyLogic):
    """Simple end-to-end strategy used to exercise the live framework.

    When the predicted number implies enough edge over the stale mock ask, the
    strategy places one marketable limit buy.  A timer cancels any remainder so
    the mock venue can exercise partial-fill/cancellation handling.  Any tokens
    actually acquired are held briefly and then sold with a marketable limit
    sell.

    This is deliberately test logic rather than a real trading strategy.
    """

    HEAVY_PAYLOADS: frozenset[type] = frozenset({PredictedNumberEvent})

    def is_heavy(self, payload: object) -> bool:
        return type(payload) in self.HEAVY_PAYLOADS

    def handle_event(
        self,
        event: EventEnvelope,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        payload = event.payload
        ts = event.timestamp

        # Market/event feeds.
        if isinstance(payload, OrderBookUpdate):
            return self._on_book_update(payload, ts, state, params)
        if isinstance(payload, PredictedNumberEvent):
            return self._on_predicted_number(
                payload,
                ts,
                event.instance_id,
                state,
                params,
            )
        if isinstance(payload, OrderBookState):
            return self._on_snapshot(payload, ts, state, params)
        if isinstance(payload, NewTargetNumberUpdate):
            return self._on_new_target(payload, ts, state, params)

        # Engine timer events.
        if isinstance(payload, TimerExpired):
            return self._on_timer_expired(
                payload,
                event.instance_id,
                state,
                params,
            )

        # Executor lifecycle events.
        if isinstance(payload, OrderPartiallyFilled):
            return self._on_partial_fill(payload, state)
        if isinstance(payload, OrderFilled):
            return self._on_order_filled(
                payload,
                event.instance_id,
                state,
                params,
            )
        if isinstance(payload, OrderCancelled):
            return self._on_order_cancelled(
                payload,
                event.instance_id,
                state,
                params,
            )
        if isinstance(payload, OrderFailed):
            return self._on_order_failed(
                payload,
                event.instance_id,
                state,
                params,
            )
        if isinstance(payload, (OrderSubmissionRejected, OrderSubmissionFailed)):
            return self._on_submission_terminal(payload.intent_id, state)
        if isinstance(payload, (OrderCancellationRejected, OrderCancellationFailed)):
            return self._on_cancellation_unsuccessful(payload.intent_id, state)

        # OrderSubmitted and unrelated payloads require no strategy state change.
        return []

    # --- market events ------------------------------------------------------

    def _on_snapshot(
        self,
        payload: OrderBookState,
        ts: datetime,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        state.bids = dict(payload.bids)
        state.asks = dict(payload.asks)
        state.book_ready = True
        self._record_price(state, ts)
        return []

    def _on_book_update(
        self,
        payload: OrderBookUpdate,
        ts: datetime,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        if not state.book_ready:
            return []

        for book, updates in (
            (state.bids, payload.updated_bids),
            (state.asks, payload.updated_asks),
        ):
            for price, size in updates.items():
                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size

        self._record_price(state, ts)
        return []

    def _on_new_target(
        self,
        payload: NewTargetNumberUpdate,
        ts: datetime,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        state.target_number = payload.target_number
        state.window_end = payload.window_end
        state.price_buffer.clear()
        return []

    # --- signal -------------------------------------------------------------

    def _on_predicted_number(
        self,
        payload: PredictedNumberEvent,
        ts: datetime,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        state.last_number = payload.number

        # Keep the mock lifecycle intentionally simple: one live order and one
        # position at a time.  Further signals are ignored until we are flat.
        if (
            state.active_order_intent_id is not None
            or state.position_quantity > 0
            or state.exit_timer_id is not None
        ):
            return []

        if not state.book_ready or state.target_number is None or state.window_end is None:
            return []

        best_ask = state.best_ask
        if best_ask is None:
            return []

        seconds_left = (state.window_end - ts).total_seconds()
        fair = self.fair_value(
            payload.number,
            state.target_number,
            seconds_left,
            params.tick_seconds,
        )

        if self.market_already_priced(state, ts, params, fair):
            return []

        if fair - best_ask < params.min_edge:
            return []

        size = min(params.max_clip, state.asks.get(best_ask, 0))
        if size <= 0:
            return []

        intent_id = self._next_id(state, instance_id, "buy")
        timer_id = self._next_id(state, instance_id, "order-timeout")

        state.active_order_intent_id = intent_id
        state.active_order_side = OrderSide.BUY
        state.active_order_filled_quantity = Decimal("0")
        state.active_cancel_intent_id = None
        state.order_timeout_timer_id = timer_id

        # The mock venue has no market orders and fills only on later ticks.
        # Using a marketable limit keeps the order executable after the book
        # reprices, while still exercising the LIMIT path in the real executor.
        return [
            PlaceOrderIntent(
                intent_id=intent_id,
                instrument_id=params.instrument_id,
                side=OrderSide.BUY,
                quantity=Decimal(size),
                order_type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                limit_expiry=params.order_timeout_seconds,
                limit_price=Decimal("1"),
            ),
            ScheduleTimer(
                timer_id=timer_id,
                delay=timedelta(seconds=params.order_timeout_seconds),
            ),
        ]

    # --- timers -------------------------------------------------------------

    def _on_timer_expired(
        self,
        payload: TimerExpired,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        if payload.timer_id == state.order_timeout_timer_id:
            state.order_timeout_timer_id = None

            if (
                state.active_order_intent_id is None
                or state.active_cancel_intent_id is not None
            ):
                return []

            cancel_intent_id = self._next_id(state, instance_id, "cancel")
            state.active_cancel_intent_id = cancel_intent_id

            return [
                CancelOrderIntent(
                    intent_id=cancel_intent_id,
                    target_intent_id=state.active_order_intent_id,
                )
            ]

        if payload.timer_id == state.exit_timer_id:
            state.exit_timer_id = None

            if (
                state.position_quantity <= 0
                or state.active_order_intent_id is not None
                or not state.book_ready
                or state.best_bid is None
            ):
                return []

            intent_id = self._next_id(state, instance_id, "sell")
            timer_id = self._next_id(state, instance_id, "order-timeout")

            state.active_order_intent_id = intent_id
            state.active_order_side = OrderSide.SELL
            state.active_order_filled_quantity = Decimal("0")
            state.active_cancel_intent_id = None
            state.order_timeout_timer_id = timer_id

            # Same testing rationale as the buy: a zero-price sell limit is
            # marketable against any bid in the mock book and therefore tests
            # fill handling rather than whether the book moved during the delay.
            return [
                PlaceOrderIntent(
                    intent_id=intent_id,
                    instrument_id=params.instrument_id,
                    side=OrderSide.SELL,
                    quantity=state.position_quantity,
                    order_type=OrderType.LIMIT,
                    time_in_force=TimeInForce.GTC,
                    limit_expiry=params.order_timeout_seconds,
                    limit_price=Decimal("0"),
                ),
                ScheduleTimer(
                    timer_id=timer_id,
                    delay=timedelta(seconds=params.order_timeout_seconds),
                ),
            ]

        return []

    # --- execution events ---------------------------------------------------

    def _on_partial_fill(
        self,
        payload: OrderPartiallyFilled,
        state: PredictedNumberState,
    ) -> list[StrategyOutput]:
        if payload.intent_id != state.active_order_intent_id:
            return []

        self._apply_incremental_fill(payload.fill_quantity, state)
        return []

    def _on_order_filled(
        self,
        payload: OrderFilled,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        if payload.intent_id != state.active_order_intent_id:
            return []

        self._apply_incremental_fill(payload.fill_quantity, state)
        return self._finish_active_order(instance_id, state, params)

    def _on_order_cancelled(
        self,
        payload: OrderCancelled,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        # Cancellation terminality consistently identifies the placement.
        if payload.intent_id != state.active_order_intent_id:
            return []

        self._apply_cumulative_fill(payload.filled_quantity, state)
        return self._finish_active_order(instance_id, state, params)

    def _on_order_failed(
        self,
        payload: OrderFailed,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        if payload.intent_id != state.active_order_intent_id:
            return []

        self._apply_cumulative_fill(payload.filled_quantity, state)
        return self._finish_active_order(instance_id, state, params)

    def _on_submission_terminal(
        self,
        intent_id: str,
        state: PredictedNumberState,
    ) -> list[StrategyOutput]:
        if intent_id != state.active_order_intent_id:
            return []

        outputs: list[StrategyOutput] = []
        if state.order_timeout_timer_id is not None:
            outputs.append(CancelTimer(state.order_timeout_timer_id))

        self._clear_active_order(state)
        return outputs

    @staticmethod
    def _on_cancellation_unsuccessful(
        intent_id: str,
        state: PredictedNumberState,
    ) -> list[StrategyOutput]:
        if intent_id == state.active_cancel_intent_id:
            # The order itself may still be live and its monitor remains the
            # source of truth.  Do not clear the active order here.
            state.active_cancel_intent_id = None
        return []

    def _finish_active_order(
        self,
        instance_id: str | None,
        state: PredictedNumberState,
        params: PredictedNumberParams,
    ) -> list[StrategyOutput]:
        completed_side = state.active_order_side
        outputs: list[StrategyOutput] = []

        if state.order_timeout_timer_id is not None:
            outputs.append(CancelTimer(state.order_timeout_timer_id))

        self._clear_active_order(state)

        # Once a buy is terminal, sell whatever actually filled after a short
        # hold.  A partially-filled/cancelled buy therefore still exercises the
        # exit path using the executor's reconciled fill quantity.
        if completed_side is OrderSide.BUY and state.position_quantity > 0:
            timer_id = self._next_id(state, instance_id, "exit")
            state.exit_timer_id = timer_id
            outputs.append(
                ScheduleTimer(
                    timer_id=timer_id,
                    delay=timedelta(seconds=params.exit_delay_seconds),
                )
            )

        # If an exit itself was cancelled/failed with quantity still held,
        # retry later rather than leaving the mock position stranded forever.
        elif completed_side is OrderSide.SELL and state.position_quantity > 0:
            timer_id = self._next_id(state, instance_id, "exit-retry")
            state.exit_timer_id = timer_id
            outputs.append(
                ScheduleTimer(
                    timer_id=timer_id,
                    delay=timedelta(seconds=params.exit_delay_seconds),
                )
            )

        return outputs

    @staticmethod
    def _apply_incremental_fill(
        fill_quantity: Decimal,
        state: PredictedNumberState,
    ) -> None:
        state.active_order_filled_quantity += fill_quantity

        if state.active_order_side is OrderSide.BUY:
            state.position_quantity += fill_quantity
        elif state.active_order_side is OrderSide.SELL:
            state.position_quantity -= fill_quantity

        if state.position_quantity < 0:
            state.position_quantity = Decimal("0")

    def _apply_cumulative_fill(
        self,
        cumulative_quantity: Decimal,
        state: PredictedNumberState,
    ) -> None:
        delta = cumulative_quantity - state.active_order_filled_quantity
        if delta > 0:
            self._apply_incremental_fill(delta, state)

    @staticmethod
    def _clear_active_order(state: PredictedNumberState) -> None:
        state.active_order_intent_id = None
        state.active_order_side = None
        state.active_order_filled_quantity = Decimal("0")
        state.active_cancel_intent_id = None
        state.order_timeout_timer_id = None

    @staticmethod
    def _next_id(
        state: PredictedNumberState,
        instance_id: str | None,
        kind: str,
    ) -> str:
        sequence = state.next_id
        state.next_id += 1
        prefix = instance_id or "mock"
        return f"{prefix}:{kind}:{sequence}"

    # --- pricing helpers ----------------------------------------------------

    @staticmethod
    def fair_value(
        number: int,
        target: int,
        seconds_left: float,
        tick_seconds: float = 1.0,
    ) -> float:
        if seconds_left <= 0:
            return 0.0

        ticks_left = seconds_left / tick_seconds
        distance = abs(target - number)
        closeness = max(0.0, 1.0 - distance / 20)
        urgency = min(1.0, ticks_left / max(distance, 1))
        return round(closeness * urgency, 2)

    @staticmethod
    def _record_price(state: PredictedNumberState, ts: datetime) -> None:
        state.price_buffer.append(
            PriceSample(
                timestamp=ts,
                best_bid=state.best_bid,
                best_ask=state.best_ask,
            )
        )

    @staticmethod
    def market_already_priced(
        state: PredictedNumberState,
        ts: datetime,
        params: PredictedNumberParams,
        fair: float,
    ) -> bool:
        cutoff = params.reaction_window_ms / 1000.0
        prior = [
            sample.best_ask
            for sample in state.price_buffer
            if sample.best_ask is not None
            and (ts - sample.timestamp).total_seconds() <= cutoff
        ]
        if not prior:
            return False

        current = state.best_ask
        if current is None:
            return False

        reference = prior[0]
        direction = 1.0 if fair > reference else -1.0
        return (current - reference) * direction > params.reaction_threshold
