from __future__ import annotations
from ....core.models.orders import TimeInForce

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..models import (
    CancelOrderResult,
    MonitorResult,
    PlaceOrderResult,
    VenueCancellationAccepted,
    VenueOrderAccepted,
    VenueOrderCancellationFailed,
    VenueOrderCancellationRejected,
    VenueOrderCancelled,
    VenueOrderFailed,
    VenueOrderFill,
    VenueOrderRejected,
)
from ..protocols import VenueAdapter
from ....tests.mock_framework.mock_market import protocol as p
from ....tests.mock_framework.mock_market.client import TradingClient


@dataclass(slots=True)
class MockSubmission:
    venue_order_id: str
    emitted_fill_count: int = 0


class MockAdapter(VenueAdapter[str, MockSubmission]):
    """Executor adapter for the socket-based mock venue.

    The server owns all order state.  Placement only returns acceptance;
    ``monitor_order`` reconciles cumulative fills by polling ``GET_ORDER`` and
    yields each fill exactly once before yielding a terminal cancellation.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8802,
        api_key: str = "test-key",
        poll_interval: float = 0.05,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")

        self.client = TradingClient(
            host=host,
            port=port,
            api_key=api_key,
        )
        self.poll_interval = poll_interval

    async def place_market_buy(
        self,
        instrument: str,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[MockSubmission]:
        raise NotImplementedError("Mock market does not support market orders")

    async def place_market_sell(
        self,
        instrument: str,
        quantity: Decimal,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[MockSubmission]:
        raise NotImplementedError("Mock market does not support market orders")

    async def place_limit_buy(
        self,
        instrument: str,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[MockSubmission]:
        if time_in_force is not TimeInForce.GTC:
            return VenueOrderRejected(reason="Mock venue only supports GTC limit orders")
        return await self._place_limit_order(
            side="buy",
            quantity=quantity,
            price=price,
        )

    async def place_limit_sell(
        self,
        instrument: str,
        quantity: Decimal,
        price: Decimal,
        expiry: float,
        *,
        time_in_force: TimeInForce,
    ) -> PlaceOrderResult[MockSubmission]:
        if time_in_force is not TimeInForce.GTC:
            return VenueOrderRejected(reason="Mock venue only supports GTC limit orders")
        return await self._place_limit_order(
            side="sell",
            quantity=quantity,
            price=price,
        )

    async def _place_limit_order(
        self,
        side: str,
        quantity: Decimal,
        price: Decimal,
    ) -> PlaceOrderResult[MockSubmission]:
        if quantity != quantity.to_integral_value():
            return VenueOrderRejected(
                reason="Mock market only supports whole-number quantities"
            )

        await self.client.connect()

        if side == "buy":
            result = await self.client.buy(float(price), int(quantity))
        elif side == "sell":
            result = await self.client.sell(float(price), int(quantity))
        else:
            raise ValueError(f"unsupported side: {side}")

        if result.get("type") == p.ERROR:
            return VenueOrderRejected(
                reason=str(result.get("reason", "mock venue rejected order"))
            )

        if result.get("type") != p.ORDER_RESULT:
            raise RuntimeError(
                f"Unexpected mock order response type: {result.get('type')!r}"
            )

        status = result.get("status")
        if status == p.REJECTED:
            return VenueOrderRejected(
                reason=str(result.get("reason", "mock venue rejected order"))
            )

        if status != p.ACCEPTED:
            raise RuntimeError(f"Unexpected mock order status: {status!r}")

        venue_order_id = result.get("venue_order_id")
        if not venue_order_id:
            raise RuntimeError("Mock venue accepted order without venue_order_id")

        return VenueOrderAccepted(
            venue_submission=MockSubmission(
                venue_order_id=str(venue_order_id),
            ),
            accepted_quantity=Decimal(str(result["requested"])),
            accepted_price=Decimal(str(result["price"])),
        )

    async def cancel_order(
        self,
        submission: MockSubmission,
    ) -> CancelOrderResult:
        try:
            await self.client.connect()
            result = await self.client.cancel_order(submission.venue_order_id)
        except Exception as exc:
            return VenueOrderCancellationFailed(reason=str(exc))

        if result.get("type") == p.ERROR:
            return VenueOrderCancellationFailed(
                reason=str(result.get("reason", "mock cancellation failed"))
            )

        if result.get("type") != p.CANCEL_RESULT:
            return VenueOrderCancellationFailed(
                reason=(
                    "Unexpected mock cancellation response type: "
                    f"{result.get('type')!r}"
                )
            )

        status = result.get("status")

        if status == p.ACCEPTED:
            return VenueCancellationAccepted()

        if status == p.REJECTED:
            return VenueOrderCancellationRejected(
                reason=str(result.get("reason", "mock cancellation rejected"))
            )

        return VenueOrderCancellationFailed(
            reason=f"Unexpected mock cancellation status: {status!r}"
        )

    async def monitor_order(
        self,
        submission: MockSubmission,
    ) -> AsyncIterator[MonitorResult[MockSubmission]]:
        try:
            await self.client.connect()
        except Exception as exc:
            yield VenueOrderFailed(
                venue_submission=submission,
                reason=str(exc),
            )
            return

        while True:
            try:
                state = await self.client.get_order(submission.venue_order_id)
            except Exception as exc:
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason=str(exc),
                )
                return

            if state.get("type") == p.ERROR:
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason=str(state.get("reason", "mock order lookup failed")),
                )
                return

            if state.get("type") != p.ORDER_STATE:
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason=(
                        "Unexpected mock order-state response type: "
                        f"{state.get('type')!r}"
                    ),
                )
                return

            fills = state.get("fills")
            if not isinstance(fills, list):
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason="Mock order state contained invalid fills data",
                )
                return

            if len(fills) < submission.emitted_fill_count:
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason="Mock venue fill history moved backwards",
                )
                return

            unseen_fills = fills[submission.emitted_fill_count :]

            for raw_fill in unseen_fills:
                if not isinstance(raw_fill, dict):
                    yield VenueOrderFailed(
                        venue_submission=submission,
                        reason="Mock order state contained an invalid fill",
                    )
                    return

                try:
                    fill_quantity = Decimal(str(raw_fill["size"]))
                    fill_price = Decimal(str(raw_fill["price"]))
                except (KeyError, TypeError, ValueError) as exc:
                    yield VenueOrderFailed(
                        venue_submission=submission,
                        reason=f"Invalid mock fill data: {exc}",
                    )
                    return

                # Advance before yielding so a consumer cancellation/restart
                # cannot make this same adapter instance emit the fill twice.
                submission.emitted_fill_count += 1

                yield VenueOrderFill(
                    venue_submission=submission,
                    fill_quantity=fill_quantity,
                    fill_price=fill_price,
                )

            status = state.get("status")

            if status == p.FILLED:
                return

            if status == p.CANCELLED:
                # All cumulative fills above are emitted before the terminal
                # cancellation, preserving the executor's lifecycle ordering.
                yield VenueOrderCancelled(
                    venue_submission=submission,
                )
                return

            if status != p.OPEN:
                yield VenueOrderFailed(
                    venue_submission=submission,
                    reason=f"Unexpected mock order status: {status!r}",
                )
                return

            await asyncio.sleep(self.poll_interval)

    async def close(self) -> None:
        await self.client.close()
