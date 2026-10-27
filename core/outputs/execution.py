# core/outputs/execution.py

from dataclasses import dataclass
from decimal import Decimal

from .base import StrategyOutput
from ..models.orders import OrderSide, OrderType, TimeInForce


@dataclass(frozen=True, slots=True)
class PlaceOrderIntent(StrategyOutput):
    intent_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    time_in_force: TimeInForce
    limit_expiry: float = 0
    limit_price: Decimal | None = None

    # No default on time_in_force on purpose. A sniping strategy almost never
    # wants a GTC order resting on the book after the market has repriced, and
    # a default would let that happen silently. The strategy has to say.

    def __post_init__(self) -> None:
        if not self.intent_id:
            raise ValueError("intent_id cannot be empty")

        if not self.instrument_id:
            raise ValueError("instrument_id cannot be empty")

        if self.quantity <= 0:
            raise ValueError("quantity must be positive")

        if self.order_type is OrderType.LIMIT:
            if self.limit_price is None:
                raise ValueError("Limit orders require a limit price")
            elif self.limit_expiry < 0:
                raise ValueError("Limit expiry must be greater than 0 seconds")

        elif self.limit_price is not None:
            raise ValueError(
                "limit_price must only be provided for limit orders"
            )

        if (
            self.order_type is OrderType.MARKET
            and self.time_in_force is TimeInForce.GTC
        ):
            raise ValueError(
                "Market orders cannot be GTC; use IOC or FOK"
            )


@dataclass(frozen=True, slots=True)
class CancelOrderIntent(StrategyOutput):
    intent_id: str
    target_intent_id: str

    def __post_init__(self) -> None:
        if not self.intent_id:
            raise ValueError("intent_id cannot be empty")

        if not self.target_intent_id:
            raise ValueError("target_intent_id cannot be empty")

        if self.intent_id == self.target_intent_id:
            raise ValueError(
                "Cancellation intent cannot target itself"
            )


ExecutionIntent = PlaceOrderIntent | CancelOrderIntent