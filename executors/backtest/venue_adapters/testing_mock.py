from decimal import Decimal

from ....core.events import EventEnvelope
from ....core.outputs import PlaceOrderIntent
from ..models import BookUpdate, VenueOrderAccepted, VenueOrderRejected
from ..protocols import VenueAdapter


class MockBacktestVenueAdapter(VenueAdapter[str]):
    """Small deterministic adapter for examples/tests; instruments are venue keys."""

    def __init__(self, processing_latency: float = 0.0, fee_rate: Decimal = Decimal('0')):
        self._processing_latency = processing_latency
        self.fee_rate = fee_rate

    @property
    def processing_latency(self) -> float:
        return self._processing_latency

    def instrument_key(self, instrument: str) -> str:
        return instrument

    def normalise_event(self, event: EventEnvelope) -> BookUpdate | None:
        return event.payload if isinstance(event.payload, BookUpdate) else None

    def validate_place_order(self, instrument: str, order: PlaceOrderIntent):
        if order.limit_price is not None and not Decimal('0') <= order.limit_price <= Decimal('1'):
            return VenueOrderRejected('Price outside prediction-market range')
        return VenueOrderAccepted(order.quantity, order.limit_price, f'mock-{order.intent_id}')

    def calculate_fee(self, instrument, order, quantity, price, *, is_maker):
        return quantity * price * self.fee_rate
