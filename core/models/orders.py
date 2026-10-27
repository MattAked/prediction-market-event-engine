# core/models/orders.py

from enum import StrEnum


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class TimeInForce(StrEnum):
    GTC = "gtc"  # rests until cancelled
    IOC = "ioc"  # fill what's available now, drop the rest
    FOK = "fok"  # fill entirely now or not at all