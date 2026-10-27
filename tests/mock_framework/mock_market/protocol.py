"""Wire format for the mock venue: newline-delimited JSON over TCP.

The feed and trading connections deliberately use a very small protocol.  The
trading side now models a real order lifecycle: placement acknowledges an
order, order state can be queried, and live orders can be cancelled.
"""

from __future__ import annotations

import json
from typing import Any

Levels = dict[float, int]


def encode_levels(levels: Levels) -> dict[str, int]:
    return {str(price): size for price, size in levels.items()}


def decode_levels(raw: dict[str, int]) -> Levels:
    return {float(price): size for price, size in raw.items()}


def dumps(message: dict[str, Any]) -> bytes:
    return (json.dumps(message, separators=(",", ":")) + "\n").encode()


def loads(line: bytes) -> dict[str, Any]:
    return json.loads(line.decode())


# --- feed messages (server -> subscriber) -----------------------------------

SNAPSHOT = "snapshot"
BOOK_UPDATE = "book_update"
NEW_TARGET = "new_target"


# --- trading messages (client <-> server) -----------------------------------

AUTH = "auth"
AUTH_OK = "auth_ok"

ORDER = "order"
ORDER_RESULT = "order_result"

GET_ORDER = "get_order"
ORDER_STATE = "order_state"

CANCEL_ORDER = "cancel_order"
CANCEL_RESULT = "cancel_result"

ERROR = "error"


# --- order / command statuses -----------------------------------------------

ACCEPTED = "accepted"
OPEN = "open"
FILLED = "filled"
PARTIAL = "partial"  # Kept for compatibility with older mock tests.
CANCELLED = "cancelled"
REJECTED = "rejected"
