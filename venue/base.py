# venue/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Self, TypeVar, cast

from core.venue_order_spec import (
    VenueOrderSpec,
    VenueOrderStatus,
    VenuePosition,
    VenueReceipt,
)

T = TypeVar("T")


def _is_tz_aware(dt: datetime) -> bool:
    """Return True if datetime is timezone-aware (tzinfo set and has a valid offset)."""
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def _require_key(d: Dict[str, Any], key: str) -> Any:
    """Fetch a required key from dict, raising ValueError if missing."""
    if key not in d:
        raise ValueError(f"Missing required field: {key}")
    return d[key]


def _as_str(v: Any, field: str) -> str:
    """Validate and convert value to str for a given field."""
    if isinstance(v, str):
        return v
    raise ValueError(f"Field {field} must be str, got {type(v).__name__}")


def _as_bool(v: Any, field: str) -> bool:
    """Validate value is bool for a given field."""
    if isinstance(v, bool):
        return v
    raise ValueError(f"Field {field} must be bool, got {type(v).__name__}")


def _as_decimal(v: Any, field: str) -> Decimal:
    """
    Validate and convert value to Decimal for a given field.

    Accepts:
      - Decimal
      - str (recommended, preserves precision)
      - int
    Rejects:
      - float (precision risk)
    """
    if isinstance(v, Decimal):
        return v
    if isinstance(v, str):
        try:
            return Decimal(v)
        except Exception as e:  # pragma: no cover
            raise ValueError(f"Field {field} must be a valid Decimal string: {v!r}") from e
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, float):
        raise ValueError(f"Field {field} must not be float (precision loss). Use str/Decimal instead.")
    raise ValueError(f"Field {field} must be Decimal/str/int, got {type(v).__name__}")


def _as_optional_decimal(v: Any, field: str) -> Optional[Decimal]:
    """Validate value is Optional[Decimal]-compatible."""
    if v is None:
        return None
    return _as_decimal(v, field)


def _as_datetime_tzaware(v: Any, field: str) -> datetime:
    """
    Validate and convert value to tz-aware datetime for a given field.

    Accepts:
      - datetime (must be tz-aware)
      - ISO8601 str (must parse to tz-aware)
    """
    if isinstance(v, datetime):
        if not _is_tz_aware(v):
            raise ValueError(f"Field {field} must be tz-aware datetime, got naive datetime.")
        return v
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
        except Exception as e:
            raise ValueError(f"Field {field} must be ISO8601 datetime string, got {v!r}") from e
        if not _is_tz_aware(dt):
            raise ValueError(f"Field {field} must be tz-aware datetime, got naive ISO8601: {v!r}")
        return dt
    raise ValueError(f"Field {field} must be datetime/ISO8601 str, got {type(v).__name__}")


@dataclass(slots=True, frozen=False)
class MarketStatus:
    """
    Market-level tradability snapshot for a symbol on a venue.

    This is used by execution/risk logic to decide whether it is safe/possible
    to send market orders, limit orders, or whether the market is effectively halted.

    Notes:
      - best_bid / best_ask are optional because some venues/markets may not provide them
        during outages, pre-open, auction, or illiquid moments.
      - updated_at must be tz-aware datetime.
    """

    symbol: str
    can_market_order: bool
    can_limit_order: bool
    is_halted: bool
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict.

        Conversions:
          - Decimal -> str
          - datetime -> ISO8601 string
        """
        return {
            "symbol": self.symbol,
            "can_market_order": self.can_market_order,
            "can_limit_order": self.can_limit_order,
            "is_halted": self.is_halted,
            "best_bid": None if self.best_bid is None else str(self.best_bid),
            "best_ask": None if self.best_ask is None else str(self.best_ask),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """
        Parse from dict with strict type checks.

        Rules:
          - symbol must be str
          - can_market_order/can_limit_order/is_halted must be bool
          - best_bid/best_ask must be Decimal/str/int or None (no float)
          - updated_at must be tz-aware datetime (or tz-aware ISO8601 string)
          - missing required fields -> ValueError
        """
        if not isinstance(d, dict):
            raise ValueError(f"Input must be dict, got {type(d).__name__}")

        symbol = _as_str(_require_key(d, "symbol"), "symbol")
        can_market_order = _as_bool(_require_key(d, "can_market_order"), "can_market_order")
        can_limit_order = _as_bool(_require_key(d, "can_limit_order"), "can_limit_order")
        is_halted = _as_bool(_require_key(d, "is_halted"), "is_halted")

        best_bid = _as_optional_decimal(_require_key(d, "best_bid"), "best_bid")
        best_ask = _as_optional_decimal(_require_key(d, "best_ask"), "best_ask")
        updated_at = _as_datetime_tzaware(_require_key(d, "updated_at"), "updated_at")

        return cls(
            symbol=symbol,
            can_market_order=can_market_order,
            can_limit_order=can_limit_order,
            is_halted=is_halted,
            best_bid=best_bid,
            best_ask=best_ask,
            updated_at=updated_at,
        )


class VenueAdapter(ABC):
    """
    Abstract base class for all venue adapters (e.g., Binance Perp, CTP).

    Responsibilities:
      - Translate standardized semantic-layer objects into venue-specific API calls.
      - Provide consistent async interfaces to Order Manager / Reconciler / Risk Governor.
      - Keep business logic OUT of adapters (adapters are translation + IO only).
    """

    @abstractmethod
    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        """
        Submit an order to the venue.

        Args:
            spec: Standardized order specification (already normalized/aligned).

        Returns:
            VenueReceipt: Immediate acknowledgment/receipt from the venue (or adapter),
            including venue order id, status, raw response, and timestamp.

        Notes:
            - Adapter should treat `spec.client_order_id` as the idempotency key.
            - No strategy logic here; just translate + IO + basic mapping.
        """
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        """
        Cancel an order by client_order_id (idempotency key).

        Args:
            client_order_id: Client-side order identifier used for de-duplication.

        Returns:
            VenueReceipt: Venue acknowledgment for cancel request (or current final status).
        """
        raise NotImplementedError

    @abstractmethod
    async def query_order(self, client_order_id: str) -> VenueOrderStatus:
        """
        Query latest order status snapshot from the venue.

        Args:
            client_order_id: Client-side order identifier (idempotency key).

        Returns:
            VenueOrderStatus: Current status, filled quantity/price, and updated timestamp.
        """
        raise NotImplementedError

    @abstractmethod
    async def query_positions(self) -> list[VenuePosition]:
        """
        Query current open positions from the venue.

        Returns:
            list[VenuePosition]: Position snapshots for reconciliation and risk checks.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_market_status(self, symbol: str) -> MarketStatus:
        """
        Get market tradability status for a given symbol.

        Args:
            symbol: Unified symbol (e.g., "BTCUSDT", "rb2510").

        Returns:
            MarketStatus: Whether market/limit orders are allowed, whether halted,
            best bid/ask (if available), and last update time.

        Use cases:
            - Circuit breaker staged-exit decision (market close vs. limit unwind).
            - Detect venue halt / limit-up-limit-down / maintenance windows.
        """
        raise NotImplementedError