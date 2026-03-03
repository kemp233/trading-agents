from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Dict, Literal, Optional, Self, TypeVar, cast


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


def _as_literal(v: Any, field: str, allowed: set[str]) -> str:
    """Validate value is a str and belongs to allowed literal set."""
    s = _as_str(v, field)
    if s not in allowed:
        raise ValueError(f"Field {field} must be one of {sorted(allowed)}, got {s!r}")
    return s


@dataclass(slots=True, frozen=False)
class VenueOrderSpec:
    """
    Standardized order specification to be sent to a venue adapter.

    Notes:
      - quantity/price are Decimal for precision (no float).
      - price must be None for MARKET orders.
    """

    symbol: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT", "STOP"]
    quantity: Decimal
    price: Optional[Decimal]
    time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC"
    reduce_only: bool = False
    post_only: bool = False
    hedge_flag: Literal["SPEC", "HEDGE"] = "SPEC"
    client_order_id: str = ""
    venue: str = ""

    _SIDE_ALLOWED: ClassVar[set[str]] = {"BUY", "SELL"}
    _TYPE_ALLOWED: ClassVar[set[str]] = {"MARKET", "LIMIT", "STOP"}
    _TIF_ALLOWED: ClassVar[set[str]] = {"GTC", "IOC", "FOK"}
    _HEDGE_ALLOWED: ClassVar[set[str]] = {"SPEC", "HEDGE"}

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict.

        Conversions:
          - Decimal -> str
        """
        return {
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "quantity": str(self.quantity),
            "price": None if self.price is None else str(self.price),
            "time_in_force": self.time_in_force,
            "reduce_only": self.reduce_only,
            "post_only": self.post_only,
            "hedge_flag": self.hedge_flag,
            "client_order_id": self.client_order_id,
            "venue": self.venue,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """
        Parse from dict with strict type checks.

        Raises:
          - ValueError: missing field, wrong type, invalid literal, or precision-unsafe float.
        """
        if not isinstance(d, dict):
            raise ValueError(f"Input must be dict, got {type(d).__name__}")

        symbol = _as_str(_require_key(d, "symbol"), "symbol")
        side = cast(Literal["BUY", "SELL"], _as_literal(_require_key(d, "side"), "side", cls._SIDE_ALLOWED))
        order_type = cast(
            Literal["MARKET", "LIMIT", "STOP"],
            _as_literal(_require_key(d, "order_type"), "order_type", cls._TYPE_ALLOWED),
        )

        quantity = _as_decimal(_require_key(d, "quantity"), "quantity")
        price = _as_optional_decimal(_require_key(d, "price"), "price")

        time_in_force_raw = d.get("time_in_force", "GTC")
        time_in_force = cast(
            Literal["GTC", "IOC", "FOK"],
            _as_literal(time_in_force_raw, "time_in_force", cls._TIF_ALLOWED),
        )

        reduce_only = _as_bool(d.get("reduce_only", False), "reduce_only")
        post_only = _as_bool(d.get("post_only", False), "post_only")

        hedge_flag_raw = d.get("hedge_flag", "SPEC")
        hedge_flag = cast(
            Literal["SPEC", "HEDGE"],
            _as_literal(hedge_flag_raw, "hedge_flag", cls._HEDGE_ALLOWED),
        )

        client_order_id = _as_str(d.get("client_order_id", ""), "client_order_id")
        venue = _as_str(d.get("venue", ""), "venue")

        # Semantic guardrail: MARKET must not carry price
        if order_type == "MARKET" and price is not None:
            raise ValueError("For MARKET orders, price must be None.")

        return cls(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            hedge_flag=hedge_flag,
            client_order_id=client_order_id,
            venue=venue,
        )


@dataclass(slots=True, frozen=False)
class VenueReceipt:
    """
    Venue order submission receipt.

    timestamp must be tz-aware datetime.
    """

    client_order_id: str
    exchange_order_id: str
    status: str  # 'SENT' | 'REJECTED' | 'FILLED'
    raw_response: Dict[str, Any]
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict.

        Conversions:
          - datetime -> ISO8601 string
        """
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "status": self.status,
            "raw_response": self.raw_response,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """
        Parse from dict with strict type checks.

        Rules:
          - timestamp must be tz-aware datetime (or tz-aware ISO8601 string).
          - raw_response must be dict.
          - missing required fields -> ValueError.
        """
        if not isinstance(d, dict):
            raise ValueError(f"Input must be dict, got {type(d).__name__}")

        client_order_id = _as_str(_require_key(d, "client_order_id"), "client_order_id")
        exchange_order_id = _as_str(_require_key(d, "exchange_order_id"), "exchange_order_id")
        status = _as_str(_require_key(d, "status"), "status")

        raw_response_any = _require_key(d, "raw_response")
        if not isinstance(raw_response_any, dict):
            raise ValueError(f"Field raw_response must be dict, got {type(raw_response_any).__name__}")
        raw_response = cast(Dict[str, Any], raw_response_any)

        timestamp = _as_datetime_tzaware(_require_key(d, "timestamp"), "timestamp")

        return cls(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status=status,
            raw_response=raw_response,
            timestamp=timestamp,
        )


@dataclass(slots=True, frozen=False)
class VenueOrderStatus:
    """
    Order status snapshot returned by querying a venue.

    updated_at must be tz-aware datetime.
    """

    client_order_id: str
    exchange_order_id: str
    status: str  # 'PENDING' | 'PARTIAL' | 'FILLED' | 'CANCELED'
    filled_quantity: Decimal
    filled_price: Decimal
    updated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict.

        Conversions:
          - Decimal -> str
          - datetime -> ISO8601 string
        """
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "status": self.status,
            "filled_quantity": str(self.filled_quantity),
            "filled_price": str(self.filled_price),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """
        Parse from dict with strict type checks.

        Rules:
          - filled_quantity/filled_price must be Decimal/str/int (no float).
          - updated_at must be tz-aware datetime (or tz-aware ISO8601 string).
          - missing required fields -> ValueError.
        """
        if not isinstance(d, dict):
            raise ValueError(f"Input must be dict, got {type(d).__name__}")

        client_order_id = _as_str(_require_key(d, "client_order_id"), "client_order_id")
        exchange_order_id = _as_str(_require_key(d, "exchange_order_id"), "exchange_order_id")
        status = _as_str(_require_key(d, "status"), "status")

        filled_quantity = _as_decimal(_require_key(d, "filled_quantity"), "filled_quantity")
        filled_price = _as_decimal(_require_key(d, "filled_price"), "filled_price")
        updated_at = _as_datetime_tzaware(_require_key(d, "updated_at"), "updated_at")

        return cls(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status=status,
            filled_quantity=filled_quantity,
            filled_price=filled_price,
            updated_at=updated_at,
        )


@dataclass(slots=True, frozen=False)
class VenuePosition:
    """
    Venue position snapshot for reconciliation.

    updated_at must be tz-aware datetime.
    """

    symbol: str
    venue: str
    side: Literal["LONG", "SHORT"]
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal
    updated_at: datetime

    _SIDE_ALLOWED: ClassVar[set[str]] = {"LONG", "SHORT"}

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict.

        Conversions:
          - Decimal -> str
          - datetime -> ISO8601 string
        """
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "side": self.side,
            "quantity": str(self.quantity),
            "entry_price": str(self.entry_price),
            "unrealized_pnl": str(self.unrealized_pnl),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """
        Parse from dict with strict type checks.

        Rules:
          - side must be 'LONG' or 'SHORT'
          - quantity/entry_price/unrealized_pnl must be Decimal/str/int (no float)
          - updated_at must be tz-aware datetime (or tz-aware ISO8601 string)
          - missing required fields -> ValueError
        """
        if not isinstance(d, dict):
            raise ValueError(f"Input must be dict, got {type(d).__name__}")

        symbol = _as_str(_require_key(d, "symbol"), "symbol")
        venue = _as_str(_require_key(d, "venue"), "venue")
        side = cast(Literal["LONG", "SHORT"], _as_literal(_require_key(d, "side"), "side", cls._SIDE_ALLOWED))

        quantity = _as_decimal(_require_key(d, "quantity"), "quantity")
        entry_price = _as_decimal(_require_key(d, "entry_price"), "entry_price")
        unrealized_pnl = _as_decimal(_require_key(d, "unrealized_pnl"), "unrealized_pnl")

        updated_at = _as_datetime_tzaware(_require_key(d, "updated_at"), "updated_at")

        return cls(
            symbol=symbol,
            venue=venue,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl,
            updated_at=updated_at,
        )