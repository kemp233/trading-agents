# core/state_schema.py
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Type, TypeVar


# ----------------------------
# Enums
# ----------------------------

class OrderStatus(str, Enum):
    PENDING_SEND = "PENDING_SEND"
    SENT = "SENT"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class RiskStateType(str, Enum):
    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED"
    CIRCUIT = "CIRCUIT"
    RECONCILING = "RECONCILING"
    VENUE_HALT = "VENUE_HALT"
    OFFLINE = "OFFLINE"


# ----------------------------
# Helpers
# ----------------------------

T = TypeVar("T")


def _require_key(d: Dict[str, Any], key: str) -> Any:
    if key not in d:
        raise ValueError(f"Missing required field: {key}")
    return d[key]


def _parse_dt(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"Field '{field_name}' must be an ISO8601 string, got {type(value).__name__}")
    # Support trailing 'Z'
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"Field '{field_name}' has invalid ISO8601 datetime: {value}") from e
    if dt.tzinfo is None:
        raise ValueError(f"Field '{field_name}' must be timezone-aware (tzinfo is required): {value}")
    return dt


def _enum_from_str(enum_cls: Type[Enum], value: Any, field_name: str) -> Enum:
    if not isinstance(value, str):
        raise ValueError(f"Field '{field_name}' must be a string for {enum_cls.__name__}, got {type(value).__name__}")
    try:
        return enum_cls(value)  # type: ignore[misc]
    except ValueError as e:
        allowed = ", ".join([getattr(m, "value", str(m)) for m in enum_cls])  # type: ignore[arg-type]
        raise ValueError(f"Field '{field_name}' invalid {enum_cls.__name__}: {value}. Allowed: {allowed}") from e


def _has_default(dc_field) -> bool:
    # dataclasses.Field: default != MISSING or default_factory != MISSING
    from dataclasses import MISSING
    return not (dc_field.default is MISSING and dc_field.default_factory is MISSING)  # type: ignore[attr-defined]


def _enforce_required_keys(cls: Type[Any], d: Dict[str, Any]) -> None:
    """
    Enforce: if a field has no default/default_factory, it must exist in dict.
    Optional fields can still be present with None; missing keys only allowed when default exists.
    """
    for f in fields(cls):
        if _has_default(f):
            continue
        if f.name not in d:
            raise ValueError(f"Missing required field: {f.name}")


# ----------------------------
# Dataclasses
# ----------------------------

@dataclass(frozen=False, slots=True)
class OrderState:
    order_id: str
    client_order_id: str
    symbol: str
    venue: str  # "binance" | "ctp"
    side: OrderSide
    quantity: float
    price: Optional[float]  # None => market order
    status: OrderStatus
    strategy_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    filled_quantity: float = 0.0
    filled_price: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "venue": self.venue,
            "side": self.side.value,
            "quantity": float(self.quantity),
            "price": None if self.price is None else float(self.price),
            "status": self.status.value,
            "strategy_id": self.strategy_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "filled_quantity": float(self.filled_quantity),
            "filled_price": float(self.filled_price),
        }

    @classmethod
    def from_dict(cls: Type["OrderState"], d: Dict[str, Any]) -> "OrderState":
        if not isinstance(d, dict):
            raise ValueError(f"{cls.__name__}.from_dict expects a dict, got {type(d).__name__}")

        _enforce_required_keys(cls, d)

        order_id = _require_key(d, "order_id")
        client_order_id = _require_key(d, "client_order_id")
        symbol = _require_key(d, "symbol")
        venue = _require_key(d, "venue")
        side = _enum_from_str(OrderSide, _require_key(d, "side"), "side")
        quantity = _require_key(d, "quantity")
        price = _require_key(d, "price")
        status = _enum_from_str(OrderStatus, _require_key(d, "status"), "status")
        strategy_id = _require_key(d, "strategy_id")
        created_at = _parse_dt(_require_key(d, "created_at"), "created_at")
        updated_at = _parse_dt(_require_key(d, "updated_at"), "updated_at")

        # Defaults allowed to be missing
        filled_quantity = d.get("filled_quantity", 0.0)
        filled_price = d.get("filled_price", 0.0)

        if not isinstance(order_id, str):
            raise ValueError("Field 'order_id' must be str")
        if not isinstance(client_order_id, str):
            raise ValueError("Field 'client_order_id' must be str")
        if not isinstance(symbol, str):
            raise ValueError("Field 'symbol' must be str")
        if not isinstance(venue, str):
            raise ValueError("Field 'venue' must be str")
        if not isinstance(quantity, (int, float)):
            raise ValueError("Field 'quantity' must be a number")
        if price is not None and not isinstance(price, (int, float)):
            raise ValueError("Field 'price' must be a number or None")
        if strategy_id is not None and not isinstance(strategy_id, str):
            raise ValueError("Field 'strategy_id' must be str or None")
        if not isinstance(filled_quantity, (int, float)):
            raise ValueError("Field 'filled_quantity' must be a number")
        if not isinstance(filled_price, (int, float)):
            raise ValueError("Field 'filled_price' must be a number")

        return cls(
            order_id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            venue=venue,
            side=side,  # type: ignore[arg-type]
            quantity=float(quantity),
            price=None if price is None else float(price),
            status=status,  # type: ignore[arg-type]
            strategy_id=strategy_id,
            created_at=created_at,
            updated_at=updated_at,
            filled_quantity=float(filled_quantity),
            filled_price=float(filled_price),
        )


@dataclass(frozen=False, slots=True)
class PositionState:
    symbol: str
    venue: str
    side: str  # "LONG" | "SHORT"
    quantity: float
    entry_price: float
    unrealized_pnl: float = 0.0
    updated_at: datetime = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # Keep strict requirement: updated_at must be provided (timezone-aware).
        if self.updated_at is None:
            raise ValueError("Field 'updated_at' is required and must be timezone-aware datetime")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "side": self.side,
            "quantity": float(self.quantity),
            "entry_price": float(self.entry_price),
            "unrealized_pnl": float(self.unrealized_pnl),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls: Type["PositionState"], d: Dict[str, Any]) -> "PositionState":
        if not isinstance(d, dict):
            raise ValueError(f"{cls.__name__}.from_dict expects a dict, got {type(d).__name__}")

        # NOTE: Because updated_at has a default placeholder (None) for dataclass typing,
        # we enforce required fields manually here.
        for k in ("symbol", "venue", "side", "quantity", "entry_price", "updated_at"):
            _require_key(d, k)

        symbol = d["symbol"]
        venue = d["venue"]
        side = d["side"]
        quantity = d["quantity"]
        entry_price = d["entry_price"]
        unrealized_pnl = d.get("unrealized_pnl", 0.0)
        updated_at = _parse_dt(d["updated_at"], "updated_at")

        if not isinstance(symbol, str):
            raise ValueError("Field 'symbol' must be str")
        if not isinstance(venue, str):
            raise ValueError("Field 'venue' must be str")
        if not isinstance(side, str):
            raise ValueError("Field 'side' must be str")
        if not isinstance(quantity, (int, float)):
            raise ValueError("Field 'quantity' must be a number")
        if not isinstance(entry_price, (int, float)):
            raise ValueError("Field 'entry_price' must be a number")
        if not isinstance(unrealized_pnl, (int, float)):
            raise ValueError("Field 'unrealized_pnl' must be a number")

        return cls(
            symbol=symbol,
            venue=venue,
            side=side,
            quantity=float(quantity),
            entry_price=float(entry_price),
            unrealized_pnl=float(unrealized_pnl),
            updated_at=updated_at,
        )


@dataclass(frozen=False, slots=True)
class RiskState:
    current_state: RiskStateType
    state_changed_at: datetime
    reason: str
    metadata: Dict[str, Any]
    previous_state: Optional[RiskStateType] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_state": self.current_state.value,
            "state_changed_at": self.state_changed_at.isoformat(),
            "reason": self.reason,
            "metadata": dict(self.metadata),
            "previous_state": None if self.previous_state is None else self.previous_state.value,
        }

    @classmethod
    def from_dict(cls: Type["RiskState"], d: Dict[str, Any]) -> "RiskState":
        if not isinstance(d, dict):
            raise ValueError(f"{cls.__name__}.from_dict expects a dict, got {type(d).__name__}")

        _enforce_required_keys(cls, d)

        current_state = _enum_from_str(RiskStateType, _require_key(d, "current_state"), "current_state")
        state_changed_at = _parse_dt(_require_key(d, "state_changed_at"), "state_changed_at")
        reason = _require_key(d, "reason")
        metadata = _require_key(d, "metadata")
        previous_state_raw = d.get("previous_state", None)

        if not isinstance(reason, str):
            raise ValueError("Field 'reason' must be str")
        if not isinstance(metadata, dict):
            raise ValueError("Field 'metadata' must be dict")

        previous_state: Optional[RiskStateType]
        if previous_state_raw is None:
            previous_state = None
        else:
            previous_state = _enum_from_str(RiskStateType, previous_state_raw, "previous_state")  # type: ignore[assignment]

        return cls(
            current_state=current_state,  # type: ignore[arg-type]
            state_changed_at=state_changed_at,
            reason=reason,
            metadata=metadata,
            previous_state=previous_state,
        )