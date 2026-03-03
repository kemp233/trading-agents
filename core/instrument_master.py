# core/instrument_master.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Literal, Optional


AssetClass = Literal["crypto_perp", "cn_futures"]


@dataclass(frozen=True)
class InstrumentSpec:
    """
    Contract master data for semantic validators:
    - tick_size / lot_size alignment
    - contract_multiplier for notional calculation
    - leverage/margin constraints
    - futures price limit and trading hours
    """

    symbol: str
    asset_class: AssetClass
    tick_size: Decimal
    lot_size: Decimal
    contract_multiplier: Decimal
    max_leverage: int
    margin_rate: Decimal
    price_limit_pct: Optional[Decimal]
    trading_hours: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to JSON-friendly dict.
        Decimals are converted to strings to preserve precision.
        """
        return {
            "symbol": self.symbol,
            "asset_class": self.asset_class,
            "tick_size": str(self.tick_size),
            "lot_size": str(self.lot_size),
            "contract_multiplier": str(self.contract_multiplier),
            "max_leverage": int(self.max_leverage),
            "margin_rate": str(self.margin_rate),
            "price_limit_pct": (str(self.price_limit_pct) if self.price_limit_pct is not None else None),
            "trading_hours": self.trading_hours,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "InstrumentSpec":
        """
        Deserialize from dict.
        Accepts Decimal fields as str|int|float|Decimal (str recommended).
        """
        if "symbol" not in data:
            raise KeyError("missing field: symbol")
        if "asset_class" not in data:
            raise KeyError("missing field: asset_class")
        if "tick_size" not in data:
            raise KeyError("missing field: tick_size")
        if "lot_size" not in data:
            raise KeyError("missing field: lot_size")
        if "contract_multiplier" not in data:
            raise KeyError("missing field: contract_multiplier")
        if "max_leverage" not in data:
            raise KeyError("missing field: max_leverage")
        if "margin_rate" not in data:
            raise KeyError("missing field: margin_rate")
        if "price_limit_pct" not in data:
            raise KeyError("missing field: price_limit_pct")
        if "trading_hours" not in data:
            raise KeyError("missing field: trading_hours")

        price_limit_raw = data["price_limit_pct"]
        price_limit = Decimal(str(price_limit_raw)) if price_limit_raw is not None else None

        return InstrumentSpec(
            symbol=str(data["symbol"]),
            asset_class=data["asset_class"],
            tick_size=Decimal(str(data["tick_size"])),
            lot_size=Decimal(str(data["lot_size"])),
            contract_multiplier=Decimal(str(data["contract_multiplier"])),
            max_leverage=int(data["max_leverage"]),
            margin_rate=Decimal(str(data["margin_rate"])),
            price_limit_pct=price_limit,
            trading_hours=dict(data["trading_hours"]),
        )


# Default registry (example data)
INSTRUMENTS: Dict[str, InstrumentSpec] = {
    "BTCUSDT": InstrumentSpec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.001"),
        contract_multiplier=Decimal("1"),
        max_leverage=20,
        margin_rate=Decimal("0.05"),
        price_limit_pct=None,
        trading_hours={"24/7": True},
    ),
    "rb2510": InstrumentSpec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        max_leverage=10,
        margin_rate=Decimal("0.10"),
        price_limit_pct=Decimal("0.07"),
        trading_hours={"day": "09:00-15:00", "night": "21:00-23:00"},
    ),
}


def get_instrument_spec(symbol: str) -> InstrumentSpec:
    """
    Get instrument spec by symbol.
    Raises KeyError if symbol not found.
    """
    return INSTRUMENTS[symbol]


def register_instrument(spec: InstrumentSpec) -> None:
    """
    Runtime dynamic registration (for tests / hot-load).
    Overwrites existing symbol entry if present.
    """
    INSTRUMENTS[spec.symbol] = spec