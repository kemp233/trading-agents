# core/instrument_master.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import yaml


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


def register_instruments(specs: Dict[str, InstrumentSpec]) -> None:
    """
    Batch registration of instrument specs.
    Overwrites existing symbol entries if present.
    """
    INSTRUMENTS.update(specs)


def load_instruments_from_yaml(path: str) -> Dict[str, InstrumentSpec]:
    """
    Load instrument specifications from a YAML file.

    Expected YAML structure:
        instruments:
            SYMBOL1:
                asset_class: ...
                tick_size: ...
                lot_size: ...
                contract_multiplier: ...
                max_leverage: ...
                margin_rate: ...
                trading_hours: ...
                price_limit_pct: ...
            SYMBOL2:
                ...

    Args:
        path: Path to the YAML file (relative or absolute)

    Returns:
        Dict mapping symbol to InstrumentSpec

    Raises:
        FileNotFoundError: If the YAML file does not exist
        ValueError: If the YAML structure is invalid or parsing fails
    """
    yaml_path = Path(path)
    if not yaml_path.is_absolute():
        # Resolve relative paths against project root (core/ is one level under it)
        yaml_path = (Path(__file__).resolve().parents[1] / yaml_path).resolve()

    if not yaml_path.exists():
        raise FileNotFoundError(f"Instrument configuration file not found: {yaml_path}")

    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML file {yaml_path}: {e}") from e
    except Exception as e:
        raise ValueError(f"Failed to read file {yaml_path}: {e}") from e

    if data is None:
        raise ValueError(f"YAML file {yaml_path} is empty or contains no data")

    if "instruments" not in data:
        raise ValueError(f"Missing 'instruments' key in YAML file {yaml_path}")

    instruments_data = data["instruments"]
    if not isinstance(instruments_data, dict):
        raise ValueError(f"'instruments' must be a dictionary in YAML file {yaml_path}")

    specs: Dict[str, InstrumentSpec] = {}

    for symbol, spec_data in instruments_data.items():
        if not isinstance(spec_data, dict):
            raise ValueError(
                f"Instrument spec for '{symbol}' must be a dictionary in YAML file {yaml_path}"
            )

        try:
            spec = InstrumentSpec.from_dict({**spec_data, "symbol": symbol})
            specs[symbol] = spec
        except KeyError as e:
            raise ValueError(
                f"Missing required field for instrument '{symbol}' in YAML file {yaml_path}: {e}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Failed to parse instrument '{symbol}' in YAML file {yaml_path}: {e}"
            ) from e

    return specs