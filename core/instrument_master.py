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
    Contract master data used by semantic validators.
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
        required = (
            "symbol",
            "asset_class",
            "tick_size",
            "lot_size",
            "contract_multiplier",
            "max_leverage",
            "margin_rate",
            "price_limit_pct",
            "trading_hours",
        )
        for field in required:
            if field not in data:
                raise KeyError(f"missing field: {field}")

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
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:00-15:00", "21:00-23:00"]},
    ),
}


def get_instrument_spec(symbol: str) -> InstrumentSpec:
    return INSTRUMENTS[symbol]


def register_instrument(spec: InstrumentSpec) -> None:
    INSTRUMENTS[spec.symbol] = spec


def register_instruments(specs: Dict[str, InstrumentSpec]) -> None:
    INSTRUMENTS.update(specs)


def load_instruments_from_yaml(path: str) -> Dict[str, InstrumentSpec]:
    yaml_path = Path(path)
    if not yaml_path.is_absolute():
        yaml_path = (Path(__file__).resolve().parents[1] / yaml_path).resolve()

    if not yaml_path.exists():
        raise FileNotFoundError(f"Instrument configuration file not found: {yaml_path}")

    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse YAML file {yaml_path}: {exc}") from exc
    except Exception as exc:
        raise ValueError(f"Failed to read file {yaml_path}: {exc}") from exc

    if data is None:
        raise ValueError(f"YAML file {yaml_path} is empty or contains no data")
    if "instruments" not in data:
        raise ValueError(f"Missing 'instruments' key in YAML file {yaml_path}")

    instruments_data = data["instruments"]
    if isinstance(instruments_data, dict):
        return _load_standard_instruments(instruments_data, yaml_path)
    if isinstance(instruments_data, list):
        return _load_cn_futures_instruments(instruments_data, yaml_path)
    raise ValueError(f"'instruments' must be a dictionary or list in YAML file {yaml_path}")


def _load_standard_instruments(instruments_data: Dict[str, Any], yaml_path: Path) -> Dict[str, InstrumentSpec]:
    specs: Dict[str, InstrumentSpec] = {}
    for symbol, spec_data in instruments_data.items():
        if not isinstance(spec_data, dict):
            raise ValueError(
                f"Instrument spec for '{symbol}' must be a dictionary in YAML file {yaml_path}"
            )
        try:
            specs[symbol] = InstrumentSpec.from_dict({**spec_data, "symbol": symbol})
        except KeyError as exc:
            raise ValueError(
                f"Missing required field for instrument '{symbol}' in YAML file {yaml_path}: {exc}"
            ) from exc
        except Exception as exc:
            raise ValueError(
                f"Failed to parse instrument '{symbol}' in YAML file {yaml_path}: {exc}"
            ) from exc
    return specs


def _load_cn_futures_instruments(instruments_data: list[Any], yaml_path: Path) -> Dict[str, InstrumentSpec]:
    specs: Dict[str, InstrumentSpec] = {}
    for idx, item in enumerate(instruments_data):
        if not isinstance(item, dict):
            raise ValueError(
                f"Instrument entry at index {idx} must be a dictionary in YAML file {yaml_path}"
            )
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            raise ValueError(f"Instrument entry at index {idx} is missing symbol in YAML file {yaml_path}")
        if symbol in specs:
            raise ValueError(f"Duplicate instrument symbol '{symbol}' in YAML file {yaml_path}")
        specs[symbol] = _cn_futures_item_to_spec(item, yaml_path)
    return specs


def _cn_futures_item_to_spec(item: Dict[str, Any], yaml_path: Path) -> InstrumentSpec:
    symbol = str(item.get("symbol", "")).strip() or "<unknown>"
    required_fields = ("price_tick", "min_volume", "lot_size", "margin_rate")
    for field in required_fields:
        if field not in item:
            raise ValueError(
                f"Missing required field '{field}' for instrument '{symbol}' in YAML file {yaml_path}"
            )

    tick_size = Decimal(str(item["price_tick"]))
    lot_size = Decimal(str(item["min_volume"]))
    contract_multiplier = Decimal(str(item["lot_size"]))
    margin_rate = Decimal(str(item["margin_rate"]))

    if tick_size <= 0:
        raise ValueError(f"Instrument '{symbol}' has invalid price_tick: {tick_size}")
    if lot_size <= 0:
        raise ValueError(f"Instrument '{symbol}' has invalid min_volume: {lot_size}")
    if contract_multiplier <= 0:
        raise ValueError(f"Instrument '{symbol}' has invalid lot_size: {contract_multiplier}")
    if margin_rate <= 0:
        raise ValueError(f"Instrument '{symbol}' has invalid margin_rate: {margin_rate}")

    price_limit_pct_raw = item.get("price_limit_pct")
    price_limit_pct = Decimal(str(price_limit_pct_raw)) if price_limit_pct_raw is not None else None
    max_leverage = max(1, int(Decimal("1") / margin_rate))

    return InstrumentSpec(
        symbol=symbol,
        asset_class="cn_futures",
        tick_size=tick_size,
        lot_size=lot_size,
        contract_multiplier=contract_multiplier,
        max_leverage=max_leverage,
        margin_rate=margin_rate,
        price_limit_pct=price_limit_pct,
        trading_hours=_normalize_trading_hours(item),
    )


def _normalize_trading_hours(item: Dict[str, Any]) -> Dict[str, Any]:
    sessions: list[str] = []
    symbol = str(item.get("symbol", "")).strip() or "<unknown>"
    for key in ("trading_hours", "night_trading_hours"):
        raw = item.get(key, [])
        if raw is None:
            continue
        if not isinstance(raw, list):
            raise ValueError(f"Field '{key}' for instrument '{symbol}' must be a list")
        for session in raw:
            if not isinstance(session, str) or "-" not in session:
                raise ValueError(f"Invalid trading session '{session}' for instrument '{symbol}'")
            sessions.append(session.strip())

    if not sessions:
        raise ValueError(f"Instrument '{symbol}' has no trading hours configured")

    return {
        "timezone": "Asia/Shanghai",
        "sessions": sessions,
    }
