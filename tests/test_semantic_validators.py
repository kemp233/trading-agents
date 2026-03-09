from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pytest

from core.instrument_master import InstrumentSpec, load_instruments_from_yaml
from validators.semantic_validators import SemanticValidators


@dataclass(slots=True)
class Intent:
    side: str
    quantity: Decimal
    price: Optional[Decimal]
    reference_price: Optional[Decimal] = None
    current_time: Optional[datetime] = None
    available_funds: Optional[Decimal] = None
    account_equity: Optional[Decimal] = None


def _make_spec(
    *,
    symbol: str,
    asset_class: str,
    tick_size: Decimal,
    lot_size: Decimal,
    contract_multiplier: Decimal,
    price_limit_pct: Optional[Decimal] = None,
    trading_hours: Optional[dict] = None,
    max_leverage: int = 20,
    margin_rate: Decimal = Decimal("0.05"),
) -> InstrumentSpec:
    return InstrumentSpec(
        symbol=symbol,
        asset_class=asset_class,
        tick_size=tick_size,
        lot_size=lot_size,
        contract_multiplier=contract_multiplier,
        max_leverage=max_leverage,
        margin_rate=margin_rate,
        price_limit_pct=price_limit_pct,
        trading_hours=trading_hours or ({"24/7": True} if asset_class == "crypto_perp" else {"timezone": "Asia/Shanghai", "sessions": ["09:00-15:00"]}),
    )


def test_load_cn_futures_yaml_maps_to_instrument_spec() -> None:
    specs = load_instruments_from_yaml("futures/config/instruments_cn.yaml")

    rb = specs["rb2510"]
    assert rb.asset_class == "cn_futures"
    assert rb.tick_size == Decimal("1.0")
    assert rb.lot_size == Decimal("1")
    assert rb.contract_multiplier == Decimal("10")
    assert rb.margin_rate == Decimal("0.05")
    assert rb.max_leverage == 20
    assert rb.trading_hours["timezone"] == "Asia/Shanghai"
    assert "21:00-23:00" in rb.trading_hours["sessions"]


def test_load_cn_futures_yaml_rejects_duplicate_symbol(tmp_path: Path) -> None:
    path = tmp_path / "dup.yaml"
    path.write_text(
        """
instruments:
  - symbol: rb2510
    price_tick: 1
    min_volume: 1
    lot_size: 10
    margin_rate: 0.05
    trading_hours: [\"09:00-15:00\"]
  - symbol: rb2510
    price_tick: 1
    min_volume: 1
    lot_size: 10
    margin_rate: 0.05
    trading_hours: [\"09:00-15:00\"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate instrument symbol 'rb2510'"):
        load_instruments_from_yaml(str(path))


def test_qty_not_aligned_to_lot_size_includes_suggest_down_align() -> None:
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.1"),
        contract_multiplier=Decimal("1"),
    )
    validator = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "999999999"})
    intent = Intent(side="BUY", quantity=Decimal("0.35"), price=Decimal("100"))

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "qty 0.35 not aligned to lot_size 0.1, suggest: 0.3" in result.errors


@pytest.mark.parametrize(
    ("side", "price", "tick", "expected"),
    [
        ("BUY", Decimal("100.05"), Decimal("0.1"), Decimal("100.1")),
        ("SELL", Decimal("100.05"), Decimal("0.1"), Decimal("100.0")),
    ],
)
def test_price_not_aligned_to_tick_size_buy_up_sell_down(side, price, tick, expected) -> None:
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=tick,
        lot_size=Decimal("0.001"),
        contract_multiplier=Decimal("1"),
    )
    validator = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "999999999"})
    intent = Intent(side=side, quantity=Decimal("0.010"), price=price)

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert f"price {price} not aligned to tick_size {tick}, suggest: {expected}" in result.errors


def test_notional_exceeds_max_uses_contract_multiplier() -> None:
    spec = _make_spec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:00-15:00"]},
    )
    validator = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "8000"})
    intent = Intent(
        side="BUY",
        quantity=Decimal("2"),
        price=Decimal("3500"),
        current_time=datetime.fromisoformat("2026-03-09T10:00:00+08:00"),
    )

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "notional 70000 exceeds max 8000 (3500*2*10)" in result.errors


def test_price_limit_rejects_outside_bounds() -> None:
    spec = _make_spec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        price_limit_pct=Decimal("0.07"),
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:00-15:00"]},
    )
    validator = SemanticValidators(state_reader=None, config={})
    intent = Intent(
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("3800"),
        reference_price=Decimal("3500"),
        current_time=datetime.fromisoformat("2026-03-09T10:00:00+08:00"),
    )

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "outside limit" in result.errors[0]


def test_non_trading_hours_rejected() -> None:
    spec = _make_spec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:00-10:15", "21:00-23:00"]},
    )
    validator = SemanticValidators(state_reader=None, config={})
    intent = Intent(
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("3500"),
        current_time=datetime.fromisoformat("2026-03-09T14:00:00+08:00"),
    )

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "outside trading sessions" in result.errors[0]


def test_missing_account_snapshot_rejected_in_strict_mode() -> None:
    spec = _make_spec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:00-15:00"]},
    )
    validator = SemanticValidators(state_reader=None, config={"require_account_snapshot": True})
    intent = Intent(
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("3500"),
        current_time=datetime.fromisoformat("2026-03-09T10:00:00+08:00"),
    )

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "missing account snapshot fields" in result.errors[0]


def test_margin_and_leverage_rejected_when_insufficient() -> None:
    spec = _make_spec(
        symbol="IF2506",
        asset_class="cn_futures",
        tick_size=Decimal("0.2"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("300"),
        trading_hours={"timezone": "Asia/Shanghai", "sessions": ["09:30-15:00"]},
        max_leverage=5,
        margin_rate=Decimal("0.1"),
    )
    validator = SemanticValidators(state_reader=None, config={"require_account_snapshot": True})
    intent = Intent(
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("4000"),
        current_time=datetime.fromisoformat("2026-03-09T10:00:00+08:00"),
        available_funds=Decimal("50000"),
        account_equity=Decimal("100000"),
    )

    result = validator.validate_trade_intent(intent, spec)

    assert result.valid is False
    assert "required margin 120000.0 exceeds available funds 50000" in result.errors
    assert "effective leverage 12 exceeds max 5" in result.errors
