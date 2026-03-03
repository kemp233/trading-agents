# tests/test_semantic_validators.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import pytest

from core.instrument_master import InstrumentSpec
from validators.semantic_validators import SemanticValidators


@dataclass(slots=True)
class Intent:
    # Minimal intent for SemanticValidators.validate_trade_intent()
    side: str  # 'BUY' | 'SELL'
    quantity: Decimal
    price: Optional[Decimal]
    reference_price: Optional[Decimal] = None  # optional, for cn_futures price-limit check if needed


def _make_spec(
    *,
    symbol: str,
    asset_class: str,
    tick_size: Decimal,
    lot_size: Decimal,
    contract_multiplier: Decimal,
    price_limit_pct: Optional[Decimal] = None,
) -> InstrumentSpec:
    return InstrumentSpec(
        symbol=symbol,
        asset_class=asset_class,  # "crypto_perp" | "cn_futures"
        tick_size=tick_size,
        lot_size=lot_size,
        contract_multiplier=contract_multiplier,
        max_leverage=20,
        margin_rate=Decimal("0.05"),
        price_limit_pct=price_limit_pct,
        trading_hours={"24/7": True} if asset_class == "crypto_perp" else {"day": "09:00-15:00"},
    )


def test_notional_multiplier_rb2510_enforced_via_max_notional():
    """rb2510：notional = price × qty × 10（用 max_notional 卡住来验证乘数被使用）"""
    spec = _make_spec(
        symbol="rb2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("10"),
        price_limit_pct=None,  # keep this test focused on multiplier
    )

    # 如果实现错误地漏乘 multiplier，则 notional=3500*2=7000 <= 8000 会通过；
    # 正确实现应为 3500*2*10=70000 > 8000 → 必须报超限
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "8000"})
    intent = Intent(side="BUY", quantity=Decimal("2"), price=Decimal("3500"))

    res = v.validate_trade_intent(intent, spec)

    notional = Decimal("3500") * Decimal("2") * Decimal("10")
    expected = (
        f"notional {notional} exceeds max {Decimal('8000')} "
        f"({intent.price}×{intent.quantity}×{spec.contract_multiplier})"
    )

    assert res.valid is False
    assert expected in res.errors
    assert res.warnings == []


def test_notional_multiplier_ag2510_enforced_via_max_notional():
    """ag2510：notional = price × qty × 15（用 max_notional 卡住来验证乘数被使用）"""
    spec = _make_spec(
        symbol="ag2510",
        asset_class="cn_futures",
        tick_size=Decimal("1"),
        lot_size=Decimal("1"),
        contract_multiplier=Decimal("15"),
        price_limit_pct=None,
    )

    # 漏乘时：5000*1=5000 <= 6000 会通过；正确：5000*1*15=75000 > 6000 → 必须报超限
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "6000"})
    intent = Intent(side="SELL", quantity=Decimal("1"), price=Decimal("5000"))

    res = v.validate_trade_intent(intent, spec)

    notional = Decimal("5000") * Decimal("1") * Decimal("15")
    expected = (
        f"notional {notional} exceeds max {Decimal('6000')} "
        f"({intent.price}×{intent.quantity}×{spec.contract_multiplier})"
    )

    assert res.valid is False
    assert expected in res.errors
    assert res.warnings == []


def test_notional_multiplier_btcusdt_valid_under_cap():
    """BTCUSDT：notional = price × qty × 1（且低于 max_notional 时应通过）"""
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.001"),
        contract_multiplier=Decimal("1"),
        price_limit_pct=None,
    )
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "600"})
    intent = Intent(side="BUY", quantity=Decimal("0.010"), price=Decimal("50000"))

    res = v.validate_trade_intent(intent, spec)

    assert res.valid is True
    assert res.errors == []
    assert res.warnings == []


def test_qty_not_aligned_to_lot_size_includes_suggest_down_align():
    """qty 非 lot_size 倍数 → invalid，并且 errors 中包含 suggest qty（向下对齐）"""
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.1"),
        contract_multiplier=Decimal("1"),
        price_limit_pct=None,
    )
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "999999999"})
    intent = Intent(side="BUY", quantity=Decimal("0.35"), price=Decimal("100"))

    res = v.validate_trade_intent(intent, spec)

    aligned = (Decimal("0.35") // Decimal("0.1")) * Decimal("0.1")  # expected: 0.3
    expected = f"qty {intent.quantity} not aligned to lot_size {spec.lot_size}, suggest: {aligned}"

    assert res.valid is False
    assert expected in res.errors


@pytest.mark.parametrize(
    "side,price,tick,expected_aligned",
    [
        ("BUY", Decimal("100.05"), Decimal("0.1"), Decimal("100.1")),  # BUY suggest up
        ("SELL", Decimal("100.05"), Decimal("0.1"), Decimal("100.0")),  # SELL suggest down
    ],
)
def test_price_not_aligned_to_tick_size_buy_up_sell_down(side, price, tick, expected_aligned):
    """price 非 tick_size 对齐 → invalid，并且 BUY suggest 向上、SELL suggest 向下"""
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=tick,
        lot_size=Decimal("0.001"),
        contract_multiplier=Decimal("1"),
        price_limit_pct=None,
    )
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": "999999999"})
    intent = Intent(side=side, quantity=Decimal("0.010"), price=price)

    res = v.validate_trade_intent(intent, spec)

    expected = f"price {intent.price} not aligned to tick_size {spec.tick_size}, suggest: {expected_aligned}"
    assert res.valid is False
    assert expected in res.errors


def test_notional_exceeds_max_includes_price_qty_multiplier_format():
    """
    覆盖一个“notional 超过 max_notional_per_trade” 的 case，
    确保报错格式含 price×qty×multiplier。
    """
    spec = _make_spec(
        symbol="BTCUSDT",
        asset_class="crypto_perp",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.001"),
        contract_multiplier=Decimal("1"),
        price_limit_pct=None,
    )
    v = SemanticValidators(state_reader=None, config={"max_notional_per_trade": 100})
    intent = Intent(side="BUY", quantity=Decimal("3"), price=Decimal("50"))

    res = v.validate_trade_intent(intent, spec)

    notional = Decimal("50") * Decimal("3") * Decimal("1")  # 150
    expected = f"notional {notional} exceeds max {Decimal('100')} ({intent.price}×{intent.quantity}×{spec.contract_multiplier})"

    assert res.valid is False
    assert expected in res.errors