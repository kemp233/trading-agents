"""Phase 1 测试 — 语义校验 (含期货乘数)"""
import pytest
from decimal import Decimal
from validators.semantic_validators import (
    SemanticValidators, InstrumentSpec, ValidationResult
)


class TestSemanticValidators:
    def _make_rb_spec(self):
        return InstrumentSpec(
            symbol='rb2510',
            asset_class='cn_futures',
            tick_size=Decimal('1'),
            lot_size=Decimal('1'),
            contract_multiplier=Decimal('10'),
            max_leverage=10,
            margin_rate=Decimal('0.10'),
            trading_hours={'day': '09:00-15:00'},
            price_limit_pct=Decimal('0.07'),
        )

    def test_lot_size_alignment(self):
        """qty 必须是 lot_size 的整数倍"""
        # TODO: implement
        pass

    def test_tick_size_alignment(self):
        """price 必须按 tick_size 对齐"""
        pass

    def test_notional_with_multiplier(self):
        """名义敞口 = price × qty × contract_multiplier"""
        pass
