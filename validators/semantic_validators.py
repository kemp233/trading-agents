"""Semantic Validators — R3 语义校验 (v3: 含期货乘数/tick/lot 强制校验)"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class InstrumentSpec:
    symbol: str
    asset_class: str  # 'crypto_perp' | 'cn_futures'
    tick_size: Decimal
    lot_size: Decimal
    contract_multiplier: Decimal  # v3 新增
    max_leverage: int
    margin_rate: Decimal
    trading_hours: dict
    price_limit_pct: Optional[Decimal] = None


@dataclass
class ValidationResult:
    valid: bool
    errors: list
    warnings: list = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class SemanticValidators:
    def __init__(self, state_reader, config: dict):
        self._state_reader = state_reader
        self._max_notional = Decimal(str(config.get('max_notional_per_trade', 10000)))

    def validate_trade_intent(self, intent, spec: InstrumentSpec) -> ValidationResult:
        errors = []
        warnings = []

        # 1. qty 必须是 lot_size 的整数倍
        if intent.quantity % spec.lot_size != 0:
            aligned = (intent.quantity // spec.lot_size) * spec.lot_size
            errors.append(
                f"qty {intent.quantity} not aligned to lot_size {spec.lot_size}, "
                f"suggest: {aligned}"
            )

        # 2. price 必须按 tick_size 对齐
        if intent.price and spec.tick_size:
            remainder = intent.price % spec.tick_size
            if remainder != 0:
                if intent.side == 'BUY':
                    aligned = intent.price - remainder + spec.tick_size
                else:
                    aligned = intent.price - remainder
                errors.append(
                    f"price {intent.price} not aligned to tick_size "
                    f"{spec.tick_size}, suggest: {aligned}"
                )

        # 3. 名义敞口 = price × qty × contract_multiplier
        if intent.price:
            notional = intent.price * intent.quantity * spec.contract_multiplier
            if notional > self._max_notional:
                errors.append(
                    f"notional {notional} exceeds max {self._max_notional} "
                    f"({intent.price}×{intent.quantity}×{spec.contract_multiplier})"
                )

        # 4. 涨跌停检查 (期货特有)
        if spec.price_limit_pct and intent.price:
            # TODO: 从 state_reader 获取结算价
            pass

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )
