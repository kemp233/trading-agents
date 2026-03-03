"""Semantic Validators — R3 语义校验 (v3: 含期货乘数/tick/lot 强制校验)"""
from dataclasses import dataclass
from decimal import Decimal
from typing import List
import logging

from core.instrument_master import InstrumentSpec

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ValidationResult:
    valid: bool
    errors: List[str]
    warnings: List[str]


class SemanticValidators:
    def __init__(self, state_reader, config: dict):
        self._state_reader = state_reader
        self._max_notional = Decimal(str(config.get("max_notional_per_trade", 10000)))

    def validate_trade_intent(self, intent, spec: InstrumentSpec) -> ValidationResult:
        errors: List[str] = []
        warnings: List[str] = []

        # 1. qty 必须是 lot_size 的整数倍
        if intent.quantity % spec.lot_size != 0:
            aligned = (intent.quantity // spec.lot_size) * spec.lot_size
            errors.append(
                f"qty {intent.quantity} not aligned to lot_size {spec.lot_size}, "
                f"suggest: {aligned}"
            )

        # 2. price 必须按 tick_size 对齐
        if intent.price is not None and spec.tick_size is not None:
            remainder = intent.price % spec.tick_size
            if remainder != 0:
                if intent.side == "BUY":
                    aligned = intent.price - remainder + spec.tick_size
                    aligned = aligned.quantize(spec.tick_size)
                else:
                    aligned = intent.price - remainder
                    aligned = aligned.quantize(spec.tick_size)

                errors.append(
                    f"price {intent.price} not aligned to tick_size "
                    f"{spec.tick_size}, suggest: {aligned}"
                )

        # 3. 名义敞口 = price × qty × contract_multiplier
        if intent.price is not None:
            notional = intent.price * intent.quantity * spec.contract_multiplier
            if notional > self._max_notional:
                errors.append(
                    f"notional {notional} exceeds max {self._max_notional} "
                    f"({intent.price}×{intent.quantity}×{spec.contract_multiplier})"
                )

        # 4. 涨跌停检查 (仅 cn_futures)
        if (
            spec.price_limit_pct is not None
            and intent.price is not None
            and spec.asset_class == "cn_futures"
        ):
            base_price = None
            if hasattr(intent, "reference_price") and intent.reference_price is not None:
                base_price = intent.reference_price
            elif hasattr(intent, "settlement_price") and intent.settlement_price is not None:
                base_price = intent.settlement_price
            elif hasattr(intent, "prev_close") and intent.prev_close is not None:
                base_price = intent.prev_close

            if base_price is not None:
                pct = spec.price_limit_pct
                lower_bound = base_price * (Decimal("1") - pct)
                upper_bound = base_price * (Decimal("1") + pct)
                if intent.price < lower_bound or intent.price > upper_bound:
                    errors.append(
                        f"price {intent.price} outside limit [{lower_bound}, {upper_bound}] "
                        f"(base: {base_price}, pct: {pct})"
                    )
            else:
                warnings.append(
                    "missing base price (reference_price/settlement_price/prev_close) "
                    "for price limit validation"
                )

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )