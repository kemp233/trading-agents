from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, List
from zoneinfo import ZoneInfo
import logging

from core.instrument_master import InstrumentSpec

logger = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class SemanticValidationError(ValueError):
    """Raised when a trade intent fails semantic validation."""


@dataclass(slots=True)
class ValidationResult:
    valid: bool
    errors: List[str]
    warnings: List[str]


class SemanticValidators:
    def __init__(self, state_reader, config: dict):
        self._state_reader = state_reader
        self._max_notional = Decimal(str(config.get("max_notional_per_trade", "1000000")))
        self._require_account_snapshot = bool(config.get("require_account_snapshot", False))
        self._require_price_limit_base = bool(config.get("require_price_limit_base", False))

    def validate_trade_intent(self, intent, spec: InstrumentSpec) -> ValidationResult:
        errors: List[str] = []
        warnings: List[str] = []

        self._validate_quantity(intent, spec, errors)
        self._validate_price_alignment(intent, spec, errors)
        self._validate_notional(intent, spec, errors)
        self._validate_price_limits(intent, spec, errors, warnings)
        self._validate_trading_hours(intent, spec, errors)
        self._validate_margin_and_leverage(intent, spec, errors, warnings)

        return ValidationResult(valid=not errors, errors=errors, warnings=warnings)

    def assert_trade_intent(self, intent, spec: InstrumentSpec) -> ValidationResult:
        result = self.validate_trade_intent(intent, spec)
        if not result.valid:
            raise SemanticValidationError("; ".join(result.errors))
        return result

    def _validate_quantity(self, intent, spec: InstrumentSpec, errors: List[str]) -> None:
        if intent.quantity % spec.lot_size != 0:
            aligned = (intent.quantity // spec.lot_size) * spec.lot_size
            errors.append(
                f"qty {intent.quantity} not aligned to lot_size {spec.lot_size}, suggest: {aligned}"
            )

    def _validate_price_alignment(self, intent, spec: InstrumentSpec, errors: List[str]) -> None:
        if intent.price is None:
            return
        remainder = intent.price % spec.tick_size
        if remainder == 0:
            return
        if intent.side == "BUY":
            aligned = intent.price - remainder + spec.tick_size
        else:
            aligned = intent.price - remainder
        aligned = aligned.quantize(spec.tick_size)
        errors.append(
            f"price {intent.price} not aligned to tick_size {spec.tick_size}, suggest: {aligned}"
        )

    def _validate_notional(self, intent, spec: InstrumentSpec, errors: List[str]) -> None:
        if intent.price is None:
            return
        notional = intent.price * intent.quantity * spec.contract_multiplier
        if notional > self._max_notional:
            errors.append(
                f"notional {notional} exceeds max {self._max_notional} "
                f"({intent.price}*{intent.quantity}*{spec.contract_multiplier})"
            )

    def _validate_price_limits(
        self,
        intent,
        spec: InstrumentSpec,
        errors: List[str],
        warnings: List[str],
    ) -> None:
        if spec.asset_class != "cn_futures" or spec.price_limit_pct is None or intent.price is None:
            return

        base_price = self._extract_base_price(intent)
        if base_price is None:
            message = (
                "missing base price (reference_price/settlement_price/prev_close) "
                "for price limit validation"
            )
            if self._require_price_limit_base:
                errors.append(message)
            else:
                warnings.append(message)
            return

        lower_bound = base_price * (Decimal("1") - spec.price_limit_pct)
        upper_bound = base_price * (Decimal("1") + spec.price_limit_pct)
        if intent.price < lower_bound or intent.price > upper_bound:
            errors.append(
                f"price {intent.price} outside limit [{lower_bound}, {upper_bound}] "
                f"(base: {base_price}, pct: {spec.price_limit_pct})"
            )

    def _validate_trading_hours(self, intent, spec: InstrumentSpec, errors: List[str]) -> None:
        if self._is_always_open(spec.trading_hours):
            return

        current_time = self._extract_current_time(intent, spec)
        sessions = self._extract_sessions(spec.trading_hours)
        if not sessions:
            errors.append(f"instrument {spec.symbol} has no trading sessions configured")
            return

        minute_of_day = current_time.hour * 60 + current_time.minute
        for session in sessions:
            start_minute, end_minute = self._parse_session(session)
            if start_minute <= end_minute:
                if start_minute <= minute_of_day <= end_minute:
                    return
            else:
                if minute_of_day >= start_minute or minute_of_day <= end_minute:
                    return

        errors.append(
            f"current time {current_time.strftime('%H:%M:%S')} outside trading sessions {sessions}"
        )

    def _validate_margin_and_leverage(
        self,
        intent,
        spec: InstrumentSpec,
        errors: List[str],
        warnings: List[str],
    ) -> None:
        if intent.price is None:
            return

        available = self._extract_decimal(intent, "available_funds", "available")
        equity = self._extract_decimal(intent, "account_equity", "equity")
        missing_fields: list[str] = []
        if available is None:
            missing_fields.append("available_funds")
        if equity is None:
            missing_fields.append("account_equity")

        if missing_fields:
            message = f"missing account snapshot fields for margin validation: {', '.join(missing_fields)}"
            if self._require_account_snapshot:
                errors.append(message)
            else:
                warnings.append(message)
            return

        notional = intent.price * intent.quantity * spec.contract_multiplier
        required_margin = notional * spec.margin_rate
        if available < required_margin:
            errors.append(
                f"required margin {required_margin} exceeds available funds {available}"
            )

        if equity <= 0:
            errors.append(f"account equity must be positive for leverage validation, got {equity}")
            return

        effective_leverage = notional / equity
        if effective_leverage > Decimal(spec.max_leverage):
            errors.append(
                f"effective leverage {effective_leverage} exceeds max {spec.max_leverage}"
            )

    def _extract_base_price(self, intent) -> Decimal | None:
        for attr in ("reference_price", "settlement_price", "prev_close"):
            value = getattr(intent, attr, None)
            if value is not None:
                return Decimal(str(value))
        return None

    def _extract_current_time(self, intent, spec: InstrumentSpec) -> datetime:
        current_time = getattr(intent, "current_time", None)
        if current_time is None:
            timezone_name = spec.trading_hours.get("timezone", "Asia/Shanghai")
            return datetime.now(ZoneInfo(timezone_name))
        if current_time.tzinfo is None:
            raise ValueError("current_time must be timezone-aware")
        timezone_name = spec.trading_hours.get("timezone")
        if timezone_name:
            return current_time.astimezone(ZoneInfo(timezone_name))
        return current_time.astimezone(SHANGHAI_TZ)

    def _extract_decimal(self, intent, *names: str) -> Decimal | None:
        for name in names:
            value = getattr(intent, name, None)
            if value is not None:
                return Decimal(str(value))
        return None

    def _extract_sessions(self, trading_hours: dict[str, Any]) -> list[str]:
        if trading_hours.get("24/7"):
            return []
        if isinstance(trading_hours.get("sessions"), list):
            return [str(session) for session in trading_hours["sessions"]]

        sessions: list[str] = []
        for key in ("day", "night"):
            value = trading_hours.get(key)
            if isinstance(value, str) and value:
                sessions.append(value)
        return sessions

    def _is_always_open(self, trading_hours: dict[str, Any]) -> bool:
        return bool(trading_hours.get("24/7"))

    def _parse_session(self, session: str) -> tuple[int, int]:
        start_str, end_str = session.split("-", 1)
        start_hour, start_minute = (int(part) for part in start_str.split(":", 1))
        end_hour, end_minute = (int(part) for part in end_str.split(":", 1))
        return start_hour * 60 + start_minute, end_hour * 60 + end_minute


def build_validation_intent(spec, **context: Any) -> Any:
    payload = {
        "symbol": spec.symbol,
        "side": spec.side,
        "order_type": spec.order_type,
        "quantity": spec.quantity,
        "price": spec.price,
        "reduce_only": spec.reduce_only,
        "post_only": spec.post_only,
        "client_order_id": spec.client_order_id,
        "venue": spec.venue,
    }
    payload.update(context)
    return SimpleNamespace(**payload)

