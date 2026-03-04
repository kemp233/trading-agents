# core/market_event.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

logger = logging.getLogger(__name__)

_CTP_INVALID_THRESHOLD: float = 1.0e300


def _safe_decimal(value: object) -> Decimal:
    """Convert a CTP price field to Decimal.

    CTP uses 1.7976931348623157e+308 (DBL_MAX) as sentinel when a field
    has no data.  Values at or above _CTP_INVALID_THRESHOLD are replaced
    with Decimal("0").
    """
    if value is None:
        return Decimal("0")
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return Decimal("0")
    if f >= _CTP_INVALID_THRESHOLD:
        return Decimal("0")
    return Decimal(str(f))


@dataclass(frozen=True)
class MarketTickEvent:
    """Normalised market data tick from CTP OnRtnDepthMarketData."""

    symbol: str
    last_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: int
    bid_price_1: Decimal
    bid_volume_1: int
    ask_price_1: Decimal
    ask_volume_1: int
    upper_limit: Decimal
    lower_limit: Decimal
    open_interest: int
    timestamp: datetime  # UTC

    @classmethod
    def from_ctp(cls, data: dict) -> "MarketTickEvent":
        """Build a MarketTickEvent from CTP OnRtnDepthMarketData callback dict.

        CTP timestamp fields:
            ActionDay:       "YYYYMMDD"
            UpdateTime:      "HH:MM:SS"
            UpdateMillisec:  int milliseconds within the second
        """
        action_day: str = data.get("ActionDay", "") or ""
        update_time: str = data.get("UpdateTime", "") or ""
        update_ms: int = int(data.get("UpdateMillisec", 0) or 0)

        try:
            naive = datetime.strptime(
                f"{action_day} {update_time}", "%Y%m%d %H:%M:%S"
            )
            ts = naive.replace(tzinfo=timezone.utc) + timedelta(milliseconds=update_ms)
        except (ValueError, TypeError):
            ts = datetime.now(timezone.utc)
            logger.warning(
                "Cannot parse CTP timestamp action_day=%r update_time=%r, using now(UTC)",
                action_day,
                update_time,
            )

        return cls(
            symbol=str(data.get("InstrumentID", "") or ""),
            last_price=_safe_decimal(data.get("LastPrice")),
            open_price=_safe_decimal(data.get("OpenPrice")),
            high_price=_safe_decimal(data.get("HighestPrice")),
            low_price=_safe_decimal(data.get("LowestPrice")),
            volume=int(data.get("Volume", 0) or 0),
            bid_price_1=_safe_decimal(data.get("BidPrice1")),
            bid_volume_1=int(data.get("BidVolume1", 0) or 0),
            ask_price_1=_safe_decimal(data.get("AskPrice1")),
            ask_volume_1=int(data.get("AskVolume1", 0) or 0),
            upper_limit=_safe_decimal(data.get("UpperLimitPrice")),
            lower_limit=_safe_decimal(data.get("LowerLimitPrice")),
            open_interest=int(data.get("OpenInterest", 0) or 0),
            timestamp=ts,
        )
