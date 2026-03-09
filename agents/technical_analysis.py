from __future__ import annotations

import logging
from collections import defaultdict, deque
from types import SimpleNamespace
from typing import Any

import numpy as np
import talib

from core.event_envelope import EventEnvelope, EventType

logger = logging.getLogger(__name__)


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class TechnicalAnalysisAgent:
    RSI_PERIOD = 14
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    BOLLINGER_PERIOD = 20
    BOLLINGER_DEV = 2
    ATR_PERIOD = 14
    EMA_FAST = 12
    EMA_SLOW = 26
    WARMUP_TICKS = max(RSI_PERIOD + 1, MACD_SLOW + MACD_SIGNAL, BOLLINGER_PERIOD, ATR_PERIOD + 1, EMA_SLOW)

    def __init__(self, event_bus, config: dict[str, Any] | None = None) -> None:
        self._event_bus = event_bus
        self._config = dict(config or {})
        self._started = False
        self._subscribed = False
        self._event_sequences: dict[str, int] = defaultdict(int)
        self._symbol_allowlist = self._parse_allowlist(self._config.get("symbol_allowlist"))
        self._buffer_size = max(int(self._config.get("buffer_size", self.WARMUP_TICKS * 4)), self.WARMUP_TICKS)
        self._buffers: dict[str, dict[str, deque[float]]] = defaultdict(self._build_buffer)

    def _build_buffer(self) -> dict[str, deque[float]]:
        return {
            "open": deque(maxlen=self._buffer_size),
            "high": deque(maxlen=self._buffer_size),
            "low": deque(maxlen=self._buffer_size),
            "close": deque(maxlen=self._buffer_size),
        }

    @staticmethod
    def _parse_allowlist(raw: Any) -> set[str] | None:
        if raw is None:
            return None
        if isinstance(raw, (list, tuple, set)):
            return {str(item) for item in raw}
        return {str(raw)}

    async def start(self) -> None:
        if self._started:
            return
        if self._event_bus is None:
            raise RuntimeError("event_bus is required for TechnicalAnalysisAgent")
        if not self._subscribed:
            self._event_bus.subscribe(EventType.MARKET_TICK, self.handle_market_tick)
            self._subscribed = True
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def health_check(self):
        return SimpleNamespace(
            ok=True,
            reason="",
            started=self._started,
            subscribed=self._subscribed,
            tracked_symbols=len(self._buffers),
        )

    async def handle_market_tick(self, envelope: EventEnvelope) -> None:
        if not self._started:
            return

        symbol = self._resolve_symbol(envelope)
        if not symbol:
            logger.warning("TA agent ignored market tick without symbol: %s", envelope.event_id)
            return
        if self._symbol_allowlist is not None and symbol not in self._symbol_allowlist:
            return

        payload = envelope.payload
        last_price = _as_float(payload.get("last_price"), default=_as_float(payload.get("price")))
        if last_price is None:
            logger.warning("TA agent ignored market tick without last_price/price: %s", envelope.event_id)
            return

        open_price = _as_float(payload.get("open_price"), default=_as_float(payload.get("open"), default=last_price))
        high_price = _as_float(payload.get("high_price"), default=_as_float(payload.get("high"), default=last_price))
        low_price = _as_float(payload.get("low_price"), default=_as_float(payload.get("low"), default=last_price))
        if open_price is None or high_price is None or low_price is None:
            logger.warning("TA agent ignored market tick with incomplete OHLC payload: %s", envelope.event_id)
            return

        buffer = self._buffers[symbol]
        buffer["open"].append(open_price)
        buffer["high"].append(high_price)
        buffer["low"].append(low_price)
        buffer["close"].append(last_price)

        if len(buffer["close"]) < self.WARMUP_TICKS:
            return

        ta_payload = self._build_ta_payload(symbol=symbol, source=envelope, buffer=buffer)
        if ta_payload is None:
            return

        stream_seq = self._event_sequences[symbol]
        self._event_sequences[symbol] += 1
        ta_signal = EventEnvelope.make(
            EventType.TA_SIGNAL,
            symbol,
            ta_payload,
            stream_seq=stream_seq,
            event_ts=envelope.event_ts,
        )
        await self._event_bus.publish(ta_signal)

    @staticmethod
    def _resolve_symbol(envelope: EventEnvelope) -> str:
        symbol = envelope.payload.get("symbol")
        if isinstance(symbol, str) and symbol:
            return symbol
        _, _, suffix = envelope.stream_id.partition(":")
        return suffix

    def _build_ta_payload(
        self,
        *,
        symbol: str,
        source: EventEnvelope,
        buffer: dict[str, deque[float]],
    ) -> dict[str, Any] | None:
        highs = np.asarray(buffer["high"], dtype=float)
        lows = np.asarray(buffer["low"], dtype=float)
        closes = np.asarray(buffer["close"], dtype=float)

        rsi = talib.RSI(closes, timeperiod=self.RSI_PERIOD)
        macd, macd_signal, macd_hist = talib.MACD(
            closes,
            fastperiod=self.MACD_FAST,
            slowperiod=self.MACD_SLOW,
            signalperiod=self.MACD_SIGNAL,
        )
        bollinger_upper, bollinger_mid, bollinger_lower = talib.BBANDS(
            closes,
            timeperiod=self.BOLLINGER_PERIOD,
            nbdevup=self.BOLLINGER_DEV,
            nbdevdn=self.BOLLINGER_DEV,
        )
        atr = talib.ATR(highs, lows, closes, timeperiod=self.ATR_PERIOD)
        ema_fast = talib.EMA(closes, timeperiod=self.EMA_FAST)
        ema_slow = talib.EMA(closes, timeperiod=self.EMA_SLOW)

        current = (
            rsi[-1],
            macd[-1],
            macd_signal[-1],
            macd_hist[-1],
            bollinger_upper[-1],
            bollinger_mid[-1],
            bollinger_lower[-1],
            atr[-1],
            ema_fast[-1],
            ema_slow[-1],
        )
        if not np.isfinite(np.asarray(current, dtype=float)).all():
            return None

        ema_cross = "none"
        previous_cross_inputs = (ema_fast[-2], ema_slow[-2], ema_fast[-1], ema_slow[-1])
        if np.isfinite(np.asarray(previous_cross_inputs, dtype=float)).all():
            prev_fast, prev_slow, current_fast, current_slow = previous_cross_inputs
            if prev_fast <= prev_slow and current_fast > current_slow:
                ema_cross = "golden"
            elif prev_fast >= prev_slow and current_fast < current_slow:
                ema_cross = "death"

        return {
            "symbol": symbol,
            "last_price": float(closes[-1]),
            "indicators": {
                "rsi": float(rsi[-1]),
                "macd": float(macd[-1]),
                "macd_signal": float(macd_signal[-1]),
                "macd_hist": float(macd_hist[-1]),
                "bollinger_upper": float(bollinger_upper[-1]),
                "bollinger_mid": float(bollinger_mid[-1]),
                "bollinger_lower": float(bollinger_lower[-1]),
                "atr": float(atr[-1]),
                "ema_fast": float(ema_fast[-1]),
                "ema_slow": float(ema_slow[-1]),
            },
            "derived": {"ema_cross": ema_cross},
            "source_event_id": source.event_id,
        }
