from __future__ import annotations

import logging
from collections import defaultdict
from types import SimpleNamespace
from typing import Any

from core.event_envelope import EventEnvelope, EventType

logger = logging.getLogger(__name__)


class StrategyAgent:
    STRATEGY_ID = "ema_crossover_v1"

    def __init__(self, event_bus, config: dict[str, Any] | None = None) -> None:
        self._event_bus = event_bus
        self._config = dict(config or {})
        self._started = False
        self._subscribed = False
        self._paused = False
        self._event_sequences: dict[str, int] = defaultdict(int)
        self._last_cross_by_symbol: dict[str, str] = {}
        self._default_venue = str(self._config.get("venue", "binance"))
        self._default_quantity = str(self._config.get("default_quantity", "0.001"))
        self._symbol_allowlist = self._parse_allowlist(self._config.get("symbol_allowlist"))

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
            raise RuntimeError("event_bus is required for StrategyAgent")
        if not self._subscribed:
            self._event_bus.subscribe(EventType.TA_SIGNAL, self.handle_ta_signal)
            self._subscribed = True
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def health_check(self):
        return SimpleNamespace(
            ok=True,
            reason="",
            started=self._started,
            paused=self._paused,
            tracked_symbols=len(self._last_cross_by_symbol),
        )

    async def handle_command(self, command: str, args: str) -> dict[str, Any]:
        _ = args
        if command == "/pause":
            self._paused = True
            return {"ok": True, "paused": True}
        if command == "/resume":
            self._paused = False
            return {"ok": True, "paused": False}
        if command == "/list":
            return {
                "strategies": [self.STRATEGY_ID],
                "active": self.STRATEGY_ID,
                "paused": self._paused,
            }
        return {"ok": False, "error": f"unknown command: {command}"}

    async def handle_ta_signal(self, envelope: EventEnvelope) -> None:
        if not self._started:
            return

        payload = envelope.payload
        symbol = self._resolve_symbol(envelope)
        if not symbol:
            logger.warning("Strategy agent ignored TA signal without symbol: %s", envelope.event_id)
            return
        if self._symbol_allowlist is not None and symbol not in self._symbol_allowlist:
            return

        derived = payload.get("derived", {})
        ema_cross = derived.get("ema_cross") if isinstance(derived, dict) else None
        if ema_cross not in {"golden", "death"}:
            return

        previous_cross = self._last_cross_by_symbol.get(symbol)
        self._last_cross_by_symbol[symbol] = str(ema_cross)
        if previous_cross == ema_cross:
            return
        if self._paused:
            return

        side = "BUY" if ema_cross == "golden" else "SELL"
        stream_seq = self._event_sequences[symbol]
        self._event_sequences[symbol] += 1
        client_order_id = f"ema-crossover-v1-{symbol}-{stream_seq:06d}-{side.lower()}"
        trade_intent_payload = {
            "symbol": symbol,
            "side": side,
            "order_type": "MARKET",
            "quantity": self._default_quantity,
            "price": None,
            "time_in_force": "IOC",
            "reduce_only": False,
            "post_only": False,
            "hedge_flag": "SPEC",
            "venue": self._default_venue,
            "strategy_id": self.STRATEGY_ID,
            "client_order_id": client_order_id,
        }
        trade_intent = EventEnvelope.make(
            EventType.TRADE_INTENT,
            symbol,
            trade_intent_payload,
            stream_seq=stream_seq,
            event_ts=envelope.event_ts,
        )
        await self._event_bus.publish(trade_intent)

    @staticmethod
    def _resolve_symbol(envelope: EventEnvelope) -> str:
        symbol = envelope.payload.get("symbol")
        if isinstance(symbol, str) and symbol:
            return symbol
        _, _, suffix = envelope.stream_id.partition(":")
        return suffix
