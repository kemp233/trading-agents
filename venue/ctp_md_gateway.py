from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from core.market_event import MarketTickEvent
from venue.ctp_gateway import CtpGatewayWrapper
from venue.ctp_utils import load_instrument_exchange_map

logger = logging.getLogger(__name__)


class CtpMdGateway:
    """Market-data facade built on top of the shared CTP gateway wrapper."""

    def __init__(
        self,
        config: dict,
        on_tick: Callable[[MarketTickEvent], None] | None = None,
        gateway_wrapper: CtpGatewayWrapper | None = None,
        instrument_config_path: Path | None = None,
    ) -> None:
        self._gateway = gateway_wrapper or CtpGatewayWrapper(config)
        self._owns_gateway = gateway_wrapper is None
        self._on_tick_callback = on_tick or (lambda _tick: None)
        self._instrument_exchange = load_instrument_exchange_map(instrument_config_path)
        self._subscribed_symbols: list[str] = []
        self._gateway.register_tick_listener(self._on_tick)

    @property
    def is_connected(self) -> bool:
        return self._gateway.is_connected

    async def connect(self, symbols: list[str] | None = None) -> None:
        if self._owns_gateway and not self._gateway.is_connected:
            await self._gateway.connect()
        if symbols:
            self.subscribe(symbols)

    async def disconnect(self) -> None:
        if self._owns_gateway:
            await self._gateway.disconnect()

    def subscribe(self, symbols: list[str]) -> None:
        requests = []
        for symbol in symbols:
            if symbol in self._subscribed_symbols:
                continue
            exchange = self._instrument_exchange.get(symbol)
            if exchange is None:
                raise ValueError(f"Unknown exchange for symbol {symbol}")
            from vnpy.trader.object import SubscribeRequest

            requests.append(SubscribeRequest(symbol=symbol, exchange=exchange))
            self._subscribed_symbols.append(symbol)

        if requests:
            self._gateway.subscribe(requests)
            logger.info("Subscribed to MD symbols: %s", [req.symbol for req in requests])

    def unsubscribe(self, symbols: list[str]) -> None:
        self._subscribed_symbols = [symbol for symbol in self._subscribed_symbols if symbol not in symbols]

    def _on_tick(self, tick) -> None:
        market_tick = MarketTickEvent.from_vnpy(tick)
        self._on_tick_callback(market_tick)
