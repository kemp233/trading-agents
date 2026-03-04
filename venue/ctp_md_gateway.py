# venue/ctp_md_gateway.py
from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from vnpy_ctp.gateway import CtpGateway

from core.market_event import MarketTickEvent

logger = logging.getLogger(__name__)


class CtpMdGateway:
    """CTP Market Data (MdApi) gateway.

    Manages an independent MdApi TCP connection separate from TradeApi.
    Design mirrors CtpGatewayWrapper in venue/ctp_gateway.py:
      - CtpGateway imported locally inside connect()
      - asyncio.Event-based login synchronisation
      - Exponential-backoff reconnect loop (1 s -> 2 s -> ... -> 60 s)
      - Automatic re-subscription after successful reconnect
    """

    def __init__(
        self,
        config: dict,
        on_tick: Callable[[MarketTickEvent], None] | None = None,
    ) -> None:
        """Initialise MD gateway.

        Args:
            config: Keys: broker_id, user_id, password, md_front_addr.
                    user_id and password fall back to CTP_USER_ID / CTP_PASSWORD
                    environment variables when omitted.
            on_tick: Optional callback invoked for every MarketTickEvent.
        """
        self.broker_id: str = config["broker_id"]
        self.user_id: str = config.get("user_id") or os.getenv("CTP_USER_ID", "")
        self.password: str = config.get("password") or os.getenv("CTP_PASSWORD", "")
        self.md_front_addr: str = config["md_front_addr"]

        self._on_tick_callback: Callable[[MarketTickEvent], None] = (
            on_tick or (lambda _: None)
        )

        self._gateway: CtpGateway | None = None
        self._connected: bool = False
        self._login_event: asyncio.Event = asyncio.Event()
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_interval: float = 1.0
        self._max_reconnect_interval: float = 60.0
        self._should_reconnect: bool = True
        self._subscribed_symbols: list[str] = []

    @property
    def is_connected(self) -> bool:
        """Return True when MD gateway is logged in and ready."""
        return self._connected

    async def connect(self, symbols: list[str] | None = None) -> None:
        """Connect to CTP MD front, login, then subscribe symbols.

        Args:
            symbols: Contracts to subscribe immediately after login.

        Raises:
            TimeoutError: If login does not complete within 10 seconds.
        """
        if self._connected:
            logger.info("CTP MD gateway already connected")
            return

        logger.info(
            "Connecting to CTP MD gateway",
            extra={
                "broker_id": self.broker_id,
                "user_id": self.user_id,
                "md_front_addr": self.md_front_addr,
            },
        )

        self._login_event.clear()
        self._should_reconnect = True

        try:
            from vnpy_ctp.gateway import CtpGateway

            self._gateway = CtpGateway()

            self._gateway.on_front_connected = self._on_front_connected
            self._gateway.on_front_disconnected = self._on_front_disconnected
            self._gateway.on_rsp_user_login = self._on_rsp_user_login
            self._gateway.on_rtn_depth_market_data = self._on_rtn_depth_market_data

            self._gateway.connect(
                {
                    "md_address": self.md_front_addr,
                    "brokerid": self.broker_id,
                    "userid": self.user_id,
                    "password": self.password,
                }
            )

            try:
                await asyncio.wait_for(self._login_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.error("CTP MD login timeout after 10 seconds")
                raise TimeoutError("CTP MD login timeout")

            self._connected = True
            logger.info("CTP MD gateway connected successfully")

            if symbols:
                self.subscribe(symbols)

            if self._reconnect_task is None or self._reconnect_task.done():
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

        except TimeoutError:
            raise
        except Exception as e:
            logger.error("Failed to connect to CTP MD gateway: %s", e, exc_info=True)
            raise

    async def disconnect(self) -> None:
        """Disconnect from CTP MD gateway and stop reconnect loop."""
        self._should_reconnect = False

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        if self._gateway:
            try:
                self._gateway.close()
            except Exception as e:
                logger.warning("Error closing CTP MD gateway: %s", e)

        self._connected = False
        self._login_event.clear()
        logger.info("CTP MD gateway disconnected")

    def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to market data.  No-op if not connected."""
        if not self._connected or self._gateway is None:
            logger.warning("subscribe() called while MD gateway is not connected")
            return

        new_syms = [s for s in symbols if s not in self._subscribed_symbols]
        if not new_syms:
            return

        self._gateway.subscribe(new_syms)
        self._subscribed_symbols.extend(new_syms)
        logger.info("Subscribed to MD symbols: %s", new_syms)

    def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from market data."""
        if self._gateway is None:
            return
        self._gateway.unsubscribe(symbols)
        self._subscribed_symbols = [
            s for s in self._subscribed_symbols if s not in symbols
        ]
        logger.info("Unsubscribed from MD symbols: %s", symbols)

    # ------------------------------------------------------------------ #
    # Callbacks                                                             #
    # ------------------------------------------------------------------ #

    def _on_front_connected(self) -> None:
        """Handle OnFrontConnected: send ReqUserLogin (no auth step for MdApi)."""
        logger.info("CTP MD front connected, sending ReqUserLogin")
        if self._gateway:
            self._gateway.login(
                {
                    "BrokerID": self.broker_id,
                    "UserID": self.user_id,
                    "Password": self.password,
                },
                reqid=1,
            )

    def _on_front_disconnected(self, reason: int) -> None:
        """Handle OnFrontDisconnected: clear state, reconnect loop will retry."""
        logger.warning("CTP MD front disconnected, reason: %d", reason)
        self._connected = False
        self._login_event.clear()

    def _on_rsp_user_login(
        self, data: dict, error: dict | None, reqid: int, last: bool
    ) -> None:
        """Handle OnRspUserLogin callback."""
        if error and error.get("ErrorID", 0) != 0:
            logger.error(
                "CTP MD login failed: %s", error.get("ErrorMsg", "Unknown error")
            )
            return

        logger.info("CTP MD login successful")
        self._login_event.set()

    def _on_rtn_depth_market_data(self, data: dict) -> None:
        """Handle OnRtnDepthMarketData: convert to MarketTickEvent and dispatch."""
        try:
            tick = MarketTickEvent.from_ctp(data)
            self._on_tick_callback(tick)
        except Exception as e:
            logger.error("Error processing depth market data: %s", e, exc_info=True)

    # ------------------------------------------------------------------ #
    # Reconnect loop                                                        #
    # ------------------------------------------------------------------ #

    async def _reconnect_loop(self) -> None:
        """Background task: exponential-backoff reconnect (mirrors TradeApi gateway)."""
        while self._should_reconnect:
            await asyncio.sleep(self._reconnect_interval)

            if self._connected or not self._should_reconnect:
                continue

            logger.info(
                "Attempting CTP MD reconnect (interval: %.1fs)",
                self._reconnect_interval,
            )

            try:
                self._login_event.clear()

                if self._gateway:
                    self._gateway.connect(
                        {
                            "md_address": self.md_front_addr,
                            "brokerid": self.broker_id,
                            "userid": self.user_id,
                            "password": self.password,
                        }
                    )

                    try:
                        await asyncio.wait_for(
                            self._login_event.wait(), timeout=10.0
                        )
                        self._connected = True
                        self._reconnect_interval = 1.0
                        logger.info("CTP MD reconnect successful")

                        if self._subscribed_symbols and self._gateway:
                            self._gateway.subscribe(self._subscribed_symbols)
                            logger.info(
                                "Re-subscribed to: %s", self._subscribed_symbols
                            )

                    except asyncio.TimeoutError:
                        logger.warning("CTP MD reconnect timeout")
                        self._reconnect_interval = min(
                            self._reconnect_interval * 2,
                            self._max_reconnect_interval,
                        )

            except Exception as e:
                logger.error("CTP MD reconnect error: %s", e, exc_info=True)
                self._reconnect_interval = min(
                    self._reconnect_interval * 2, self._max_reconnect_interval
                )
