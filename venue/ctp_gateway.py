from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import vnpy.trader.utility as trader_utility

from vnpy.trader.event import (
    EVENT_ACCOUNT,
    EVENT_LOG,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_TICK,
    EVENT_TRADE,
)
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)

from venue.ctp_utils import account_to_snapshot, build_ctp_runtime_config, build_vnpy_setting

if TYPE_CHECKING:
    from core.state_writer import StateWriter

logger = logging.getLogger(__name__)


class CtpGatewayWrapper:
    """Async wrapper around the current vnpy_ctp gateway and event engine."""

    def __init__(self, config: dict, state_writer: StateWriter | None = None) -> None:
        self._runtime_config = build_ctp_runtime_config(config)
        self._setting = build_vnpy_setting(config)
        self._state_writer = state_writer

        self.broker_id = self._runtime_config["broker_id"]
        self.user_id = self._runtime_config["user_id"]
        self.password = self._runtime_config["password"]
        self.app_id = self._runtime_config["app_id"]
        self.auth_code = self._runtime_config["auth_code"]
        self.td_front_addr = self._runtime_config["ctp_td_front_addr"]
        self.md_front_addr = self._runtime_config["ctp_md_front_addr"]
        self.counter_env = self._runtime_config["ctp_counter_env"]
        self.auth_enabled = self._runtime_config.get("auth_enabled", True)

        self._event_engine = None
        self._gateway = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._connected = False
        self._login_event = asyncio.Event()
        self._account_event = asyncio.Event()
        self._position_event = asyncio.Event()
        self._login_error: str | None = None
        self._auth_warning: str | None = None
        self._should_reconnect = True
        self._reconnect_task: asyncio.Task | None = None

        self._last_account: AccountData | None = None
        self._positions: dict[str, PositionData] = {}
        self._last_position_monotonic: float = 0.0

        self._order_listeners: list[Callable[[OrderData], Any]] = []
        self._trade_listeners: list[Callable[[TradeData], Any]] = []
        self._tick_listeners: list[Callable[[TickData], Any]] = []
        self._account_listeners: list[Callable[[AccountData], Any]] = []
        self._position_listeners: list[Callable[[PositionData], Any]] = []
        self._subscriptions: dict[str, SubscribeRequest] = {}

    @property
    def is_connected(self) -> bool:
        return self._connected

    def register_order_listener(self, callback: Callable[[OrderData], Any]) -> None:
        self._order_listeners.append(callback)

    def register_trade_listener(self, callback: Callable[[TradeData], Any]) -> None:
        self._trade_listeners.append(callback)

    def register_tick_listener(self, callback: Callable[[TickData], Any]) -> None:
        self._tick_listeners.append(callback)

    def register_account_listener(self, callback: Callable[[AccountData], Any]) -> None:
        self._account_listeners.append(callback)

    def register_position_listener(self, callback: Callable[[PositionData], Any]) -> None:
        self._position_listeners.append(callback)

    async def connect(self, timeout: float = 30.0) -> None:
        if self._connected:
            return

        from vnpy.event import EventEngine
        from vnpy_ctp.gateway import CtpGateway

        self._loop = asyncio.get_running_loop()
        self._should_reconnect = True
        self._login_error = None
        self._auth_warning = None
        self._login_event.clear()
        self._account_event.clear()
        self._position_event.clear()

        if self._event_engine is None:
            self._event_engine = EventEngine()
            self._event_engine.register(EVENT_LOG, self._on_log)
            self._event_engine.register(EVENT_ACCOUNT, self._on_account)
            self._event_engine.register(EVENT_POSITION, self._on_position)
            self._event_engine.register(EVENT_ORDER, self._on_order)
            self._event_engine.register(EVENT_TRADE, self._on_trade)
            self._event_engine.register(EVENT_TICK, self._on_tick)
            self._event_engine.start()

        self._prepare_vnpy_runtime()

        if self._gateway is None:
            self._gateway = CtpGateway(self._event_engine, "CTP")

        await self._write_connection_status("CONNECTING", detail=self._describe_fronts())
        self._gateway.connect(dict(self._setting))

        try:
            await asyncio.wait_for(self._login_event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            await self._write_connection_status(
                "LOGIN_FAILED",
                detail=f"CTP login timeout after {timeout:.0f}s",
            )
            raise TimeoutError("CTP login timeout") from exc

        if self._login_error:
            await self._write_connection_status("LOGIN_FAILED", detail=self._login_error)
            raise ConnectionError(self._login_error)

        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def disconnect(self) -> None:
        self._should_reconnect = False
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        if self._gateway is not None:
            try:
                self._gateway.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Error closing CTP gateway: %s", exc)

        if self._event_engine is not None:
            try:
                self._event_engine.stop()
            except Exception as exc:  # pragma: no cover
                logger.warning("Error stopping EventEngine: %s", exc)

        self._gateway = None
        self._event_engine = None
        self._connected = False
        self._login_event.clear()
        self._account_event.clear()
        self._position_event.clear()
        await self._write_connection_status("DISCONNECTED", detail="disconnect requested")

    async def refresh_account(self, timeout: float = 10.0) -> AccountData:
        if not self._connected or self._gateway is None:
            raise ConnectionError("CTP gateway not connected")

        self._account_event.clear()
        self._gateway.query_account()
        try:
            await asyncio.wait_for(self._account_event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Query account timeout") from exc

        if self._last_account is None:
            raise RuntimeError("CTP account query returned no data")
        return self._last_account

    async def refresh_positions(
        self,
        timeout: float = 10.0,
        settle_delay: float = 0.3,
    ) -> list[PositionData]:
        if not self._connected or self._gateway is None:
            raise ConnectionError("CTP gateway not connected")

        self._positions = {}
        self._last_position_monotonic = 0.0
        self._position_event.clear()
        self._gateway.query_position()

        try:
            await asyncio.wait_for(self._position_event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Query positions timeout") from exc

        last_seen = self._last_position_monotonic
        while True:
            await asyncio.sleep(settle_delay)
            if self._last_position_monotonic == last_seen:
                break
            last_seen = self._last_position_monotonic

        return list(self._positions.values())

    def send_order(self, req: OrderRequest) -> str:
        if not self._connected or self._gateway is None:
            raise ConnectionError("CTP gateway not connected")
        return self._gateway.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        if not self._connected or self._gateway is None:
            raise ConnectionError("CTP gateway not connected")
        self._gateway.cancel_order(req)

    def subscribe(self, requests: list[SubscribeRequest]) -> None:
        for request in requests:
            self._subscriptions[self._subscription_key(request)] = request
            if self._connected and self._gateway is not None:
                self._gateway.subscribe(request)

    def get_gateway(self):
        if self._gateway is None:
            raise RuntimeError("CTP gateway is not initialized")
        return self._gateway

    def _prepare_vnpy_runtime(self) -> Path:
        runtime_temp = Path.cwd() / ".vntrader_runtime"
        runtime_temp.mkdir(parents=True, exist_ok=True)
        trader_utility.TEMP_DIR = runtime_temp
        trader_utility.TRADER_DIR = Path.cwd()
        try:
            import vnpy_ctp.gateway.ctp_gateway as ctp_gateway_module
        except ModuleNotFoundError:
            ctp_gateway_module = None
        if ctp_gateway_module is not None:
            ctp_gateway_module.get_folder_path = trader_utility.get_folder_path

        flow_root = trader_utility.get_folder_path("ctp")
        flow_root.joinpath("Td").mkdir(parents=True, exist_ok=True)
        flow_root.joinpath("Md").mkdir(parents=True, exist_ok=True)
        logger.info("Prepared vnpy runtime path: %s", flow_root)
        return flow_root

    def _on_log(self, event) -> None:
        log = event.data
        msg = getattr(log, "msg", str(log))
        logger.info("[CTP] %s", msg)

        lowered = msg.lower()
        if "认证码错误" in msg and "豁免终端认证" in msg:
            self._auth_warning = msg
            self._schedule_task(self._write_connection_status("AUTH_WARNING", detail=msg))
            return

        if any(token in msg for token in ("不合法的登录", "登录失败", "认证失败")):
            self._login_error = msg
            self._signal(self._login_event)
            return

        if "shake hand err" in lowered or "decode err" in lowered:
            self._login_error = msg
            self._signal(self._login_event)
            return

        if "连接断开" in msg or "已断开" in msg:
            if self._connected:
                self._connected = False
                self._login_event.clear()
                self._schedule_task(self._write_connection_status("DISCONNECTED", detail=msg))
            return

    def _on_account(self, event) -> None:
        account: AccountData = event.data
        self._last_account = account
        self._signal(self._account_event)

        if not self._connected:
            self._connected = True
            self._signal(self._login_event)
            detail = self._auth_warning or self._describe_fronts()
            self._schedule_task(self._write_connection_status("CONNECTED", detail=detail))
            self._resubscribe_all()

        for callback in self._account_listeners:
            self._dispatch_listener(callback, account)

        self._schedule_task(self._write_account_snapshot(account))

    def _on_position(self, event) -> None:
        position: PositionData = event.data
        self._positions[position.vt_positionid] = position
        if self._loop is not None:
            self._last_position_monotonic = self._loop.time()
        self._signal(self._position_event)

        for callback in self._position_listeners:
            self._dispatch_listener(callback, position)

    def _on_order(self, event) -> None:
        order: OrderData = event.data
        for callback in self._order_listeners:
            self._dispatch_listener(callback, order)

    def _on_trade(self, event) -> None:
        trade: TradeData = event.data
        for callback in self._trade_listeners:
            self._dispatch_listener(callback, trade)

    def _on_tick(self, event) -> None:
        tick: TickData = event.data
        for callback in self._tick_listeners:
            self._dispatch_listener(callback, tick)

    async def _reconnect_loop(self) -> None:
        interval = 1.0
        max_interval = 60.0

        while self._should_reconnect:
            await asyncio.sleep(interval)
            if self._connected or self._gateway is None:
                interval = 1.0
                continue

            await self._write_connection_status("RECONNECTING", detail=f"retry in {interval:.0f}s")
            self._login_error = None
            self._login_event.clear()
            self._prepare_vnpy_runtime()
            self._gateway.connect(dict(self._setting))

            try:
                await asyncio.wait_for(self._login_event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                interval = min(interval * 2, max_interval)
                continue

            if self._login_error:
                await self._write_connection_status("LOGIN_FAILED", detail=self._login_error)
                interval = min(interval * 2, max_interval)
                continue

            interval = 1.0

    def _resubscribe_all(self) -> None:
        if not self._connected or self._gateway is None:
            return
        for request in self._subscriptions.values():
            self._gateway.subscribe(request)

    def _dispatch_listener(self, callback: Callable[[Any], Any], data: Any) -> None:
        def invoke() -> None:
            result = callback(data)
            if inspect.isawaitable(result):
                asyncio.create_task(result)

        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(invoke)

    def _signal(self, event: asyncio.Event) -> None:
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(event.set)

    def _schedule_task(self, coro: Any) -> None:
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(asyncio.create_task, coro)
        elif inspect.iscoroutine(coro):
            coro.close()

    def _describe_fronts(self) -> str:
        auth_status = "ON" if getattr(self, "auth_enabled", True) else "OFF"
        return f"td={self.td_front_addr}; md={self.md_front_addr}; env={self.counter_env}; auth={auth_status}"

    async def _write_connection_status(
        self,
        status: str,
        detail: str | None = None,
        session_id: str = "",
    ) -> None:
        if self._state_writer is None:
            return
        await self._state_writer.write_connection_log(
            status=status,
            front_addr=self.td_front_addr,
            session_id=session_id,
            detail=detail or "",
        )

    async def _write_account_snapshot(self, account: AccountData) -> None:
        if self._state_writer is None:
            return
        snapshot = account_to_snapshot(account, self.user_id, self.broker_id)
        await self._state_writer.write_account_info(**snapshot)

    @staticmethod
    def _subscription_key(request: SubscribeRequest) -> str:
        return f"{request.symbol}.{request.exchange.value}"
