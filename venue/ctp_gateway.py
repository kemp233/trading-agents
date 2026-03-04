from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# vnpy 事件类型常量
EVENT_LOG     = "eLog"
EVENT_ACCOUNT = "eAccount."


class CtpGatewayWrapper:
    """封装 vnpy_ctp.CtpGateway，管理连接生命周期。"""

    def __init__(self, config: dict) -> None:
        self.broker_id: str  = config["broker_id"]
        self.user_id: str    = config.get("user_id")   or os.getenv("CTP_USER_ID", "")
        self.password: str   = config.get("password")  or os.getenv("CTP_PASSWORD", "")
        self.app_id: str     = config["app_id"]
        self.auth_code: str  = config.get("auth_code") or os.getenv("CTP_AUTH_CODE", "")
        # 兼容 front_addr 与 ctp_front_addr 两种 key
        self.front_addr: str = config.get("front_addr") or config.get("ctp_front_addr", "")

        self._event_engine  = None   # vnpy EventEngine
        self._gateway       = None   # vnpy CtpGateway
        self._connected: bool               = False
        self._login_event: asyncio.Event    = asyncio.Event()
        self._login_error: Optional[str]    = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._reconnect_task: Optional[asyncio.Task]    = None
        self._should_reconnect: bool = True

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── vnpy 要求的中文设置字典 ───────────────────────────────────────
    def _build_setting(self) -> dict:
        return {
            "用户名":   self.user_id,
            "密码":     self.password,
            "经纪商代码": self.broker_id,
            "交易服务器": self.front_addr,
            "行情服务器": self.front_addr,   # SimNow TD/MD 同地址
            "产品名称": self.app_id,
            "授权编码": self.auth_code,
            "产品信息": "",
        }

    # ── 事件回调（运行在 EventEngine 线程，需 call_soon_threadsafe）──
    def _on_log(self, event) -> None:
        log = event.data
        msg: str = getattr(log, "msg", str(log))
        logger.debug(f"[CTP] {msg}")

        if self._login_event.is_set():
            return

        # 登录成功关键词（vnpy_ctp 源码固定输出）
        if "交易服务器登录成功" in msg:
            self._connected = True
            self._safe_set_event()
        # 登录失败关键词
        elif any(k in msg for k in ["登录失败", "密码错误", "AuthCode", "认证失败", "ErrorID"]):
            self._login_error = f"CTP 登录失败: {msg}"
            self._safe_set_event()

    def _on_account(self, event) -> None:
        """收到账户信息表示登录成功。"""
        if not self._connected:
            self._connected = True
        if not self._login_event.is_set():
            self._safe_set_event()

    def _safe_set_event(self) -> None:
        """线程安全地唤醒 asyncio.Event。"""
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._login_event.set)

    # ── 公共接口 ────────────────────────────────────────────────────────────
    async def connect(self) -> None:
        if self._connected:
            logger.info("CTP 已连接")
            return

        from vnpy.event import EventEngine
        from vnpy_ctp.gateway import CtpGateway

        logger.info(
            f"[CTP] 连接 broker={self.broker_id} user={self.user_id} addr={self.front_addr}"
        )

        self._loop = asyncio.get_event_loop()
        self._login_event.clear()
        self._login_error = None
        self._connected   = False
        self._should_reconnect = True

        # 创建 EventEngine 并启动
        self._event_engine = EventEngine()
        self._event_engine.register(EVENT_LOG,     self._on_log)
        self._event_engine.register(EVENT_ACCOUNT, self._on_account)
        self._event_engine.start()

        # 创建并连接 CtpGateway
        self._gateway = CtpGateway(self._event_engine, "CTP")
        self._gateway.connect(self._build_setting())

        # 等待登录完成（最多 30 秒）
        try:
            await asyncio.wait_for(self._login_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            raise TimeoutError(
                "CTP 登录超时（>30s）——请检查网络或前置地址是否可达"
            )

        if self._login_error:
            raise ConnectionError(self._login_error)

        logger.info("[CTP] 登录成功")

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

        if self._gateway:
            try:
                self._gateway.close()
            except Exception as e:
                logger.warning(f"[CTP] 关闭网关异常: {e}")

        if self._event_engine:
            try:
                self._event_engine.stop()
            except Exception as e:
                logger.warning(f"[CTP] 停止EventEngine异常: {e}")

        self._connected = False
        self._login_event.clear()
        logger.info("[CTP] 已断开连接")

    async def _reconnect_loop(self) -> None:
        interval = 1.0
        max_interval = 60.0

        while self._should_reconnect:
            await asyncio.sleep(interval)
            if self._connected or not self._should_reconnect:
                interval = 1.0
                continue

            logger.info(f"[CTP] 尝试重连 (interval={interval:.0f}s)")
            try:
                self._login_event.clear()
                self._login_error = None
                if self._gateway:
                    self._gateway.connect(self._build_setting())
                    try:
                        await asyncio.wait_for(self._login_event.wait(), timeout=30.0)
                        if self._login_error:
                            logger.warning(f"[CTP] 重连失败: {self._login_error}")
                            interval = min(interval * 2, max_interval)
                        else:
                            self._connected = True
                            interval = 1.0
                            logger.info("[CTP] 重连成功")
                    except asyncio.TimeoutError:
                        logger.warning("[CTP] 重连超时")
                        interval = min(interval * 2, max_interval)
            except Exception as e:
                logger.error(f"[CTP] 重连异常: {e}")
                interval = min(interval * 2, max_interval)

    def get_gateway(self):
        if self._gateway is None:
            raise RuntimeError("CTP 网关未初始化")
        return self._gateway
