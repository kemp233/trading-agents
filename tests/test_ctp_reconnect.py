from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from vnpy.trader.object import SubscribeRequest
from vnpy.trader.constant import Exchange

from venue.ctp_gateway import CtpGatewayWrapper


CONFIG = {
    "broker_id": "9999",
    "user_id": "test_user",
    "password": "test_pass",
    "app_id": "simnow_client_test",
    "auth_code": "0000000000000000",
    "ctp_td_front_addr": "tcp://182.254.243.31:40001",
    "ctp_md_front_addr": "tcp://182.254.243.31:40011",
    "ctp_counter_env": "实盘",
}


def test_disconnection_log_clears_connected_flag() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    wrapper._connected = True
    wrapper._on_log(SimpleNamespace(data=SimpleNamespace(msg="交易服务器连接断开")))
    assert wrapper.is_connected is False


@pytest.mark.asyncio
async def test_reconnect_loop_retries_and_resets_interval() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    wrapper._gateway = MagicMock()
    wrapper._connected = False
    wrapper._should_reconnect = True
    wrapper._loop = asyncio.get_running_loop()
    wrapper._write_connection_status = AsyncMock()

    attempts = {"count": 0}

    async def fake_wait_for(awaitable, timeout):  # noqa: ARG001
        if hasattr(awaitable, "close"):
            awaitable.close()
        attempts["count"] += 1
        wrapper._connected = True
        wrapper._login_event.set()

    async def fake_sleep(_delay: float) -> None:
        wrapper._should_reconnect = False

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(asyncio, "wait_for", fake_wait_for)
        mp.setattr(asyncio, "sleep", fake_sleep)
        await wrapper._reconnect_loop()

    assert attempts["count"] == 1
    assert wrapper.is_connected is True


def test_subscribe_caches_requests_for_resubscribe() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    wrapper._gateway = MagicMock()
    wrapper._connected = True
    request = SubscribeRequest(symbol="rb2510", exchange=Exchange.SHFE)

    wrapper.subscribe([request])
    wrapper._connected = True
    wrapper._resubscribe_all()

    assert wrapper._gateway.subscribe.call_count >= 2

