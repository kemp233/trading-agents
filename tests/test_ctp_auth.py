from __future__ import annotations

import asyncio
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from vnpy.trader.object import AccountData

from venue.ctp_gateway import CtpGatewayWrapper
from venue.ctp_utils import build_vnpy_setting


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


class _FakeEventEngine:
    def register(self, *_args, **_kwargs) -> None:
        return None

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None


def _fake_modules(gw_instance: MagicMock) -> dict:
    fake_cls = MagicMock(return_value=gw_instance)
    fake_vnpy_ctp = ModuleType("vnpy_ctp")
    fake_gateway = ModuleType("vnpy_ctp.gateway")
    fake_gateway.CtpGateway = fake_cls  # type: ignore[attr-defined]
    fake_vnpy_ctp.gateway = fake_gateway  # type: ignore[attr-defined]

    fake_vnpy = ModuleType("vnpy")
    fake_event = ModuleType("vnpy.event")
    fake_event.EventEngine = _FakeEventEngine  # type: ignore[attr-defined]
    fake_vnpy.event = fake_event  # type: ignore[attr-defined]

    return {
        "vnpy": fake_vnpy,
        "vnpy.event": fake_event,
        "vnpy_ctp": fake_vnpy_ctp,
        "vnpy_ctp.gateway": fake_gateway,
    }


def test_build_vnpy_setting_includes_counter_env() -> None:
    setting = build_vnpy_setting(CONFIG)
    assert setting["用户名"] == "test_user"
    assert setting["交易服务器"] == "tcp://182.254.243.31:40001"
    assert setting["行情服务器"] == "tcp://182.254.243.31:40011"
    assert setting["柜台环境"] == "实盘"


@pytest.mark.asyncio
async def test_connect_timeout_raises_timeout_error() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    gateway = MagicMock()
    gateway.connect = MagicMock()

    async def fake_wait_for(awaitable, timeout):  # noqa: ARG001
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError

    with patch.dict(sys.modules, _fake_modules(gateway)):
        with patch("asyncio.wait_for", side_effect=fake_wait_for):
            with pytest.raises(TimeoutError, match="CTP login timeout"):
                await wrapper.connect()


def test_auth_warning_log_does_not_set_login_error() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    event = SimpleNamespace(data=SimpleNamespace(msg="CTP:认证码错误，当前系统或者用户豁免终端认证，可以登录"))
    wrapper._on_log(event)
    assert wrapper._auth_warning is not None
    assert wrapper._login_error is None


def test_login_failure_log_sets_login_error() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    event = SimpleNamespace(data=SimpleNamespace(msg="CTP:不合法的登录"))
    wrapper._on_log(event)
    assert wrapper._login_error == "CTP:不合法的登录"


@pytest.mark.asyncio
async def test_account_event_marks_connected() -> None:
    wrapper = CtpGatewayWrapper(CONFIG)
    wrapper._loop = asyncio.get_running_loop()
    account = AccountData(gateway_name="CTP", accountid="test_user", balance=1000, frozen=200)

    wrapper._on_account(SimpleNamespace(data=account))
    await asyncio.sleep(0)

    assert wrapper.is_connected is True
    assert wrapper._login_event.is_set() is True
    assert wrapper._account_event.is_set() is True

