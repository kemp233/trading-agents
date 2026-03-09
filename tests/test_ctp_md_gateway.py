from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from vnpy.trader.constant import Exchange
from vnpy.trader.object import TickData

from core.market_event import MarketTickEvent
from venue.ctp_md_gateway import CtpMdGateway


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


def test_subscribe_uses_shared_wrapper() -> None:
    wrapper = MagicMock()
    wrapper.is_connected = True
    wrapper.register_tick_listener = MagicMock()
    wrapper.subscribe = MagicMock()

    gw = CtpMdGateway(CONFIG, gateway_wrapper=wrapper)
    gw.subscribe(["rb2510", "ag2510"])

    requests = wrapper.subscribe.call_args[0][0]
    assert len(requests) == 2
    assert requests[0].symbol == "rb2510"
    assert requests[0].exchange == Exchange.SHFE


def test_on_tick_converts_vnpy_tick() -> None:
    received: list[MarketTickEvent] = []
    wrapper = MagicMock()
    wrapper.is_connected = True
    wrapper.register_tick_listener = MagicMock()

    gw = CtpMdGateway(CONFIG, gateway_wrapper=wrapper, on_tick=received.append)
    tick = TickData(
        gateway_name="CTP",
        symbol="rb2510",
        exchange=Exchange.SHFE,
        datetime=datetime(2026, 3, 6, 9, 1, tzinfo=timezone.utc),
        last_price=3550,
        open_price=3500,
        high_price=3560,
        low_price=3490,
        volume=100,
        bid_price_1=3549,
        bid_volume_1=8,
        ask_price_1=3551,
        ask_volume_1=9,
        limit_up=3800,
        limit_down=3300,
        open_interest=1200,
    )

    gw._on_tick(tick)

    assert len(received) == 1
    assert received[0].symbol == "rb2510"
    assert received[0].last_price == Decimal("3550.0")
    assert received[0].timestamp.tzinfo == timezone.utc

