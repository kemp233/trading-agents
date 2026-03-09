from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from vnpy.trader.object import AccountData

from venue.ctp_adapter import CTPAdapter, VenueAccountInfo


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


@pytest.mark.asyncio
async def test_query_account_maps_account_data() -> None:
    wrapper = MagicMock()
    wrapper.register_order_listener = MagicMock()
    wrapper.register_trade_listener = MagicMock()
    wrapper.refresh_account = AsyncMock(
        return_value=AccountData(gateway_name="CTP", accountid="test_user", balance=500000, frozen=120000)
    )

    with patch("venue.ctp_adapter.CtpGatewayWrapper", return_value=wrapper):
        adapter = CTPAdapter(CONFIG)
        result = await adapter.query_account()

    assert isinstance(result, VenueAccountInfo)
    assert result.account_id == "test_user"
    assert result.broker_id == "9999"
    assert result.balance == Decimal("500000")
    assert result.available == Decimal("380000")
    assert result.margin == Decimal("120000")


@pytest.mark.asyncio
async def test_query_account_propagates_connection_error() -> None:
    wrapper = MagicMock()
    wrapper.register_order_listener = MagicMock()
    wrapper.register_trade_listener = MagicMock()
    wrapper.refresh_account = AsyncMock(side_effect=ConnectionError("CTP gateway not connected"))

    with patch("venue.ctp_adapter.CtpGatewayWrapper", return_value=wrapper):
        adapter = CTPAdapter(CONFIG)
        with pytest.raises(ConnectionError, match="CTP gateway not connected"):
            await adapter.query_account()


@pytest.mark.asyncio
async def test_query_account_propagates_timeout() -> None:
    wrapper = MagicMock()
    wrapper.register_order_listener = MagicMock()
    wrapper.register_trade_listener = MagicMock()
    wrapper.refresh_account = AsyncMock(side_effect=TimeoutError("Query account timeout"))

    with patch("venue.ctp_adapter.CtpGatewayWrapper", return_value=wrapper):
        adapter = CTPAdapter(CONFIG)
        with pytest.raises(TimeoutError, match="Query account timeout"):
            await adapter.query_account()

