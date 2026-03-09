from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType, Status
from vnpy.trader.object import OrderData, PositionData

from core.venue_order_spec import VenueOrderSpec
from venue.ctp_adapter import CTPAdapter


@pytest.fixture
def ctp_config() -> dict:
    return {
        "broker_id": "9999",
        "user_id": "test_user",
        "password": "test_pass",
        "app_id": "simnow_client_test",
        "auth_code": "0000000000000000",
        "ctp_td_front_addr": "tcp://182.254.243.31:40001",
        "ctp_md_front_addr": "tcp://182.254.243.31:40011",
        "ctp_counter_env": "实盘",
    }


@pytest.fixture
def wrapper_mock() -> MagicMock:
    wrapper = MagicMock()
    wrapper.is_connected = True
    wrapper.register_order_listener = MagicMock()
    wrapper.register_trade_listener = MagicMock()
    wrapper.connect = AsyncMock()
    wrapper.disconnect = AsyncMock()
    wrapper.send_order = MagicMock(return_value="CTP-001")
    wrapper.cancel_order = MagicMock()
    wrapper.refresh_positions = AsyncMock(return_value=[])
    return wrapper


@pytest.fixture
def adapter(ctp_config, wrapper_mock):
    with patch("venue.ctp_adapter.CtpGatewayWrapper", return_value=wrapper_mock):
        yield CTPAdapter(ctp_config)


@pytest.mark.asyncio
async def test_submit_order_success(adapter, wrapper_mock) -> None:
    spec = VenueOrderSpec(
        symbol="rb2510",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("1"),
        price=Decimal("4000"),
        client_order_id="ord-1",
    )

    async def trigger() -> None:
        await asyncio.sleep(0.01)
        adapter._on_order_event(
            OrderData(
                gateway_name="CTP",
                symbol="rb2510",
                exchange=Exchange.SHFE,
                orderid="CTP-001",
                type=OrderType.LIMIT,
                direction=Direction.LONG,
                offset=Offset.OPEN,
                price=4000,
                volume=1,
                traded=0,
                status=Status.NOTTRADED,
                datetime=datetime.now(timezone.utc),
                reference="ord-1",
            )
        )

    asyncio.create_task(trigger())
    receipt = await adapter.submit_order(spec)

    assert receipt.client_order_id == "ord-1"
    assert receipt.exchange_order_id == "CTP-001"
    assert receipt.status == "SENT"
    assert wrapper_mock.send_order.called


@pytest.mark.asyncio
async def test_duplicate_order_is_rejected(adapter) -> None:
    spec = VenueOrderSpec(
        symbol="rb2510",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("1"),
        price=Decimal("4000"),
        client_order_id="dup-1",
    )
    adapter._submitted_orders.add("dup-1")

    receipt = await adapter.submit_order(spec)

    assert receipt.status == "REJECTED"
    assert receipt.raw_response["error"] == "Duplicate client_order_id"


@pytest.mark.asyncio
async def test_cancel_order_uses_cached_order(adapter, wrapper_mock) -> None:
    adapter._on_order_event(
        OrderData(
            gateway_name="CTP",
            symbol="rb2510",
            exchange=Exchange.SHFE,
            orderid="CTP-002",
            type=OrderType.LIMIT,
            direction=Direction.LONG,
            offset=Offset.OPEN,
            price=4000,
            volume=1,
            traded=0,
            status=Status.NOTTRADED,
            datetime=datetime.now(timezone.utc),
            reference="ord-2",
        )
    )

    async def trigger() -> None:
        await asyncio.sleep(0.01)
        adapter._on_order_event(
            OrderData(
                gateway_name="CTP",
                symbol="rb2510",
                exchange=Exchange.SHFE,
                orderid="CTP-002",
                type=OrderType.LIMIT,
                direction=Direction.LONG,
                offset=Offset.OPEN,
                price=4000,
                volume=1,
                traded=0,
                status=Status.CANCELLED,
                datetime=datetime.now(timezone.utc),
                reference="ord-2",
            )
        )

    asyncio.create_task(trigger())
    receipt = await adapter.cancel_order("ord-2")

    assert receipt.status == "CANCELED"
    assert wrapper_mock.cancel_order.called


@pytest.mark.asyncio
async def test_query_positions_maps_vnpy_objects(adapter, wrapper_mock) -> None:
    wrapper_mock.refresh_positions.return_value = [
        PositionData(
            gateway_name="CTP",
            symbol="rb2510",
            exchange=Exchange.SHFE,
            direction=Direction.LONG,
            volume=2,
            price=4010,
            pnl=150,
        )
    ]

    positions = await adapter.query_positions()

    assert len(positions) == 1
    assert positions[0].symbol == "rb2510"
    assert positions[0].side == "LONG"
    assert positions[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_query_order_uses_cached_status(adapter) -> None:
    adapter._on_order_event(
        OrderData(
            gateway_name="CTP",
            symbol="rb2510",
            exchange=Exchange.SHFE,
            orderid="CTP-003",
            type=OrderType.LIMIT,
            direction=Direction.SHORT,
            offset=Offset.CLOSE,
            price=3990,
            volume=1,
            traded=1,
            status=Status.ALLTRADED,
            datetime=datetime.now(timezone.utc),
            reference="ord-3",
        )
    )

    status = await adapter.query_order("ord-3")

    assert status.exchange_order_id == "CTP-003"
    assert status.status == "FILLED"
    assert status.filled_quantity == Decimal("1")


@pytest.mark.asyncio
async def test_submit_order_requires_known_symbol(adapter) -> None:
    spec = VenueOrderSpec(
        symbol="unknown999",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("1"),
        price=Decimal("1"),
        client_order_id="bad-symbol",
    )

    with pytest.raises(ValueError, match="Unknown exchange"):
        await adapter.submit_order(spec)

