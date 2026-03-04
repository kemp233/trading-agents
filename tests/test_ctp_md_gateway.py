"""Tests for CTP Market Data (MdApi) gateway — Issue #18.

Covers:
- Connect flow: front connected -> login
- Login success / failure
- Connect timeout
- Subscribe after login
- Re-subscribe after reconnect
- MarketTickEvent.from_ctp(): normal data, invalid prices, timestamp parsing
"""
from __future__ import annotations

import asyncio
import sys
from datetime import timezone
from decimal import Decimal
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from core.market_event import MarketTickEvent
from venue.ctp_md_gateway import CtpMdGateway


CONFIG = {
    "broker_id": "9999",
    "user_id": "test_user",
    "password": "test_pass",
    "md_front_addr": "tcp://180.168.146.187:10131",
}


def _fake_vnpy_modules(gw_instance: MagicMock) -> dict:
    """Build minimal fake vnpy_ctp.gateway sys.modules entries.

    CtpGateway is imported *inside* connect() as a local import, so we
    inject into sys.modules rather than patching venue.ctp_md_gateway namespace.
    """
    fake_gw_cls = MagicMock(return_value=gw_instance)

    fake_vnpy = ModuleType("vnpy_ctp")
    fake_vnpy_gateway = ModuleType("vnpy_ctp.gateway")
    fake_vnpy_gateway.CtpGateway = fake_gw_cls  # type: ignore[attr-defined]
    fake_vnpy.gateway = fake_vnpy_gateway  # type: ignore[attr-defined]

    return {"vnpy_ctp": fake_vnpy, "vnpy_ctp.gateway": fake_vnpy_gateway}


class TestCtpMdGatewayConnect:
    """Tests for connection and login flow."""

    def _make_gateway(self) -> CtpMdGateway:
        return CtpMdGateway(CONFIG)

    def test_on_front_connected_calls_login(self) -> None:
        """_on_front_connected must call gateway.login with correct credentials."""
        gw = self._make_gateway()
        mock_ctp = MagicMock()
        gw._gateway = mock_ctp

        gw._on_front_connected()

        mock_ctp.login.assert_called_once()
        req = mock_ctp.login.call_args[0][0]
        assert req["BrokerID"] == "9999"
        assert req["UserID"] == "test_user"
        assert req["Password"] == "test_pass"

    def test_on_rsp_user_login_success_sets_event(self) -> None:
        """Successful login response must set _login_event."""
        gw = self._make_gateway()
        assert not gw._login_event.is_set()

        gw._on_rsp_user_login(
            data={"BrokerID": "9999"},
            error={"ErrorID": 0, "ErrorMsg": ""},
            reqid=1,
            last=True,
        )

        assert gw._login_event.is_set()

    def test_on_rsp_user_login_failure_does_not_set_event(self) -> None:
        """Failed login must NOT set _login_event."""
        gw = self._make_gateway()

        gw._on_rsp_user_login(
            data={},
            error={"ErrorID": 20, "ErrorMsg": "\u5bc6\u7801\u9519\u8bef"},
            reqid=1,
            last=True,
        )

        assert not gw._login_event.is_set()

    @pytest.mark.asyncio
    async def test_connect_timeout_raises_timeout_error(self) -> None:
        """connect() must raise TimeoutError when _login_event never fires."""
        gw = self._make_gateway()
        mock_ctp = MagicMock()
        mock_ctp.connect = MagicMock()

        fake_modules = _fake_vnpy_modules(mock_ctp)

        with patch.dict(sys.modules, fake_modules):
            with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
                with pytest.raises(TimeoutError, match="CTP MD login timeout"):
                    await gw.connect()


class TestCtpMdGatewaySubscribe:
    """Tests for subscription and re-subscription behaviour."""

    def _make_gateway(self) -> CtpMdGateway:
        return CtpMdGateway(CONFIG)

    def test_subscribe_after_login(self) -> None:
        """subscribe() must forward new symbols to the underlying CTP gateway."""
        gw = self._make_gateway()
        mock_ctp = MagicMock()
        gw._gateway = mock_ctp
        gw._connected = True

        gw.subscribe(["rb2510", "ag2512"])

        mock_ctp.subscribe.assert_called_once_with(["rb2510", "ag2512"])
        assert gw._subscribed_symbols == ["rb2510", "ag2512"]

    @pytest.mark.asyncio
    async def test_resubscribe_after_reconnect(self) -> None:
        """Reconnect loop must re-subscribe all saved symbols after successful login."""
        gw = self._make_gateway()
        mock_ctp = MagicMock()
        gw._gateway = mock_ctp
        gw._connected = False
        gw._should_reconnect = True
        gw._subscribed_symbols = ["rb2510", "ag2512"]

        iteration = 0

        async def controlled_sleep(_delay: float) -> None:
            nonlocal iteration
            iteration += 1
            if iteration >= 2:
                gw._should_reconnect = False

        async def instant_wait_for(coro: object, timeout: float) -> None:  # noqa: ARG001
            # Close the coroutine to avoid "coroutine never awaited" warning.
            if hasattr(coro, "close"):
                coro.close()  # type: ignore[union-attr]
            gw._connected = True

        with patch("asyncio.sleep", side_effect=controlled_sleep):
            with patch("asyncio.wait_for", side_effect=instant_wait_for):
                await gw._reconnect_loop()

        mock_ctp.subscribe.assert_called_with(["rb2510", "ag2512"])


class TestMarketTickEvent:
    """Tests for MarketTickEvent.from_ctp()."""

    def _normal_ctp_data(self) -> dict:
        return {
            "InstrumentID": "rb2510",
            "LastPrice": 3550.0,
            "OpenPrice": 3500.0,
            "HighestPrice": 3580.0,
            "LowestPrice": 3480.0,
            "Volume": 120000,
            "BidPrice1": 3548.0,
            "BidVolume1": 10,
            "AskPrice1": 3552.0,
            "AskVolume1": 8,
            "UpperLimitPrice": 3850.0,
            "LowerLimitPrice": 3250.0,
            "OpenInterest": 850000,
            "ActionDay": "20260304",
            "UpdateTime": "14:30:00",
            "UpdateMillisec": 500,
        }

    def test_from_ctp_normal_data(self) -> None:
        """Normal data dict must map to correct MarketTickEvent fields."""
        tick = MarketTickEvent.from_ctp(self._normal_ctp_data())

        assert tick.symbol == "rb2510"
        assert tick.last_price == Decimal("3550.0")
        assert tick.volume == 120000
        assert tick.bid_price_1 == Decimal("3548.0")
        assert tick.ask_volume_1 == 8
        assert tick.open_interest == 850000

    def test_from_ctp_invalid_price_replaced_with_zero(self) -> None:
        """CTP sentinel price (1.79e308) must be converted to Decimal('0')."""
        data = self._normal_ctp_data()
        data["HighestPrice"] = 1.7976931348623157e308
        data["LowestPrice"] = 1.7976931348623157e308

        tick = MarketTickEvent.from_ctp(data)

        assert tick.high_price == Decimal("0")
        assert tick.low_price == Decimal("0")
        # Valid fields must remain unaffected
        assert tick.last_price == Decimal("3550.0")

    def test_from_ctp_timestamp_parsed(self) -> None:
        """ActionDay + UpdateTime + UpdateMillisec must parse to correct UTC datetime."""
        tick = MarketTickEvent.from_ctp(self._normal_ctp_data())

        assert tick.timestamp.tzinfo == timezone.utc
        assert tick.timestamp.year == 2026
        assert tick.timestamp.month == 3
        assert tick.timestamp.day == 4
        assert tick.timestamp.hour == 14
        assert tick.timestamp.minute == 30
        assert tick.timestamp.second == 0
        assert tick.timestamp.microsecond == 500_000  # 500 ms
