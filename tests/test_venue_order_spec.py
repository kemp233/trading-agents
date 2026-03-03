# tests/test_venue_order_spec.py
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

# Allow flexible import paths depending on repo layout.
try:
    from core.venue_order_spec import VenueOrderSpec, VenueReceipt, VenuePosition
except ModuleNotFoundError:  # pragma: no cover
    from venue_order_spec import VenueOrderSpec, VenueReceipt, VenuePosition


def test_venue_order_spec_to_dict():
    """
    Create a BTCUSDT LIMIT BUY order, call to_dict(),
    verify Decimal is serialized as str.

    Note: VenueOrderSpec has no datetime fields in current implementation.
    """
    spec = VenueOrderSpec(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("50000.1"),
        time_in_force="GTC",
        reduce_only=False,
        post_only=False,
        hedge_flag="SPEC",
        client_order_id="cid-001",
        venue="binance",
    )

    d = spec.to_dict()

    # Decimal fields -> str
    assert d["quantity"] == "0.001"
    assert isinstance(d["quantity"], str)

    assert d["price"] == "50000.1"
    assert isinstance(d["price"], str)

    # No datetime objects should exist in dict
    assert not any(isinstance(v, datetime) for v in d.values())


def test_venue_order_spec_from_dict_roundtrip():
    spec = VenueOrderSpec(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("50000.1"),
        time_in_force="IOC",
        reduce_only=True,
        post_only=True,
        hedge_flag="SPEC",
        client_order_id="cid-rt",
        venue="binance",
    )

    d = spec.to_dict()
    spec2 = VenueOrderSpec.from_dict(d)

    # dataclass equality should work field-by-field
    assert spec2 == spec


def test_venue_order_spec_missing_required_field():
    d = {
        # "symbol" missing
        "side": "BUY",
        "order_type": "LIMIT",
        "quantity": "0.001",
        "price": "50000.1",
        "time_in_force": "GTC",
        "reduce_only": False,
        "post_only": False,
        "hedge_flag": "SPEC",
        "client_order_id": "cid-miss",
        "venue": "binance",
    }

    with pytest.raises(ValueError):
        VenueOrderSpec.from_dict(d)


def test_venue_receipt_to_dict():
    ts = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)

    receipt = VenueReceipt(
        client_order_id="cid-rcpt",
        exchange_order_id="ex-123",
        status="SENT",
        raw_response={"ok": True, "foo": "bar"},
        timestamp=ts,
    )

    d = receipt.to_dict()

    assert d["client_order_id"] == "cid-rcpt"
    assert d["exchange_order_id"] == "ex-123"
    assert d["status"] == "SENT"
    assert d["raw_response"] == {"ok": True, "foo": "bar"}

    # datetime -> ISO8601 str (must include timezone offset)
    assert d["timestamp"] == ts.isoformat()
    assert isinstance(d["timestamp"], str)
    assert ("+" in d["timestamp"]) or (d["timestamp"].endswith("Z"))


def test_venue_position_from_dict_invalid_datetime():
    # naive ISO8601 (no tz offset) should raise ValueError
    d = {
        "symbol": "BTCUSDT",
        "venue": "binance",
        "side": "LONG",
        "quantity": "1",
        "entry_price": "50000",
        "unrealized_pnl": "12.5",
        "updated_at": "2026-03-03T12:00:00",  # naive
    }

    with pytest.raises(ValueError):
        VenuePosition.from_dict(d)


def test_venue_order_spec_market_order_price_none():
    spec = VenueOrderSpec(
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        time_in_force="GTC",
        reduce_only=False,
        post_only=False,
        hedge_flag="SPEC",
        client_order_id="cid-mkt",
        venue="binance",
    )

    d = spec.to_dict()
    assert d["price"] is None

    # Roundtrip should not raise and should preserve None
    spec2 = VenueOrderSpec.from_dict(d)
    assert spec2.price is None
    assert spec2 == spec