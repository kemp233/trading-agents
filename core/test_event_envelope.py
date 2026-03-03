# tests/test_event_envelope.py
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.event_envelope import EventEnvelope


def test_make_creates_valid_envelope() -> None:
    # Purpose: make() should generate a valid envelope with correct derived fields.
    env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
    assert env.event_id != ""
    assert env.stream_id == "MarketData:BTCUSDT"
    assert env.idempotency_key == "MarketData:BTCUSDT:0"
    assert isinstance(env.recv_ts, datetime)
    assert env.recv_ts.tzinfo is not None and env.recv_ts.utcoffset() is not None


def test_to_dict_and_from_dict_roundtrip() -> None:
    # Purpose: to_dict() then from_dict() should preserve all fields exactly.
    env1 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
    d = env1.to_dict()
    env2 = EventEnvelope.from_dict(d)

    assert env2 == env1
    assert isinstance(env2.event_ts, datetime)
    assert isinstance(env2.recv_ts, datetime)


def test_to_dict_datetime_is_isoformat_string() -> None:
    # Purpose: to_dict() must serialize datetime fields as ISO8601 strings.
    env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
    d = env.to_dict()

    assert isinstance(d["event_ts"], str)
    assert isinstance(d["recv_ts"], str)

    # Should be parseable by datetime.fromisoformat()
    _ = datetime.fromisoformat(d["event_ts"])
    _ = datetime.fromisoformat(d["recv_ts"])


def test_validate_raises_on_negative_seq() -> None:
    # Purpose: validate() should reject negative stream_seq.
    now = datetime.now(timezone.utc)
    env = EventEnvelope(
        event_id="00000000-0000-4000-8000-000000000000",
        event_type="MarketData",
        stream_id="MarketData:BTCUSDT",
        stream_seq=-1,
        event_ts=now,
        recv_ts=now,
        payload={"price": 50000},
        idempotency_key="MarketData:BTCUSDT:-1",
    )
    with pytest.raises(ValueError):
        env.validate()


def test_validate_raises_on_empty_event_id() -> None:
    # Purpose: validate() should reject empty event_id.
    now = datetime.now(timezone.utc)
    env = EventEnvelope(
        event_id="",
        event_type="MarketData",
        stream_id="MarketData:BTCUSDT",
        stream_seq=0,
        event_ts=now,
        recv_ts=now,
        payload={"price": 50000},
        idempotency_key="MarketData:BTCUSDT:0",
    )
    with pytest.raises(ValueError):
        env.validate()


def test_from_dict_raises_on_missing_field() -> None:
    # Purpose: from_dict() should raise if any required field is missing.
    now = datetime.now(timezone.utc)
    d = {
        # "event_id" is intentionally missing
        "event_type": "MarketData",
        "stream_id": "MarketData:BTCUSDT",
        "stream_seq": 0,
        "event_ts": now.isoformat(),
        "recv_ts": now.isoformat(),
        "payload": {"price": 50000},
        "idempotency_key": "MarketData:BTCUSDT:0",
    }
    with pytest.raises(ValueError):
        _ = EventEnvelope.from_dict(d)