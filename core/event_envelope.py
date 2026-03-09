# core/event_envelope.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Mapping, TypeVar
from uuid import uuid4


_T = TypeVar("_T")


def _is_non_empty_str(x: object) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _require_type(name: str, value: object, typ: type[_T]) -> _T:
    if not isinstance(value, typ):
        raise ValueError(f"{name} must be of type {typ.__name__}, got {type(value).__name__}")
    return value


def _require_mapping_str_any(name: str, value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping/dict, got {type(value).__name__}")
    # Ensure keys are strings (practical invariant for JSON-like payloads)
    for k in value.keys():
        if not isinstance(k, str):
            raise ValueError(f"{name} must have str keys, got key type {type(k).__name__}")
    return value  # type: ignore[return-value]


def _parse_iso_datetime(name: str, value: object) -> datetime:
    s = _require_type(name, value, str)
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"{name} must be an ISO8601 datetime string, got {s!r}") from e
    return dt


def _is_tz_aware(dt: datetime) -> bool:
    # tzinfo is present AND utcoffset is not None
    return dt.tzinfo is not None and dt.utcoffset() is not None


class EventType:
    """String constants for the event_type field of EventEnvelope.

    Using a plain class (not Enum) keeps backward compatibility: existing
    code that passes raw string literals continues to work unchanged.
    """

    MARKET_TICK = "market_tick"
    TA_SIGNAL = "ta_signal"
    TRADE_INTENT = "trade_intent"
    ORDER_UPDATE = "order_update"
    ACCOUNT_UPDATE = "account_update"


@dataclass(frozen=False, slots=True)
class EventEnvelope:
    """
    Standard event wrapper carrying metadata + payload for event-driven trading systems.

    Fields are all required:
    - event_id: UUID v4 string
    - event_type: e.g. "MarketData" | "TASignal" | "TradeIntent"
    - stream_id: f"{event_type}:{symbol}"
    - stream_seq: per-stream monotonic sequence (>=0)
    - event_ts: event generation timestamp (source/exchange), tz-aware
    - recv_ts: system receive timestamp (local), tz-aware
    - payload: business data (dict)
    - idempotency_key: f"{stream_id}:{stream_seq}"
    """

    event_id: str
    event_type: str
    stream_id: str
    stream_seq: int
    event_ts: datetime
    recv_ts: datetime
    payload: Dict[str, Any]
    idempotency_key: str

    _REQUIRED_FIELDS: ClassVar[tuple[str, ...]] = (
        "event_id",
        "event_type",
        "stream_id",
        "stream_seq",
        "event_ts",
        "recv_ts",
        "payload",
        "idempotency_key",
    )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert envelope to a JSON-serializable dictionary.

        - datetime fields are serialized via ISO8601 (datetime.isoformat()).
        """
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "stream_id": self.stream_id,
            "stream_seq": self.stream_seq,
            "event_ts": self.event_ts.isoformat(),
            "recv_ts": self.recv_ts.isoformat(),
            "payload": self.payload,
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EventEnvelope":
        """
        Build an EventEnvelope from a dict.

        - Parses event_ts/recv_ts from ISO8601 strings using datetime.fromisoformat().
        - Raises ValueError if any required field is missing or has wrong type.
        """
        if not isinstance(d, dict):
            raise ValueError(f"input must be a dict, got {type(d).__name__}")

        for k in cls._REQUIRED_FIELDS:
            if k not in d:
                raise ValueError(f"missing required field: {k}")

        event_id = _require_type("event_id", d["event_id"], str)
        event_type = _require_type("event_type", d["event_type"], str)
        stream_id = _require_type("stream_id", d["stream_id"], str)

        stream_seq_obj = d["stream_seq"]
        if not isinstance(stream_seq_obj, int):
            raise ValueError(f"stream_seq must be int, got {type(stream_seq_obj).__name__}")
        stream_seq = stream_seq_obj

        event_ts = _parse_iso_datetime("event_ts", d["event_ts"])
        recv_ts = _parse_iso_datetime("recv_ts", d["recv_ts"])

        payload_map = _require_mapping_str_any("payload", d["payload"])
        payload: Dict[str, Any] = dict(payload_map)

        idempotency_key = _require_type("idempotency_key", d["idempotency_key"], str)

        env = cls(
            event_id=event_id,
            event_type=event_type,
            stream_id=stream_id,
            stream_seq=stream_seq,
            event_ts=event_ts,
            recv_ts=recv_ts,
            payload=payload,
            idempotency_key=idempotency_key,
        )
        env.validate()
        return env

    def validate(self) -> None:
        """
        Validate envelope invariants.

        Checks:
        - event_id is a non-empty string
        - stream_seq >= 0
        - event_ts and recv_ts are tz-aware datetime
        - payload is a dict
        Raises:
        - ValueError with a specific reason if invalid.
        """
        if not _is_non_empty_str(self.event_id):
            raise ValueError("event_id must be a non-empty string")

        if not isinstance(self.stream_seq, int):
            raise ValueError(f"stream_seq must be int, got {type(self.stream_seq).__name__}")
        if self.stream_seq < 0:
            raise ValueError("stream_seq must be >= 0")

        if not isinstance(self.event_ts, datetime):
            raise ValueError(f"event_ts must be datetime, got {type(self.event_ts).__name__}")
        if not _is_tz_aware(self.event_ts):
            raise ValueError("event_ts must be timezone-aware (tzinfo required)")

        if not isinstance(self.recv_ts, datetime):
            raise ValueError(f"recv_ts must be datetime, got {type(self.recv_ts).__name__}")
        if not _is_tz_aware(self.recv_ts):
            raise ValueError("recv_ts must be timezone-aware (tzinfo required)")

        if not isinstance(self.payload, dict):
            raise ValueError(f"payload must be a dict, got {type(self.payload).__name__}")

    @classmethod
    def make(
        cls,
        event_type: str,
        symbol: str,
        payload: Dict[str, Any],
        *,
        stream_seq: int = 0,
        event_ts: datetime | None = None,
    ) -> "EventEnvelope":
        """
        Factory method to create a valid envelope with sensible defaults.

        - event_id: uuid4()
        - stream_id: f"{event_type}:{symbol}"
        - idempotency_key: f"{stream_id}:{stream_seq}"
        - recv_ts: datetime.now(timezone.utc)
        - event_ts: defaults to recv_ts (can be overridden via argument)
        - stream_seq: defaults to 0 (callers can override)
        """
        if not _is_non_empty_str(event_type):
            raise ValueError("event_type must be a non-empty string")
        if not _is_non_empty_str(symbol):
            raise ValueError("symbol must be a non-empty string")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        if not isinstance(stream_seq, int):
            raise ValueError("stream_seq must be int")
        if stream_seq < 0:
            raise ValueError("stream_seq must be >= 0")

        stream_id = f"{event_type}:{symbol}"
        recv_ts = datetime.now(timezone.utc)
        final_event_ts = event_ts if event_ts is not None else recv_ts

        env = cls(
            event_id=str(uuid4()),
            event_type=event_type,
            stream_id=stream_id,
            stream_seq=stream_seq,
            event_ts=final_event_ts,
            recv_ts=recv_ts,
            payload=payload,
            idempotency_key=f"{stream_id}:{stream_seq}",
        )
        env.validate()
        return env
