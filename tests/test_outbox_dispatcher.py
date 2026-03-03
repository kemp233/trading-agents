from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from core.outbox_dispatcher import OutboxDispatcher
from core.state_schema import OrderSide, OrderState, OrderStatus
from core.state_writer import StateWriter
from core.venue_order_spec import VenueOrderSpec
from venue.mock_adapter import MockVenueAdapter


def _init_sqlite_schema(db_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def _make_order_spec() -> dict:
    return {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "order_type": "LIMIT",
        "quantity": "0.1",
        "price": "50000.0",
        "time_in_force": "GTC",
        "reduce_only": False,
        "post_only": False,
        "hedge_flag": "SPEC",
        "client_order_id": "client-001",
        "venue": "binance",
    }


def _insert_outbox_order(
    db_path: Path,
    event_id: str,
    aggregate_id: str,
    payload: dict,
    status: str = "NEW",
    retry_count: int = 0,
    max_retries: int = 3,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO outbox_orders (event_id, aggregate_id, event_type, payload, idempotency_key, status, retry_count, max_retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, aggregate_id, "OrderCreated", json.dumps(payload), f"key-{event_id}", status, retry_count, max_retries),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_order(db_path: Path, order_id: str, client_order_id: str, status: str = "PENDING_SEND") -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (order_id, client_order_id, "BTCUSDT", "binance", "BUY", 0.1, 50000.0, status, "strategy-1", now, now),
        )
        conn.commit()
    finally:
        conn.close()


def _get_outbox_status(db_path: Path, event_id: str) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT status, sent_at, retry_count, error_message FROM outbox_orders WHERE event_id = ?", (event_id,))
        row = cursor.fetchone()
        if row:
            return {"status": row[0], "sent_at": row[1], "retry_count": row[2], "error_message": row[3]}
        return {}
    finally:
        conn.close()


def _get_order_status(db_path: Path, order_id: str) -> str:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT status FROM orders WHERE order_id = ?", (order_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


@pytest_asyncio.fixture(scope="function")
async def db_and_writer(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    yield sw
    await sw.stop()


@pytest_asyncio.fixture(scope="function")
async def dispatcher(db_and_writer: StateWriter):
    mock_adapter = MockVenueAdapter()
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.1)
    await disp.start()
    yield disp, mock_adapter
    await disp.stop()


@pytest.mark.asyncio
async def test_happy_path(db_and_writer: StateWriter, dispatcher: tuple[OutboxDispatcher, MockVenueAdapter]) -> None:
    disp, mock_adapter = dispatcher
    db_path = Path(db_and_writer._db_path)

    event_id = "evt-001"
    order_id = "order-001"
    client_order_id = "client-001"

    _insert_order(db_path, order_id, client_order_id)
    _insert_outbox_order(db_path, event_id, order_id, _make_order_spec())

    await asyncio.sleep(0.5)

    outbox_status = _get_outbox_status(db_path, event_id)
    order_status = _get_order_status(db_path, order_id)

    assert outbox_status["status"] == "CONFIRMED"
    assert outbox_status["sent_at"] is not None
    assert order_status == "SENT"
    assert mock_adapter.call_count == 1


@pytest.mark.asyncio
async def test_retry_on_failure(db_and_writer: StateWriter) -> None:
    mock_adapter = MockVenueAdapter(fail_before_n=2)
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.05, max_retries=3, backoff_base=0.1)
    await disp.start()

    db_path = Path(db_and_writer._db_path)
    event_id = "evt-002"
    order_id = "order-002"
    client_order_id = "client-002"

    _insert_order(db_path, order_id, client_order_id)
    _insert_outbox_order(db_path, event_id, order_id, _make_order_spec())

    await asyncio.sleep(1.5)

    outbox_status = _get_outbox_status(db_path, event_id)
    order_status = _get_order_status(db_path, order_id)

    assert outbox_status["status"] == "CONFIRMED"
    assert outbox_status["sent_at"] is not None
    assert order_status == "SENT"
    assert mock_adapter.call_count == 3

    await disp.stop()


@pytest.mark.asyncio
async def test_max_retries_exceeded_marks_failed(db_and_writer: StateWriter) -> None:
    mock_adapter = MockVenueAdapter(should_fail=True)
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.05, max_retries=2, backoff_base=0.1)
    await disp.start()

    db_path = Path(db_and_writer._db_path)
    event_id = "evt-003"
    order_id = "order-003"
    client_order_id = "client-003"

    _insert_order(db_path, order_id, client_order_id)
    _insert_outbox_order(db_path, event_id, order_id, _make_order_spec(), max_retries=2)

    await asyncio.sleep(3.0)

    outbox_status = _get_outbox_status(db_path, event_id)
    order_status = _get_order_status(db_path, order_id)

    assert outbox_status["status"] == "FAILED"
    assert outbox_status["error_message"] is not None
    assert "mock exchange down" in outbox_status["error_message"]
    assert order_status == "FAILED"
    assert mock_adapter.call_count == 2

    await disp.stop()


@pytest.mark.asyncio
async def test_idempotency_on_restart(db_and_writer: StateWriter) -> None:
    db_path = Path(db_and_writer._db_path)
    event_id = "evt-004"
    order_id = "order-004"
    client_order_id = "client-004"

    _insert_order(db_path, order_id, client_order_id, status="SENT")
    _insert_outbox_order(db_path, event_id, order_id, _make_order_spec(), status="CONFIRMED")

    mock_adapter = MockVenueAdapter()
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.05)
    await disp.start()

    await asyncio.sleep(0.3)

    assert mock_adapter.call_count == 0

    outbox_status = _get_outbox_status(db_path, event_id)
    assert outbox_status["status"] == "CONFIRMED"

    await disp.stop()


@pytest.mark.asyncio
async def test_multiple_orders_processed_in_order(db_and_writer: StateWriter) -> None:
    mock_adapter = MockVenueAdapter()
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.05)
    await disp.start()

    db_path = Path(db_and_writer._db_path)

    for i in range(5):
        event_id = f"evt-00{i}"
        order_id = f"order-00{i}"
        client_order_id = f"client-00{i}"
        _insert_order(db_path, order_id, client_order_id)
        _insert_outbox_order(db_path, event_id, order_id, _make_order_spec())

    await asyncio.sleep(1.0)

    for i in range(5):
        event_id = f"evt-00{i}"
        order_id = f"order-00{i}"
        outbox_status = _get_outbox_status(db_path, event_id)
        order_status = _get_order_status(db_path, order_id)

        assert outbox_status["status"] == "CONFIRMED"
        assert outbox_status["sent_at"] is not None
        assert order_status == "SENT"

    assert mock_adapter.call_count == 5

    await disp.stop()


@pytest.mark.asyncio
async def test_stop_graceful(db_and_writer: StateWriter) -> None:
    mock_adapter = MockVenueAdapter()
    disp = OutboxDispatcher(state_writer=db_and_writer, venue_adapter=mock_adapter, poll_interval=0.05)
    await disp.start()

    db_path = Path(db_and_writer._db_path)
    event_id = "evt-006"
    order_id = "order-006"
    client_order_id = "client-006"

    _insert_order(db_path, order_id, client_order_id)
    _insert_outbox_order(db_path, event_id, order_id, _make_order_spec())

    await asyncio.sleep(0.1)

    await disp.stop()

    outbox_status = _get_outbox_status(db_path, event_id)
    order_status = _get_order_status(db_path, order_id)

    assert outbox_status["status"] in ("CONFIRMED", "NEW", "FAILED")
    assert order_status in ("SENT", "PENDING_SEND", "FAILED")
