from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from core.outbox_dispatcher import OutboxDispatcher
from core.state_writer import StateWriter
from core.venue_order_spec import VenueOrderStatus, VenueReceipt
from venue.mock_adapter import MockVenueAdapter

_SEMANTIC_CONFIG = {"current_time": datetime.fromisoformat("2026-03-09T10:00:00+08:00")}


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


def _make_order_spec(client_order_id: str = "client-atomicity") -> dict:
    return {
        "symbol": "rb2510",
        "side": "BUY",
        "order_type": "LIMIT",
        "quantity": "1",
        "price": "3500",
        "time_in_force": "GTC",
        "reduce_only": False,
        "post_only": False,
        "hedge_flag": "SPEC",
        "client_order_id": client_order_id,
        "venue": "ctp",
    }


def _insert_order(
    db_path: Path,
    order_id: str,
    client_order_id: str,
    *,
    status: str = "PENDING_SEND",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (order_id, client_order_id, "rb2510", "ctp", "BUY", 1.0, 3500.0, status, "strategy-1", now, now),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_outbox_order(
    db_path: Path,
    *,
    event_id: str,
    aggregate_id: str,
    payload: dict,
    idempotency_key: str,
    status: str = "NEW",
    retry_count: int = 0,
    max_retries: int = 3,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO outbox_orders (event_id, aggregate_id, event_type, payload, idempotency_key, status, retry_count, max_retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                aggregate_id,
                "OrderCreated",
                json.dumps(payload),
                idempotency_key,
                status,
                retry_count,
                max_retries,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _get_outbox_row(db_path: Path, event_id: str) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "SELECT status, sent_at, retry_count, error_message FROM outbox_orders WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        return {
            "status": row[0],
            "sent_at": row[1],
            "retry_count": row[2],
            "error_message": row[3],
        }
    finally:
        conn.close()


def _get_order_row(db_path: Path, order_id: str) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "SELECT status, filled_quantity, filled_price FROM orders WHERE order_id = ?",
            (order_id,),
        )
        row = cursor.fetchone()
        return {
            "status": row[0],
            "filled_quantity": row[1],
            "filled_price": row[2],
        }
    finally:
        conn.close()


def _get_system_events(db_path: Path, event_type: str) -> list[tuple[str, str]]:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "SELECT event_type, detail FROM system_log WHERE event_type = ? ORDER BY id",
            (event_type,),
        )
        return list(cursor.fetchall())
    finally:
        conn.close()


class TimeoutThenQueryableAdapter(MockVenueAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.timeout_client_ids: set[str] = set()

    async def submit_order(self, spec):
        if spec.client_order_id not in self.timeout_client_ids:
            self.timeout_client_ids.add(spec.client_order_id)
            self.call_count += 1
            self.submitted_orders.append(spec)
            self._spec_by_client_id[spec.client_order_id] = spec
            self._exchange_id_by_client_id[spec.client_order_id] = "MOCK-TIMEOUT-001"
            self._status_by_client_id[spec.client_order_id] = "FILLED"
            raise TimeoutError("submit_order timeout after venue accepted order")
        return await super().submit_order(spec)


class RecoveryOnlyAdapter(MockVenueAdapter):
    def seed_existing_fill(self, client_order_id: str) -> None:
        spec = type("SpecHolder", (), {})()
        spec.quantity = 1
        spec.price = 3500
        self._spec_by_client_id[client_order_id] = spec
        self._exchange_id_by_client_id[client_order_id] = "MOCK-RECOVERY-001"
        self._status_by_client_id[client_order_id] = "FILLED"

    async def submit_order(self, spec):
        raise AssertionError("recovered order should not be resubmitted")


@pytest_asyncio.fixture()
async def db_and_writer(tmp_path: Path):
    db_path = tmp_path / "outbox-atomicity.sqlite"
    _init_sqlite_schema(db_path)
    writer = StateWriter(str(db_path))
    await writer.start()
    try:
        yield db_path, writer
    finally:
        await writer.stop()


class TestOutboxAtomicity:
    def test_duplicate_trade_intent(self, db_and_writer) -> None:
        db_path, _ = db_and_writer
        _insert_order(db_path, "order-dup-1", "client-dup-1")
        _insert_outbox_order(
            db_path,
            event_id="evt-dup-1",
            aggregate_id="order-dup-1",
            payload=_make_order_spec("client-dup-1"),
            idempotency_key="intent-rb2510-dup",
        )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_outbox_order(
                db_path,
                event_id="evt-dup-2",
                aggregate_id="order-dup-1",
                payload=_make_order_spec("client-dup-1"),
                idempotency_key="intent-rb2510-dup",
            )

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute("SELECT COUNT(*) FROM outbox_orders WHERE idempotency_key = ?", ("intent-rb2510-dup",)).fetchone()
            assert row[0] == 1
        finally:
            conn.close()

    @pytest.mark.asyncio
    async def test_crash_after_outbox_write_before_send(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order(db_path, "order-crash-1", "client-crash-1")
        _insert_outbox_order(
            db_path,
            event_id="evt-crash-1",
            aggregate_id="order-crash-1",
            payload=_make_order_spec("client-crash-1"),
            idempotency_key="intent-crash-before-send",
        )

        adapter = MockVenueAdapter()
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(0.4)
        await dispatcher.stop()

        outbox_row = _get_outbox_row(db_path, "evt-crash-1")
        order_row = _get_order_row(db_path, "order-crash-1")

        assert outbox_row["status"] == "CONFIRMED"
        assert order_row["status"] == "SENT"
        assert adapter.call_count == 1

    @pytest.mark.asyncio
    async def test_crash_after_send_before_confirm(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order(db_path, "order-crash-2", "client-crash-2")
        _insert_outbox_order(
            db_path,
            event_id="evt-crash-2",
            aggregate_id="order-crash-2",
            payload=_make_order_spec("client-crash-2"),
            idempotency_key="intent-crash-after-send",
            retry_count=1,
        )

        adapter = RecoveryOnlyAdapter()
        adapter.seed_existing_fill("client-crash-2")

        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(0.4)
        await dispatcher.stop()

        outbox_row = _get_outbox_row(db_path, "evt-crash-2")
        order_row = _get_order_row(db_path, "order-crash-2")

        assert outbox_row["status"] == "CONFIRMED"
        assert order_row["status"] == "FILLED"
        assert order_row["filled_quantity"] == 1.0
        assert adapter.call_count == 0

    @pytest.mark.asyncio
    async def test_submit_timeout_reconciles_without_duplicate_order(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order(db_path, "order-timeout-1", "client-timeout-1")
        _insert_outbox_order(
            db_path,
            event_id="evt-timeout-1",
            aggregate_id="order-timeout-1",
            payload=_make_order_spec("client-timeout-1"),
            idempotency_key="intent-timeout-query-reconcile",
        )

        adapter = TimeoutThenQueryableAdapter()
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            backoff_base=0.05,
            max_retries=3,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(0.5)
        await dispatcher.stop()

        outbox_row = _get_outbox_row(db_path, "evt-timeout-1")
        order_row = _get_order_row(db_path, "order-timeout-1")

        assert outbox_row["status"] == "CONFIRMED"
        assert order_row["status"] == "FILLED"
        assert adapter.call_count == 1
        assert _get_system_events(db_path, "OUTBOX_RECOVERED_CONFIRMATION")

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order(db_path, "order-fail-1", "client-fail-1")
        _insert_outbox_order(
            db_path,
            event_id="evt-fail-1",
            aggregate_id="order-fail-1",
            payload=_make_order_spec("client-fail-1"),
            idempotency_key="intent-fail-max-retries",
            max_retries=2,
        )

        adapter = MockVenueAdapter(should_fail=True)
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            backoff_base=0.05,
            max_retries=2,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(0.5)
        await dispatcher.stop()

        outbox_row = _get_outbox_row(db_path, "evt-fail-1")
        order_row = _get_order_row(db_path, "order-fail-1")

        assert outbox_row["status"] == "FAILED"
        assert "mock exchange down" in outbox_row["error_message"]
        assert order_row["status"] == "FAILED"
        assert adapter.call_count == 2
        assert _get_system_events(db_path, "OUTBOX_MAX_RETRIES_EXCEEDED")
