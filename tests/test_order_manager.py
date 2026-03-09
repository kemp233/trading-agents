from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from agents.order_manager import OrderManager
from agents.risk_governor import RiskGovernor
from core.event_bus import EventBus
from core.event_envelope import EventEnvelope, EventType
from core.outbox_dispatcher import OutboxDispatcher
from core.state_writer import StateWriter
from core.venue_order_spec import VenueOrderStatus, VenuePosition
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


def _base_config() -> dict:
    return {
        "order_manager": {"status_poll_interval_sec": 0.05},
        "flash_crash": {
            "atr_multiplier": 3,
            "abs_return_1s_threshold": 0.03,
            "abs_return_5s_threshold": 0.05,
        },
        "reconciliation": {
            "check_interval_sec": 0.05,
            "drift_threshold_pct": 0.01,
            "max_drift_before_halt": 0.05,
        },
        "circuit_breaker": {
            "attempt_flatten_timeout_sec": 0.1,
            "staged_exit_batch_pct": 0.5,
            "staged_exit_interval_sec": 0,
        },
        "recovery": {
            "exponential_backoff_base_sec": 5,
            "max_backoff_sec": 300,
            "cooldown_after_recovery_sec": 0,
            "post_recovery_scale": 0.5,
            "post_recovery_duration_sec": 0,
        },
    }


def _make_payload(**overrides) -> dict:
    payload = {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "order_type": "LIMIT",
        "quantity": "0.010",
        "price": "50000.0",
        "time_in_force": "GTC",
        "reduce_only": False,
        "post_only": False,
        "hedge_flag": "SPEC",
        "venue": "binance",
        "client_order_id": "client-001",
    }
    payload.update(overrides)
    return payload


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cursor.fetchone()[0])
    finally:
        conn.close()


def _query_outbox_rows(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT aggregate_id, status, idempotency_key, error_message FROM outbox_orders ORDER BY created_at, event_id"
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def _insert_order(
    db_path: Path,
    *,
    order_id: str,
    client_order_id: str,
    status: str,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    quantity: float = 0.01,
    price: float = 50000.0,
    venue: str = "binance",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order_id,
                client_order_id,
                symbol,
                venue,
                side,
                quantity,
                price,
                status,
                "strategy-1",
                now,
                now,
                0.0,
                0.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


async def _wait_for_status(state_writer: StateWriter, order_id: str, expected: str, timeout: float = 2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        order = await state_writer.query_order(order_id)
        if order is not None and order.status.value == expected:
            return order
        await asyncio.sleep(0.05)
    order = await state_writer.query_order(order_id)
    raise AssertionError(f"order {order_id} did not reach {expected}, got {None if order is None else order.status.value}")


class PositionAwareMockAdapter(MockVenueAdapter):
    def __init__(self, positions: list[VenuePosition] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._positions = list(positions or [])

    async def query_positions(self) -> list[VenuePosition]:
        return list(self._positions)


class PartialFillAdapter(MockVenueAdapter):
    def __init__(self) -> None:
        super().__init__()
        self._query_count: dict[str, int] = {}

    async def query_order(self, client_order_id: str) -> VenueOrderStatus:
        count = self._query_count.get(client_order_id, 0) + 1
        self._query_count[client_order_id] = count
        spec = self._spec_by_client_id[client_order_id]
        if count == 1:
            return VenueOrderStatus(
                client_order_id=client_order_id,
                exchange_order_id=self._exchange_id_by_client_id[client_order_id],
                status="PARTIAL",
                filled_quantity=spec.quantity / Decimal("2"),
                filled_price=spec.price or Decimal("0"),
                updated_at=datetime.now(timezone.utc),
            )
        return await super().query_order(client_order_id)


@pytest_asyncio.fixture
async def runtime(tmp_path: Path):
    db_path = tmp_path / "order-manager.sqlite"
    _init_sqlite_schema(db_path)
    state_writer = StateWriter(str(db_path))
    await state_writer.start()
    event_bus = EventBus(state_writer)
    await event_bus.start()
    risk_governor = RiskGovernor(config=_base_config(), event_bus=event_bus, state_writer=state_writer)
    yield db_path, state_writer, event_bus, risk_governor
    await risk_governor.stop()
    await state_writer.stop()


@pytest.mark.asyncio
async def test_trade_intent_subscription_writes_order_and_outbox(runtime) -> None:
    db_path, state_writer, event_bus, risk_governor = runtime
    manager = OrderManager(event_bus, state_writer, MockVenueAdapter(), risk_governor, _base_config())
    await manager.start()

    payload = _make_payload(client_order_id="sub-001")
    envelope = EventEnvelope.make(EventType.TRADE_INTENT, payload["symbol"], payload, stream_seq=0)

    published = await event_bus.publish(envelope)
    assert published is True

    await asyncio.sleep(0.1)
    assert _count_rows(db_path, "orders") == 1
    assert _count_rows(db_path, "outbox_orders") == 1

    await manager.stop()


@pytest.mark.asyncio
async def test_semantic_validation_failure_rejects_without_outbox_and_broadcasts(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    manager = OrderManager(event_bus, state_writer, MockVenueAdapter(), risk_governor, _base_config())
    updates: list[dict] = []

    async def capture(envelope):
        updates.append(dict(envelope.payload))

    event_bus.subscribe(EventType.ORDER_UPDATE, capture)
    await manager.start()

    result = await manager.submit_trade_intent(_make_payload(client_order_id="bad-001", quantity="0.0005"))

    assert result.accepted is False
    assert result.status == "REJECTED"
    assert await state_writer.query_order(result.order_id) is not None
    assert len(_query_outbox_rows(Path(state_writer._db_path))) == 0
    assert updates[-1]["status"] == "REJECTED"
    assert "not aligned to lot_size" in updates[-1]["error"]

    await manager.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_name", "mutator"),
    [
        ("RECONCILING", lambda gov: gov.evaluate_reconciliation_risk(drift_pct=0.06)),
        ("CIRCUIT", lambda gov: gov.evaluate_market_risk(last_price="80", reference_price="100", atr="1", return_1s="0", return_5s="0")),
    ],
)
async def test_high_risk_states_reject_new_orders(runtime, state_name, mutator) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    manager = OrderManager(event_bus, state_writer, MockVenueAdapter(), risk_governor, _base_config())
    mutator(risk_governor)

    result = await manager.submit_trade_intent(_make_payload(client_order_id=f"{state_name}-001"))

    assert result.accepted is False
    assert state_name in result.error
    assert len(_query_outbox_rows(Path(state_writer._db_path))) == 0


@pytest.mark.asyncio
async def test_degraded_blocks_open_orders_but_allows_reduce_only(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    manager = OrderManager(event_bus, state_writer, MockVenueAdapter(), risk_governor, _base_config())
    risk_governor.set_degraded("test")

    rejected = await manager.submit_trade_intent(_make_payload(client_order_id="deg-open-001"))
    allowed = await manager.submit_trade_intent(
        _make_payload(
            client_order_id="deg-reduce-001",
            side="SELL",
            reduce_only=True,
        )
    )

    assert rejected.accepted is False
    assert allowed.accepted is True
    assert len(_query_outbox_rows(Path(state_writer._db_path))) == 1


@pytest.mark.asyncio
async def test_dispatch_and_status_loop_drive_sent_then_filled_and_broadcast(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    adapter = MockVenueAdapter()
    dispatcher = OutboxDispatcher(state_writer, adapter, poll_interval=0.05)
    manager = OrderManager(event_bus, state_writer, adapter, risk_governor, _base_config())
    updates: list[str] = []

    async def capture(envelope):
        updates.append(envelope.payload["status"])

    event_bus.subscribe(EventType.ORDER_UPDATE, capture)
    await dispatcher.start()
    await manager.start()

    try:
        result = await manager.submit_trade_intent(_make_payload(client_order_id="flow-001"))
        filled_order = await _wait_for_status(state_writer, result.order_id, "FILLED")

        assert filled_order.client_order_id == "flow-001"
        assert "SENT" in updates
        assert "FILLED" in updates
        assert updates[0] == "PENDING_SEND"
        assert filled_order.filled_quantity == pytest.approx(0.01)
    finally:
        await manager.stop()
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_partial_fill_then_filled(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    adapter = PartialFillAdapter()
    dispatcher = OutboxDispatcher(state_writer, adapter, poll_interval=0.05)
    manager = OrderManager(event_bus, state_writer, adapter, risk_governor, _base_config())
    await dispatcher.start()
    await manager.start()

    result = await manager.submit_trade_intent(_make_payload(client_order_id="partial-001"))

    partial_order = await _wait_for_status(state_writer, result.order_id, "PARTIALLY_FILLED")
    filled_order = await _wait_for_status(state_writer, result.order_id, "FILLED")

    assert partial_order.filled_quantity == pytest.approx(0.005)
    assert filled_order.filled_quantity == pytest.approx(0.01)

    await manager.stop()
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_venue_reject_updates_order_to_rejected(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    adapter = MockVenueAdapter(reject_symbols=["BTCUSDT"])
    dispatcher = OutboxDispatcher(state_writer, adapter, poll_interval=0.05)
    manager = OrderManager(event_bus, state_writer, adapter, risk_governor, _base_config())
    await dispatcher.start()
    await manager.start()

    result = await manager.submit_trade_intent(_make_payload(client_order_id="rej-001"))
    rejected_order = await _wait_for_status(state_writer, result.order_id, "REJECTED")

    assert rejected_order.filled_quantity == 0
    assert rejected_order.filled_price == 0

    await manager.stop()
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_cancel_all_cancels_pending_and_sent_orders(runtime) -> None:
    db_path, state_writer, event_bus, risk_governor = runtime
    adapter = MockVenueAdapter()
    manager = OrderManager(event_bus, state_writer, adapter, risk_governor, _base_config())

    _insert_order(db_path, order_id="pending-001", client_order_id="pending-cid", status="PENDING_SEND")
    _insert_order(db_path, order_id="sent-001", client_order_id="sent-cid", status="SENT")
    adapter._exchange_id_by_client_id["pending-cid"] = "MOCK-pending"
    adapter._exchange_id_by_client_id["sent-cid"] = "MOCK-sent"

    result = await manager.cancel_all()

    pending_order = await _wait_for_status(state_writer, "pending-001", "CANCELED")
    sent_order = await _wait_for_status(state_writer, "sent-001", "CANCELED")

    assert result["canceled_locally"] == 1
    assert result["synced"] >= 1
    assert pending_order.status.value == "CANCELED"
    assert sent_order.status.value == "CANCELED"
    assert set(adapter.canceled_order_ids) == {"pending-cid", "sent-cid"}


@pytest.mark.asyncio
async def test_flatten_creates_reverse_reduce_only_market_order(runtime) -> None:
    _, state_writer, event_bus, risk_governor = runtime
    position = VenuePosition(
        symbol="BTCUSDT",
        venue="binance",
        side="LONG",
        quantity=Decimal("0.020"),
        entry_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        updated_at=datetime.now(timezone.utc),
    )
    adapter = PositionAwareMockAdapter([position])
    dispatcher = OutboxDispatcher(state_writer, adapter, poll_interval=0.05)
    manager = OrderManager(event_bus, state_writer, adapter, risk_governor, _base_config())
    await dispatcher.start()
    await manager.start()

    results = await manager.flatten("BTCUSDT")
    order = await _wait_for_status(state_writer, results[0].order_id, "FILLED")

    assert len(results) == 1
    assert order.status.value == "FILLED"
    assert adapter.submitted_orders[-1].symbol == "BTCUSDT"
    assert adapter.submitted_orders[-1].side == "SELL"
    assert adapter.submitted_orders[-1].order_type == "MARKET"
    assert adapter.submitted_orders[-1].reduce_only is True

    await manager.stop()
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_duplicate_client_order_id_and_idempotency_key_do_not_duplicate_rows(runtime) -> None:
    db_path, state_writer, event_bus, risk_governor = runtime
    manager = OrderManager(event_bus, state_writer, MockVenueAdapter(), risk_governor, _base_config())

    first = await manager.submit_trade_intent(
        _make_payload(client_order_id="dup-001", idempotency_key="same-key")
    )
    second = await manager.submit_trade_intent(
        _make_payload(client_order_id="dup-001", idempotency_key="same-key")
    )
    third = await manager.submit_trade_intent(
        _make_payload(client_order_id="dup-002", idempotency_key="same-key")
    )

    assert first.accepted is True
    assert second.duplicate is True
    assert third.duplicate is True
    assert _count_rows(db_path, "orders") == 1
    assert _count_rows(db_path, "outbox_orders") == 1
