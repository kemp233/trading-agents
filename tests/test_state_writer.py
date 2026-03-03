# tests/test_state_writer.py
import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.state_schema import OrderSide, OrderState, OrderStatus, PositionState, RiskState, RiskStateType
from core.state_writer import StateWriter


def _init_sqlite_schema(db_path: Path) -> None:
    """Initialize a fresh SQLite DB by executing db/schema.sql."""
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def _make_order(order_id: str, client_order_id: str, status: OrderStatus) -> OrderState:
    now = datetime.now(timezone.utc)
    return OrderState(
        order_id=order_id,
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        venue="binance",
        side=OrderSide.BUY,
        quantity=0.1,
        price=50000.0,
        status=status,
        strategy_id="strategy-1",
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_write_and_query_order(tmp_path: Path) -> None:
    # Purpose: write an order then query it back by order_id.
    db_path = tmp_path / "t1.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        order = _make_order("test-order-1", "client-1", OrderStatus.PENDING_SEND)
        await sw.write_order(order)
        await asyncio.sleep(0.2)

        got = await sw.query_order("test-order-1")
        assert got is not None
        assert got.order_id == "test-order-1"
        assert got.status == OrderStatus.PENDING_SEND
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_query_orders_by_status(tmp_path: Path) -> None:
    # Purpose: query orders filtered by status and verify count.
    db_path = tmp_path / "t2.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        await sw.write_order(_make_order("o1", "c1", OrderStatus.PENDING_SEND))
        await sw.write_order(_make_order("o2", "c2", OrderStatus.PENDING_SEND))
        await sw.write_order(_make_order("o3", "c3", OrderStatus.FILLED))
        await asyncio.sleep(0.2)

        pending = await sw.query_orders_by_status("PENDING_SEND")
        assert len(pending) == 2
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_write_and_query_position(tmp_path: Path) -> None:
    # Purpose: write a position then query positions list and verify content.
    db_path = tmp_path / "t3.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        pos = PositionState(
            symbol="BTCUSDT",
            venue="binance",
            side="LONG",
            quantity=0.5,
            entry_price=45000.0,
            unrealized_pnl=123.45,
            updated_at=datetime.now(timezone.utc),
        )
        await sw.write_position(pos)
        await asyncio.sleep(0.2)

        positions = await sw.query_positions()
        assert len(positions) >= 1
        assert any(p.symbol == "BTCUSDT" for p in positions)
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_write_and_query_risk_state(tmp_path: Path) -> None:
    # Purpose: write a risk state then query latest risk state and verify enum value.
    db_path = tmp_path / "t4.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        risk = RiskState(
            current_state=RiskStateType.NORMAL,
            previous_state=None,
            state_changed_at=datetime.now(timezone.utc),
            reason="test",
            metadata={"k": "v"},
        )
        await sw.write_risk_state(risk)
        await asyncio.sleep(0.2)

        got = await sw.query_risk_state()
        assert got is not None
        assert got.current_state == RiskStateType.NORMAL
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_batch_write_multiple_orders(tmp_path: Path) -> None:
    # Purpose: write many orders quickly and verify batch flush persists them all.
    db_path = tmp_path / "t5.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        for i in range(10):
            await sw.write_order(_make_order(f"bo-{i}", f"bc-{i}", OrderStatus.PENDING_SEND))
        await asyncio.sleep(0.5)

        pending = await sw.query_orders_by_status("PENDING_SEND")
        assert len(pending) == 10
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_save_and_load_checkpoint(tmp_path: Path) -> None:
    # Purpose: save stream checkpoints then load them back.
    db_path = tmp_path / "t6.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        await sw.save_checkpoint({"MarketData:BTCUSDT": 42}, {"MarketData:BTCUSDT:42"})
        got = await sw.load_checkpoints()
        assert got == {"MarketData:BTCUSDT": 42}
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_load_processed_events(tmp_path: Path) -> None:
    # Purpose: save processed event keys then load them back with a limit.
    db_path = tmp_path / "t7.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        keys = {"stream:0", "stream:1", "stream:2"}
        await sw.save_checkpoint({}, keys)
        got = await sw.load_processed_events(limit=10)
        assert keys.issubset(got)
    finally:
        await sw.stop()


@pytest.mark.asyncio
async def test_async_context_manager(tmp_path: Path) -> None:
    # Purpose: verify async context manager starts and stops the writer correctly.
    db_path = tmp_path / "t8.sqlite"
    _init_sqlite_schema(db_path)

    async with StateWriter(str(db_path)) as sw:
        assert sw._running is True
        await sw.write_order(_make_order("ctx-1", "ctx-c1", OrderStatus.PENDING_SEND))
        await asyncio.sleep(0.2)

    assert sw._running is False