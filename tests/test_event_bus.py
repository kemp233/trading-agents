import asyncio
from pathlib import Path

import aiosqlite
import pytest

from core.event_bus import EventBus
from core.event_envelope import EventEnvelope
from core.state_writer import StateWriter


async def _init_sqlite_db(db_path: Path) -> None:
    """Create tables for a fresh test database."""
    schema_path = Path(__file__).resolve().parents[1] / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(schema_sql)
        await db.commit()


async def _make_bus(tmp_path: Path, *, checkpoint_interval: int = 100) -> tuple[EventBus, StateWriter]:
    """Build a started StateWriter + EventBus over a per-test SQLite file."""
    db_path = tmp_path / "test.sqlite3"
    await _init_sqlite_db(db_path)
    writer = StateWriter(str(db_path))
    await writer.start()
    bus = EventBus(writer, checkpoint_interval=checkpoint_interval)
    await bus.start()
    return bus, writer


@pytest.mark.asyncio
async def test_publish_returns_true_for_valid_event(tmp_path: Path) -> None:
    # publish a valid envelope returns True
    bus, writer = await _make_bus(tmp_path)
    try:
        env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
        ok = await bus.publish(env)
        assert ok is True
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_publish_dedup_same_idempotency_key(tmp_path: Path) -> None:
    # publishing the same envelope twice is deduplicated
    bus, writer = await _make_bus(tmp_path)
    try:
        env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
        ok1 = await bus.publish(env)
        ok2 = await bus.publish(env)
        assert ok1 is True
        assert ok2 is False
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_publish_rejects_seq_rollback(tmp_path: Path) -> None:
    # stream_seq rollback should be rejected
    bus, writer = await _make_bus(tmp_path)
    try:
        env5 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000}, stream_seq=5)
        env3 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 51000}, stream_seq=3)
        ok1 = await bus.publish(env5)
        ok2 = await bus.publish(env3)
        assert ok1 is True
        assert ok2 is False
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_publish_allows_seq_skip(tmp_path: Path) -> None:
    # stream_seq skipping forward is allowed
    bus, writer = await _make_bus(tmp_path)
    try:
        env1 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000}, stream_seq=1)
        env5 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 51000}, stream_seq=5)
        ok1 = await bus.publish(env1)
        ok2 = await bus.publish(env5)
        assert ok1 is True
        assert ok2 is True
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_subscriber_receives_event(tmp_path: Path) -> None:
    # subscriber should receive the published envelope
    bus, writer = await _make_bus(tmp_path)
    try:
        got: list[EventEnvelope] = []
        seen = asyncio.Event()

        async def handler(envelope: EventEnvelope) -> None:
            got.append(envelope)
            seen.set()

        bus.subscribe("MarketData", handler)
        env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
        ok = await bus.publish(env)
        assert ok is True
        await asyncio.wait_for(seen.wait(), timeout=1.0)
        assert got == [env]
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_subscriber_exception_does_not_affect_others(tmp_path: Path) -> None:
    # one subscriber exception must not prevent other subscribers
    bus, writer = await _make_bus(tmp_path)
    try:
        called: list[str] = []
        done = asyncio.Event()

        def bad_handler(_: EventEnvelope) -> None:
            raise RuntimeError("boom")

        def good_handler(envelope: EventEnvelope) -> None:
            called.append(envelope.event_id)
            done.set()

        bus.subscribe("MarketData", bad_handler)
        bus.subscribe("MarketData", good_handler)
        env = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000})
        ok = await bus.publish(env)
        assert ok is True
        await asyncio.wait_for(done.wait(), timeout=1.0)
        assert called == [env.event_id]
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_checkpoint_saved_after_interval(tmp_path: Path) -> None:
    # checkpoint is persisted after checkpoint_interval publishes
    bus, writer = await _make_bus(tmp_path, checkpoint_interval=3)
    try:
        env0 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 50000}, stream_seq=0)
        env1 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 51000}, stream_seq=1)
        env2 = EventEnvelope.make("MarketData", "BTCUSDT", {"price": 52000}, stream_seq=2)

        assert await bus.publish(env0) is True
        assert await bus.publish(env1) is True
        assert await bus.publish(env2) is True

        await asyncio.sleep(0.2)
        checkpoints = await writer.load_checkpoints()
        assert checkpoints
    finally:
        await writer.stop()